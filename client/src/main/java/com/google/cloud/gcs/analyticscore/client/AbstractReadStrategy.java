/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.List;
import javax.annotation.Nullable;

abstract class AbstractReadStrategy implements ReadStrategy {
  protected volatile GcsItemId itemId;
  protected final GcsReadOptions options;
  protected final Storage storage;
  protected volatile GcsItemInfo itemInfo;

  private static final int SKIP_BUFFER_SIZE = 128 * 1024; // 128 KiB

  /**
   * Per-thread scratch buffer used only to discard bytes during an in-place skip. Its contents are
   * never read. Thread-local (not shared) so concurrent skips neither race nor contend on cache
   * lines; soft-referenced so it can be reclaimed under memory pressure.
   */
  static final ThreadLocal<SoftReference<ByteBuffer>> SKIP_BUFFER = new ThreadLocal<>();

  protected ReadChannel channel;
  protected long position = 0;

  AbstractReadStrategy(
      Storage storage, GcsItemId itemId, GcsReadOptions options, GcsItemInfo itemInfo) {
    this.storage = storage;
    this.itemId = itemId;
    this.options = options;
    this.itemInfo = itemInfo;
  }

  @Override
  @Nullable
  public ReadChannel getSdkReadChannel() {
    return channel;
  }

  @Override
  public void updateItemInfo(GcsItemInfo itemInfo) {
    checkArgument(itemInfo.getSize() >= 0, "Resolved item info must have a size, got %s", itemInfo);
    this.itemInfo = itemInfo;
    if (itemInfo.getItemId().getContentGeneration().isPresent()) {
      this.itemId = itemInfo.getItemId();
    }
  }

  @Override
  public void position(long newPosition) {
    this.position = newPosition;
  }

  @Override
  public void close() throws IOException {
    if (channel != null) {
      channel.close();
      channel = null;
    }
  }

  @Override
  public boolean isEof(long position) {
    if (itemInfo != null) {
      return position >= itemInfo.getSize();
    }

    return true;
  }

  ReadChannel openSdkReadChannel() throws IOException {
    checkArgument(itemId.isGcsObject(), "Expected Gcs Object but got %s", itemId);
    String bucketName = itemId.getBucketName();
    String objectName = itemId.getObjectName().get();
    BlobId blobId =
        itemId
            .getContentGeneration()
            .map(gen -> BlobId.of(bucketName, objectName, gen))
            .orElse(BlobId.of(bucketName, objectName));
    List<BlobSourceOption> sourceOptions = Lists.newArrayList();
    options.getUserProjectId().ifPresent(id -> sourceOptions.add(BlobSourceOption.userProject(id)));
    options
        .getDecryptionKey()
        .ifPresent(key -> sourceOptions.add(BlobSourceOption.decryptionKey(key)));
    ReadChannel sdkReadChannel =
        storage.reader(blobId, sourceOptions.toArray(new BlobSourceOption[0]));
    options.getChunkSize().ifPresent(sdkReadChannel::setChunkSize);

    return sdkReadChannel;
  }

  private static ByteBuffer acquireSkipBuffer() {
    SoftReference<ByteBuffer> ref = SKIP_BUFFER.get();
    ByteBuffer buffer = (ref != null) ? ref.get() : null;
    if (buffer == null) {
      buffer = ByteBuffer.allocate(SKIP_BUFFER_SIZE);
      SKIP_BUFFER.set(new SoftReference<>(buffer));
    }
    return buffer;
  }

  boolean skipInPlace(long seekDistance) throws IOException {
    ByteBuffer skipBuffer = acquireSkipBuffer();
    while (seekDistance > 0) {
      int bufferSize = (int) Math.min((long) skipBuffer.capacity(), seekDistance);
      skipBuffer.clear();
      skipBuffer.limit(bufferSize);
      int bytesRead = channel.read(skipBuffer);
      if (bytesRead <= 0) {
        channel.close();
        return false;
      }
      seekDistance -= bytesRead;
    }
    return true;
  }

  boolean performPendingSeeks(long requestedPosition) throws IOException {
    if (requestedPosition == position) {
      return true;
    }
    long seekDistance = requestedPosition - position;
    boolean success = true;
    if (shouldSkipInPlace(seekDistance)) {
      success = skipInPlace(seekDistance);
    } else {
      channel.seek(requestedPosition);
    }
    if (success) {
      position = requestedPosition;
    }

    return success;
  }

  private boolean shouldSkipInPlace(long seekDistance) {
    return seekDistance > 0 && seekDistance <= options.getInplaceSeekLimit();
  }
}
