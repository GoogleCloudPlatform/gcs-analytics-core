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
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

abstract class AbstractReadStrategy implements ReadStrategy {
  protected final GcsItemId itemId;
  protected final GcsReadOptions options;
  protected final Storage storage;
  protected final GcsItemInfo itemInfo;

  private static final int SKIP_BUFFER_SIZE = 128 * 1024; // 128 KiB
  protected static final ThreadLocal<ByteBuffer> SKIP_BUFFER_HOLDER =
      ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(SKIP_BUFFER_SIZE));

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

    return getLimit() == Long.MAX_VALUE || position >= getLimit();
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
    List<Storage.BlobSourceOption> sourceOptions = Lists.newArrayList();
    options
        .getUserProjectId()
        .ifPresent(id -> sourceOptions.add(Storage.BlobSourceOption.userProject(id)));
    options
        .getDecryptionKey()
        .ifPresent(key -> sourceOptions.add(Storage.BlobSourceOption.decryptionKey(key)));
    ReadChannel sdkReadChannel =
        storage.reader(blobId, sourceOptions.toArray(new Storage.BlobSourceOption[0]));
    options.getChunkSize().ifPresent(sdkReadChannel::setChunkSize);

    return sdkReadChannel;
  }

  boolean skipInPlace(long seekDistance) throws IOException {
    ByteBuffer skipBuffer = SKIP_BUFFER_HOLDER.get();
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
