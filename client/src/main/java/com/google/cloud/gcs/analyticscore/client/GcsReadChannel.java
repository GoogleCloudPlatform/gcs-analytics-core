/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.IntFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GcsReadChannel implements VectoredSeekableByteChannel {
  private static final Logger LOG = LoggerFactory.getLogger(GcsReadChannel.class);
  private Storage storage;
  private GcsReadOptions readOptions;
  private ReadChannel readChannel;
  private GcsItemInfo itemInfo;
  private long position = 0;
  private Supplier<ExecutorService> executorServiceSupplier;

  GcsReadChannel(
      Storage storage,
      GcsItemInfo itemInfo,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier)
      throws IOException {
    checkNotNull(storage, "Storage instance cannot be null");
    checkNotNull(itemInfo, "Item info cannot be null");
    checkNotNull(executorServiceSupplier, "Thread pool supplier must not be null");
    this.storage = storage;
    this.readOptions = readOptions;
    this.itemInfo = itemInfo;
    this.executorServiceSupplier = executorServiceSupplier;
    this.readChannel = openReadChannel(itemInfo, readOptions);
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    int bytesRead = readChannel.read(dst);
    position += bytesRead;

    return bytesRead;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public long position() throws IOException {
    return position;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    validatePosition(newPosition);
    readChannel.seek(newPosition);
    position = newPosition;

    return this;
  }

  @Override
  public long size() throws IOException {
    return itemInfo.getSize();
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public boolean isOpen() {
    return readChannel.isOpen();
  }

  @Override
  public void close() throws IOException {
    if (readChannel.isOpen()) {
      readChannel.close();
    }
  }

  @Override
  public void readVectored(List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    ExecutorService executorService = executorServiceSupplier.get();
    checkNotNull(executorService, "Thread pool must not be null");
    GcsVectoredReadOptions vectoredReadOptions = readOptions.getGcsVectoredReadOptions();
    ImmutableList<GcsObjectCombinedRange> combinedRanges =
        VectoredIoUtil.mergeGcsObjectRanges(
            ImmutableList.copyOf(ranges),
            vectoredReadOptions.getMaxMergeGap(),
            vectoredReadOptions.getMaxMergeSize());

    for (GcsObjectCombinedRange combinedRange : combinedRanges) {
      var unused =
          executorService.submit(
              () -> {
                readCombinedRange(combinedRange, allocate);
              });
    }
  }

  void readCombinedRange(
      GcsObjectCombinedRange combinedObjectRange, IntFunction<ByteBuffer> allocate) {
    try (ReadChannel channel = openReadChannel(itemInfo, readOptions)) {
      validatePosition(combinedObjectRange.getOffset());
      channel.seek(combinedObjectRange.getOffset());
      ByteBuffer dataBuffer = allocate.apply(combinedObjectRange.getLength());
      int numOfBytesRead = channel.read(dataBuffer);
      if (numOfBytesRead < 0) {
        throw new EOFException(
            String.format(
                "EOF reached while reading combinedObjectRange, range: %s, item: "
                    + "%s, numRead: %d",
                combinedObjectRange, itemInfo.getItemId(), numOfBytesRead));
      }
      // making it ready for reading
      dataBuffer.flip();
      for (GcsObjectRange underlyingRange : combinedObjectRange.getUnderlyingRanges()) {
        populateGcsObjectRangeFromCombinedObjectRange(
            combinedObjectRange, underlyingRange, numOfBytesRead, dataBuffer);
      }
    } catch (Exception e) {
      LOG.atWarn()
          .setCause(e)
          .log(
              "Exception while reading combinedFileRange:%s for path: %s",
              combinedObjectRange, itemInfo.getItemId());
      completeWithException(combinedObjectRange, e);
    }
  }

  private void populateGcsObjectRangeFromCombinedObjectRange(
      GcsObjectCombinedRange combinedObjectRange,
      GcsObjectRange objectRange,
      long numOfBytesRead,
      ByteBuffer dataBuffer)
      throws EOFException {
    long maxPosition = combinedObjectRange.getOffset() + numOfBytesRead;
    long objectRangeEndPosition = objectRange.getOffset() + objectRange.getLength();
    if (objectRangeEndPosition <= maxPosition) {
      ByteBuffer childBuffer =
          VectoredIoUtil.fetchUnderlyingRangeData(dataBuffer, combinedObjectRange, objectRange);
      objectRange.getByteBufferFuture().complete(childBuffer);
    } else {
      throw new EOFException(
          String.format(
              "EOF reached before all child ranges can be populated, "
                  + "combinedObjectRange: %s, "
                  + "expected length: %s, readBytes: %s, path: %s",
              combinedObjectRange,
              combinedObjectRange.getLength(),
              numOfBytesRead,
              itemInfo.getItemId()));
    }
  }

  private void completeWithException(GcsObjectCombinedRange combinedObjectRange, Throwable e) {
    for (GcsObjectRange child : combinedObjectRange.getUnderlyingRanges()) {
      if (!child.getByteBufferFuture().isDone()) {
        LOG.atWarn()
            .setCause(e)
            .log(
                "Marking child:%s as `completeExceptionally` of combinedRange:%s",
                child, combinedObjectRange);
        child
            .getByteBufferFuture()
            .completeExceptionally(
                new IOException(
                    String.format(
                        "Error while populating childRange: %s from combinedRange: %s",
                        child, combinedObjectRange),
                    e));
      }
    }
  }

  protected ReadChannel openReadChannel(GcsItemInfo itemInfo, GcsReadOptions readOptions)
      throws IOException {
    checkArgument(
        itemInfo.getItemId().isGcsObject(), "Expected Gcs Object but got %s", itemInfo.getItemId());
    String bucketName = itemInfo.getItemId().getBucketName();
    String objectName = itemInfo.getItemId().getObjectName().get();
    BlobId blobId =
        itemInfo
            .getContentGeneration()
            .map(gen -> BlobId.of(bucketName, objectName, gen))
            .orElse(BlobId.of(bucketName, objectName));
    List<Storage.BlobSourceOption> sourceOptions = Lists.newArrayList();
    readOptions
        .getProjectId()
        .ifPresent(id -> sourceOptions.add(Storage.BlobSourceOption.userProject(id)));
    readOptions
        .getDecryptionKey()
        .ifPresent(key -> sourceOptions.add(Storage.BlobSourceOption.decryptionKey(key)));

    return storage.reader(blobId, sourceOptions.toArray(new Storage.BlobSourceOption[0]));
  }

  private void validatePosition(long position) throws IOException {
    if (position < 0) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be >= 0 for '%s'",
              position, itemInfo.getItemId()));
    }
    if (itemInfo.getSize() >= 0 && position > itemInfo.getSize()) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d)"
                  + " must be between 0 "
                  + "and"
                  + " %d "
                  + "for '%s'",
              position, itemInfo.getSize(), itemInfo.getItemId()));
    }
  }
}
