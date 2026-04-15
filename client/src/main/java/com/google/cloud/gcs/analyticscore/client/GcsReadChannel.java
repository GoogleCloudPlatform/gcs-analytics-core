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
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Attribute;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.Operation;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.IntFunction;

class GcsReadChannel implements VectoredSeekableByteChannel {
  private Storage storage;
  private GcsReadOptions readOptions;
  private ReadChannel sdkReadChannel;
  protected GcsItemInfo itemInfo;
  protected GcsItemId itemId;
  private long gcsReadChannelPosition = 0;
  private Supplier<ExecutorService> executorServiceSupplier;
  private static final ImmutableMap<String, String> COMMON_ATTRIBUTES =
      ImmutableMap.of(Attribute.CLASS_NAME.name(), GcsReadChannel.class.getName());
  private final Telemetry telemetry;
  private long sdkReadChannelPosition = -1;
  private static final int SKIP_BUFFER_SIZE = 128 * 1024; // 128 KiB
  private static final ThreadLocal<ByteBuffer> SKIP_BUFFER_HOLDER =
      ThreadLocal.withInitial(() -> ByteBuffer.allocate(SKIP_BUFFER_SIZE));

  GcsReadChannel(
      Storage storage,
      GcsItemInfo itemInfo,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry)
      throws IOException {
    this(
        storage,
        itemInfo,
        checkNotNull(itemInfo, "Item info cannot be null").getItemId(),
        readOptions,
        executorServiceSupplier,
        telemetry);
  }

  GcsReadChannel(
      Storage storage,
      GcsItemId itemId,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry)
      throws IOException {
    this(storage, null, itemId, readOptions, executorServiceSupplier, telemetry);
  }

  private GcsReadChannel(
      Storage storage,
      GcsItemInfo itemInfo,
      GcsItemId itemId,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry)
      throws IOException {
    checkNotNull(storage, "Storage instance cannot be null");
    checkNotNull(itemId, "Item id cannot be null");
    checkNotNull(executorServiceSupplier, "Thread pool supplier must not be null");
    checkNotNull(telemetry, "Telemetry instance cannot be null");
    this.storage = storage;
    this.readOptions = readOptions;
    this.itemInfo = itemInfo;
    this.itemId = itemId;
    this.executorServiceSupplier = executorServiceSupplier;
    this.telemetry = telemetry;
    this.sdkReadChannel = openSdkReadChannel(itemId, readOptions);
    this.sdkReadChannelPosition = 0;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    if (dst.remaining() == 0) {
      return 0;
    }
    performPendingSeeks();
    int bytesRead = sdkReadChannel.read(dst);
    if (bytesRead > 0) {
      gcsReadChannelPosition += bytesRead;
      sdkReadChannelPosition += bytesRead;
    }

    return bytesRead;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public long position() throws IOException {
    return gcsReadChannelPosition;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    validatePosition(newPosition);
    gcsReadChannelPosition = newPosition;

    return this;
  }

  @Override
  public long size() throws IOException {
    if (null != itemInfo) {
      return itemInfo.getSize();
    }
    throw new IOException("Object metadata not initialized");
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public boolean isOpen() {
    return sdkReadChannel.isOpen();
  }

  @Override
  public void close() throws IOException {
    if (sdkReadChannel.isOpen()) {
      sdkReadChannel.close();
    }
  }

  private void performPendingSeeks() throws IOException {
    if (gcsReadChannelPosition == sdkReadChannelPosition) {
      return;
    }
    if (canSeekInPlace()) {
      skipInPlace();
      return;
    }
    sdkReadChannel.seek(gcsReadChannelPosition);
    sdkReadChannelPosition = gcsReadChannelPosition;
  }

  private boolean canSeekInPlace() {
    long seekDistance = gcsReadChannelPosition - sdkReadChannelPosition;
    return seekDistance > 0 && seekDistance <= readOptions.getInplaceSeekLimit();
  }

  private void skipInPlace() throws IOException {
    ByteBuffer skipBuffer = SKIP_BUFFER_HOLDER.get();
    long seekDistance = gcsReadChannelPosition - sdkReadChannelPosition;
    while (seekDistance > 0) {
      int bufferSize = (int) Math.min((long) skipBuffer.capacity(), seekDistance);
      skipBuffer.clear();
      skipBuffer.limit(bufferSize);
      int bytesRead = sdkReadChannel.read(skipBuffer);
      if (bytesRead <= 0) {
        // EOF, fallback to seek
        sdkReadChannel.seek(gcsReadChannelPosition);
        sdkReadChannelPosition = gcsReadChannelPosition;
        return;
      }
      seekDistance -= bytesRead;
      sdkReadChannelPosition += bytesRead;
    }
  }

  @Override
  public void readVectored(List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    Operation operation =
        Operation.builder()
            .setName(GcsAnalyticsCoreTelemetryConstants.Operation.VECTORED_READ.name())
            .setDurationMetric(Metric.READ_DURATION)
            .setAttributes(COMMON_ATTRIBUTES)
            .build();
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
                readCombinedRange(combinedRange, allocate, operation);
              });
    }
  }

  void readCombinedRange(
      GcsObjectCombinedRange combinedObjectRange,
      IntFunction<ByteBuffer> allocate,
      Operation operation) {
    telemetry.measure(
        operation,
        recorder -> {
          try (ReadChannel channel = openSdkReadChannel(itemId, readOptions)) {
            validatePosition(combinedObjectRange.getOffset());
            channel.seek(combinedObjectRange.getOffset());
            channel.limit(combinedObjectRange.getOffset() + combinedObjectRange.getLength());
            ByteBuffer dataBuffer = allocate.apply(combinedObjectRange.getLength());
            int numOfBytesRead = 0;
            while (dataBuffer.hasRemaining()) {
              int bytesRead = channel.read(dataBuffer);
              if (bytesRead < 0) {
                // EOF reached.
                break;
              }
              recorder.record(Metric.READ_BYTES, bytesRead, Collections.emptyMap());
              numOfBytesRead += bytesRead;
            }
            if (numOfBytesRead < combinedObjectRange.getLength()) {
              throw new EOFException(
                  String.format(
                      "EOF reached while reading combinedObjectRange, range: %s, item: "
                          + "%s, numRead: %d, expected: %d",
                      combinedObjectRange,
                      itemId,
                      numOfBytesRead,
                      combinedObjectRange.getLength()));
            }
            // making it ready for reading
            dataBuffer.flip();
            for (GcsObjectRange underlyingRange : combinedObjectRange.getUnderlyingRanges()) {
              populateGcsObjectRangeFromCombinedObjectRange(
                  combinedObjectRange, underlyingRange, numOfBytesRead, dataBuffer);
            }
          } catch (Exception e) {
            completeWithException(combinedObjectRange, e);
          }
          return null;
        });
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
              combinedObjectRange, combinedObjectRange.getLength(), numOfBytesRead, itemId));
    }
  }

  private void completeWithException(GcsObjectCombinedRange combinedObjectRange, Throwable e) {
    for (GcsObjectRange child : combinedObjectRange.getUnderlyingRanges()) {
      if (!child.getByteBufferFuture().isDone()) {
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

  protected ReadChannel openSdkReadChannel(GcsItemId gcsItemId, GcsReadOptions readOptions)
      throws IOException {
    checkArgument(gcsItemId.isGcsObject(), "Expected Gcs Object but got %s", gcsItemId);
    String bucketName = gcsItemId.getBucketName();
    String objectName = gcsItemId.getObjectName().get();
    BlobId blobId =
        gcsItemId
            .getContentGeneration()
            .map(gen -> BlobId.of(bucketName, objectName, gen))
            .orElse(BlobId.of(bucketName, objectName));
    List<Storage.BlobSourceOption> sourceOptions = Lists.newArrayList();
    readOptions
        .getUserProjectId()
        .ifPresent(id -> sourceOptions.add(Storage.BlobSourceOption.userProject(id)));
    readOptions
        .getDecryptionKey()
        .ifPresent(key -> sourceOptions.add(Storage.BlobSourceOption.decryptionKey(key)));
    ReadChannel sdkReadChannel =
        storage.reader(blobId, sourceOptions.toArray(new Storage.BlobSourceOption[0]));
    readOptions.getChunkSize().ifPresent(sdkReadChannel::setChunkSize);

    return sdkReadChannel;
  }

  private void validatePosition(long position) throws IOException {
    if (position < 0) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be >= 0 for '%s'", position, itemId));
    }
  }
}
