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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.ReadChannel;
import com.google.cloud.gcs.analyticscore.client.GcsReadChannelMetadataExtractor.ExtractedMetadata;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Attribute;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.MetricsRecorder;
import com.google.cloud.gcs.analyticscore.common.telemetry.Operation;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.Storage;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GcsReadChannel implements VectoredSeekableByteChannel {
  private static final Logger LOG = LoggerFactory.getLogger(GcsReadChannel.class);

  @FunctionalInterface
  public interface ItemInfoProvider {
    GcsItemInfo getItemInfo(GcsItemId itemId) throws IOException;
  }

  protected Storage storage;
  protected GcsReadOptions readOptions;
  protected volatile GcsItemInfo itemInfo;
  protected volatile GcsItemId itemId;
  private long gcsReadChannelPosition = 0;
  protected Supplier<ExecutorService> executorServiceSupplier;
  private static final ImmutableMap<String, String> COMMON_ATTRIBUTES =
      ImmutableMap.of(Attribute.CLASS_NAME.name(), GcsReadChannel.class.getName());
  protected final Telemetry telemetry;
  private final ReadStrategy strategy;
  private volatile boolean isGcsReadChannelOpen = true;
  private volatile boolean metadataExtractionAttempted = false;
  protected final ItemInfoProvider itemInfoProvider;

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
    this(storage, itemId, readOptions, executorServiceSupplier, telemetry, null);
  }

  GcsReadChannel(
      Storage storage,
      GcsItemId itemId,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry,
      ItemInfoProvider itemInfoProvider)
      throws IOException {
    checkNotNull(storage, "Storage instance cannot be null");
    checkNotNull(itemId, "Item id cannot be null");
    checkNotNull(executorServiceSupplier, "Thread pool supplier must not be null");
    checkNotNull(telemetry, "Telemetry instance cannot be null");
    this.storage = storage;
    this.itemId = itemId;
    this.readOptions = readOptions;
    this.executorServiceSupplier = executorServiceSupplier;
    this.telemetry = telemetry;
    this.itemInfoProvider = itemInfoProvider;
    this.strategy = createReadStrategy(storage, itemId, readOptions, null);
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
    this.itemInfoProvider = null;
    this.strategy = createReadStrategy(storage, itemId, readOptions, itemInfo);
  }

  protected ReadStrategy createReadStrategy(
      Storage storage, GcsItemId itemId, GcsReadOptions readOptions, GcsItemInfo itemInfo)
      throws IOException {
    return new AdaptiveReadStrategy(storage, itemId, readOptions, itemInfo);
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    checkChannelOpen();
    if (dst.remaining() == 0) {
      return 0;
    }
    int totalBytesRead = 0;
    while (dst.hasRemaining()) {
      int bytesRead = readNextChunk(dst);
      if (bytesRead < 0) {
        return totalBytesRead == 0 ? -1 : totalBytesRead;
      }
      totalBytesRead += bytesRead;
    }

    return totalBytesRead;
  }

  private int readNextChunk(ByteBuffer dst) throws IOException {
    ReadChannel sdkChannel = strategy.getReadChannel(gcsReadChannelPosition, dst.remaining());
    int bytesRead = sdkChannel.read(dst);
    if (bytesRead >= 0) {
      gcsReadChannelPosition += bytesRead;
      strategy.position(gcsReadChannelPosition);
      return bytesRead;
    }
    if (strategy.isEof(gcsReadChannelPosition)) {
      return -1;
    }
    throw createUnexpectedEofException();
  }

  private void checkChannelOpen() throws ClosedChannelException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
  }

  private IOException createUnexpectedEofException() {
    long itemSize = itemInfo != null ? itemInfo.getSize() : -1;
    return new IOException(
        String.format(
            "Received end of stream signal before all requestedBytes were received; "
                + "EndOf stream signal received at offset: %d for resource: %s of size: %d",
            gcsReadChannelPosition, itemId, itemSize));
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public long position() throws IOException {
    checkChannelOpen();

    return gcsReadChannelPosition;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    checkChannelOpen();
    validatePosition(newPosition);
    gcsReadChannelPosition = newPosition;

    return this;
  }

  @Override
  public long size() throws IOException {
    if (itemInfo != null || extractMetadataAfterRead(this.strategy)) {
      return itemInfo.getSize();
    }
    if (itemInfoProvider == null) {
      throw new IOException("ItemInfo is not initialized and no ItemInfoProvider was provided.");
    }

    itemInfo = itemInfoProvider.getItemInfo(itemId);
    itemId = itemInfo.getItemId();
    return itemInfo.getSize();
  }

  @Override
  @Nullable
  public GcsItemInfo getItemInfo() {
    return itemInfo;
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public boolean isOpen() {
    return isGcsReadChannelOpen;
  }

  @Override
  public void close() throws IOException {
    if (!isGcsReadChannelOpen) {
      return;
    }
    synchronized (this) {
      isGcsReadChannelOpen = false;
      strategy.close();
    }
  }

  @Override
  public void readVectored(List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    readVectored(ranges, allocate, buffer -> {});
  }

  @Override
  public void readVectored(
      List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate, Consumer<ByteBuffer> release)
      throws IOException {
    checkNotNull(release, "Buffer release function must not be null");
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
                readCombinedRange(combinedRange, allocate, release, operation);
              });
    }
  }

  void readCombinedRange(
      GcsObjectCombinedRange combinedObjectRange,
      IntFunction<ByteBuffer> allocate,
      Consumer<ByteBuffer> release,
      Operation operation) {
    telemetry.measure(
        operation,
        recorder -> {
          ReadStrategy readStrategy =
              new RandomReadStrategy(storage, itemId, readOptions, itemInfo);
          try (ReadChannel channel =
              readStrategy.getReadChannel(
                  combinedObjectRange.getOffset(), combinedObjectRange.getLength())) {
            validatePosition(combinedObjectRange.getOffset());
            PreparedCombinedRead preparedRead =
                readCombinedRangeBuffers(
                    channel, readStrategy, combinedObjectRange, allocate, release, recorder);
            // Returned buffers are ready for publication. Completion and close failures must not
            // release their parent, since a caller may already have received a slice.
            boolean published = false;
            for (PreparedRange preparedRange : preparedRead.ranges) {
              published |= preparedRange.range.getByteBufferFuture().complete(preparedRange.buffer);
            }
            if (!published) {
              IllegalStateException failure =
                  new IllegalStateException("No range future accepted the prepared buffer");
              VectoredIoUtil.releaseOnFailure(preparedRead.parent, release, failure);
              if (failure.getSuppressed().length > 0) {
                LOG.warn("Failed to release an unpublished vectored read buffer", failure);
              }
            }
          } catch (Throwable e) {
            completeWithException(combinedObjectRange, e);
            if (e instanceof Error) {
              LOG.error("Vectored read failed for combined range {}", combinedObjectRange, e);
              throw (Error) e;
            }
          }
          return null;
        });
  }

  private PreparedCombinedRead readCombinedRangeBuffers(
      ReadChannel channel,
      ReadStrategy readStrategy,
      GcsObjectCombinedRange combinedObjectRange,
      IntFunction<ByteBuffer> allocate,
      Consumer<ByteBuffer> release,
      MetricsRecorder recorder)
      throws IOException {
    ByteBuffer dataBuffer = allocate.apply(combinedObjectRange.getLength());
    if (dataBuffer == null) {
      throw new IllegalArgumentException(
          String.format(
              "Buffer allocation returned null for combinedObjectRange: %s", combinedObjectRange));
    }
    // This method owns the allocation until it returns all prepared slices.
    try {
      if (dataBuffer.capacity() < combinedObjectRange.getLength()) {
        throw new IllegalArgumentException(
            String.format(
                "Allocated buffer is too small for combinedObjectRange: %s", combinedObjectRange));
      }
      dataBuffer.clear();
      dataBuffer.limit(combinedObjectRange.getLength());
      int numOfBytesRead = 0;
      while (dataBuffer.hasRemaining()) {
        int bytesRead = channel.read(dataBuffer);
        extractMetadataAfterRead(readStrategy);
        if (bytesRead < 0) {
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
                combinedObjectRange, itemId, numOfBytesRead, combinedObjectRange.getLength()));
      }
      dataBuffer.flip();
      List<PreparedRange> underlyingBuffers = new ArrayList<>();
      // Keep preparation separate from the caller's publication loop: if any slice creation
      // fails (including OOME), no future has received a slice of the parent we release below.
      for (GcsObjectRange underlyingRange : combinedObjectRange.getUnderlyingRanges()) {
        underlyingBuffers.add(
            new PreparedRange(
                underlyingRange,
                getUnderlyingRangeDataFromCombinedObjectRange(
                    combinedObjectRange, underlyingRange, numOfBytesRead, dataBuffer)));
      }
      return new PreparedCombinedRead(dataBuffer, underlyingBuffers);
    } catch (IOException | RuntimeException | Error e) {
      VectoredIoUtil.releaseOnFailure(dataBuffer, release, e);
      throw e;
    }
  }

  private static final class PreparedCombinedRead {
    private final ByteBuffer parent;
    private final List<PreparedRange> ranges;

    private PreparedCombinedRead(ByteBuffer parent, List<PreparedRange> ranges) {
      this.parent = parent;
      this.ranges = ranges;
    }
  }

  private static final class PreparedRange {
    private final GcsObjectRange range;
    private final ByteBuffer buffer;

    private PreparedRange(GcsObjectRange range, ByteBuffer buffer) {
      this.range = range;
      this.buffer = buffer;
    }
  }

  private ByteBuffer getUnderlyingRangeDataFromCombinedObjectRange(
      GcsObjectCombinedRange combinedObjectRange,
      GcsObjectRange objectRange,
      long numOfBytesRead,
      ByteBuffer dataBuffer)
      throws EOFException {
    long maxPosition = combinedObjectRange.getOffset() + numOfBytesRead;
    long objectRangeEndPosition = objectRange.getOffset() + objectRange.getLength();
    // Purely defensive for inconsistent combined ranges. With valid, non-overflowing offsets,
    // merging contains every child and the full-read check guarantees maxPosition >= parent end,
    // so the EOF branch below is unreachable for ranges produced by the normal merge path.
    if (objectRangeEndPosition <= maxPosition) {
      return VectoredIoUtil.fetchUnderlyingRangeData(dataBuffer, combinedObjectRange, objectRange);
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
        if (e instanceof Error) {
          // Avoid allocating a wrapper while handling allocation failures.
          child.getByteBufferFuture().completeExceptionally(e);
          continue;
        }
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

  private void validatePosition(long position) throws IOException {
    if (position < 0) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be >= 0 for '%s'", position, itemId));
    }
  }

  private boolean extractMetadataAfterRead(ReadStrategy strategy) {
    if (itemInfo != null || metadataExtractionAttempted) {
      return itemInfo != null;
    }
    synchronized (this) {
      if (itemInfo != null || metadataExtractionAttempted) {
        return itemInfo != null;
      }
      ExtractedMetadata metadata =
          GcsReadChannelMetadataExtractor.extract(strategy.getSdkReadChannel());
      if (metadata != null) {
        updateGcsItemMetadata(metadata);
      }
      metadataExtractionAttempted = true;
      return metadata != null;
    }
  }

  private void updateGcsItemMetadata(ExtractedMetadata metadata) {
    long genToSet =
        metadata.getGeneration() >= 0
            ? metadata.getGeneration()
            : itemId.getContentGeneration().orElse(-1L);

    GcsItemId.Builder itemIdBuilder = GcsItemId.builder().setBucketName(itemId.getBucketName());
    itemId.getObjectName().ifPresent(itemIdBuilder::setObjectName);

    GcsItemInfo.Builder itemInfoBuilder = GcsItemInfo.builder().setSize(metadata.getSize());

    if (genToSet >= 0) {
      itemIdBuilder.setContentGeneration(genToSet);
      itemInfoBuilder.setContentGeneration(genToSet);
    }

    itemId = itemIdBuilder.build();
    itemInfo = itemInfoBuilder.setItemId(itemId).build();
  }
}
