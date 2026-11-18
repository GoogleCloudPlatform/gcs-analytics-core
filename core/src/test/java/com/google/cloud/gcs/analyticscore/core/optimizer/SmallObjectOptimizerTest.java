/*
 * Copyright 2026 Google LLC
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

package com.google.cloud.gcs.analyticscore.core.optimizer;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.FakeGcsClientImpl;
import com.google.cloud.gcs.analyticscore.client.FakeGcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsCacheOptions;
import com.google.cloud.gcs.analyticscore.client.GcsClientOptions;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants;
import com.google.cloud.gcs.analyticscore.common.telemetry.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.MetricKey;
import com.google.cloud.gcs.analyticscore.common.telemetry.Operation;
import com.google.cloud.gcs.analyticscore.common.telemetry.OperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SmallObjectOptimizerTest {

  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName("b").setObjectName("test.parquet").build();
  private static final GcsItemInfo ITEM_INFO =
      GcsItemInfo.builder().setItemId(ITEM_ID).setSize(100).build();
  private static final GcsFileInfo FILE_INFO =
      GcsFileInfo.builder()
          .setItemInfo(ITEM_INFO)
          .setUri(URI.create("gs://b/test.parquet"))
          .setAttributes(ImmutableMap.of())
          .build();

  private GcsCacheOptions cacheOptions;
  private GcsReadOptions readOptions;
  private Telemetry telemetry;
  private VectoredSeekableByteChannel realSource;
  private SmallObjectOptimizer optimizer;
  private AnalyticsCacheManager cacheManager;

  @BeforeEach
  void initializeOptimizerAndFakeStorage() throws IOException {
    cacheOptions =
        GcsCacheOptions.builder()
            .setSmallObjectCacheEnabled(true)
            .setSmallObjectCacheMaxSizeBytes(200)
            .build();
    cacheManager = new AnalyticsCacheManager(cacheOptions);
    telemetry = new Telemetry(ImmutableList.of());

    readOptions = GcsReadOptions.builder().setSmallObjectCacheThresholdBytes(200).build();
    optimizer = new SmallObjectOptimizer(cacheOptions, readOptions, telemetry);

    GcsClientOptions clientOptions =
        GcsClientOptions.builder().setGcsReadOptions(readOptions).build();
    GcsFileSystemOptions fileSystemOptions =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(clientOptions)
            .setGcsCacheOptions(cacheOptions)
            .build();
    FakeGcsFileSystemImpl fakeFileSystem = new FakeGcsFileSystemImpl(fileSystemOptions);

    byte[] testData = new byte[100];
    for (int i = 0; i < 100; i++) {
      testData[i] = (byte) i;
    }
    FakeGcsClientImpl.storage.create(
        BlobInfo.newBuilder(ITEM_ID.getBucketName(), ITEM_ID.getObjectName().get(), 1L).build(),
        testData);

    realSource = fakeFileSystem.open(FILE_INFO, GcsReadOptions.builder().build());
  }

  @Test
  void isApplicable_fileInfo_smallFile_returnsTrue() {
    assertThat(optimizer.isApplicable(FILE_INFO)).isTrue();
  }

  @Test
  void isApplicable_itemId_returnsFalse() {
    assertThat(optimizer.isApplicable(ITEM_ID)).isFalse();
  }

  @Test
  void isApplicable_fileInfo_orcFile_returnsTrue() {
    GcsItemId orcItemId = GcsItemId.builder().setBucketName("b").setObjectName("test.orc").build();
    GcsItemInfo orcInfo = GcsItemInfo.builder().setItemId(orcItemId).setSize(100).build();
    GcsFileInfo orcFile = FILE_INFO.toBuilder().setItemInfo(orcInfo).build();
    assertThat(optimizer.isApplicable(orcFile)).isTrue();
  }

  @Test
  void isApplicable_fileInfo_nonDataFile_returnsFalse() {
    GcsItemId csvItemId = GcsItemId.builder().setBucketName("b").setObjectName("test.csv").build();
    GcsItemInfo csvInfo = GcsItemInfo.builder().setItemId(csvItemId).setSize(100).build();
    GcsFileInfo csvFile = FILE_INFO.toBuilder().setItemInfo(csvInfo).build();
    assertThat(optimizer.isApplicable(csvFile)).isFalse();
  }

  @Test
  void isApplicable_fileInfo_caseInsensitiveExtensions_returnTrue() {
    GcsFileInfo uppercaseParquet =
        FILE_INFO.toBuilder()
            .setItemInfo(
                ITEM_INFO.toBuilder()
                    .setItemId(
                        GcsItemId.builder()
                            .setBucketName("b")
                            .setObjectName("PART.PARQUET")
                            .build())
                    .build())
            .build();
    GcsFileInfo mixedcaseOrc =
        FILE_INFO.toBuilder()
            .setItemInfo(
                ITEM_INFO.toBuilder()
                    .setItemId(
                        GcsItemId.builder().setBucketName("b").setObjectName("data.OrC").build())
                    .build())
            .build();
    GcsFileInfo exactOrc =
        FILE_INFO.toBuilder()
            .setItemInfo(
                ITEM_INFO.toBuilder()
                    .setItemId(GcsItemId.builder().setBucketName("b").setObjectName(".orc").build())
                    .build())
            .build();

    assertThat(optimizer.isApplicable(uppercaseParquet)).isTrue();
    assertThat(optimizer.isApplicable(mixedcaseOrc)).isTrue();
    assertThat(optimizer.isApplicable(exactOrc)).isTrue();
  }

  @Test
  void isApplicable_fileInfo_shortNames_returnFalse() {
    GcsFileInfo shortOrc =
        FILE_INFO.toBuilder()
            .setItemInfo(
                ITEM_INFO.toBuilder()
                    .setItemId(GcsItemId.builder().setBucketName("b").setObjectName("orc").build())
                    .build())
            .build();
    GcsFileInfo singleChar =
        FILE_INFO.toBuilder()
            .setItemInfo(
                ITEM_INFO.toBuilder()
                    .setItemId(GcsItemId.builder().setBucketName("b").setObjectName("a").build())
                    .build())
            .build();

    assertThat(optimizer.isApplicable(shortOrc)).isFalse();
    assertThat(optimizer.isApplicable(singleChar)).isFalse();
  }

  @Test
  void isApplicable_fileInfo_missingObjectName_returnsFalse() {
    GcsFileInfo bucketOnly =
        FILE_INFO.toBuilder()
            .setItemInfo(
                ITEM_INFO.toBuilder()
                    .setItemId(GcsItemId.builder().setBucketName("b").build())
                    .build())
            .build();

    assertThat(optimizer.isApplicable(bucketOnly)).isFalse();
  }

  @Test
  void isApplicable_fileInfo_largeFile_returnsFalse() {
    GcsItemInfo largeInfo = GcsItemInfo.builder().setItemId(ITEM_ID).setSize(300).build();
    GcsFileInfo largeFile =
        GcsFileInfo.builder()
            .setItemInfo(largeInfo)
            .setUri(FILE_INFO.getUri())
            .setAttributes(FILE_INFO.getAttributes())
            .build();

    assertThat(optimizer.isApplicable(largeFile)).isFalse();
  }

  @Test
  void isApplicable_fileInfo_returnsFalseIfThresholdIsZero() {
    assertThat(optimizer.isApplicable(FILE_INFO)).isTrue();
    GcsCacheOptions cacheOptions =
        GcsCacheOptions.builder()
            .setSmallObjectCacheEnabled(true)
            .setSmallObjectCacheMaxSizeBytes(200)
            .build();
    GcsReadOptions readOptions0 =
        GcsReadOptions.builder().setSmallObjectCacheThresholdBytes(0).build();
    optimizer = new SmallObjectOptimizer(cacheOptions, readOptions0, telemetry);
    assertThat(optimizer.isApplicable(FILE_INFO)).isFalse();
  }

  @Test
  void isApplicable_fileInfo_returnsFalseIfCacheMaxIsZero() {
    GcsCacheOptions cacheOptions0 =
        GcsCacheOptions.builder()
            .setSmallObjectCacheEnabled(false)
            .setSmallObjectCacheMaxSizeBytes(0)
            .build();
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setSmallObjectCacheThresholdBytes(200).build();
    optimizer = new SmallObjectOptimizer(cacheOptions0, readOptions, telemetry);
    assertThat(optimizer.isApplicable(FILE_INFO)).isFalse();
  }

  @Test
  void onOpen_itemIdOnly_isNoOp() {
    optimizer.onOpen(ITEM_ID, cacheManager);
  }

  @Test
  void read_smallFile_cachesAndServes() throws IOException {
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);
    realSource.position(50L);

    int bytesRead = optimizer.read(10, dst, realSource);

    assertThat(bytesRead).isEqualTo(10);
    assertThat(dst.array()).isEqualTo(new byte[] {10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
    assertThat(realSource.position()).isEqualTo(50L); // Position restored
    // Delete the file from storage to prove second read comes from cache
    FakeGcsClientImpl.storage.delete(ITEM_ID.getBucketName(), ITEM_ID.getObjectName().get());
    dst.clear();
    int secondBytesRead = optimizer.read(20, dst, realSource);
    assertThat(secondBytesRead).isEqualTo(10);
    assertThat(dst.array()).isEqualTo(new byte[] {20, 21, 22, 23, 24, 25, 26, 27, 28, 29});
  }

  @Test
  void read_largeFile_returnsZero() throws IOException {
    GcsCacheOptions cacheOptions50 =
        GcsCacheOptions.builder()
            .setSmallObjectCacheEnabled(true)
            .setSmallObjectCacheMaxSizeBytes(50)
            .build();
    cacheManager = new AnalyticsCacheManager(cacheOptions50);
    optimizer =
        new SmallObjectOptimizer(
            cacheOptions50,
            GcsReadOptions.builder().setSmallObjectCacheThresholdBytes(50).build(),
            telemetry);
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(0, dst, realSource);

    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void read_unexpectedEofDuringCaching_throwsIOException() throws IOException {
    VectoredSeekableByteChannel mockSource = mock(VectoredSeekableByteChannel.class);
    when(mockSource.position()).thenReturn(0L);
    when(mockSource.size()).thenReturn(100L);
    when(mockSource.read(any(ByteBuffer.class))).thenReturn(-1);

    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);

    IOException exception =
        assertThrows(IOException.class, () -> optimizer.read(0, dst, mockSource));

    assertThat(exception)
        .hasMessageThat()
        .contains("Unexpected EOF encountered while reading small object");
  }

  @Test
  void readVectored_uninitializedFileSize_returnsOriginalRanges() throws IOException {
    optimizer.onOpen(ITEM_ID, cacheManager);
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    List<GcsObjectRange> ranges = List.of(range);
    List<GcsObjectRange> remaining =
        optimizer.readVectored(ranges, (size) -> ByteBuffer.allocate(size));

    assertThat(remaining).isSameInstanceAs(ranges);
  }

  @Test
  void read_pastEOF_returnsMinusOne() throws IOException {
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesReadEof = optimizer.read(100, dst, realSource);

    assertThat(bytesReadEof).isEqualTo(-1);
    int bytesReadPastEof = optimizer.read(110, dst, realSource);
    assertThat(bytesReadPastEof).isEqualTo(-1);
  }

  @Test
  void read_lazyInitFileSize_whenOnOpenWithItemIdUsed() throws IOException {
    optimizer.onOpen(ITEM_ID, cacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(10, dst, realSource);

    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void serveFromCache_pastEOF_returnsMinusOne() throws IOException {
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);

    optimizer.read(0, ByteBuffer.allocate(10), realSource); // Trigger caching
    ByteBuffer dst = ByteBuffer.allocate(10);
    int bytesRead = optimizer.read(150, dst, realSource);

    assertThat(bytesRead).isEqualTo(-1);
  }

  @Test
  void readVectored_pastEOF_completesWithEOFException() throws IOException {
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);

    optimizer.read(0, ByteBuffer.allocate(10), realSource); // Trigger caching
    GcsObjectRange pastEofRange =
        GcsObjectRange.builder()
            .setOffset(110)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    optimizer.readVectored(List.of(pastEofRange), (size) -> ByteBuffer.allocate(size));
    var exception =
        assertThrows(ExecutionException.class, () -> pastEofRange.getByteBufferFuture().get());

    assertThat(exception.getCause()).isInstanceOf(java.io.EOFException.class);
  }

  @Test
  void readVectored_notApplicable_returnsOriginalRanges() throws IOException {
    GcsCacheOptions cacheOptions50 =
        GcsCacheOptions.builder()
            .setSmallObjectCacheEnabled(true)
            .setSmallObjectCacheMaxSizeBytes(50)
            .build();
    cacheManager = new AnalyticsCacheManager(cacheOptions50);
    optimizer =
        new SmallObjectOptimizer(
            cacheOptions50,
            GcsReadOptions.builder().setSmallObjectCacheThresholdBytes(50).build(),
            telemetry);
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    List<GcsObjectRange> ranges = List.of(range);
    List<GcsObjectRange> remaining =
        optimizer.readVectored(ranges, (size) -> ByteBuffer.allocate(size));

    assertThat(remaining).isSameInstanceAs(ranges);
  }

  @Test
  void readVectored_notYetCached_returnsOriginalRanges() throws IOException {
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    List<GcsObjectRange> ranges = List.of(range);
    List<GcsObjectRange> remaining =
        optimizer.readVectored(ranges, (size) -> ByteBuffer.allocate(size));

    assertThat(remaining).isSameInstanceAs(ranges);
  }

  @Test
  void readVectored_partialRead_completesExceptionally() throws IOException {
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);

    optimizer.read(0, ByteBuffer.allocate(10), realSource); // Trigger caching
    GcsObjectRange partialRange =
        GcsObjectRange.builder()
            .setOffset(90)
            .setLength(20) // Only 10 bytes available before EOF
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    optimizer.readVectored(List.of(partialRange), (size) -> ByteBuffer.allocate(size));
    var exception =
        assertThrows(ExecutionException.class, () -> partialRange.getByteBufferFuture().get());

    assertThat(exception.getCause()).isInstanceOf(java.io.EOFException.class);
  }

  @Test
  void readVectored_allocateReturnsNull_completesExceptionally() throws Exception {
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);
    optimizer.read(0, ByteBuffer.allocate(10), realSource); // Trigger caching

    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    optimizer.readVectored(ImmutableList.of(range), size -> null);

    ExecutionException exception =
        assertThrows(ExecutionException.class, () -> range.getByteBufferFuture().get());
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException.class);
    assertThat(exception)
        .hasCauseThat()
        .hasMessageThat()
        .contains("Buffer allocation returned null");
  }

  @Test
  void readVectored_success_returnsEmptyList() throws Exception {
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);

    optimizer.read(0, ByteBuffer.allocate(10), realSource); // Trigger caching
    GcsObjectRange validRange =
        GcsObjectRange.builder()
            .setOffset(10)
            .setLength(20)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    List<GcsObjectRange> remaining =
        optimizer.readVectored(List.of(validRange), ByteBuffer::allocate);

    assertThat(remaining).isEmpty();
    ByteBuffer result = validRange.getByteBufferFuture().get();
    assertThat(result.remaining()).isEqualTo(20);
    assertThat(result.get()).isEqualTo((byte) 10);
    assertThat(result.get(19)).isEqualTo((byte) 29);
  }

  @Test
  void read_warmSharedCache_servesFromCacheAndRecordsHit() throws IOException {
    optimizer.onOpen(FILE_INFO, cacheManager);
    optimizer.read(0, ByteBuffer.allocate(10), realSource);

    FakeGcsClientImpl.storage.delete(ITEM_ID.getBucketName(), ITEM_ID.getObjectName().get());

    List<Metric> recordedMetrics = new ArrayList<>();
    OperationListener listener =
        new OperationListener() {
          @Override
          public void onOperationStart(Operation operation) {}

          @Override
          public void onOperationEnd(Operation operation, Map<MetricKey, Long> metrics) {
            for (MetricKey key : metrics.keySet()) {
              recordedMetrics.add(key.getMetric());
            }
          }
        };

    try (Telemetry hitTelemetry = new Telemetry(ImmutableList.of(listener))) {
      SmallObjectOptimizer warmOptimizer =
          new SmallObjectOptimizer(cacheOptions, readOptions, hitTelemetry);
      warmOptimizer.onOpen(FILE_INFO, cacheManager);

      ByteBuffer dst = ByteBuffer.allocate(10);
      int bytesRead = warmOptimizer.read(15, dst, realSource);

      assertThat(bytesRead).isEqualTo(10);
      assertThat(dst.array()).isEqualTo(new byte[] {15, 16, 17, 18, 19, 20, 21, 22, 23, 24});
      assertThat(recordedMetrics)
          .contains(GcsAnalyticsCoreTelemetryConstants.Metric.SMALL_OBJECT_CACHE_HIT);
      assertThat(recordedMetrics)
          .doesNotContain(GcsAnalyticsCoreTelemetryConstants.Metric.SMALL_OBJECT_CACHE_MISS);
    }
  }

  @Test
  void readVectored_nullArgumentsOrElement_throwsNullPointerException() throws IOException {
    List<GcsObjectRange> listWithNull = Arrays.asList((GcsObjectRange) null);

    // Unopened (fileSize == -1) pass-through path still fails fast at method entry.
    NullPointerException unopenedNpe =
        assertThrows(
            NullPointerException.class,
            () -> optimizer.readVectored(listWithNull, ByteBuffer::allocate));
    assertThat(unopenedNpe).hasMessageThat().isEqualTo("range cannot be null");

    // Cold-cache (onOpen without read) path still fails fast at method entry.
    optimizer.onOpen(FILE_INFO, cacheManager);
    NullPointerException coldCacheNpe =
        assertThrows(
            NullPointerException.class,
            () -> optimizer.readVectored(listWithNull, ByteBuffer::allocate));
    assertThat(coldCacheNpe).hasMessageThat().isEqualTo("range cannot be null");

    // Warm-cache path and top-level null arguments.
    optimizer.read(0, ByteBuffer.allocate(10), realSource);
    assertThrows(
        NullPointerException.class, () -> optimizer.readVectored(null, ByteBuffer::allocate));
    assertThrows(
        NullPointerException.class, () -> optimizer.readVectored(ImmutableList.of(), null));
    NullPointerException warmCacheNpe =
        assertThrows(
            NullPointerException.class,
            () -> optimizer.readVectored(listWithNull, ByteBuffer::allocate));
    assertThat(warmCacheNpe).hasMessageThat().isEqualTo("range cannot be null");
  }

  @Test
  void readVectored_nullBetweenValidRanges_failsFastWithoutCompletingAnyFuture()
      throws IOException {
    AtomicLong hitCount = new AtomicLong(0);
    try (Telemetry hitTelemetry = telemetryRecordingCacheHits(hitCount)) {
      SmallObjectOptimizer opt = new SmallObjectOptimizer(cacheOptions, readOptions, hitTelemetry);
      opt.onOpen(FILE_INFO, cacheManager);
      opt.read(0, ByteBuffer.allocate(10), realSource);
      hitCount.set(0);

      GcsObjectRange first = range(0L, 4);
      GcsObjectRange last = range(4L, 4);
      List<GcsObjectRange> mixedWithNull = Arrays.asList(first, null, last);
      AtomicInteger allocCalls = new AtomicInteger(0);

      NullPointerException thrown =
          assertThrows(
              NullPointerException.class,
              () ->
                  opt.readVectored(
                      mixedWithNull,
                      size -> {
                        allocCalls.incrementAndGet();
                        return ByteBuffer.allocate(size);
                      }));

      assertThat(thrown).hasMessageThat().isEqualTo("range cannot be null");
      assertThat(allocCalls.get()).isEqualTo(0);
      assertThat(first.getByteBufferFuture().isDone()).isFalse();
      assertThat(last.getByteBufferFuture().isDone()).isFalse();
      assertThat(hitCount.get()).isEqualTo(0L);
    }
  }

  @Test
  void readVectored_emptyRanges_returnsOriginalWithoutAllocOrMetric() throws IOException {
    AtomicLong hitCount = new AtomicLong(0);
    AtomicInteger allocCalls = new AtomicInteger(0);
    try (Telemetry hitTelemetry = telemetryRecordingCacheHits(hitCount)) {
      SmallObjectOptimizer opt = new SmallObjectOptimizer(cacheOptions, readOptions, hitTelemetry);
      opt.onOpen(FILE_INFO, cacheManager);
      opt.read(0, ByteBuffer.allocate(10), realSource);
      List<GcsObjectRange> ranges = ImmutableList.of();

      List<GcsObjectRange> remaining =
          opt.readVectored(
              ranges,
              size -> {
                allocCalls.incrementAndGet();
                return ByteBuffer.allocate(size);
              });

      assertThat(remaining).isSameInstanceAs(ranges);
      assertThat(allocCalls.get()).isEqualTo(0);
      assertThat(hitCount.get()).isEqualTo(0L);
    }
  }

  @Test
  void readVectored_negativeOffset_completesWithEofAndContinues() throws Exception {
    optimizer.onOpen(FILE_INFO, cacheManager);
    optimizer.read(0, ByteBuffer.allocate(10), realSource);
    AtomicInteger allocCalls = new AtomicInteger(0);
    GcsObjectRange negZero = range(-1L, 0);
    GcsObjectRange negTrunc = range(-(1L << 32) + 10L, 5);
    GcsObjectRange valid = range(5L, 4);

    optimizer.readVectored(
        ImmutableList.of(negZero, negTrunc, valid),
        size -> {
          allocCalls.incrementAndGet();
          return ByteBuffer.allocate(size);
        });

    assertThat(allocCalls.get()).isEqualTo(1);
    assertThat(assertThrows(ExecutionException.class, () -> negZero.getByteBufferFuture().get()))
        .hasCauseThat()
        .isInstanceOf(EOFException.class);
    assertThat(assertThrows(ExecutionException.class, () -> negTrunc.getByteBufferFuture().get()))
        .hasCauseThat()
        .isInstanceOf(EOFException.class);
    ByteBuffer buf = valid.getByteBufferFuture().get();
    assertThat(buf.remaining()).isEqualTo(4);
    assertThat(buf.get(0)).isEqualTo((byte) 5);
  }

  @Test
  void readVectored_negativeLength_completesWithIllegalArgumentWithoutAlloc() throws IOException {
    optimizer.onOpen(FILE_INFO, cacheManager);
    optimizer.read(0, ByteBuffer.allocate(10), realSource);
    AtomicInteger allocCalls = new AtomicInteger(0);
    GcsObjectRange negLen = range(0L, -5);

    optimizer.readVectored(
        ImmutableList.of(negLen),
        size -> {
          allocCalls.incrementAndGet();
          return ByteBuffer.allocate(size);
        });

    assertThat(allocCalls.get()).isEqualTo(0);
    assertThat(assertThrows(ExecutionException.class, () -> negLen.getByteBufferFuture().get()))
        .hasCauseThat()
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readVectored_zeroLength_completesEmptyWithoutAllocOrMetric() throws Exception {
    AtomicLong hitCount = new AtomicLong(0);
    try (Telemetry hitTelemetry = telemetryRecordingCacheHits(hitCount)) {
      SmallObjectOptimizer opt = new SmallObjectOptimizer(cacheOptions, readOptions, hitTelemetry);
      opt.onOpen(FILE_INFO, cacheManager);
      opt.read(0, ByteBuffer.allocate(10), realSource);
      AtomicInteger allocCalls = new AtomicInteger(0);
      GcsObjectRange inBounds = range(10L, 0);
      GcsObjectRange atEof = range(100L, 0);
      GcsObjectRange pastEof = range(150L, 0);

      opt.readVectored(
          ImmutableList.of(inBounds, atEof, pastEof),
          size -> {
            allocCalls.incrementAndGet();
            return ByteBuffer.allocate(size);
          });

      assertThat(allocCalls.get()).isEqualTo(0);
      assertThat(hitCount.get()).isEqualTo(0L);
      assertThat(inBounds.getByteBufferFuture().get().remaining()).isEqualTo(0);
      assertThat(atEof.getByteBufferFuture().get().remaining()).isEqualTo(0);
      assertThat(pastEof.getByteBufferFuture().get().remaining()).isEqualTo(0);
    }
  }

  @Test
  void readVectored_maxOffset_completesWithEofWithoutAlloc() throws IOException {
    optimizer.onOpen(FILE_INFO, cacheManager);
    optimizer.read(0, ByteBuffer.allocate(10), realSource);
    AtomicInteger allocCalls = new AtomicInteger(0);
    GcsObjectRange maxOffset = range(Long.MAX_VALUE, 10);

    optimizer.readVectored(
        ImmutableList.of(maxOffset),
        size -> {
          allocCalls.incrementAndGet();
          return ByteBuffer.allocate(size);
        });

    assertThat(allocCalls.get()).isEqualTo(0);
    assertThat(assertThrows(ExecutionException.class, () -> maxOffset.getByteBufferFuture().get()))
        .hasCauseThat()
        .isInstanceOf(EOFException.class);
  }

  @Test
  void readVectored_allocatorThrows_failsOnlyThatRangeAndRecordsServedHits() throws Exception {
    AtomicLong hitCount = new AtomicLong(0);
    try (Telemetry hitTelemetry = telemetryRecordingCacheHits(hitCount)) {
      SmallObjectOptimizer opt = new SmallObjectOptimizer(cacheOptions, readOptions, hitTelemetry);
      opt.onOpen(FILE_INFO, cacheManager);
      opt.read(0, ByteBuffer.allocate(10), realSource);
      GcsObjectRange failing = range(0L, 10);
      GcsObjectRange valid = range(20L, 5);
      AtomicInteger call = new AtomicInteger(0);

      opt.readVectored(
          ImmutableList.of(failing, valid),
          size -> {
            if (call.getAndIncrement() == 0) {
              throw new IllegalStateException("pool exhausted");
            }
            return ByteBuffer.allocate(size);
          });

      assertThat(call.get()).isEqualTo(2);
      assertThat(hitCount.get()).isEqualTo(1L);
      assertThat(assertThrows(ExecutionException.class, () -> failing.getByteBufferFuture().get()))
          .hasCauseThat()
          .isInstanceOf(IllegalStateException.class);
      ByteBuffer buf = valid.getByteBufferFuture().get();
      assertThat(buf.remaining()).isEqualTo(5);
      assertThat(buf.get(0)).isEqualTo((byte) 20);
    }
  }

  @Test
  void readVectored_undersizedBuffer_completesWithIllegalArgument() throws IOException {
    optimizer.onOpen(FILE_INFO, cacheManager);
    optimizer.read(0, ByteBuffer.allocate(10), realSource);
    GcsObjectRange r = range(0L, 10);

    optimizer.readVectored(ImmutableList.of(r), size -> ByteBuffer.allocate(5));

    assertThat(assertThrows(ExecutionException.class, () -> r.getByteBufferFuture().get()))
        .hasCauseThat()
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readVectored_slabBuffer_preservesPrefixAndSetsSliceBounds() throws Exception {
    optimizer.onOpen(FILE_INFO, cacheManager);
    optimizer.read(0, ByteBuffer.allocate(10), realSource);
    ByteBuffer slab = ByteBuffer.allocate(64);
    for (int i = 0; i < 16; i++) {
      slab.put(i, (byte) 0x5A);
    }
    slab.position(16);
    GcsObjectRange r = range(30L, 10);

    optimizer.readVectored(ImmutableList.of(r), size -> slab);

    ByteBuffer result = r.getByteBufferFuture().get();
    assertThat(result).isSameInstanceAs(slab);
    assertThat(result.position()).isEqualTo(16);
    assertThat(result.limit()).isEqualTo(26);
    assertThat(result.remaining()).isEqualTo(10);
    for (int i = 0; i < 16; i++) {
      assertThat(result.get(i)).isEqualTo((byte) 0x5A);
    }
    for (int i = 0; i < 10; i++) {
      assertThat(result.get(16 + i)).isEqualTo((byte) (30 + i));
    }
  }

  private static GcsObjectRange range(long offset, int length) {
    return GcsObjectRange.builder()
        .setOffset(offset)
        .setLength(length)
        .setByteBufferFuture(new CompletableFuture<>())
        .build();
  }

  private static Telemetry telemetryRecordingCacheHits(AtomicLong hitCount) {
    OperationListener listener =
        new OperationListener() {
          @Override
          public void onOperationStart(Operation operation) {}

          @Override
          public void onOperationEnd(Operation operation, Map<MetricKey, Long> metrics) {
            for (Map.Entry<MetricKey, Long> entry : metrics.entrySet()) {
              if (entry.getKey().getMetric()
                  == GcsAnalyticsCoreTelemetryConstants.Metric.SMALL_OBJECT_CACHE_HIT) {
                hitCount.addAndGet(entry.getValue());
              }
            }
          }
        };
    return new Telemetry(ImmutableList.of(listener));
  }
}
