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

package com.google.cloud.gcs.analyticscore.core.prefetch;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.FakeVectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.client.GcsCacheOptions;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsPrefetchOptions;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.RecordingOperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PredictivePrefetchOptimizerTest {

  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName("bucket").setObjectName("data.parquet").build();
  private static final int RECORD_COUNT = 500;
  private static final int MULTI_ROW_GROUP_RECORD_COUNT = 5000;
  private static final int FEW_ROW_GROUPS_RECORD_COUNT = 500;
  private static final int SLICE_LENGTH = 64;

  @TempDir File temporaryDirectory;

  private byte[] content;
  private ParquetFileLayout layout;
  private FakeVectoredSeekableByteChannel channel;
  private RecordingOperationListener metricListener;
  private Telemetry telemetry;
  private AnalyticsCacheManager cacheManager;
  private PredictivePrefetchOptimizer optimizer;

  @BeforeEach
  void createOptimizerOverParquetObject() throws IOException {
    content =
        ParquetTestFiles.readAllBytes(
            ParquetTestFiles.createFile(
                temporaryDirectory,
                "data.parquet",
                RECORD_COUNT,
                ParquetTestFiles.SINGLE_ROW_GROUP_SIZE_BYTES));
    layout = parseLayout(content);
    metricListener = new RecordingOperationListener();
    telemetry = new Telemetry(ImmutableList.of(metricListener));
    channel = new FakeVectoredSeekableByteChannel(content);
    optimizer = createOptimizer(/* enabled= */ true);
    cacheManager.getSchemaAccessHistory().get().invalidateAll();
  }

  @AfterEach
  void closeOptimizer() {
    optimizer.onClose();
    telemetry.close();
    channel.close();
  }

  @Test
  void isApplicable_parquetObject_returnsTrue() {
    assertThat(optimizer.isApplicable(ITEM_ID)).isTrue();
  }

  @Test
  void isApplicable_uppercaseParquetExtension_returnsTrue() {
    GcsItemId uppercaseItemId =
        GcsItemId.builder().setBucketName("bucket").setObjectName("DATA.PARQUET").build();

    assertThat(optimizer.isApplicable(uppercaseItemId)).isTrue();
  }

  @Test
  void isApplicable_nonParquetObject_returnsFalse() {
    GcsItemId csvItemId =
        GcsItemId.builder().setBucketName("bucket").setObjectName("data.csv").build();

    assertThat(optimizer.isApplicable(csvItemId)).isFalse();
  }

  @Test
  void isApplicable_prefetchDisabled_returnsFalse() {
    PredictivePrefetchOptimizer disabledOptimizer =
        new PredictivePrefetchOptimizer(GcsPrefetchOptions.builder().setEnabled(false).build());

    assertThat(disabledOptimizer.isApplicable(ITEM_ID)).isFalse();
  }

  @Test
  void onOpen_prefetchDisabledInCacheManager_throwsIllegalStateException() {
    AnalyticsCacheManager disabledCacheManager =
        new AnalyticsCacheManager(
            GcsCacheOptions.builder().build(),
            GcsPrefetchOptions.builder().setEnabled(false).build(),
            telemetry);
    PredictivePrefetchOptimizer unopenedOptimizer =
        new PredictivePrefetchOptimizer(GcsPrefetchOptions.builder().setEnabled(true).build());

    assertThrows(
        IllegalStateException.class, () -> unopenedOptimizer.onOpen(ITEM_ID, disabledCacheManager));
  }

  @Test
  void onClose_beforeOnOpen_doesNotThrow() {
    PredictivePrefetchOptimizer unopenedOptimizer =
        new PredictivePrefetchOptimizer(GcsPrefetchOptions.builder().setEnabled(true).build());

    unopenedOptimizer.onClose();
  }

  @Test
  void readVectored_withoutFooterLayout_recordsNoCacheMiss() {
    cacheManager.invalidateFooter(ITEM_ID);
    GcsObjectRange range = createRange(0, SLICE_LENGTH);

    optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate, channel);

    assertThat(metricListener.getTotal(Metric.PREFETCH_CACHE_MISS)).isEqualTo(0);
  }

  @Test
  void read_firstReadOfDataPage_returnsZero() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    int servedBytes =
        optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(servedBytes).isEqualTo(0);
  }

  @Test
  void read_firstReadOfDataPage_recordsCacheMiss() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(metricListener.getTotal(Metric.PREFETCH_CACHE_MISS)).isEqualTo(1);
  }

  @Test
  void read_afterSpeculation_servesTheRequestedLength() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    prefetchFirstRowGroupIdColumn();
    long prefetchedOffset = idChunk.getDataPageOffset() + SLICE_LENGTH;

    int servedBytes = optimizer.read(prefetchedOffset, ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(servedBytes).isEqualTo(SLICE_LENGTH);
  }

  @Test
  void read_servedFromCache_returnsTheSameBytesAsTheObject() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    prefetchFirstRowGroupIdColumn();
    long prefetchedOffset = idChunk.getDataPageOffset() + SLICE_LENGTH;

    ByteBuffer destination = ByteBuffer.allocate(SLICE_LENGTH);
    optimizer.read(prefetchedOffset, destination, channel);

    assertThat(destination.array()).isEqualTo(sliceOfContent(prefetchedOffset, SLICE_LENGTH));
  }

  @Test
  void read_servedFromCache_recordsCacheHit() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    prefetchFirstRowGroupIdColumn();

    optimizer.read(
        idChunk.getDataPageOffset() + SLICE_LENGTH, ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(metricListener.getTotal(Metric.PREFETCH_CACHE_HIT)).isEqualTo(1);
  }

  @Test
  void read_servedFromCache_recordsBytesConsumed() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    prefetchFirstRowGroupIdColumn();

    optimizer.read(
        idChunk.getDataPageOffset() + SLICE_LENGTH, ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(metricListener.getTotal(Metric.PREFETCH_BYTES_CONSUMED)).isEqualTo(SLICE_LENGTH);
  }

  @Test
  void read_partialCacheHit_recordsOnlyTheServedBytesAsAccessed() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk categoryChunk = columnChunk(layout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    long cachedOffset = categoryChunk.getStartOffset() - 16;
    cacheManager
        .getPrefetchBufferCache()
        .get()
        .registerRange(
            ITEM_ID,
            cachedOffset,
            16,
            CompletableFuture.completedFuture(ByteBuffer.wrap(sliceOfContent(cachedOffset, 16))));

    optimizer.read(cachedOffset, ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(idChunk.getEndOffset()).isEqualTo(categoryChunk.getStartOffset());
    assertThat(
            cacheManager
                .getSchemaAccessHistory()
                .get()
                .getDataColumns(layout.getSchemaFingerprint()))
        .containsExactly(ParquetTestFiles.ID_COLUMN);
  }

  @Test
  void read_withoutFooterLayout_recordsNoCacheMiss() throws IOException {
    cacheManager.invalidateFooter(ITEM_ID);

    optimizer.read(0, ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(metricListener.getTotal(Metric.PREFETCH_CACHE_MISS)).isEqualTo(0);
  }

  @Test
  void readVectored_uncachedRange_recordsCacheMiss() {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    optimizer.readVectored(
        ImmutableList.of(rangeOverChunk(idChunk)), ByteBuffer::allocate, channel);

    assertThat(metricListener.getTotal(Metric.PREFETCH_CACHE_MISS)).isEqualTo(1);
  }

  @Test
  void read_offsetBeyondLastRowGroup_returnsZero() throws IOException {
    int servedBytes = optimizer.read(content.length - 16, ByteBuffer.allocate(16), channel);

    assertThat(servedBytes).isEqualTo(0);
  }

  @Test
  void read_offsetBeyondLastRowGroup_schedulesNothingWhenSchemaIsUnlearned() throws IOException {
    optimizer.read(content.length - 16, ByteBuffer.allocate(16), channel);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void read_footerNotInGlobalCache_skipsPrefetching() throws IOException {
    cacheManager.invalidateFooter(ITEM_ID);
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void read_alreadyCachedRemainder_schedulesNoAdditionalDuplicateRequests() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    prefetchFirstRowGroupIdColumn();
    int scheduledAfterFirstRead = channel.getRequestedOffsets().size();

    optimizer.read(
        idChunk.getDataPageOffset() + SLICE_LENGTH, ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(channel.getRequestedOffsets()).hasSize(scheduledAfterFirstRead);
  }

  @Test
  void read_smallSliceOfOneColumn_doesNotRecordOtherColumns() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(16), channel);

    assertThat(
            cacheManager
                .getSchemaAccessHistory()
                .get()
                .getDataColumns(layout.getSchemaFingerprint()))
        .containsExactly(ParquetTestFiles.ID_COLUMN);
  }

  @Test
  void readVectored_nothingPrefetched_returnsAllRanges() {
    GcsObjectRange range = createRange(0, SLICE_LENGTH);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate, channel);

    assertThat(unservedRanges).containsExactly(range);
  }

  @Test
  void readVectored_rangeInsidePrefetchedChunk_returnsNoUnservedRanges() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    prefetchFirstRowGroupIdColumn();
    GcsObjectRange range = createRange(idChunk.getDataPageOffset() + 16, 16);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate, channel);

    assertThat(unservedRanges).isEmpty();
  }

  @Test
  void readVectored_rangeInsidePrefetchedChunk_completesWithObjectBytes() throws Exception {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    prefetchFirstRowGroupIdColumn();
    long prefetchedOffset = idChunk.getDataPageOffset() + 16;
    GcsObjectRange range = createRange(prefetchedOffset, 16);

    optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate, channel);

    assertThat(range.getByteBufferFuture().get(5, TimeUnit.SECONDS).array())
        .isEqualTo(sliceOfContent(prefetchedOffset, 16));
  }

  @Test
  void readVectored_rangeThatWasNeverPrefetched_returnsTheRange() throws IOException {
    prefetchFirstRowGroupIdColumn();
    GcsObjectRange range = createRange(content.length - 16, 16);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate, channel);

    assertThat(unservedRanges).containsExactly(range);
  }

  @Test
  void readVectored_rangeStillInFlight_returnsNoUnservedRanges() throws IOException {
    long inFlightOffset = scheduleWithoutCompleting();
    GcsObjectRange range = createRange(inFlightOffset, 16);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate, channel);

    assertThat(unservedRanges).isEmpty();
  }

  @Test
  void readVectored_rangeStillInFlight_completesOnceThePrefetchArrives() throws Exception {
    long inFlightOffset = scheduleWithoutCompleting();
    GcsObjectRange range = createRange(inFlightOffset, 16);
    optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate, channel);

    channel.completeDeferredRanges();

    assertThat(range.getByteBufferFuture().get(5, TimeUnit.SECONDS).array())
        .isEqualTo(sliceOfContent(inFlightOffset, 16));
  }

  @Test
  void readVectored_firstVectoredRequest_recordsTheAccessedColumn() {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    optimizer.readVectored(
        ImmutableList.of(rangeOverChunk(idChunk)), ByteBuffer::allocate, channel);

    assertThat(
            cacheManager
                .getSchemaAccessHistory()
                .get()
                .getDataColumns(layout.getSchemaFingerprint()))
        .contains(ParquetTestFiles.ID_COLUMN);
  }

  @Test
  void readVectored_coalescedRangeAcrossAdjacentPrefetchedColumns_servesFromCache()
      throws Exception {
    byte[] multiRowGroupContent = createMultiRowGroupContent();
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(/* enabled= */ true);
    int fingerprint = multiRowGroupLayout.getSchemaFingerprint();
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg1FirstChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk rg1SecondChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.CATEGORY_COLUMN);
    optimizer.read(
        rg1SecondChunk.getDictionaryPageOffset().getAsLong(), ByteBuffer.allocate(8), channel);
    long coalescedStart = Math.min(rg1FirstChunk.getStartOffset(), rg1SecondChunk.getStartOffset());
    long coalescedEnd = Math.max(rg1FirstChunk.getEndOffset(), rg1SecondChunk.getEndOffset());
    int coalescedLength = (int) (coalescedEnd - coalescedStart);
    GcsObjectRange coalescedRange = createRange(coalescedStart, coalescedLength);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(coalescedRange), ByteBuffer::allocate, channel);

    assertThat(unservedRanges).isEmpty();
    assertThat(coalescedRange.getByteBufferFuture().get(5, TimeUnit.SECONDS).array())
        .isEqualTo(
            Arrays.copyOfRange(
                multiRowGroupContent,
                (int) coalescedStart,
                (int) coalescedStart + coalescedLength));
  }

  @Test
  void read_allKnownDictionariesReadInSmallRowGroup_prefetchesThatRowGroupsDataPages()
      throws IOException {
    ParquetFileLayout multiRowGroupLayout = openMultiRowGroupFileWithLearnedFilterSchema();
    ParquetColumnChunk rg0DictChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg0DataChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);

    optimizer.read(
        rg0DictChunk.getDictionaryPageOffset().getAsLong(), ByteBuffer.allocate(8), channel);

    assertThat(channel.getRequestedOffsets()).contains(rg0DataChunk.getStartOffset());
  }

  @Test
  void read_dictionariesOfLaterRowGroup_prefetchesItsDataPages() throws IOException {
    ParquetFileLayout multiRowGroupLayout = openMultiRowGroupFileWithLearnedFilterSchema();
    ParquetColumnChunk rg0DictChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg1DictChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg1DataChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.ID_COLUMN);
    optimizer.read(
        rg0DictChunk.getDictionaryPageOffset().getAsLong(), ByteBuffer.allocate(8), channel);

    optimizer.read(
        rg1DictChunk.getDictionaryPageOffset().getAsLong(), ByteBuffer.allocate(8), channel);

    assertThat(channel.getRequestedOffsets()).contains(rg1DataChunk.getStartOffset());
  }

  @Test
  void read_dictionaryTriggerBeforeDataColumnsAreKnown_retriesOnNextDictionaryRead()
      throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent(FEW_ROW_GROUPS_RECORD_COUNT);
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(/* enabled= */ true);
    int fingerprint = multiRowGroupLayout.getSchemaFingerprint();
    ParquetColumnChunk rg0DictChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    long rg0DictOffset = rg0DictChunk.getDictionaryPageOffset().getAsLong();
    ParquetColumnChunk rg0DataChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    optimizer.read(rg0DictOffset, ByteBuffer.allocate(8), channel);
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);

    optimizer.read(rg0DictOffset + 8, ByteBuffer.allocate(8), channel);

    assertThat(channel.getRequestedOffsets()).contains(rg0DataChunk.getStartOffset());
  }

  @Test
  void read_dictionaryTriggerWithUncachedDictionaryPage_prefetchesTheWholeColumnChunk()
      throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent(FEW_ROW_GROUPS_RECORD_COUNT);
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(/* enabled= */ true);
    int fingerprint = multiRowGroupLayout.getSchemaFingerprint();
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg0Chunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);

    optimizer.read(rg0Chunk.getDictionaryPageOffset().getAsLong(), ByteBuffer.allocate(8), channel);

    assertThat(channel.getRequestedOffsets()).contains(rg0Chunk.getStartOffset());
  }

  @Test
  void read_dictionaryPageAfterDataPageOfSameRowGroup_doesNotPrefetchRowGroupDataPages()
      throws IOException {
    ParquetFileLayout multiRowGroupLayout = openMultiRowGroupFileWithLearnedFilterSchema();
    ParquetColumnChunk rg0DictChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg0DataChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    optimizer.read(rg0DataChunk.getDataPageOffset(), ByteBuffer.allocate(8), channel);

    optimizer.read(
        rg0DictChunk.getDictionaryPageOffset().getAsLong(), ByteBuffer.allocate(8), channel);

    assertThat(channel.getRequestedOffsets()).doesNotContain(rg0DataChunk.getStartOffset());
  }

  /**
   * Opens a small-row-group file whose schema history already knows {@code id} as a data column,
   * and returns its layout.
   */
  private ParquetFileLayout openMultiRowGroupFileWithLearnedFilterSchema() throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent(FEW_ROW_GROUPS_RECORD_COUNT);
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(/* enabled= */ true);
    int fingerprint = multiRowGroupLayout.getSchemaFingerprint();
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);
    return multiRowGroupLayout;
  }

  /** Leaves a speculative request outstanding and returns the start offset it covers. */
  private long scheduleWithoutCompleting() throws IOException {
    channel.deferVectoredCompletion();
    prefetchFirstRowGroupIdColumn();
    return channel.getRequestedOffsets().get(0);
  }

  /**
   * Learns {@code id} as a data column and reads the {@code category} dictionary page of the first
   * row group, which prefetches that row group's {@code id} column chunk.
   */
  private void prefetchFirstRowGroupIdColumn() throws IOException {
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(layout.getSchemaFingerprint(), ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk categoryChunk = columnChunk(layout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    optimizer.read(
        categoryChunk.getDictionaryPageOffset().getAsLong(), ByteBuffer.allocate(16), channel);
  }

  private static ParquetFileLayout parseLayout(byte[] objectContent) {
    return ParquetFooterParser.parse(ByteBuffer.wrap(objectContent)).get();
  }

  private static ParquetColumnChunk columnChunk(
      ParquetFileLayout fileLayout, int rowGroupIndex, String columnPath) {
    return fileLayout.getRowGroup(rowGroupIndex).get().getColumnChunk(columnPath).get();
  }

  private static GcsObjectRange createRange(long offset, int length) {
    return GcsObjectRange.builder()
        .setOffset(offset)
        .setLength(length)
        .setByteBufferFuture(new CompletableFuture<>())
        .build();
  }

  private static GcsObjectRange rangeOverChunk(ParquetColumnChunk chunk) {
    return createRange(
        chunk.getStartOffset(), (int) (chunk.getEndOffset() - chunk.getStartOffset()));
  }

  private byte[] createMultiRowGroupContent() throws IOException {
    return createMultiRowGroupContent(MULTI_ROW_GROUP_RECORD_COUNT);
  }

  private byte[] createMultiRowGroupContent(int recordCount) throws IOException {
    File multiRowGroupFile =
        ParquetTestFiles.createFile(
            temporaryDirectory,
            "multi.parquet",
            recordCount,
            ParquetTestFiles.TINY_ROW_GROUP_SIZE_BYTES);
    return ParquetTestFiles.readAllBytes(multiRowGroupFile);
  }

  private byte[] sliceOfContent(long offset, int length) {
    return Arrays.copyOfRange(content, (int) offset, (int) offset + length);
  }

  private PredictivePrefetchOptimizer createOptimizer(boolean enabled) throws IOException {
    if (optimizer != null) {
      optimizer.onClose();
    }
    GcsPrefetchOptions prefetchOptions = GcsPrefetchOptions.builder().setEnabled(enabled).build();
    if (cacheManager == null) {
      cacheManager =
          new AnalyticsCacheManager(
              GcsCacheOptions.builder().setFooterCacheEnabled(true).build(),
              prefetchOptions,
              telemetry);
    }
    if (channel != null && channel.size() > 0) {
      ByteBuffer footer = ByteBuffer.wrap(channel.sliceContent(0, (int) channel.size()));
      cacheManager.invalidateFooter(ITEM_ID);
      cacheManager.getFooter(ITEM_ID, id -> footer);
    }
    PredictivePrefetchOptimizer createdOptimizer = new PredictivePrefetchOptimizer(prefetchOptions);
    createdOptimizer.onOpen(ITEM_ID, cacheManager);
    return createdOptimizer;
  }
}
