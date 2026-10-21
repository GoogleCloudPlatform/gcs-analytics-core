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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GcsFooterOptimizerTest {

  private static final long MB = 1024L * 1024;
  private static final long GB = 1024L * MB;

  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName("b").setObjectName("test.parquet").build();
  private static final GcsItemInfo ITEM_INFO =
      GcsItemInfo.builder().setItemId(ITEM_ID).setSize(1000).build();
  private static final GcsFileInfo FILE_INFO =
      GcsFileInfo.builder()
          .setItemInfo(ITEM_INFO)
          .setUri(URI.create("gs://b/test.parquet"))
          .setAttributes(ImmutableMap.of())
          .build();
  private static final GcsItemId PINNED_ITEM_ID =
      GcsItemId.builder()
          .setBucketName("b")
          .setObjectName("test.parquet")
          .setContentGeneration(7L)
          .build();
  private static final GcsItemInfo PINNED_ITEM_INFO =
      GcsItemInfo.builder()
          .setItemId(PINNED_ITEM_ID)
          .setSize(1000)
          .setContentGeneration(7L)
          .build();

  private GcsReadOptions readOptions;
  private Telemetry telemetry;
  private AnalyticsCacheManager mockCacheManager;
  private VectoredSeekableByteChannel realSource;
  private GcsFooterOptimizer optimizer;
  private byte[] testData;

  @BeforeEach
  void initializeOptimizerAndFakeStorage() throws IOException {
    readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchEnabled(true)
            .setFooterPrefetchSizeSmallFile(100)
            .setFooterPrefetchSizeLargeFile(500)
            .build();

    telemetry = spy(new Telemetry(ImmutableList.of()));
    mockCacheManager = mock(AnalyticsCacheManager.class);
    optimizer = new GcsFooterOptimizer(readOptions, telemetry);

    GcsClientOptions clientOptions =
        GcsClientOptions.builder().setGcsReadOptions(readOptions).build();
    GcsFileSystemOptions fileSystemOptions =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(clientOptions)
            .setGcsCacheOptions(GcsCacheOptions.builder().build())
            .build();
    FakeGcsFileSystemImpl fakeFileSystem = new FakeGcsFileSystemImpl(fileSystemOptions);

    testData = new byte[1000];
    for (int i = 0; i < 1000; i++) {
      testData[i] = (byte) (i % 256);
    }
    FakeGcsClientImpl.storage.create(
        BlobInfo.newBuilder(ITEM_ID.getBucketName(), ITEM_ID.getObjectName().get(), 1L).build(),
        testData);

    realSource = fakeFileSystem.open(FILE_INFO, readOptions);
  }

  @Test
  void isApplicable_footerPrefetchEnabled_returnsTrue() {
    assertThat(optimizer.isApplicable(ITEM_ID)).isTrue();
  }

  @Test
  void isApplicable_orcFile_returnsTrue() {
    GcsItemId orcItemId = GcsItemId.builder().setBucketName("b").setObjectName("test.orc").build();
    assertThat(optimizer.isApplicable(orcItemId)).isTrue();
  }

  @Test
  void isApplicable_nonParquetFile_returnsFalse() {
    GcsItemId csvItemId = GcsItemId.builder().setBucketName("b").setObjectName("test.csv").build();
    assertThat(optimizer.isApplicable(csvItemId)).isFalse();
  }

  @Test
  public void isApplicable_fileInfo_footerPrefetchEnabled_returnsTrue() {
    assertThat(optimizer.isApplicable(FILE_INFO)).isTrue();
  }

  @Test
  public void isApplicable_fileInfo_nonParquetFile_returnsFalse() {
    GcsItemId csvItemId = GcsItemId.builder().setBucketName("b").setObjectName("test.csv").build();
    GcsItemInfo csvInfo = GcsItemInfo.builder().setItemId(csvItemId).setSize(1000).build();
    GcsFileInfo csvFileInfo = FILE_INFO.toBuilder().setItemInfo(csvInfo).build();
    assertThat(optimizer.isApplicable(csvFileInfo)).isFalse();
  }

  @Test
  public void isApplicable_fileInfo_footerPrefetchDisabled_returnsFalse() {
    readOptions = GcsReadOptions.builder().setFooterPrefetchEnabled(false).build();
    optimizer = new GcsFooterOptimizer(readOptions, telemetry);
    assertThat(optimizer.isApplicable(FILE_INFO)).isFalse();
  }

  @Test
  void isApplicable_footerPrefetchDisabled_returnsFalse() {
    readOptions = GcsReadOptions.builder().setFooterPrefetchEnabled(false).build();
    optimizer = new GcsFooterOptimizer(readOptions, telemetry);
    assertThat(optimizer.isApplicable(ITEM_ID)).isFalse();
  }

  @Test
  void onOpen_withFileInfo_initializesState() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(ByteBuffer.allocate(100));

    int bytesRead = optimizer.read(990, dst, realSource);

    assertThat(bytesRead).isEqualTo(10);
    verify(mockCacheManager).getFooter(eq(ITEM_ID), any());
  }

  @Test
  void onOpen_withItemId_initializesState() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(ByteBuffer.allocate(100));

    int bytesRead = optimizer.read(990, dst, realSource);

    assertThat(bytesRead).isEqualTo(10);
    verify(mockCacheManager).getFooter(eq(ITEM_ID), any());
  }

  @Test
  void read_footerHit_servesFromCache() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer cachedFooter = ByteBuffer.wrap(new byte[100]);
    cachedFooter.put(0, (byte) 42); // position 900
    cachedFooter.put(90, (byte) 99); // position 990
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(cachedFooter);
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(990, dst, realSource);

    assertThat(bytesRead).isEqualTo(10);
    assertThat(dst.array()[0]).isEqualTo((byte) 99);
  }

  @Test
  void read_outsideFooterRange_returnsZero() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);
    int bytesRead = optimizer.read(0, dst, realSource);
    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void read_pastEOF_returnsMinusOne() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesReadEof = optimizer.read(1000, dst, realSource);

    assertThat(bytesReadEof).isEqualTo(-1);
    int bytesReadPastEof = optimizer.read(1010, dst, realSource);
    assertThat(bytesReadPastEof).isEqualTo(-1);
  }

  @Test
  void read_footerMiss_callsLoaderAndCaches() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    realSource.position(500L);
    // Let the loader actually execute to hit the real source
    when(mockCacheManager.getFooter(eq(ITEM_ID), any()))
        .thenAnswer(
            invocation -> {
              AnalyticsCacheManager.FooterLoader loader = invocation.getArgument(1);
              return loader.load(ITEM_ID);
            });
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(990, dst, realSource);

    assertThat(bytesRead).isEqualTo(10);
    assertThat(dst.array()[0]).isEqualTo(testData[990]);
    assertThat(realSource.position()).isEqualTo(500L); // Position should be restored
  }

  @Test
  void read_loaderEncountersUnexpectedEof_throwsIOException() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    // Overwrite with a smaller file to force EOF during prefetch
    FakeGcsClientImpl.storage.create(
        BlobInfo.newBuilder(ITEM_ID.getBucketName(), ITEM_ID.getObjectName().get(), 1L).build(),
        new byte[50]);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any()))
        .thenAnswer(
            invocation -> {
              AnalyticsCacheManager.FooterLoader loader = invocation.getArgument(1);
              return loader.load(ITEM_ID);
            });
    ByteBuffer dst = ByteBuffer.allocate(10);

    assertThrows(IOException.class, () -> optimizer.read(990, dst, realSource));
  }

  @Test
  void read_largeFile_usesLargeFilePrefetchSize() throws IOException {
    long largeSize = 2 * GB;
    GcsItemInfo largeInfo = GcsItemInfo.builder().setItemId(ITEM_ID).setSize(largeSize).build();
    GcsFileInfo largeFile = FILE_INFO.toBuilder().setItemInfo(largeInfo).build();
    // Have to create the file in Fake Storage or else lazy sizing might fail, but wait, lazy sizing
    // is only used if size is -1.
    // If the file is not really 2GB, reading from it will fail if it tries to read that far.
    // We only need to check the Cache Manager call!
    optimizer.onOpen(largeFile, mockCacheManager);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(ByteBuffer.allocate(500));
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(largeSize - 500, dst, realSource);

    assertThat(bytesRead).isEqualTo(10);
    verify(mockCacheManager).getFooter(eq(ITEM_ID), any());
  }

  @Test
  void read_lazyInitFileSize_whenOnOpenWithItemIdUsed() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(ByteBuffer.allocate(100));
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(990, dst, realSource);

    assertThat(bytesRead).isEqualTo(10);
  }

  @Test
  void read_footerPrefetchDisabled_returnsZeroAndDoesNotCache() throws IOException {
    readOptions = GcsReadOptions.builder().setFooterPrefetchEnabled(false).build();
    optimizer = new GcsFooterOptimizer(readOptions, telemetry);
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(990, dst, realSource);

    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void calculatePrefetchSize_largeFile_isCappedByFileSize() {
    long largeSize = 1400 * MB;
    readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchEnabled(true)
            .setFooterPrefetchSizeLargeFile((int) (1500 * MB))
            .build();

    long prefetchSize = GcsFooterOptimizer.calculatePrefetchSize(largeSize, readOptions);

    assertThat(prefetchSize).isEqualTo(largeSize);
  }

  @Test
  void read_smallFile_prefetchSizeIsCappedByFileSize_servesWholeObjectFromFooter()
      throws IOException {
    GcsItemInfo smallInfo = GcsItemInfo.builder().setItemId(ITEM_ID).setSize(50).build();
    optimizer.onOpen(FILE_INFO.toBuilder().setItemInfo(smallInfo).build(), mockCacheManager);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any()))
        .thenAnswer(
            invocation ->
                invocation.getArgument(1, AnalyticsCacheManager.FooterLoader.class).load(ITEM_ID));
    VectoredSeekableByteChannel mockSource = fakeChannel(50, new AtomicInteger(0), smallInfo);
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(0, dst, mockSource);

    assertThat(bytesRead).isEqualTo(10);
    assertThat(dst.array()).isEqualTo(Arrays.copyOfRange(testData, 0, 10));
  }

  @Test
  void read_multipleReads_usesLocalBufferAndRecordsTelemetry() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    realSource.position(500L);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any()))
        .thenAnswer(
            invocation -> {
              AnalyticsCacheManager.FooterLoader loader = invocation.getArgument(1);
              return loader.load(ITEM_ID);
            });
    ByteBuffer dst1 = ByteBuffer.allocate(10);
    ByteBuffer dst2 = ByteBuffer.allocate(10);

    int bytesRead1 = optimizer.read(990, dst1, realSource);
    int bytesRead2 = optimizer.read(980, dst2, realSource);

    assertThat(bytesRead1).isEqualTo(10);
    assertThat(dst1.array()[0]).isEqualTo(testData[990]);
    assertThat(bytesRead2).isEqualTo(10);
    assertThat(dst2.array()[0]).isEqualTo(testData[980]);
    verify(mockCacheManager, times(1)).getFooter(eq(ITEM_ID), any());
    verify(telemetry, times(1)).recordMetric(eq(Metric.FOOTER_CACHE_MISS), eq(1L), any());
    verify(telemetry, times(1)).recordMetric(eq(Metric.FOOTER_PREFETCH_HIT), eq(1L), any());
  }

  @Test
  void read_fileSizeUninitialized_sourceGetItemInfoReturnsNull_returnsZero() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    VectoredSeekableByteChannel mockSource = mock(VectoredSeekableByteChannel.class);
    when(mockSource.getItemInfo()).thenReturn(null);
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(990, dst, mockSource);

    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void read_fileSizeUninitialized_usesChannelItemInfo_doesNotCallSize() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    VectoredSeekableByteChannel mockSource = mock(VectoredSeekableByteChannel.class);
    when(mockSource.getItemInfo()).thenReturn(ITEM_INFO);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(ByteBuffer.allocate(100));
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(990, dst, mockSource);

    assertThat(bytesRead).isEqualTo(10);
    verify(mockSource, times(0)).size();
  }

  @Test
  void read_fileSizeUnknown_speculativeFooterRead_prefetchesFooterAndResolvesSizeWithoutSizeCall()
      throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    when(mockCacheManager.getFooter(eq(PINNED_ITEM_ID), any()))
        .thenAnswer(
            invocation ->
                invocation
                    .getArgument(1, AnalyticsCacheManager.FooterLoader.class)
                    .load(PINNED_ITEM_ID));
    AtomicInteger dataReadCount = new AtomicInteger(0);
    VectoredSeekableByteChannel mockSource = lazyMetadataChannel(dataReadCount);
    ByteBuffer dst = ByteBuffer.allocate(8);

    int bytesRead = optimizer.read(992, dst, mockSource);

    assertThat(bytesRead).isEqualTo(8);
    assertThat(dst.array()).isEqualTo(Arrays.copyOfRange(testData, 992, 1000));
    assertThat(dataReadCount.get()).isEqualTo(1);
    verify(mockSource, times(0)).size();
    verify(mockCacheManager).getFooter(eq(PINNED_ITEM_ID), any());
    assertThat(mockSource.position()).isEqualTo(0);

    ByteBuffer footerStartDst = ByteBuffer.allocate(4);
    int footerStartBytesRead = optimizer.read(900, footerStartDst, mockSource);

    assertThat(footerStartBytesRead).isEqualTo(4);
    assertThat(footerStartDst.array()).isEqualTo(Arrays.copyOfRange(testData, 900, 904));
    assertThat(dataReadCount.get()).isEqualTo(1);
  }

  @Test
  void read_fileSizeUnknown_warmCache_doesNotRecordFooterCacheHit() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    when(mockCacheManager.getFooter(any(), any()))
        .thenReturn(ByteBuffer.wrap(Arrays.copyOfRange(testData, 900, 1000)));
    VectoredSeekableByteChannel mockSource = lazyMetadataChannel(new AtomicInteger(0));

    int unused = optimizer.read(992, ByteBuffer.allocate(8), mockSource);

    verify(telemetry, times(0)).recordMetric(eq(Metric.FOOTER_CACHE_HIT), anyLong(), any());
  }

  @Test
  void read_fileSizeUnknown_readTooLargeForFooter_passesThroughToDelegate() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    AtomicInteger dataReadCount = new AtomicInteger(0);
    VectoredSeekableByteChannel mockSource = lazyMetadataChannel(dataReadCount);
    ByteBuffer dst = ByteBuffer.allocate(600);

    int bytesRead = optimizer.read(200, dst, mockSource);

    assertThat(bytesRead).isEqualTo(0);
    assertThat(dataReadCount.get()).isEqualTo(0);
    verify(mockSource, times(0)).size();
  }

  @Test
  void read_fileSizeUnknown_readAtStartOfObject_passesThroughToDelegate() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    AtomicInteger dataReadCount = new AtomicInteger(0);
    VectoredSeekableByteChannel mockSource = lazyMetadataChannel(dataReadCount);
    ByteBuffer dst = ByteBuffer.allocate(8);

    int bytesRead = optimizer.read(0, dst, mockSource);

    assertThat(bytesRead).isEqualTo(0);
    assertThat(dataReadCount.get()).isEqualTo(0);
    verify(mockSource, times(0)).size();
  }

  @Test
  void read_metadataNeverResolves_fallsBackToChannelSize_andReusesPrefetchedBytes()
      throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    AtomicInteger dataReadCount = new AtomicInteger(0);
    VectoredSeekableByteChannel mockSource =
        fakeChannel(testData.length, dataReadCount, /* itemInfo= */ null);
    when(mockSource.size()).thenReturn((long) testData.length);

    int firstBytesRead = optimizer.read(992, ByteBuffer.allocate(8), mockSource);
    int secondBytesRead = optimizer.read(992, ByteBuffer.allocate(8), mockSource);
    int thirdBytesRead = optimizer.read(992, ByteBuffer.allocate(8), mockSource);

    assertThat(firstBytesRead).isEqualTo(8);
    assertThat(secondBytesRead).isEqualTo(8);
    assertThat(thirdBytesRead).isEqualTo(8);
    assertThat(dataReadCount.get()).isEqualTo(1);
    verify(mockSource, times(1)).size();
  }

  @Test
  void read_speculativeWindowNotAtEndOfObject_laterFooterReadIsNotShort() throws IOException {
    GcsItemInfo largeItemInfo = GcsItemInfo.builder().setItemId(ITEM_ID).setSize(5000).build();
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any()))
        .thenAnswer(
            invocation ->
                invocation.getArgument(1, AnalyticsCacheManager.FooterLoader.class).load(ITEM_ID));
    AtomicInteger dataReadCount = new AtomicInteger(0);
    VectoredSeekableByteChannel mockSource = fakeChannel(5000, dataReadCount, largeItemInfo);
    int unused = optimizer.read(4450, ByteBuffer.allocate(8), mockSource);

    ByteBuffer dst = ByteBuffer.allocate(100);
    int bytesRead = optimizer.read(4900, dst, mockSource);

    assertThat(bytesRead).isEqualTo(100);
    assertThat(dst.array()).isEqualTo(bytesAt(4900, 100));
    assertThat(dataReadCount.get()).isEqualTo(2);
  }

  @Test
  void read_fileSizeUnknown_objectLargerThanSpeculativeWindow_servesReadFromPrefetchedBytes()
      throws IOException {
    GcsItemInfo largeItemInfo =
        GcsItemInfo.builder()
            .setItemId(PINNED_ITEM_ID)
            .setSize(5000)
            .setContentGeneration(7L)
            .build();
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    AtomicInteger dataReadCount = new AtomicInteger(0);
    VectoredSeekableByteChannel mockSource = fakeChannel(5000, dataReadCount, largeItemInfo);
    ByteBuffer dst = ByteBuffer.allocate(8);

    int bytesRead = optimizer.read(4450, dst, mockSource);

    assertThat(bytesRead).isEqualTo(8);
    assertThat(dst.array()).isEqualTo(bytesAt(4450, 8));
    verify(mockCacheManager, never()).getFooter(any(), any());
  }

  @Test
  void read_fileSizeUnknown_largeFile_retainsPrefetchedTailWhenPrefetchSizeExceedsWindow()
      throws IOException {
    long largeSize = 2 * GB;
    GcsItemInfo largeItemInfo =
        GcsItemInfo.builder()
            .setItemId(PINNED_ITEM_ID)
            .setSize(largeSize)
            .setContentGeneration(7L)
            .build();
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    AtomicInteger dataReadCount = new AtomicInteger(0);
    VectoredSeekableByteChannel mockSource = fakeChannel(largeSize, dataReadCount, largeItemInfo);
    int unused = optimizer.read(largeSize - 8, ByteBuffer.allocate(8), mockSource);

    ByteBuffer dst = ByteBuffer.allocate(40);
    int bytesRead = optimizer.read(largeSize - 48, dst, mockSource);

    assertThat(bytesRead).isEqualTo(40);
    assertThat(dst.array()).isEqualTo(bytesAt(largeSize - 48, 40));
    assertThat(dataReadCount.get()).isEqualTo(1);
    verify(mockCacheManager, never()).getFooter(any(), any());
  }

  @Test
  void read_fileSizeUnknown_resolvedItemInfoWithoutGeneration_keepsFooterLocal()
      throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    AtomicInteger dataReadCount = new AtomicInteger(0);
    VectoredSeekableByteChannel mockSource = fakeChannel(testData.length, dataReadCount, ITEM_INFO);

    int bytesRead = optimizer.read(992, ByteBuffer.allocate(8), mockSource);
    int secondBytesRead = optimizer.read(900, ByteBuffer.allocate(4), mockSource);

    assertThat(bytesRead).isEqualTo(8);
    assertThat(secondBytesRead).isEqualTo(4);
    assertThat(dataReadCount.get()).isEqualTo(1);
    verify(mockCacheManager, never()).getFooter(any(), any());
  }

  @Test
  void read_fileSizeUnknown_speculativeWindow_isSizedBySmallFilePrefetchSize() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    when(mockCacheManager.getFooter(eq(PINNED_ITEM_ID), any()))
        .thenAnswer(
            invocation ->
                invocation
                    .getArgument(1, AnalyticsCacheManager.FooterLoader.class)
                    .load(PINNED_ITEM_ID));
    VectoredSeekableByteChannel mockSource = lazyMetadataChannel(new AtomicInteger(0));

    int unused = optimizer.read(992, ByteBuffer.allocate(8), mockSource);

    verify(mockSource).position(892L);
  }

  @Test
  void read_cachedFooterShorterThanPrefetchSize_servesFromTheCorrectOffset() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer cachedFooter = ByteBuffer.wrap(Arrays.copyOfRange(testData, 960, 1000));
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(cachedFooter);
    VectoredSeekableByteChannel mockSource = mock(VectoredSeekableByteChannel.class);
    ByteBuffer dst = ByteBuffer.allocate(8);

    int bytesRead = optimizer.read(992, dst, mockSource);

    assertThat(bytesRead).isEqualTo(8);
    assertThat(dst.array()).isEqualTo(Arrays.copyOfRange(testData, 992, 1000));
  }

  @Test
  void read_cachedFooterDoesNotCoverPosition_passesThroughToDelegate() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer cachedFooter = ByteBuffer.wrap(Arrays.copyOfRange(testData, 960, 1000));
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(cachedFooter);
    VectoredSeekableByteChannel mockSource = mock(VectoredSeekableByteChannel.class);
    ByteBuffer dst = ByteBuffer.allocate(8);

    int bytesRead = optimizer.read(920, dst, mockSource);

    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void read_cachedFooterDoesNotCoverPosition_doesNotRecordFooterCacheHit() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer cachedFooter = ByteBuffer.wrap(Arrays.copyOfRange(testData, 960, 1000));
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(cachedFooter);
    VectoredSeekableByteChannel mockSource = mock(VectoredSeekableByteChannel.class);

    int unused = optimizer.read(920, ByteBuffer.allocate(8), mockSource);

    verify(telemetry, never()).recordMetric(eq(Metric.FOOTER_CACHE_HIT), anyLong(), any());
  }

  @Test
  void read_fileSizeUnknown_positionBeyondEndOfObject_resolvesSizeAndReturnsMinusOne()
      throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    AtomicInteger dataReadCount = new AtomicInteger(0);
    VectoredSeekableByteChannel mockSource = lazyMetadataChannel(dataReadCount);
    ByteBuffer dst = ByteBuffer.allocate(8);

    int bytesRead = optimizer.read(1050, dst, mockSource);

    assertThat(bytesRead).isEqualTo(-1);
    assertThat(dataReadCount.get()).isEqualTo(1);
  }

  @Test
  void read_fileSizeUnknown_readAtEndOfObject_returnsMinusOneAndKeepsFooterForLaterReads()
      throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    when(mockCacheManager.getFooter(eq(PINNED_ITEM_ID), any()))
        .thenAnswer(
            invocation ->
                invocation
                    .getArgument(1, AnalyticsCacheManager.FooterLoader.class)
                    .load(PINNED_ITEM_ID));
    AtomicInteger dataReadCount = new AtomicInteger(0);
    VectoredSeekableByteChannel mockSource = lazyMetadataChannel(dataReadCount);

    int eofBytesRead = optimizer.read(1000, ByteBuffer.allocate(8), mockSource);
    int footerBytesRead = optimizer.read(990, ByteBuffer.allocate(10), mockSource);

    assertThat(eofBytesRead).isEqualTo(-1);
    assertThat(footerBytesRead).isEqualTo(10);
    assertThat(dataReadCount.get()).isEqualTo(1);
  }

  @Test
  void read_fileSizeUnknown_speculativeReadBeyondEndOfObject_metadataUnresolved_passesThrough()
      throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    AtomicInteger dataReadCount = new AtomicInteger(0);
    VectoredSeekableByteChannel mockSource =
        fakeChannel(testData.length, dataReadCount, /* itemInfo= */ null);
    ByteBuffer dst = ByteBuffer.allocate(8);

    int bytesRead = optimizer.read(1200, dst, mockSource);

    assertThat(bytesRead).isEqualTo(0);
    assertThat(dataReadCount.get()).isEqualTo(0);
  }

  @Test
  void read_itemInfoResolvedBySizeCall_usesGenerationPinnedItemIdAsCacheKey() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    when(mockCacheManager.getFooter(any(), any()))
        .thenAnswer(
            invocation ->
                invocation
                    .getArgument(1, AnalyticsCacheManager.FooterLoader.class)
                    .load(invocation.getArgument(0)));
    AtomicBoolean sizeCalled = new AtomicBoolean(false);
    VectoredSeekableByteChannel mockSource =
        fakeChannel(testData.length, new AtomicInteger(0), /* itemInfo= */ null);
    when(mockSource.size())
        .thenAnswer(
            invocation -> {
              sizeCalled.set(true);
              return (long) testData.length;
            });
    when(mockSource.getItemInfo())
        .thenAnswer(invocation -> sizeCalled.get() ? PINNED_ITEM_INFO : null);
    int unused = optimizer.read(992, ByteBuffer.allocate(8), mockSource);

    int bytesRead = optimizer.read(992, ByteBuffer.allocate(8), mockSource);

    assertThat(bytesRead).isEqualTo(8);
    verify(mockCacheManager).getFooter(eq(PINNED_ITEM_ID), any());
  }

  @Test
  void read_footerLoadHitsEofBeforePrefetchSize_throwsIOException() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any()))
        .thenAnswer(
            invocation ->
                invocation.getArgument(1, AnalyticsCacheManager.FooterLoader.class).load(ITEM_ID));
    VectoredSeekableByteChannel mockSource = mock(VectoredSeekableByteChannel.class);
    when(mockSource.read(any(ByteBuffer.class))).thenReturn(-1);
    ByteBuffer dst = ByteBuffer.allocate(10);

    IOException e = assertThrows(IOException.class, () -> optimizer.read(990, dst, mockSource));

    assertThat(e).hasMessageThat().contains("Unexpected EOF");
  }

  @Test
  void read_footerPrefetchDisabled_fileSizeUnknown_doesNotSpeculativelyRead() throws IOException {
    readOptions = GcsReadOptions.builder().setFooterPrefetchEnabled(false).build();
    optimizer = new GcsFooterOptimizer(readOptions, telemetry);
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    AtomicInteger dataReadCount = new AtomicInteger(0);
    VectoredSeekableByteChannel mockSource = lazyMetadataChannel(dataReadCount);

    int bytesRead = optimizer.read(992, ByteBuffer.allocate(8), mockSource);

    assertThat(bytesRead).isEqualTo(0);
    assertThat(dataReadCount.get()).isEqualTo(0);
  }

  @Test
  void read_fileSizeUnknown_emptyBuffer_doesNotSpeculativelyRead() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    AtomicInteger dataReadCount = new AtomicInteger(0);
    VectoredSeekableByteChannel mockSource = lazyMetadataChannel(dataReadCount);

    int bytesRead = optimizer.read(992, ByteBuffer.allocate(0), mockSource);

    assertThat(bytesRead).isEqualTo(0);
    assertThat(dataReadCount.get()).isEqualTo(0);
  }

  @Test
  void readVectored_mixedRanges_servesWindowRangesFromCacheAndReturnsRestInOrder()
      throws Exception {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    cacheFooter(cachedFooter());
    GcsObjectRange dataRange = range(0, 10);
    GcsObjectRange tailRange = range(990, 10);
    GcsObjectRange straddlingRange = range(850, 100);
    GcsObjectRange footerStartRange = range(900, 50);

    List<GcsObjectRange> remaining =
        optimizer.readVectored(
            ImmutableList.of(dataRange, tailRange, straddlingRange, footerStartRange),
            ByteBuffer::allocate);

    assertThat(remaining).containsExactly(dataRange, straddlingRange).inOrder();
    assertThat(toBytes(tailRange.getByteBufferFuture().get())).isEqualTo(bytesAt(990, 10));
    assertThat(toBytes(footerStartRange.getByteBufferFuture().get())).isEqualTo(bytesAt(900, 50));
    assertThat(dataRange.getByteBufferFuture().isDone()).isFalse();
    assertThat(straddlingRange.getByteBufferFuture().isDone()).isFalse();
    verify(telemetry, times(1)).recordMetric(eq(Metric.FOOTER_CACHE_HIT), eq(2L), any());
  }

  @Test
  void readVectored_rangesOutsideFooterWindow_returnedUntouchedWithoutCacheLookup()
      throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    List<GcsObjectRange> ranges =
        ImmutableList.of(range(0, 10), range(850, 100), range(995, 10), range(1000, 10));

    List<GcsObjectRange> remaining = optimizer.readVectored(ranges, ByteBuffer::allocate);

    assertThat(remaining).isSameInstanceAs(ranges);
    assertThat(ranges.stream().noneMatch(r -> r.getByteBufferFuture().isDone())).isTrue();
    verify(mockCacheManager, never()).getFooterIfPresent(any());
  }

  @Test
  void readVectored_fileSizeUnknown_returnsRangesUntouched() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    List<GcsObjectRange> ranges = ImmutableList.of(range(990, 10));

    List<GcsObjectRange> remaining = optimizer.readVectored(ranges, ByteBuffer::allocate);

    assertThat(remaining).isSameInstanceAs(ranges);
    verify(mockCacheManager, never()).getFooterIfPresent(any());
  }

  @Test
  void readVectored_footerNotCached_returnsRangesUntouchedWithoutLoading() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager); // unstubbed probe returns Optional.empty()
    List<GcsObjectRange> ranges = ImmutableList.of(range(990, 10), range(0, 10));

    List<GcsObjectRange> remaining = optimizer.readVectored(ranges, ByteBuffer::allocate);

    assertThat(remaining).isSameInstanceAs(ranges);
    assertThat(ranges.stream().noneMatch(r -> r.getByteBufferFuture().isDone())).isTrue();
    verify(mockCacheManager, never()).getFooter(any(), any());
  }

  @Test
  void readVectored_allocatorReturnsNullOrUndersizedBuffer_failsRangesWithoutCountingHits()
      throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    cacheFooter(cachedFooter());
    GcsObjectRange nullBufferRange = range(990, 10);
    GcsObjectRange undersizedBufferRange = range(950, 20);
    List<GcsObjectRange> ranges = ImmutableList.of(nullBufferRange, undersizedBufferRange);

    List<GcsObjectRange> remaining =
        optimizer.readVectored(ranges, size -> size == 10 ? null : ByteBuffer.allocate(4));

    assertThat(remaining).isEmpty();
    ExecutionException nullFailure =
        assertThrows(ExecutionException.class, () -> nullBufferRange.getByteBufferFuture().get());
    assertThat(nullFailure).hasCauseThat().hasMessageThat().contains("returned null");
    ExecutionException undersizedFailure =
        assertThrows(
            ExecutionException.class, () -> undersizedBufferRange.getByteBufferFuture().get());
    assertThat(undersizedFailure).hasCauseThat().hasMessageThat().contains("too small");
    verify(telemetry, never()).recordMetric(eq(Metric.FOOTER_CACHE_HIT), anyLong(), any());
  }

  @Test
  void readVectored_allocatorReturnsOversizedBuffer_completesWithExactlyRangeLength()
      throws Exception {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    cacheFooter(cachedFooter());
    GcsObjectRange r = range(920, 10);

    List<GcsObjectRange> remaining =
        optimizer.readVectored(ImmutableList.of(r), n -> ByteBuffer.allocate(n + 16).position(4));

    assertThat(remaining).isEmpty();
    ByteBuffer completed = r.getByteBufferFuture().get();
    assertThat(completed.position()).isEqualTo(4);
    assertThat(toBytes(completed)).isEqualTo(bytesAt(920, 10));
  }

  @Test
  void readVectored_allocatorThrowsForOneRange_failsThatRangeAndStillServesOthers()
      throws Exception {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    cacheFooter(cachedFooter());
    RuntimeException allocationFailure = new RuntimeException("allocator exploded");
    AtomicBoolean firstAllocation = new AtomicBoolean(true);
    GcsObjectRange failingRange = range(990, 10);
    GcsObjectRange servedRange = range(950, 20);
    GcsObjectRange dataRange = range(0, 10);

    List<GcsObjectRange> remaining =
        optimizer.readVectored(
            ImmutableList.of(failingRange, servedRange, dataRange),
            size -> {
              if (firstAllocation.getAndSet(false)) {
                throw allocationFailure;
              }
              return ByteBuffer.allocate(size);
            });

    assertThat(remaining).containsExactly(dataRange);
    ExecutionException e =
        assertThrows(ExecutionException.class, () -> failingRange.getByteBufferFuture().get());
    assertThat(e).hasCauseThat().isSameInstanceAs(allocationFailure);
    assertThat(toBytes(servedRange.getByteBufferFuture().get())).isEqualTo(bytesAt(950, 20));
    verify(telemetry, times(1)).recordMetric(eq(Metric.FOOTER_CACHE_HIT), eq(1L), any());
  }

  @Test
  void readVectored_footerAlreadyLocal_servesWithoutCacheLookupAndRecordsPrefetchHit()
      throws Exception {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any()))
        .thenAnswer(
            invocation ->
                invocation.getArgument(1, AnalyticsCacheManager.FooterLoader.class).load(ITEM_ID));
    int unusedBytesRead = optimizer.read(990, ByteBuffer.allocate(10), realSource);
    GcsObjectRange pageIndexRange = range(920, 30);

    List<GcsObjectRange> remaining =
        optimizer.readVectored(ImmutableList.of(pageIndexRange), ByteBuffer::allocate);

    assertThat(remaining).isEmpty();
    assertThat(toBytes(pageIndexRange.getByteBufferFuture().get())).isEqualTo(bytesAt(920, 30));
    verify(mockCacheManager, never()).getFooterIfPresent(any());
    verify(telemetry, times(1)).recordMetric(eq(Metric.FOOTER_PREFETCH_HIT), eq(1L), any());
  }

  @Test
  void readVectored_zeroLengthRangeInFooterWindow_isLeftToDelegate() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    cacheFooter(cachedFooter());
    GcsObjectRange emptyRange = range(990, 0);

    List<GcsObjectRange> remaining =
        optimizer.readVectored(ImmutableList.of(emptyRange, range(995, 5)), ByteBuffer::allocate);

    assertThat(remaining).containsExactly(emptyRange);
    assertThat(emptyRange.getByteBufferFuture().isDone()).isFalse();
  }

  @Test
  void readVectored_cachedFooterShorterThanPrefetchSize_servesOnlyCoveredRanges()
      throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    cacheFooter(ByteBuffer.wrap(Arrays.copyOfRange(testData, 960, 1000)));
    GcsObjectRange coveredRange = range(990, 10);
    GcsObjectRange uncoveredRange = range(950, 20);

    List<GcsObjectRange> remaining =
        optimizer.readVectored(
            ImmutableList.of(coveredRange, uncoveredRange), ByteBuffer::allocate);

    assertThat(remaining).containsExactly(uncoveredRange);
    assertThat(coveredRange.getByteBufferFuture().isDone()).isTrue();
  }

  @Test
  void readVectored_cachedFooterCoversNoWindowRange_returnsSameListWithoutMetric()
      throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    cacheFooter(ByteBuffer.wrap(Arrays.copyOfRange(testData, 960, 1000)));
    List<GcsObjectRange> ranges = ImmutableList.of(range(950, 20), range(0, 10));

    List<GcsObjectRange> remaining = optimizer.readVectored(ranges, ByteBuffer::allocate);

    assertThat(remaining).isSameInstanceAs(ranges);
    assertThat(ranges.stream().noneMatch(r -> r.getByteBufferFuture().isDone())).isTrue();
    verify(telemetry, never()).recordMetric(eq(Metric.FOOTER_CACHE_HIT), anyLong(), any());
  }

  private static GcsObjectRange range(long offset, int length) {
    return GcsObjectRange.builder()
        .setOffset(offset)
        .setLength(length)
        .setByteBufferFuture(new CompletableFuture<>())
        .build();
  }

  private void cacheFooter(ByteBuffer footer) {
    when(mockCacheManager.getFooterIfPresent(ITEM_ID)).thenReturn(Optional.of(footer));
  }

  private ByteBuffer cachedFooter() {
    return ByteBuffer.wrap(Arrays.copyOfRange(testData, 900, 1000));
  }

  private static byte[] toBytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }

  private VectoredSeekableByteChannel lazyMetadataChannel(AtomicInteger dataReadCount)
      throws IOException {
    return fakeChannel(testData.length, dataReadCount, PINNED_ITEM_INFO);
  }

  private VectoredSeekableByteChannel fakeChannel(
      long size, AtomicInteger dataReadCount, GcsItemInfo itemInfo) throws IOException {
    long[] channelPosition = {0};
    VectoredSeekableByteChannel mockSource = mock(VectoredSeekableByteChannel.class);
    when(mockSource.position()).thenAnswer(invocation -> channelPosition[0]);
    when(mockSource.position(anyLong()))
        .thenAnswer(
            invocation -> {
              channelPosition[0] = invocation.getArgument(0);
              return mockSource;
            });
    when(mockSource.getItemInfo())
        .thenAnswer(invocation -> dataReadCount.get() > 0 ? itemInfo : null);
    when(mockSource.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer dst = invocation.getArgument(0);
              long startPosition = channelPosition[0];
              int length = (int) Math.min(dst.remaining(), size - startPosition);
              if (length <= 0) {
                return -1;
              }
              dataReadCount.incrementAndGet();
              dst.put(bytesAt(startPosition, length));
              channelPosition[0] = startPosition + length;
              return length;
            });

    return mockSource;
  }

  private static byte[] bytesAt(long position, int length) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) ((position + i) % 256);
    }
    return bytes;
  }
}
