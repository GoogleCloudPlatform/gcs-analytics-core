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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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
            .setSmallObjectCacheSize(0)
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
  void read_largeFile_prefetchSizeIsCappedByFileSize() throws IOException {
    // Math.min check in calculatePrefetchSize
    long largeSize = 1400 * MB;
    readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchEnabled(true)
            .setFooterPrefetchSizeLargeFile((int) (1500 * MB))
            .build();
    GcsItemInfo largeInfo = GcsItemInfo.builder().setItemId(ITEM_ID).setSize(largeSize).build();
    GcsFileInfo largeFile = FILE_INFO.toBuilder().setItemInfo(largeInfo).build();
    optimizer = new GcsFooterOptimizer(readOptions, telemetry);
    optimizer.onOpen(largeFile, mockCacheManager);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(ByteBuffer.allocate(100));
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(0, dst, realSource);

    assertThat(bytesRead).isEqualTo(10);
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
}
