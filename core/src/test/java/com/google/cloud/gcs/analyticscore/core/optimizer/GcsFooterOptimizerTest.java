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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GcsFooterOptimizerTest {

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

  private static final long MB = 1024L * 1024;
  private static final long GB = 1024L * MB;

  private GcsReadOptions readOptions;
  private Telemetry mockTelemetry;
  private AnalyticsCacheManager mockCacheManager;
  private VectoredSeekableByteChannel mockSource;
  private GcsFooterOptimizer optimizer;

  @BeforeEach
  void setUp() {
    readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchEnabled(true)
            .setFooterPrefetchSizeSmallFile(100)
            .setFooterPrefetchSizeLargeFile(500)
            .setSmallObjectCacheSize(0)
            .build();
    mockTelemetry = mock(Telemetry.class);
    mockCacheManager = mock(AnalyticsCacheManager.class);
    mockSource = mock(VectoredSeekableByteChannel.class);
    optimizer = new GcsFooterOptimizer(readOptions, mockTelemetry);
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
    optimizer = new GcsFooterOptimizer(readOptions, mockTelemetry);
    assertThat(optimizer.isApplicable(FILE_INFO)).isFalse();
  }

  @Test
  void isApplicable_footerPrefetchDisabled_returnsFalse() {
    readOptions = GcsReadOptions.builder().setFooterPrefetchEnabled(false).build();
    optimizer = new GcsFooterOptimizer(readOptions, mockTelemetry);
    assertThat(optimizer.isApplicable(ITEM_ID)).isFalse();
  }

  @Test
  void onOpen_itemIdOnly_setsItemIdAndCacheManager() {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    // Verified via read() call
  }

  @Test
  void read_footerHit_servesFromCache() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer cachedFooter = ByteBuffer.wrap(new byte[100]);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(cachedFooter);
    ByteBuffer dst = ByteBuffer.allocate(10);
    // Read last 10 bytes (position 990 to 1000)

    int bytesRead = optimizer.read(990, dst, mockSource);

    assertThat(bytesRead).isEqualTo(10);
    assertThat(dst.position()).isEqualTo(10);
  }

  @Test
  void read_outsideFooterRange_returnsZero() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);
    // Read at position 0, which is far from the 100-byte footer at 900-1000

    int bytesRead = optimizer.read(0, dst, mockSource);

    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void read_pastEOF_returnsMinusOne() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);
    // Read at position 1000 (EOF)

    int bytesReadEof = optimizer.read(1000, dst, mockSource);

    assertThat(bytesReadEof).isEqualTo(-1);
    // Read past position 1000
    int bytesReadPastEof = optimizer.read(1010, dst, mockSource);
    assertThat(bytesReadPastEof).isEqualTo(-1);
  }

  @Test
  void read_footerMiss_callsLoaderAndCaches() throws IOException {
    // Arrange
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    when(mockSource.size()).thenReturn(1000L);
    when(mockSource.position()).thenReturn(500L);
    when(mockSource.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer bb = invocation.getArgument(0);

              // Act
              int remaining = bb.remaining();
              bb.position(bb.position() + remaining);
              return remaining;
            });
    // We need to capture the loader and call it to verify the source interactions
    when(mockCacheManager.getFooter(eq(ITEM_ID), any()))
        .thenAnswer(
            invocation -> {
              AnalyticsCacheManager.FooterLoader loader = invocation.getArgument(1);
              return loader.load(ITEM_ID);
            });
    ByteBuffer dst = ByteBuffer.allocate(10);
    optimizer.read(990, dst, mockSource);

    // Assert
    verify(mockSource).position(900L); // Start of 100-byte footer
    verify(mockSource).position(500L); // Restored original position
  }

  @Test
  void read_loaderEncountersUnexpectedEof_throwsIOException() throws IOException {
    // Arrange
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    when(mockSource.size()).thenReturn(1000L);
    when(mockSource.read(any(ByteBuffer.class))).thenReturn(-1); // Unexpected EOF
    when(mockCacheManager.getFooter(eq(ITEM_ID), any()))
        .thenAnswer(
            invocation -> {
              AnalyticsCacheManager.FooterLoader loader = invocation.getArgument(1);
              return loader.load(ITEM_ID);
            });
    ByteBuffer dst = ByteBuffer.allocate(10);

    // Act
    assertThrows(IOException.class, () -> optimizer.read(990, dst, mockSource));
  }

  @Test
  void read_largeFile_usesLargeFilePrefetchSize() throws IOException {
    // Arrange
    long largeSize = 2 * GB;
    GcsItemInfo largeInfo = GcsItemInfo.builder().setItemId(ITEM_ID).setSize(largeSize).build();
    GcsFileInfo largeFile = FILE_INFO.toBuilder().setItemInfo(largeInfo).build();
    optimizer.onOpen(largeFile, mockCacheManager);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(ByteBuffer.allocate(500));
    ByteBuffer dst = ByteBuffer.allocate(10);
    // Read at (largeSize - 500)

    // Act
    int bytesRead = optimizer.read(largeSize - 500, dst, mockSource);

    // Assert
    assertThat(bytesRead).isEqualTo(10);
    verify(mockCacheManager).getFooter(eq(ITEM_ID), any());
  }

  @Test
  void read_lazyInitPrefetchSize_whenOnOpenWithItemIdUsed() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    when(mockSource.size()).thenReturn(1000L);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(ByteBuffer.allocate(100));
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(990, dst, mockSource);

    assertThat(bytesRead).isEqualTo(10);
    verify(mockSource, times(1)).size();
  }

  @Test
  void read_footerPrefetchDisabled_returnsZeroAndDoesNotCache() throws IOException {
    readOptions = GcsReadOptions.builder().setFooterPrefetchEnabled(false).build();
    optimizer = new GcsFooterOptimizer(readOptions, mockTelemetry);
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    // Should return 0 because prefetchSize is 0
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(990, dst, mockSource);

    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void read_largeFile_prefetchSizeIsCappedByFileSize() throws IOException {
    // Arrange
    // Large file threshold is 1GB. We set prefetch size to 1.5GB to verify Math.min logic.
    long largeSize = 1400 * MB;
    readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchEnabled(true)
            .setFooterPrefetchSizeLargeFile((int) (1500 * MB))
            .build();
    GcsItemInfo largeInfo = GcsItemInfo.builder().setItemId(ITEM_ID).setSize(largeSize).build();
    GcsFileInfo largeFile = FILE_INFO.toBuilder().setItemInfo(largeInfo).build();
    optimizer = new GcsFooterOptimizer(readOptions, mockTelemetry);
    optimizer.onOpen(largeFile, mockCacheManager);
    // The prefetch size should be min(2GB, 1.5GB) = 1.5GB (fileSize)
    // So position 0 should be within prefetch range
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(ByteBuffer.allocate(100));
    ByteBuffer dst = ByteBuffer.allocate(10);

    // Act
    int bytesRead = optimizer.read(0, dst, mockSource);

    // Assert
    assertThat(bytesRead).isEqualTo(10);
  }
}
