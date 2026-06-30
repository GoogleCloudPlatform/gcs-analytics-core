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
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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

  private Telemetry telemetry;
  private VectoredSeekableByteChannel realSource;
  private SmallObjectOptimizer optimizer;
  private AnalyticsCacheManager cacheManager;

  @BeforeEach
  void initializeOptimizerAndFakeStorage() throws IOException {
    GcsCacheOptions cacheOptions =
        GcsCacheOptions.builder()
            .setSmallObjectCacheEnabled(true)
            .setSmallObjectCacheMaxSizeBytes(200)
            .build();
    cacheManager = new AnalyticsCacheManager(cacheOptions);
    telemetry = new Telemetry(ImmutableList.of());

    GcsReadOptions readOptions =
        GcsReadOptions.builder().setSmallObjectCacheThresholdBytes(200).build();
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
}
