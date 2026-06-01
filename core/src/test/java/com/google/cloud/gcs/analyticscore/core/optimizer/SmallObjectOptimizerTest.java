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

package com.google.cloud.gcs.analyticscore.core.optimizer;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
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
      GcsItemId.builder().setBucketName("b").setObjectName("test.csv").build();
  private static final GcsItemInfo ITEM_INFO =
      GcsItemInfo.builder().setItemId(ITEM_ID).setSize(100).build();
  private static final GcsFileInfo FILE_INFO =
      GcsFileInfo.builder()
          .setItemInfo(ITEM_INFO)
          .setUri(URI.create("gs://b/test.csv"))
          .setAttributes(ImmutableMap.of())
          .build();

  private GcsReadOptions readOptions;
  private Telemetry mockTelemetry;
  private VectoredSeekableByteChannel mockSource;
  private SmallObjectOptimizer optimizer;

  @BeforeEach
  void setUp() {
    readOptions = GcsReadOptions.builder().setSmallObjectCacheSize(200).build();
    mockTelemetry = mock(Telemetry.class);
    mockSource = mock(VectoredSeekableByteChannel.class);
    optimizer = new SmallObjectOptimizer(readOptions, mockTelemetry);
  }

  @Test
  void isApplicable_fileInfo_smallFile_returnsTrue() {
    assertThat(optimizer.isApplicable(FILE_INFO)).isTrue();
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
  void isApplicable_itemId_returnsTrueIfCacheEnabled() {
    assertThat(optimizer.isApplicable(ITEM_ID)).isTrue();

    readOptions = GcsReadOptions.builder().setSmallObjectCacheSize(0).build();
    optimizer = new SmallObjectOptimizer(readOptions, mockTelemetry);
    assertThat(optimizer.isApplicable(ITEM_ID)).isFalse();
  }

  @Test
  void onOpen_itemIdOnly_isNoOp() {
    optimizer.onOpen(ITEM_ID, null);
  }

  @Test
  void read_smallFile_cachesAndServes() throws IOException {
    optimizer.onOpen(FILE_INFO, null);
    when(mockSource.position()).thenReturn(50L);
    when(mockSource.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer bb = invocation.getArgument(0);
              int remaining = bb.remaining();
              bb.position(bb.position() + remaining);
              return remaining;
            });
    ByteBuffer dst = ByteBuffer.allocate(10);

    // Act
    int bytesRead = optimizer.read(10, dst, mockSource);

    // Assert
    assertThat(bytesRead).isEqualTo(10);
    verify(mockSource).position(0); // Cache miss triggers read from 0
    verify(mockSource).position(50L); // Restored original position

    // Second read should be from cache
    dst.clear();
    int secondBytesRead = optimizer.read(20, dst, mockSource);
    assertThat(secondBytesRead).isEqualTo(10);
    verify(mockSource, times(1)).read(any()); // Only one read from source
  }

  @Test
  void read_largeFile_returnsZero() throws IOException {
    readOptions = GcsReadOptions.builder().setSmallObjectCacheSize(50).build();
    optimizer = new SmallObjectOptimizer(readOptions, mockTelemetry);
    optimizer.onOpen(FILE_INFO, null);
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(0, dst, mockSource);

    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void read_pastEOF_returnsMinusOne() throws IOException {
    optimizer.onOpen(FILE_INFO, null);
    ByteBuffer dst = ByteBuffer.allocate(10);

    // Read at position 100 (EOF)
    int bytesReadEof = optimizer.read(100, dst, mockSource);
    assertThat(bytesReadEof).isEqualTo(-1);

    // Read past position 100
    int bytesReadPastEof = optimizer.read(110, dst, mockSource);
    assertThat(bytesReadPastEof).isEqualTo(-1);
  }

  @Test
  void read_lazyInitFileSize_whenOnOpenWithItemIdUsed() throws IOException {
    optimizer.onOpen(ITEM_ID, null);
    when(mockSource.size()).thenReturn(100L);
    when(mockSource.position()).thenReturn(0L);
    when(mockSource.read(any(ByteBuffer.class)))
        .thenAnswer(
            inv -> {
              ByteBuffer bb = inv.getArgument(0);
              bb.position(bb.limit());
              return 100;
            });

    ByteBuffer dst = ByteBuffer.allocate(10);
    int bytesRead = optimizer.read(10, dst, mockSource);

    assertThat(bytesRead).isEqualTo(10);
    verify(mockSource).size();
  }

  @Test
  void readVectored_pastEOF_completesWithEOFException() throws IOException {
    optimizer.onOpen(FILE_INFO, null);
    // Trigger prefetch
    when(mockSource.position()).thenReturn(0L);
    when(mockSource.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer bb = invocation.getArgument(0);
              bb.position(bb.limit());
              return 100;
            });
    optimizer.read(0, ByteBuffer.allocate(10), mockSource);

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
    readOptions = GcsReadOptions.builder().setSmallObjectCacheSize(50).build();
    optimizer = new SmallObjectOptimizer(readOptions, mockTelemetry);
    optimizer.onOpen(FILE_INFO, null);

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
  void readVectored_notYetPrefetched_returnsOriginalRanges() throws IOException {
    optimizer.onOpen(FILE_INFO, null);

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
}
