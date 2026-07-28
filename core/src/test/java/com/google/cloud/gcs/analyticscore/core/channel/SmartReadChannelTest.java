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

package com.google.cloud.gcs.analyticscore.core.channel;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.core.optimizer.FormatOptimizer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SmartReadChannelTest {

  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName("b").setObjectName("o").build();
  private static final GcsItemInfo ITEM_INFO = GcsItemInfo.builder().setItemId(ITEM_ID).build();
  private static final GcsFileInfo FILE_INFO =
      GcsFileInfo.builder()
          .setItemInfo(ITEM_INFO)
          .setUri(URI.create("gs://b/o"))
          .setAttributes(ImmutableMap.of())
          .build();

  private VectoredSeekableByteChannel mockDelegate;
  private AnalyticsCacheManager mockCacheManager;
  private FormatOptimizer mockOptimizer;

  @BeforeEach
  void initializeMocks() throws IOException {
    mockDelegate = mock(VectoredSeekableByteChannel.class);
    mockCacheManager = mock(AnalyticsCacheManager.class);
    mockOptimizer = mock(FormatOptimizer.class);

    when(mockDelegate.position()).thenReturn(0L);
    when(mockDelegate.isOpen()).thenReturn(true);
    when(mockOptimizer.isApplicable(any(GcsItemId.class))).thenReturn(true);
    when(mockOptimizer.isApplicable(any(GcsFileInfo.class))).thenReturn(true);
  }

  @Test
  void constructor_withFileInfo_applicableOptimizer_callsOnOpenWithFileInfo() throws IOException {
    SmartReadChannel.builder()
        .setDelegate(mockDelegate)
        .setItemId(ITEM_ID)
        .setFileInfo(FILE_INFO)
        .setCacheManager(mockCacheManager)
        .addOptimizer(mockOptimizer)
        .build();

    verify(mockOptimizer).onOpen(FILE_INFO, mockCacheManager);
    verify(mockOptimizer, never()).onOpen(eq(ITEM_ID), any());
  }

  @Test
  void constructor_itemIdOnly_applicableOptimizer_callsOnOpenWithItemId() throws IOException {
    SmartReadChannel.builder()
        .setDelegate(mockDelegate)
        .setItemId(ITEM_ID)
        .setCacheManager(mockCacheManager)
        .addOptimizer(mockOptimizer)
        .build();

    verify(mockOptimizer).onOpen(ITEM_ID, mockCacheManager);
    verify(mockOptimizer, never()).onOpen(any(GcsFileInfo.class), any());
  }

  @Test
  void constructor_inapplicableOptimizer_skipsOnOpen() throws IOException {
    when(mockOptimizer.isApplicable(any(GcsItemId.class))).thenReturn(false);
    when(mockOptimizer.isApplicable(any(GcsFileInfo.class))).thenReturn(false);

    SmartReadChannel.builder()
        .setDelegate(mockDelegate)
        .setItemId(ITEM_ID)
        .setCacheManager(mockCacheManager)
        .addOptimizer(mockOptimizer)
        .build();

    verify(mockOptimizer, never()).onOpen(any(GcsItemId.class), any());
    verify(mockOptimizer, never()).onOpen(any(GcsFileInfo.class), any());
  }

  @Test
  void read_optimizerHit_returnsBytesAndUpdatesPosition() throws IOException {
    int bytesToRead = 5;
    int bufferSize = 10;

    long[] delegatePosition = new long[] {0L};
    doAnswer(inv -> delegatePosition[0]).when(mockDelegate).position();
    doAnswer(
            inv -> {
              delegatePosition[0] = inv.getArgument(0);
              return mockDelegate;
            })
        .when(mockDelegate)
        .position(anyLong());

    when(mockOptimizer.read(eq(0L), any(ByteBuffer.class), eq(mockDelegate)))
        .thenReturn(bytesToRead);

    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(mockOptimizer)
            .build();
    ByteBuffer dst = ByteBuffer.allocate(bufferSize);

    int bytesRead = smartChannel.read(dst);

    assertThat(bytesRead).isEqualTo(bytesToRead);
    assertThat(smartChannel.position()).isEqualTo(bytesToRead);
  }

  @Test
  void read_optimizerMiss_delegatesToDelegate() throws IOException {
    when(mockOptimizer.read(eq(0L), any(ByteBuffer.class), eq(mockDelegate))).thenReturn(0);
    when(mockDelegate.read(any(ByteBuffer.class))).thenReturn(10);
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(mockOptimizer)
            .build();
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = smartChannel.read(dst);

    assertThat(bytesRead).isEqualTo(10);
    verify(mockDelegate).read(dst);
  }

  @Test
  void read_optimizerMiss_restoresPositionIfChanged() throws IOException {
    long[] delegatePosition = new long[] {0L};
    doAnswer(inv -> delegatePosition[0]).when(mockDelegate).position();
    doAnswer(
            inv -> {
              delegatePosition[0] = inv.getArgument(0);
              return mockDelegate;
            })
        .when(mockDelegate)
        .position(anyLong());

    when(mockOptimizer.read(eq(0L), any(ByteBuffer.class), eq(mockDelegate)))
        .thenAnswer(
            inv -> {
              delegatePosition[0] = 50L; // Simulate optimizer changing position but missing
              return 0;
            });
    when(mockDelegate.read(any(ByteBuffer.class))).thenReturn(10);

    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(mockOptimizer)
            .build();
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = smartChannel.read(dst);

    assertThat(bytesRead).isEqualTo(10);
    assertThat(delegatePosition[0]).isEqualTo(0L); // Should be restored to original
    verify(mockDelegate).position(0L);
    verify(mockDelegate).read(dst);
  }

  @Test
  void read_optimizerEof_returnsMinusOne() throws IOException {
    when(mockOptimizer.read(eq(0L), any(ByteBuffer.class), eq(mockDelegate))).thenReturn(-1);
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(mockOptimizer)
            .build();
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = smartChannel.read(dst);

    assertThat(bytesRead).isEqualTo(-1);
    verify(mockDelegate, never()).position(anyLong());
  }

  @Test
  void readVectored_delegatesToDelegate() throws IOException {
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .build();
    List<GcsObjectRange> ranges = ImmutableList.of();
    IntFunction<ByteBuffer> allocate = (i) -> ByteBuffer.allocate(i);

    smartChannel.readVectored(ranges, allocate);

    verify(mockDelegate).readVectored(ranges, allocate);
  }

  @Test
  void write_delegatesToDelegate() throws IOException {
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .build();
    ByteBuffer src = ByteBuffer.allocate(10);
    when(mockDelegate.write(src)).thenReturn(10);

    int written = smartChannel.write(src);

    assertThat(written).isEqualTo(10);
    verify(mockDelegate).write(src);
  }

  @Test
  void position_get_delegatesToDelegate() throws IOException {
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .build();
    when(mockDelegate.position()).thenReturn(100L);

    assertThat(smartChannel.position()).isEqualTo(100L);
  }

  @Test
  void position_set_delegatesToDelegate() throws IOException {
    long targetPosition = 200L;
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .build();

    when(mockDelegate.position()).thenReturn(targetPosition);

    VectoredSeekableByteChannel returnedChannel = smartChannel.position(targetPosition);

    verify(mockDelegate).position(targetPosition);
    assertThat(returnedChannel).isSameInstanceAs(smartChannel);
    assertThat(smartChannel.position()).isEqualTo(targetPosition);
  }

  @Test
  void size_delegatesToDelegate() throws IOException {
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .build();
    when(mockDelegate.size()).thenReturn(500L);

    assertThat(smartChannel.size()).isEqualTo(500L);
  }

  @Test
  void truncate_delegatesToDelegate() throws IOException {
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .build();

    smartChannel.truncate(100L);

    verify(mockDelegate).truncate(100L);
  }

  @Test
  void isOpen_delegatesToDelegate() throws IOException {
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .build();

    assertThat(smartChannel.isOpen()).isTrue();
  }

  @Test
  void close_whenNotOpen_doesNothing() throws IOException {
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(mockOptimizer)
            .build();
    when(mockDelegate.isOpen()).thenReturn(false);

    smartChannel.close();

    verify(mockOptimizer, never()).onClose();
    verify(mockDelegate, never()).close();
  }

  @Test
  void close_default_callsOnCloseAndDelegateClose() throws IOException {
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(mockOptimizer)
            .build();

    smartChannel.close();

    verify(mockOptimizer).onClose();
    verify(mockDelegate).close();
  }

  @Test
  void builder_missingDelegate_throwsException() {
    SmartReadChannel.Builder builder =
        SmartReadChannel.builder().setItemId(ITEM_ID).setCacheManager(mockCacheManager);

    assertThrows(NullPointerException.class, builder::build);
  }

  @Test
  void builder_missingCacheManager_throwsException() {
    SmartReadChannel.Builder builder =
        SmartReadChannel.builder().setDelegate(mockDelegate).setItemId(ITEM_ID);

    assertThrows(NullPointerException.class, builder::build);
  }

  @Test
  void builder_missingItemIdAndFileInfo_throwsException() {
    SmartReadChannel.Builder builder =
        SmartReadChannel.builder().setDelegate(mockDelegate).setCacheManager(mockCacheManager);

    assertThrows(NullPointerException.class, builder::build);
  }

  @Test
  void close_optimizerThrowsException_closesDelegateAndThrows() throws IOException {
    FormatOptimizer failingOptimizer = mock(FormatOptimizer.class);
    when(failingOptimizer.isApplicable(any(GcsItemId.class))).thenReturn(true);
    doThrow(new IOException("optimizer failed")).when(failingOptimizer).onClose();

    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(failingOptimizer)
            .build();

    IOException exception = assertThrows(IOException.class, smartChannel::close);
    assertThat(exception).hasMessageThat().isEqualTo("optimizer failed");
    verify(mockDelegate).close();
  }

  @Test
  void close_delegateThrowsException_throws() throws IOException {
    doThrow(new IOException("delegate failed")).when(mockDelegate).close();

    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(mockOptimizer)
            .build();

    IOException exception = assertThrows(IOException.class, smartChannel::close);
    assertThat(exception).hasMessageThat().isEqualTo("delegate failed");
    verify(mockOptimizer).onClose();
  }

  @Test
  void close_multipleExceptions_areSuppressed() throws IOException {
    FormatOptimizer failingOptimizer = mock(FormatOptimizer.class);
    when(failingOptimizer.isApplicable(any(GcsItemId.class))).thenReturn(true);
    doThrow(new IOException("optimizer failed")).when(failingOptimizer).onClose();
    doThrow(new IOException("delegate failed")).when(mockDelegate).close();

    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(failingOptimizer)
            .build();

    IOException exception = assertThrows(IOException.class, smartChannel::close);
    assertThat(exception).hasMessageThat().isEqualTo("optimizer failed");
    assertThat(exception.getSuppressed()).hasLength(1);
    assertThat(exception.getSuppressed()[0]).hasMessageThat().isEqualTo("delegate failed");
  }

  @Test
  void constructor_withFileInfo_optimizerThrowsException_closesPreviousOptimizers()
      throws IOException {
    FormatOptimizer successfulOptimizer = mock(FormatOptimizer.class);
    when(successfulOptimizer.isApplicable(any(GcsFileInfo.class))).thenReturn(true);
    doThrow(new IOException("close failed")).when(successfulOptimizer).onClose();

    FormatOptimizer failingOptimizer = mock(FormatOptimizer.class);
    when(failingOptimizer.isApplicable(any(GcsFileInfo.class))).thenReturn(true);
    doThrow(new IOException("init failed"))
        .when(failingOptimizer)
        .onOpen(any(GcsFileInfo.class), any());

    SmartReadChannel.Builder builder =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setFileInfo(FILE_INFO)
            .setCacheManager(mockCacheManager)
            .addOptimizer(successfulOptimizer)
            .addOptimizer(failingOptimizer);

    IOException exception = assertThrows(IOException.class, builder::build);
    assertThat(exception).hasMessageThat().isEqualTo("init failed");
    assertThat(exception.getSuppressed()).hasLength(1);
    assertThat(exception.getSuppressed()[0]).hasMessageThat().isEqualTo("close failed");

    verify(successfulOptimizer).onClose();
    verify(mockDelegate, never()).close();
  }

  @Test
  void constructor_optimizerThrowsException_closesPreviousOptimizers() throws IOException {
    FormatOptimizer successfulOptimizer = mock(FormatOptimizer.class);
    when(successfulOptimizer.isApplicable(any(GcsItemId.class))).thenReturn(true);
    doThrow(new RuntimeException("close failed")).when(successfulOptimizer).onClose();

    FormatOptimizer failingOptimizer = mock(FormatOptimizer.class);
    when(failingOptimizer.isApplicable(any(GcsItemId.class))).thenReturn(true);
    doThrow(new IOException("init failed"))
        .when(failingOptimizer)
        .onOpen(any(GcsItemId.class), any());

    SmartReadChannel.Builder builder =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(successfulOptimizer)
            .addOptimizer(failingOptimizer);

    IOException exception = assertThrows(IOException.class, builder::build);
    assertThat(exception).hasMessageThat().isEqualTo("init failed");
    assertThat(exception.getSuppressed()).hasLength(1);
    assertThat(exception.getSuppressed()[0]).hasMessageThat().isEqualTo("close failed");

    verify(successfulOptimizer).onClose();
    verify(mockDelegate, never()).close();
  }

  @Test
  void close_throwsRuntimeException() throws IOException {
    doThrow(new RuntimeException("runtime error")).when(mockDelegate).close();

    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .build();

    RuntimeException exception = assertThrows(RuntimeException.class, smartChannel::close);
    assertThat(exception).hasMessageThat().isEqualTo("runtime error");
  }

  @Test
  void close_throwsError() throws IOException {
    doThrow(new Error("fatal error")).when(mockDelegate).close();

    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .build();

    Error exception = assertThrows(Error.class, smartChannel::close);
    assertThat(exception).hasMessageThat().isEqualTo("fatal error");
  }

  @Test
  void readVectored_delegatesToOptimizersAndThenChannel() throws IOException {
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    List<GcsObjectRange> ranges = List.of(range);
    FormatOptimizer mockOptimizer1 = mock(FormatOptimizer.class);
    when(mockOptimizer1.isApplicable(any(GcsItemId.class))).thenReturn(true);
    when(mockOptimizer1.readVectored(any(), any())).thenReturn(ranges);
    FormatOptimizer mockOptimizer2 = mock(FormatOptimizer.class);
    when(mockOptimizer2.isApplicable(any(GcsItemId.class))).thenReturn(true);
    when(mockOptimizer2.readVectored(any(), any())).thenReturn(java.util.Collections.emptyList());
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(mockOptimizer1)
            .addOptimizer(mockOptimizer2)
            .build();

    smartChannel.readVectored(ranges, ByteBuffer::allocate);

    verify(mockOptimizer1).readVectored(eq(ranges), any());
    verify(mockOptimizer2).readVectored(eq(ranges), any());
    verify(mockDelegate, org.mockito.Mockito.never()).readVectored(any(), any());
  }

  @Test
  void readVectored_delegatesToChannelWhenOptimizersDoNotSatisfy() throws IOException {
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    List<GcsObjectRange> ranges = List.of(range);
    FormatOptimizer mockOptimizer = mock(FormatOptimizer.class);
    when(mockOptimizer.isApplicable(any(GcsItemId.class))).thenReturn(true);
    when(mockOptimizer.readVectored(any(), any())).thenReturn(ranges);
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(mockOptimizer)
            .build();

    smartChannel.readVectored(ranges, ByteBuffer::allocate);

    verify(mockOptimizer).readVectored(eq(ranges), any());
    verify(mockDelegate).readVectored(eq(ranges), any());
  }

  @Test
  void init_throwsRuntimeException_suppressedExceptionPreserved() throws IOException {
    FormatOptimizer successfulOptimizer = mock(FormatOptimizer.class);
    when(successfulOptimizer.isApplicable(any(GcsFileInfo.class))).thenReturn(true);
    org.mockito.Mockito.doThrow(new IOException("close failed"))
        .when(successfulOptimizer)
        .onClose();
    FormatOptimizer failingOptimizer = mock(FormatOptimizer.class);
    when(failingOptimizer.isApplicable(any(GcsFileInfo.class))).thenReturn(true);
    org.mockito.Mockito.doThrow(new RuntimeException("init runtime failed"))
        .when(failingOptimizer)
        .onOpen(any(GcsFileInfo.class), any());
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                SmartReadChannel.builder()
                    .setDelegate(mockDelegate)
                    .setFileInfo(FILE_INFO)
                    .setCacheManager(mockCacheManager)
                    .addOptimizer(successfulOptimizer)
                    .addOptimizer(failingOptimizer)
                    .build());

    assertThat(exception).hasMessageThat().isEqualTo("init runtime failed");
    assertThat(exception.getSuppressed()).hasLength(1);
    assertThat(exception.getSuppressed()[0]).hasMessageThat().isEqualTo("close failed");
    verify(successfulOptimizer).onClose();
  }

  @Test
  void close_success_closesAllOptimizersAndDelegate() throws IOException {
    FormatOptimizer mockOptimizer1 = mock(FormatOptimizer.class);
    when(mockOptimizer1.isApplicable(any(GcsItemId.class))).thenReturn(true);
    FormatOptimizer mockOptimizer2 = mock(FormatOptimizer.class);
    when(mockOptimizer2.isApplicable(any(GcsItemId.class))).thenReturn(true);
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(mockOptimizer1)
            .addOptimizer(mockOptimizer2)
            .build();

    smartChannel.close();

    verify(mockOptimizer1).onClose();
    verify(mockOptimizer2).onClose();
    verify(mockDelegate).close();
  }

  @Test
  void close_throwsIOException_rethrowsIOException() throws IOException {
    FormatOptimizer mockOptimizer = mock(FormatOptimizer.class);
    when(mockOptimizer.isApplicable(any(GcsItemId.class))).thenReturn(true);
    org.mockito.Mockito.doThrow(new IOException("IO close failed")).when(mockOptimizer).onClose();
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(mockOptimizer)
            .build();

    IOException exception = assertThrows(IOException.class, smartChannel::close);

    assertThat(exception).hasMessageThat().isEqualTo("IO close failed");
  }

  @Test
  void close_throwsRuntimeException_rethrowsRuntimeException() throws IOException {
    FormatOptimizer mockOptimizer = mock(FormatOptimizer.class);
    when(mockOptimizer.isApplicable(any(GcsItemId.class))).thenReturn(true);
    org.mockito.Mockito.doThrow(new RuntimeException("Runtime close failed"))
        .when(mockOptimizer)
        .onClose();
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(mockOptimizer)
            .build();

    RuntimeException exception = assertThrows(RuntimeException.class, smartChannel::close);

    assertThat(exception).hasMessageThat().isEqualTo("Runtime close failed");
  }

  @Test
  void close_throwsError_rethrowsError() throws IOException {
    FormatOptimizer mockOptimizer = mock(FormatOptimizer.class);
    when(mockOptimizer.isApplicable(any(GcsItemId.class))).thenReturn(true);
    org.mockito.Mockito.doThrow(new Error("Error close failed")).when(mockOptimizer).onClose();
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(mockOptimizer)
            .build();

    Error exception = assertThrows(Error.class, smartChannel::close);

    assertThat(exception).hasMessageThat().isEqualTo("Error close failed");
  }

  @Test
  void getItemInfo_delegatesToDelegate() throws IOException {
    SmartReadChannel channel =
        SmartReadChannel.builder()
            .setDelegate(mockDelegate)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .build();
    GcsItemInfo expectedInfo = GcsItemInfo.builder().setItemId(ITEM_ID).build();
    when(mockDelegate.getItemInfo()).thenReturn(expectedInfo);

    GcsItemInfo actualInfo = channel.getItemInfo();

    assertThat(actualInfo).isEqualTo(expectedInfo);
    verify(mockDelegate).getItemInfo();
  }
}
