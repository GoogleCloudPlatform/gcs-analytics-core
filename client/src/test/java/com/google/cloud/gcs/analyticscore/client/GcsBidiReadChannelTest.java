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
package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.ZeroCopySupport.DisposableByteString;
import com.google.common.base.Supplier;
import com.google.protobuf.ByteString;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class GcsBidiReadChannelTest {

  @Mock private Storage storage;
  @Mock private BlobReadSession blobReadSession;
  @Mock private ApiFuture<BlobReadSession> sessionFuture;
  @Mock private DisposableByteString disposableByteString;
  @Mock private Telemetry telemetry;

  private GcsItemId itemId;
  private GcsBidiReadChannel reader;
  private ExecutorService directExecutor;
  private Supplier<ExecutorService> executorServiceSupplier;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    itemId = GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    directExecutor = com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService();
    executorServiceSupplier = () -> directExecutor;

    when(storage.blobReadSession(any(BlobId.class))).thenReturn(sessionFuture);
    when(sessionFuture.get(anyLong(), any())).thenReturn(blobReadSession);

    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    reader =
        new GcsBidiReadChannel(storage, itemId, readOptions, executorServiceSupplier, telemetry);
  }

  @Test
  void readVectored_success_populatesByteBuffer() throws Exception {
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    byte[] data = new byte[10];
    Arrays.fill(data, (byte) 1);
    ByteString byteString = ByteString.copyFrom(data);

    when(blobReadSession.readAs(any()))
        .thenReturn(ApiFutures.immediateFuture(disposableByteString));
    when(disposableByteString.byteString()).thenReturn(byteString);

    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    reader.readVectored(Arrays.asList(range), allocate);

    ByteBuffer result = range.getByteBufferFuture().get();
    assertThat(result).isNotNull();
    assertThat(result.remaining()).isEqualTo(10);
    assertThat(result.get(0)).isEqualTo((byte) 1);
  }

  @Test
  void readVectored_failure_completesExceptionally() throws Exception {
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    Exception exception = new RuntimeException("Read failed");
    when(blobReadSession.readAs(any())).thenReturn(ApiFutures.immediateFailedFuture(exception));

    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    reader.readVectored(Arrays.asList(range), allocate);

    CompletableFuture<ByteBuffer> future = range.getByteBufferFuture();
    assertThat(future.isCompletedExceptionally()).isTrue();
  }

  @Test
  void constructor_nullItemId_throwsNullPointerException() {
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();

    Executable action =
        () ->
            new GcsBidiReadChannel(
                storage, (GcsItemId) null, readOptions, executorServiceSupplier, telemetry);

    assertThrows(NullPointerException.class, action);
  }

  @Test
  void close_validSession_closesBlobReadSession() throws Exception {
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    byte[] data = new byte[10];
    ByteString byteString = ByteString.copyFrom(data);

    when(blobReadSession.readAs(any()))
        .thenReturn(ApiFutures.immediateFuture(disposableByteString));
    when(disposableByteString.byteString()).thenReturn(byteString);

    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;
    reader.readVectored(Arrays.asList(range), allocate);

    reader.close();

    verify(blobReadSession, times(1)).close();
  }

  @Test
  void readVectored_interruptedGettingSession_throwsIOExceptionAndSetsInterrupted()
      throws Exception {
    reset(sessionFuture);
    when(sessionFuture.get(anyLong(), any())).thenThrow(new InterruptedException("Interrupted"));

    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;
    Thread.interrupted();

    Executable action = () -> reader.readVectored(Arrays.asList(range), allocate);

    IOException exception = assertThrows(IOException.class, action);
    assertThat(exception.getMessage())
        .contains("Failed to get BlobReadSession due to thread interruption");
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
    Thread.interrupted();
  }

  @Test
  void readVectored_sessionFileNotFound_throwsFileNotFoundException() throws Exception {
    reset(sessionFuture);
    when(sessionFuture.get(anyLong(), any()))
        .thenThrow(new ExecutionException(new StorageException(404, "Not found")));

    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    Executable action = () -> reader.readVectored(Arrays.asList(range), allocate);

    FileNotFoundException exception = assertThrows(FileNotFoundException.class, action);
    assertThat(exception.getMessage()).contains("Object not found: ");
    assertThat(exception.getMessage()).contains("test-bucket");
    assertThat(exception.getMessage()).contains("test-object");
  }

  @Test
  void readVectored_sessionStorageExceptionOther_throwsIOException() throws Exception {
    reset(sessionFuture);
    when(sessionFuture.get(anyLong(), any()))
        .thenThrow(new ExecutionException(new StorageException(500, "Internal Server Error")));

    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    Executable action = () -> reader.readVectored(Arrays.asList(range), allocate);

    IOException exception = assertThrows(IOException.class, action);
    assertThat(exception).isNotInstanceOf(FileNotFoundException.class);
    assertThat(exception.getMessage()).contains("Failed to get BlobReadSession");
  }

  @Test
  void readVectored_sessionTimeoutException_throwsIOException() throws Exception {
    reset(sessionFuture);
    when(sessionFuture.get(anyLong(), any())).thenThrow(new TimeoutException("Timeout occurred"));

    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    Executable action = () -> reader.readVectored(Arrays.asList(range), allocate);

    IOException exception = assertThrows(IOException.class, action);
    assertThat(exception.getMessage())
        .contains("Failed to get BlobReadSession due to client timeout limit");
  }

  @Test
  void readVectored_afterClose_throwsIOExceptionAndCompletesExceptionally() throws Exception {
    reader.close();

    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    Executable action = () -> reader.readVectored(Arrays.asList(range), allocate);

    IOException exception = assertThrows(IOException.class, action);
    assertThat(exception.getMessage()).contains("Reader is closed.");
    assertThat(range.getByteBufferFuture().isCompletedExceptionally()).isTrue();

    ExecutionException executionException =
        assertThrows(ExecutionException.class, () -> range.getByteBufferFuture().get());
    assertThat(executionException.getCause()).isInstanceOf(IOException.class);
    assertThat(executionException.getCause().getMessage()).contains("Reader is closed.");
  }

  @Test
  void readVectored_nullAllocator_completesExceptionallyWithNullPointerException()
      throws Exception {
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    byte[] data = new byte[10];
    Arrays.fill(data, (byte) 1);
    ByteString byteString = ByteString.copyFrom(data);

    when(blobReadSession.readAs(any()))
        .thenReturn(ApiFutures.immediateFuture(disposableByteString));
    when(disposableByteString.byteString()).thenReturn(byteString);

    IntFunction<ByteBuffer> allocate = size -> null;

    reader.readVectored(Arrays.asList(range), allocate);

    CompletableFuture<ByteBuffer> future = range.getByteBufferFuture();
    assertThat(future.isCompletedExceptionally()).isTrue();

    ExecutionException executionException =
        assertThrows(ExecutionException.class, () -> future.get());
    assertThat(executionException.getCause()).isInstanceOf(NullPointerException.class);
    assertThat(executionException.getCause().getMessage())
        .contains("Allocator returned a null ByteBuffer!");
  }

  @Test
  void close_calledMultipleTimes_isIdempotent() throws Exception {
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    byte[] data = new byte[10];
    ByteString byteString = ByteString.copyFrom(data);

    when(blobReadSession.readAs(any()))
        .thenReturn(ApiFutures.immediateFuture(disposableByteString));
    when(disposableByteString.byteString()).thenReturn(byteString);

    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;
    reader.readVectored(Arrays.asList(range), allocate);

    // Call close() the first time
    reader.close();
    verify(blobReadSession, times(1)).close();

    // Call close() the second time
    reader.close();
    // Verify that blobReadSession.close() was still called only once
    verify(blobReadSession, times(1)).close();
  }

  @Test
  void testDummyReadStrategy_getSdkReadChannel_throwsUnsupported() {
    ReadStrategy strategy =
        reader.createReadStrategy(storage, itemId, GcsReadOptions.builder().build(), null);

    assertThrows(UnsupportedOperationException.class, strategy::getSdkReadChannel);
  }

  @Test
  void read_success_returnsBytesReadAndUpdatesPosition() throws Exception {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(20L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    GcsBidiReadChannel seekableReader =
        new GcsBidiReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);

    byte[] data = new byte[10];
    Arrays.fill(data, (byte) 7);
    ByteString byteString = ByteString.copyFrom(data);

    when(blobReadSession.readAs(any()))
        .thenReturn(ApiFutures.immediateFuture(disposableByteString));
    when(disposableByteString.byteString()).thenReturn(byteString);

    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = seekableReader.read(dst);

    assertThat(bytesRead).isEqualTo(10);
    assertThat(seekableReader.position()).isEqualTo(10L);
    dst.flip();
    assertThat(dst.get(0)).isEqualTo((byte) 7);
  }

  @Test
  void read_atEndOfFile_returnsMinusOne() throws Exception {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(10L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    GcsBidiReadChannel seekableReader =
        new GcsBidiReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);

    seekableReader.position(10L);
    ByteBuffer dst = ByteBuffer.allocate(5);

    int bytesRead = seekableReader.read(dst);

    assertThat(bytesRead).isEqualTo(-1);
  }

  @Test
  void read_noRemainingSpaceInBuffer_returnsZero() throws Exception {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(20L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    GcsBidiReadChannel seekableReader =
        new GcsBidiReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);

    ByteBuffer dst = ByteBuffer.allocate(0);

    int bytesRead = seekableReader.read(dst);

    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void position_validSeekAndRead_returnsBytesReadAndUpdatesPosition() throws Exception {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(20L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    GcsBidiReadChannel seekableReader =
        new GcsBidiReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);

    byte[] data = new byte[5];
    Arrays.fill(data, (byte) 3);
    ByteString byteString = ByteString.copyFrom(data);

    when(blobReadSession.readAs(any()))
        .thenReturn(ApiFutures.immediateFuture(disposableByteString));
    when(disposableByteString.byteString()).thenReturn(byteString);

    ByteBuffer dst = ByteBuffer.allocate(5);

    long initialPosition = seekableReader.position();
    seekableReader.position(5L);
    long setPosition = seekableReader.position();
    int bytesRead = seekableReader.read(dst);
    long finalPosition = seekableReader.position();

    assertThat(initialPosition).isEqualTo(0L);
    assertThat(setPosition).isEqualTo(5L);
    assertThat(bytesRead).isEqualTo(5);
    assertThat(finalPosition).isEqualTo(10L);
  }

  @Test
  void position_negativeSeek_throwsEOFException() throws Exception {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(20L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    GcsBidiReadChannel seekableReader =
        new GcsBidiReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);

    Executable action = () -> seekableReader.position(-1L);

    assertThrows(EOFException.class, action);
  }

  @Test
  void readAndPositionAndSize_closedChannel_throwsClosedChannelException() throws Exception {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(20L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    GcsBidiReadChannel seekableReader =
        new GcsBidiReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);
    seekableReader.close();
    ByteBuffer dst = ByteBuffer.allocate(10);

    Executable readAction = () -> seekableReader.read(dst);
    Executable positionGetAction = () -> seekableReader.position();
    Executable positionSetAction = () -> seekableReader.position(5L);
    Executable sizeAction = () -> seekableReader.size();

    assertThrows(ClosedChannelException.class, readAction);
    assertThrows(ClosedChannelException.class, positionGetAction);
    assertThrows(ClosedChannelException.class, positionSetAction);
    assertThrows(ClosedChannelException.class, sizeAction);
  }

  @Test
  void read_interruptedDuringStreamRead_throwsIOExceptionAndSetsInterrupted() throws Exception {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(20L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    GcsBidiReadChannel seekableReader =
        new GcsBidiReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);

    ApiFuture<DisposableByteString> interruptedFuture = mock(ApiFuture.class);
    when(interruptedFuture.get(anyLong(), any()))
        .thenThrow(new InterruptedException("Interrupted"));
    when(blobReadSession.readAs(any())).thenReturn(interruptedFuture);

    ByteBuffer dst = ByteBuffer.allocate(10);

    Executable action = () -> seekableReader.read(dst);

    IOException exception = assertThrows(IOException.class, action);
    assertThat(exception.getMessage()).contains("Thread interrupted while reading from stream");
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
    Thread.interrupted(); // clear status
  }

  @Test
  void read_executionExceptionDuringStreamRead_throwsIOException() throws Exception {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(20L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    GcsBidiReadChannel seekableReader =
        new GcsBidiReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);

    ApiFuture<DisposableByteString> failedFuture = mock(ApiFuture.class);
    when(failedFuture.get(anyLong(), any()))
        .thenThrow(new ExecutionException(new Exception("Generic Error")));
    when(blobReadSession.readAs(any())).thenReturn(failedFuture);

    ByteBuffer dst = ByteBuffer.allocate(10);

    Executable action = () -> seekableReader.read(dst);

    IOException exception = assertThrows(IOException.class, action);
    assertThat(exception.getMessage()).contains("Failed to read bytes from bidirectional session");
  }

  @Test
  void read_runtimeExceptionDuringStreamRead_propagatesException() throws Exception {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(20L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    GcsBidiReadChannel seekableReader =
        new GcsBidiReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);

    ApiFuture<DisposableByteString> failedFuture = mock(ApiFuture.class);
    when(failedFuture.get(anyLong(), any()))
        .thenThrow(new ExecutionException(new RuntimeException("gRPC Error")));
    when(blobReadSession.readAs(any())).thenReturn(failedFuture);

    ByteBuffer dst = ByteBuffer.allocate(10);

    Executable action = () -> seekableReader.read(dst);

    RuntimeException exception = assertThrows(RuntimeException.class, action);
    assertThat(exception.getMessage()).contains("gRPC Error");
  }

  @Test
  void read_fileNotFoundDuringStreamRead_throwsFileNotFoundException() throws Exception {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(20L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    GcsBidiReadChannel seekableReader =
        new GcsBidiReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);

    ApiFuture<DisposableByteString> failedFuture = mock(ApiFuture.class);
    when(failedFuture.get(anyLong(), any()))
        .thenThrow(new ExecutionException(new StorageException(404, "Not found")));
    when(blobReadSession.readAs(any())).thenReturn(failedFuture);

    ByteBuffer dst = ByteBuffer.allocate(10);

    Executable action = () -> seekableReader.read(dst);

    FileNotFoundException exception = assertThrows(FileNotFoundException.class, action);
    assertThat(exception.getMessage()).contains("Object not found during read:");
  }

  @Test
  void read_timeoutDuringStreamRead_throwsIOException() throws Exception {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(20L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    GcsBidiReadChannel seekableReader =
        new GcsBidiReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);

    ApiFuture<DisposableByteString> timeoutFuture = mock(ApiFuture.class);
    when(timeoutFuture.get(anyLong(), any())).thenThrow(new TimeoutException("Timeout"));
    when(blobReadSession.readAs(any())).thenReturn(timeoutFuture);

    ByteBuffer dst = ByteBuffer.allocate(10);

    Executable action = () -> seekableReader.read(dst);

    IOException exception = assertThrows(IOException.class, action);
    assertThat(exception.getMessage())
        .contains("Timed out waiting for bytes from bidirectional stream");
  }

  @Test
  void read_bufferLargerThanRemainingBytes_readsOnlyRemainingBytes() throws Exception {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(15L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    GcsBidiReadChannel seekableReader =
        new GcsBidiReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);

    seekableReader.position(10L);

    byte[] data = new byte[5];
    Arrays.fill(data, (byte) 3);
    ByteString byteString = ByteString.copyFrom(data);

    when(blobReadSession.readAs(any()))
        .thenReturn(ApiFutures.immediateFuture(disposableByteString));
    when(disposableByteString.byteString()).thenReturn(byteString);

    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = seekableReader.read(dst);

    assertThat(bytesRead).isEqualTo(5);
    assertThat(seekableReader.position()).isEqualTo(15L);
    assertThat(dst.position()).isEqualTo(5);
    assertThat(dst.limit()).isEqualTo(10);
  }

  @Test
  void size_whenOpen_returnsCorrectSize() throws Exception {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(42L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    GcsBidiReadChannel seekableReader =
        new GcsBidiReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);

    assertThat(seekableReader.size()).isEqualTo(42L);
  }

  @Test
  void size_lazilyFetchedFromSession_returnsCorrectSize() throws Exception {
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(-1L).build(); // Simulate uninitialized size
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    GcsBidiReadChannel seekableReader =
        new GcsBidiReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);

    BlobInfo mockBlobInfo = mock(BlobInfo.class);
    when(mockBlobInfo.getSize()).thenReturn(150L);
    when(blobReadSession.getBlobInfo()).thenReturn(mockBlobInfo);

    // Execute
    long retrievedSize = seekableReader.size();

    // Assert
    assertThat(retrievedSize).isEqualTo(150L);
    verify(blobReadSession, times(1)).getBlobInfo();
  }

  @Test
  void size_calledMultipleTimes_cachesMetadata() throws Exception {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(-1L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    GcsBidiReadChannel seekableReader =
        new GcsBidiReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);

    BlobInfo mockBlobInfo = mock(BlobInfo.class);
    when(mockBlobInfo.getSize()).thenReturn(150L);
    when(blobReadSession.getBlobInfo()).thenReturn(mockBlobInfo);

    // Call size multiple times
    long firstCall = seekableReader.size();
    long secondCall = seekableReader.size();

    // Assert size matches and is cached (only 1 invocation to getBlobInfo)
    assertThat(firstCall).isEqualTo(150L);
    assertThat(secondCall).isEqualTo(150L);
    verify(blobReadSession, times(1)).getBlobInfo();
  }

  @Test
  void size_blobInfoReturnsNull_fallsBackToSuperSize() throws Exception {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(100L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    GcsBidiReadChannel seekableReader =
        new GcsBidiReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);

    when(blobReadSession.getBlobInfo()).thenReturn(null);

    long resultSize = seekableReader.size();

    assertThat(resultSize).isEqualTo(100L);
  }

  @Test
  void size_getBlobReadSessionThrows_fallsBackToSuperSize() throws Exception {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(100L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();

    Storage mockStorage = mock(Storage.class);
    when(mockStorage.blobReadSession(any(BlobId.class)))
        .thenReturn(ApiFutures.immediateFailedFuture(new IOException("Failed to get session")));

    GcsBidiReadChannel seekableReader =
        new GcsBidiReadChannel(
            mockStorage, itemInfo, readOptions, executorServiceSupplier, telemetry);

    long resultSize = seekableReader.size();

    assertThat(resultSize).isEqualTo(100L);
  }

  @Test
  void read_seekBeyondSize_returnsMinusOne() throws Exception {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(10L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    GcsBidiReadChannel seekableReader =
        new GcsBidiReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);

    seekableReader.position(15L);
    ByteBuffer dst = ByteBuffer.allocate(5);

    int bytesRead = seekableReader.read(dst);

    assertThat(bytesRead).isEqualTo(-1);
  }
}
