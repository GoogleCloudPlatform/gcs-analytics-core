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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.ZeroCopySupport.DisposableByteString;
import com.google.common.base.Supplier;
import com.google.protobuf.ByteString;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
  void testReadVectored_Success() throws Exception {
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
  void testReadVectored_Failure() throws Exception {
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
  void testConstructor_nullItemId() {
    GcsReadOptions readOptions = GcsReadOptions.builder().setBidiTimeout(10).build();
    org.junit.jupiter.api.Assertions.assertThrows(
        NullPointerException.class,
        () ->
            new GcsBidiReadChannel(
                storage, (GcsItemId) null, readOptions, executorServiceSupplier, telemetry));
  }

  @Test
  void testClose_closesBlobReadSession() throws Exception {
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
  void testBlobReadSessionInterruptedException() throws Exception {
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

    java.io.IOException exception =
        org.junit.jupiter.api.Assertions.assertThrows(
            java.io.IOException.class, () -> reader.readVectored(Arrays.asList(range), allocate));

    assertThat(exception.getMessage())
        .contains("Failed to get BlobReadSession due to thread interruption");
    assertThat(Thread.currentThread().isInterrupted()).isTrue();

    Thread.interrupted();
  }

  @Test
  void testBlobReadSessionFileNotFoundException() throws Exception {
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

    FileNotFoundException exception =
        org.junit.jupiter.api.Assertions.assertThrows(
            FileNotFoundException.class, () -> reader.readVectored(Arrays.asList(range), allocate));

    assertThat(exception.getMessage()).contains("Object not found: ");
    assertThat(exception.getMessage()).contains("test-bucket");
    assertThat(exception.getMessage()).contains("test-object");
  }

  @Test
  void testBlobReadSessionStorageExceptionOtherIOException() throws Exception {
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

    java.io.IOException exception =
        org.junit.jupiter.api.Assertions.assertThrows(
            java.io.IOException.class, () -> reader.readVectored(Arrays.asList(range), allocate));

    assertThat(exception).isNotInstanceOf(FileNotFoundException.class);
    assertThat(exception.getMessage()).contains("Failed to get BlobReadSession");
  }

  @Test
  void testBlobReadSessionTimeoutException() throws Exception {
    reset(sessionFuture);
    when(sessionFuture.get(anyLong(), any())).thenThrow(new TimeoutException("Timeout occurred"));

    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    java.io.IOException exception =
        org.junit.jupiter.api.Assertions.assertThrows(
            java.io.IOException.class, () -> reader.readVectored(Arrays.asList(range), allocate));

    assertThat(exception.getMessage())
        .contains("Failed to get BlobReadSession due to client timeout limit");
  }

  @Test
  void testReadVectored_afterClose_throwsClosedStateException() throws Exception {
    reader.close();

    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    IOException exception =
        org.junit.jupiter.api.Assertions.assertThrows(
            IOException.class, () -> reader.readVectored(Arrays.asList(range), allocate));

    assertThat(exception.getMessage()).contains("Reader is closed.");
    assertThat(range.getByteBufferFuture().isCompletedExceptionally()).isTrue();

    ExecutionException executionException =
        org.junit.jupiter.api.Assertions.assertThrows(
            ExecutionException.class, () -> range.getByteBufferFuture().get());
    assertThat(executionException.getCause()).isInstanceOf(IOException.class);
    assertThat(executionException.getCause().getMessage()).contains("Reader is closed.");
  }

  @Test
  void testReadVectored_nullAllocator_throwsNullPointerException() throws Exception {
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
        org.junit.jupiter.api.Assertions.assertThrows(ExecutionException.class, () -> future.get());
    assertThat(executionException.getCause()).isInstanceOf(NullPointerException.class);
    assertThat(executionException.getCause().getMessage())
        .contains("Allocator returned a null ByteBuffer!");
  }

  @Test
  void testClose_isIdempotent() throws Exception {
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
}
