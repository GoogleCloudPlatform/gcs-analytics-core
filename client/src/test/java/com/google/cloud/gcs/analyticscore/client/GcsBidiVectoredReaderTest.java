package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.ZeroCopySupport.DisposableByteString;
import com.google.protobuf.ByteString;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class GcsBidiVectoredReaderTest {

  @Mock private Storage storage;
  @Mock private BlobReadSession blobReadSession;
  @Mock private ApiFuture<BlobReadSession> sessionFuture;
  @Mock private DisposableByteString disposableByteString;

  private GcsItemId itemId;
  private GcsBidiVectoredReader reader;
  private ExecutorService directExecutor;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    itemId = GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    directExecutor = com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService();

    when(storage.blobReadSession(any(BlobId.class))).thenReturn(sessionFuture);
    when(sessionFuture.get(anyLong(), any())).thenReturn(blobReadSession);

    reader = new GcsBidiVectoredReader(storage, itemId, directExecutor, 10);
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
    org.junit.jupiter.api.Assertions.assertThrows(
        NullPointerException.class,
        () -> new GcsBidiVectoredReader(storage, null, directExecutor, 10));
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
  void testBlobReadSessionStorageException404FileNotFoundException() throws Exception {
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
  void testBlobReadSessionRespectsCustomTimeout() throws Exception {
    long customTimeout = 45L;
    GcsBidiVectoredReader customReader =
        new GcsBidiVectoredReader(storage, itemId, directExecutor, customTimeout);

    reset(sessionFuture);
    when(sessionFuture.get(eq(customTimeout), eq(TimeUnit.SECONDS))).thenReturn(blobReadSession);

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

    customReader.readVectored(Arrays.asList(range), allocate);

    verify(sessionFuture, times(1)).get(eq(customTimeout), eq(TimeUnit.SECONDS));
  }
}
