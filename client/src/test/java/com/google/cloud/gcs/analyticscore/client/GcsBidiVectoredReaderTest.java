package com.google.cloud.gcs.analyticscore.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.ZeroCopySupport.DisposableByteString;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
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
    
    reader = new GcsBidiVectoredReader(storage, itemId, directExecutor);
  }

  @Test
  void testReadVectored_Success() throws Exception {
    GcsObjectRange range = GcsObjectRange.builder()
        .setOffset(0)
        .setLength(10)
        .setByteBufferFuture(new CompletableFuture<>())
        .build();
        
    byte[] data = new byte[10];
    Arrays.fill(data, (byte) 1);
    ByteString byteString = ByteString.copyFrom(data);
    
    when(blobReadSession.readAs(any())).thenReturn(ApiFutures.immediateFuture(disposableByteString));
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
    GcsObjectRange range = GcsObjectRange.builder()
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
}
