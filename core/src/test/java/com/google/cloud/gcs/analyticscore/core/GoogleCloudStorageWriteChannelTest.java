package com.google.cloud.gcs.analyticscore.core;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.client.FakeGcsClientImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.common.telemetry.MetricsRecorder;
import com.google.cloud.gcs.analyticscore.common.telemetry.OperationSupplier;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class GoogleCloudStorageWriteChannelTest {

  @Mock private GcsFileSystem mockGcsFileSystem;
  @Mock private Telemetry mockTelemetry;
  @Mock private MetricsRecorder mockRecorder;
  @Mock private Storage mockStorage;
  @Mock private BlobWriteSession mockBlobWriteSession;
  @Mock private WritableByteChannel mockInternalChannel;

  private BlobInfo blobInfo;
  private GcsWriteOptions writeOptions;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    blobInfo = BlobInfo.newBuilder(BlobId.of("test-bucket", "test-object")).build();
    writeOptions = GcsWriteOptions.builder().setChecksumValidationEnabled(true).build();
    // Mock telemetry to immediately execute the lambda function
    when(mockGcsFileSystem.getTelemetry()).thenReturn(mockTelemetry);
    when(mockTelemetry.measure(anyString(), any(), anyMap(), any())).thenAnswer(invocation -> {
      OperationSupplier<?, ?> supplier = invocation.getArgument(3);
      return supplier.get(mockRecorder);
    });
    // Mock the standard Java SDK initialization chain
    when(mockStorage.blobWriteSession(eq(blobInfo), any(BlobWriteOption[].class)))
        .thenReturn(mockBlobWriteSession);
    when(mockBlobWriteSession.open()).thenReturn(mockInternalChannel);
    when(mockInternalChannel.isOpen()).thenReturn(true);
  }

  @Test
  void testWriteDelegationAndByteTracking() throws Exception {
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, mockStorage, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5});
    when(mockInternalChannel.write(buffer)).thenReturn(5);

    int bytesWritten = channel.write(buffer);

    assertThat(bytesWritten).isEqualTo(5);
    assertThat(channel.getBytesWritten()).isEqualTo(5L);
    verify(mockInternalChannel).write(buffer);
  }

  @Test
  void testExceptionTranslation_FileAlreadyExistsOnCreate() throws Exception {
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockBlobWriteSession.open()).thenThrow(e412);

    assertThrows(
        FileAlreadyExistsException.class,
        () -> new GoogleCloudStorageWriteChannel(mockGcsFileSystem, mockStorage, blobInfo, writeOptions));
  }

  @Test
  void testExceptionTranslation_AccessDeniedOnWrite() throws Exception {
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, mockStorage, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    
    StorageException e403 = new StorageException(403, "Forbidden");
    when(mockInternalChannel.write(buffer)).thenThrow(e403);

    assertThrows(AccessDeniedException.class, () -> channel.write(buffer));
  }

  @Test
  void testExceptionTranslation_NotFoundOnClose() throws Exception {
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, mockStorage, blobInfo, writeOptions);
    StorageException e404 = new StorageException(404, "Not Found");
    doThrow(e404).when(mockInternalChannel).close();

    assertThrows(FileNotFoundException.class, channel::close);
  }

  @Test
  void testWriteWhenClosedThrowsException() throws Exception {
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, mockStorage, blobInfo, writeOptions);
    when(mockInternalChannel.isOpen()).thenReturn(false);

    assertThrows(
        ClosedChannelException.class,
        () -> channel.write(ByteBuffer.wrap(new byte[]{1, 2, 3})));
  }

  @Test
  void testWriteWithFakeStorage() throws Exception {
    Storage fakeStorage = FakeGcsClientImpl.storage;
    BlobId blobId = BlobId.of("test-bucket", "test-write-object");
    BlobInfo bInfo = BlobInfo.newBuilder(blobId).build();    
    GcsFileSystem fakeGcsFileSystem = mockGcsFileSystem;
    Telemetry realTelemetry = new Telemetry(ImmutableList.of());
    when(fakeGcsFileSystem.getTelemetry()).thenReturn(realTelemetry);
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(fakeGcsFileSystem, fakeStorage, bInfo, writeOptions);
    byte[] data = new byte[] {1, 2, 3, 4, 5};
    ByteBuffer buffer = ByteBuffer.wrap(data);

    int bytesWritten = channel.write(buffer);
    channel.close();

    assertThat(bytesWritten).isEqualTo(5);
    byte[] readData = fakeStorage.readAllBytes(blobId);
    assertThat(readData).isEqualTo(data);
  }

  @Test
  void testWriteChunksWithFakeStorage() throws Exception {
    Storage fakeStorage = FakeGcsClientImpl.storage;
    BlobId blobId = BlobId.of("test-bucket", "test-write-chunks-object");
    BlobInfo bInfo = BlobInfo.newBuilder(blobId).build();
    GcsFileSystem fakeGcsFileSystem = mockGcsFileSystem;
    Telemetry realTelemetry = new Telemetry(ImmutableList.of());
    when(fakeGcsFileSystem.getTelemetry()).thenReturn(realTelemetry);
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(fakeGcsFileSystem, fakeStorage, bInfo, writeOptions);
    byte[] data1 = new byte[] {1, 2, 3};
    byte[] data2 = new byte[] {4, 5};
    ByteBuffer buffer1 = ByteBuffer.wrap(data1);
    ByteBuffer buffer2 = ByteBuffer.wrap(data2);
  
    // write chunk1
    channel.write(buffer1);
    //write chunk 2
    channel.write(buffer2);
    channel.close();

    byte[] readData = fakeStorage.readAllBytes(blobId);
    byte[] expectedData = new byte[] {1, 2, 3, 4, 5};
    assertThat(readData).isEqualTo(expectedData);
  }

  @Test
  void testWrite_GenericException() throws Exception {
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, mockStorage, blobInfo, writeOptions);
    
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    
    // Simulate a non-StorageException (e.g., a RuntimeException from the SDK or network)
    // to hit the `catch (Exception e)` fallback branch in write()
    when(mockInternalChannel.write(buffer)).thenThrow(new RuntimeException("Generic Network Error"));

    assertThrows(IOException.class, () -> channel.write(buffer));
  }

  @Test
  void testClose_GenericException() throws Exception {
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, mockStorage, blobInfo, writeOptions);
    
    // Simulate a non-StorageException during close to hit the `catch (Exception e)` fallback branch
    doThrow(new RuntimeException("Generic Close Error")).when(mockInternalChannel).close();

    assertThrows(IOException.class, channel::close);
  }

  @Test
  void testClose_AlreadyClosed() throws Exception {
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, mockStorage, blobInfo, writeOptions);
    
    channel.close(); // First close executes normally
    channel.close(); // Second close should trigger the early return branch: `if (closed) return;`

    // Verify the internal channel was only closed once despite calling close() twice
    verify(mockInternalChannel, times(1)).close();
  }

  @Test
  void testExceptionTranslation_NotFoundOnWrite() throws Exception {
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, mockStorage, blobInfo, writeOptions);
    
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    
    // Simulate a 404 during write (e.g. bucket deleted mid-flight)
    StorageException e404 = new StorageException(404, "Not Found");
    when(mockInternalChannel.write(buffer)).thenThrow(e404);

    assertThrows(FileNotFoundException.class, () -> channel.write(buffer));
  }

  @Test
  void testExceptionTranslation_AccessDeniedOnCreate() throws Exception {
    // Simulate 403 Forbidden during initialization
    StorageException e403 = new StorageException(403, "Forbidden");
    when(mockBlobWriteSession.open()).thenThrow(e403);

    assertThrows(
        AccessDeniedException.class,
        () -> new GoogleCloudStorageWriteChannel(mockGcsFileSystem, mockStorage, blobInfo, writeOptions));
  }

  @Test
  void testGenerateWriteOptions_NullOptions() throws Exception {
    // Tests the early return branch `if (writeOptions == null)` in generateWriteOptions
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, mockStorage, blobInfo, null);
    
    assertThat(channel.isOpen()).isTrue();
  }

  @Test
  void testGenerateWriteOptions_AllOptionsEnabled() throws Exception {
    // This single test hits EVERY setter in GcsWriteOptions.Builder (boosting it to ~100% coverage)
    // AND hits every 'if' branch inside the `generateWriteOptions` method!
    GcsWriteOptions allOptions = GcsWriteOptions.builder()
        .setChecksumValidationEnabled(true)
        .setDisableGzipContent(true)
        .setOverwriteExisting(false)
        .setUploadChunkSize(1024)
        .setUploadType(GcsWriteOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD)
        .setPcuBufferCount(2)
        .setPcuBufferCapacity(2048)
        .setKmsKeyName("kms-key")
        .setEncryptionKey("MDEyMzQ1Njc4OUFCQ0RFRkdISUpLTE1OT1BRUlNUVVU=")
        .setUserProject("user-project")
        .build();
        
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, mockStorage, blobInfo, allOptions);
    
    assertThat(channel.isOpen()).isTrue();
  }

  @Test
  void testGenerateWriteOptions_WithGenerationId() throws Exception {
    // Tests the `if (blobInfo.getBlobId().getGeneration() != null)` branch in generateWriteOptions
    BlobInfo blobInfoWithGen = BlobInfo.newBuilder(BlobId.of("test-bucket", "test-object", 12345L)).build();
    
    when(mockStorage.blobWriteSession(eq(blobInfoWithGen), any(BlobWriteOption[].class)))
        .thenReturn(mockBlobWriteSession);
        
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, mockStorage, blobInfoWithGen, writeOptions);
        
    assertThat(channel.isOpen()).isTrue();
  }
}
