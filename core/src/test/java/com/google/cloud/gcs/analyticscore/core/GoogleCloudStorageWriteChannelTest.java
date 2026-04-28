package com.google.cloud.gcs.analyticscore.core;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class GoogleCloudStorageWriteChannelTest {

  @Mock private Storage mockStorage;
  @Mock private BlobWriteSession mockBlobWriteSession;
  @Mock private WritableByteChannel mockInternalChannel;

  private BlobInfo blobInfo;
  private GcsWriteOptions writeOptions;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    blobInfo = BlobInfo.newBuilder(BlobId.of("test-bucket", "test-object")).build();
    // Assuming a basic builder exists for the options
    writeOptions = GcsWriteOptions.builder().setChecksumValidationEnabled(true).build();

    // Mock the Java SDK chain: storage.blobWriteSession(...) -> session.open() -> channel
    when(mockStorage.blobWriteSession(eq(blobInfo), any(BlobWriteOption[].class)))
        .thenReturn(mockBlobWriteSession);
    when(mockBlobWriteSession.open()).thenReturn(mockInternalChannel);
    when(mockInternalChannel.isOpen()).thenReturn(true);
  }

  @Test
  void testWriteDelegationAndByteTracking() throws Exception {
    // Arrange
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockStorage, blobInfo, writeOptions);
    
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5});
    when(mockInternalChannel.write(buffer)).thenReturn(5);

    // Act
    int bytesWritten = channel.write(buffer);

    // Assert
    assertThat(bytesWritten).isEqualTo(5);
    assertThat(channel.getBytesWritten()).isEqualTo(5);
    verify(mockInternalChannel).write(buffer);
  }

  @Test
  void testCloseDelegation() throws Exception {
    // Arrange
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockStorage, blobInfo, writeOptions);

    // Act
    channel.close();

    // Assert
    verify(mockInternalChannel).close();
    assertThat(channel.isOpen()).isFalse();
  }

  @Test
  void testIsOpenDelegation() throws Exception {
    // Arrange
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockStorage, blobInfo, writeOptions);

    // Act & Assert
    assertThat(channel.isOpen()).isTrue();
    
    when(mockInternalChannel.isOpen()).thenReturn(false);
    assertThat(channel.isOpen()).isFalse();
  }

  @Test
  void testWriteWhenClosedThrowsException() throws Exception {
    // Arrange
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockStorage, blobInfo, writeOptions);
    
    // Simulate the channel being closed
    when(mockInternalChannel.isOpen()).thenReturn(false);

    // Act & Assert
    assertThrows(
        ClosedChannelException.class,
        () -> channel.write(ByteBuffer.wrap(new byte[]{1, 2, 3})));
  }
}
