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
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.common.telemetry.MetricsRecorder;
import com.google.cloud.gcs.analyticscore.common.telemetry.OperationSupplier;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
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

class GcsWriteChannelTest {

  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_OBJECT = "test-object";

  @Mock private Telemetry mockTelemetry;
  @Mock private MetricsRecorder mockRecorder;
  @Mock private WritableByteChannel mockInternalChannel;

  private BlobInfo blobInfo;
  private GcsWriteOptions writeOptions;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    blobInfo = BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT)).build();
    writeOptions = GcsWriteOptions.builder().setChecksumValidationEnabled(true).build();

    // Mock telemetry to immediately execute the lambda function
    when(mockTelemetry.measure(anyString(), any(), anyMap(), any()))
        .thenAnswer(
            invocation -> {
              OperationSupplier<?, ?> supplier = invocation.getArgument(3);
              return supplier.get(mockRecorder);
            });

    when(mockInternalChannel.isOpen()).thenReturn(true);
  }

  @Test
  void testWriteDelegationAndByteTracking() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5});
    when(mockInternalChannel.write(buffer)).thenReturn(5);

    int bytesWritten = channel.write(buffer);

    assertThat(bytesWritten).isEqualTo(5);
    assertThat(channel.getBytesWritten()).isEqualTo(5L);
    verify(mockInternalChannel).write(buffer);
  }

  @Test
  void testWriteWhenClosedThrowsException() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);
    when(mockInternalChannel.isOpen()).thenReturn(false);

    assertThrows(
        ClosedChannelException.class, () -> channel.write(ByteBuffer.wrap(new byte[] {1, 2, 3})));
  }

  @Test
  void testWriteWithFakeStorage() throws Exception {
    Storage fakeStorage = LocalStorageHelper.getOptions().getService();
    BlobId blobId = BlobId.of("test-bucket", "test-write-object");
    BlobInfo bInfo = BlobInfo.newBuilder(blobId).build();
    Telemetry realTelemetry = new Telemetry(ImmutableList.of());
    WritableByteChannel internalChannel = fakeStorage.blobWriteSession(bInfo).open();
    GcsWriteChannel channel =
        new GcsWriteChannel(internalChannel, bInfo, writeOptions, realTelemetry);
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
    Storage fakeStorage = LocalStorageHelper.getOptions().getService();
    BlobId blobId = BlobId.of("test-bucket", "test-write-chunks-object");
    BlobInfo bInfo = BlobInfo.newBuilder(blobId).build();
    Telemetry realTelemetry = new Telemetry(ImmutableList.of());
    WritableByteChannel internalChannel = fakeStorage.blobWriteSession(bInfo).open();
    GcsWriteChannel channel =
        new GcsWriteChannel(internalChannel, bInfo, writeOptions, realTelemetry);
    byte[] data1 = new byte[] {1, 2, 3};
    byte[] data2 = new byte[] {4, 5};

    channel.write(ByteBuffer.wrap(data1));
    channel.write(ByteBuffer.wrap(data2));
    channel.close();

    byte[] readData = fakeStorage.readAllBytes(blobId);
    byte[] expectedData = new byte[] {1, 2, 3, 4, 5};
    assertThat(readData).isEqualTo(expectedData);
  }

  @Test
  void testExceptionTranslation_AccessDeniedOnWrite() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e403 = new StorageException(403, "Forbidden");

    when(mockInternalChannel.write(any(ByteBuffer.class))).thenThrow(e403);

    assertThrows(AccessDeniedException.class, () -> channel.write(buffer));
  }

  @Test
  void testExceptionTranslation_NotFoundOnWrite() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e404 = new StorageException(404, "Not Found");

    when(mockInternalChannel.write(any(ByteBuffer.class))).thenThrow(e404);

    assertThrows(FileNotFoundException.class, () -> channel.write(buffer));
  }

  @Test
  void testExceptionTranslation_NotFoundOnClose() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);
    StorageException e404 = new StorageException(404, "Not Found");
    doThrow(e404).when(mockInternalChannel).close();

    assertThrows(FileNotFoundException.class, channel::close);
  }

  @Test
  void testWrite_GenericStorageException() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e500 = new StorageException(500, "Internal Server Error");
    when(mockInternalChannel.write(any(ByteBuffer.class))).thenThrow(e500);

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown.getCause().getMessage()).contains("Internal Server Error");
  }

  @Test
  void testClose_GenericStorageException() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);
    StorageException e500 = new StorageException(500, "Internal Server Error");
    doThrow(e500).when(mockInternalChannel).close();

    IOException thrown = assertThrows(IOException.class, channel::close);

    assertThat(thrown.getCause()).hasMessageThat().contains("Internal Server Error");
  }

  @Test
  void testClose_IOException() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);
    doThrow(new IOException("Generic Close Error")).when(mockInternalChannel).close();

    IOException thrown = assertThrows(IOException.class, channel::close);

    assertThat(thrown).hasMessageThat().contains("Generic Close Error");
  }

  @Test
  void testWrite_IOException() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    when(mockInternalChannel.write(any(ByteBuffer.class)))
        .thenThrow(new IOException("Generic I/O Error"));

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown).hasMessageThat().contains("Generic I/O Error");
  }

  @Test
  void testClose_AlreadyClosed() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);

    channel.close(); // First close executes normally
    channel.close(); // Second close should trigger the early return branch

    verify(mockInternalChannel, times(1)).close();
  }

  @Test
  void testExceptionTranslation_AccessDeniedOnWrite_Wrapped() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e403 = new StorageException(403, "Forbidden");
    IOException wrappedException = new IOException("Wrapper exception", e403);

    when(mockInternalChannel.write(any(ByteBuffer.class))).thenThrow(wrappedException);

    assertThrows(AccessDeniedException.class, () -> channel.write(buffer));
  }

  @Test
  void testExceptionTranslation_NotFoundOnWrite_Wrapped() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e404 = new StorageException(404, "Not Found");
    IOException wrappedException = new IOException("Wrapper exception", e404);

    when(mockInternalChannel.write(any(ByteBuffer.class))).thenThrow(wrappedException);

    assertThrows(FileNotFoundException.class, () -> channel.write(buffer));
  }

  @Test
  void testExceptionTranslation_NotFoundOnClose_Wrapped() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);
    StorageException e404 = new StorageException(404, "Not Found");
    IOException wrappedException = new IOException("Wrapper exception", e404);
    doThrow(wrappedException).when(mockInternalChannel).close();

    assertThrows(FileNotFoundException.class, channel::close);
  }

  @Test
  void testWrite_GenericIOException_RethrownDirectly() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    IOException genericException = new IOException("Connection reset by peer");
    when(mockInternalChannel.write(any(ByteBuffer.class))).thenThrow(genericException);

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown).isSameInstanceAs(genericException);
  }

  @Test
  void testWrite_RuntimeException_PropagatedUnchanged() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    NullPointerException npe = new NullPointerException("Simulated NPE");
    when(mockInternalChannel.write(any(ByteBuffer.class))).thenThrow(npe);

    NullPointerException thrown =
        assertThrows(NullPointerException.class, () -> channel.write(buffer));

    assertThat(thrown).isSameInstanceAs(npe);
  }

  @Test
  void testClose_RuntimeException_PropagatedUnchanged() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);
    NullPointerException npe = new NullPointerException("Simulated NPE during close");
    doThrow(npe).when(mockInternalChannel).close();

    NullPointerException thrown = assertThrows(NullPointerException.class, channel::close);

    assertThat(thrown).isSameInstanceAs(npe);
  }

  @Test
  void testClose_ClosesTemporaryStorageResource() throws Exception {
    AutoCloseable mockResource = mock(AutoCloseable.class);
    GcsWriteChannel channel =
        new GcsWriteChannel(
            mockInternalChannel, blobInfo, writeOptions, mockTelemetry, mockResource);

    channel.close();

    verify(mockResource, times(1)).close();
  }

  @Test
  void testClose_CloseFailureOnTemporaryStorage_DoesNotThrow() throws Exception {
    AutoCloseable mockResource = mock(AutoCloseable.class);
    doThrow(new Exception("Failed to close resource")).when(mockResource).close();
    GcsWriteChannel channel =
        new GcsWriteChannel(
            mockInternalChannel, blobInfo, writeOptions, mockTelemetry, mockResource);

    // Should not throw exception
    channel.close();

    verify(mockResource, times(1)).close();
    verify(mockInternalChannel, times(1)).close();
  }

  @Test
  void testExceptionTranslation_AlreadyExistsOnWrite() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e409 = new StorageException(409, "Conflict");

    when(mockInternalChannel.write(any(ByteBuffer.class))).thenThrow(e409);

    assertThrows(FileAlreadyExistsException.class, () -> channel.write(buffer));
  }

  @Test
  void testExceptionTranslation_PreconditionFailed_OverwriteDisabled() throws Exception {
    GcsWriteOptions overwriteDisabledOptions =
        GcsWriteOptions.builder().setOverwriteExisting(false).build();
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, overwriteDisabledOptions, mockTelemetry);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockInternalChannel.write(any(ByteBuffer.class))).thenThrow(e412);

    assertThrows(FileAlreadyExistsException.class, () -> channel.write(buffer));
  }

  @Test
  void testExceptionTranslation_PreconditionFailed_GenerationMismatch() throws Exception {
    BlobInfo blobInfoWithGen =
        BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT, 123L)).build();
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfoWithGen, writeOptions, mockTelemetry);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockInternalChannel.write(any(ByteBuffer.class))).thenThrow(e412);

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown).hasMessageThat().contains("Generation mismatch");
  }

  @Test
  void testIsOpen_AfterClose_ReturnsFalse() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);

    assertThat(channel.isOpen()).isTrue();

    channel.close();

    assertThat(channel.isOpen()).isFalse();
  }

  @Test
  void testExceptionTranslation_PreconditionFailed_Fallback() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, writeOptions, mockTelemetry);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockInternalChannel.write(any(ByteBuffer.class))).thenThrow(e412);

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown).hasMessageThat().contains("Error during write to GCS");
  }

  @Test
  void testExceptionTranslation_PreconditionFailed_NullOptions() throws Exception {
    GcsWriteChannel channel =
        new GcsWriteChannel(mockInternalChannel, blobInfo, null, mockTelemetry);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockInternalChannel.write(any(ByteBuffer.class))).thenThrow(e412);

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown).hasMessageThat().contains("Error during write to GCS");
  }
}
