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
import com.google.cloud.gcs.analyticscore.client.GcsWriteOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.MetricsRecorder;
import com.google.cloud.gcs.analyticscore.common.telemetry.OperationSupplier;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class GoogleCloudStorageWriteChannelTest {

  @Mock private GcsFileSystem mockGcsFileSystem;
  @Mock private Telemetry mockTelemetry;
  @Mock private MetricsRecorder mockRecorder;
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
    when(mockTelemetry.measure(anyString(), any(), anyMap(), any()))
        .thenAnswer(
            invocation -> {
              OperationSupplier<?, ?> supplier = invocation.getArgument(3);
              return supplier.get(mockRecorder);
            });

    // Mock the filesystem factory method instead of the SDK
    when(mockGcsFileSystem.create(eq(blobInfo), eq(writeOptions))).thenReturn(mockInternalChannel);
    when(mockInternalChannel.isOpen()).thenReturn(true);
  }

  @Test
  void testWriteDelegationAndByteTracking() throws Exception {
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5});
    when(mockInternalChannel.write(buffer)).thenReturn(5);

    int bytesWritten = channel.write(buffer);

    assertThat(bytesWritten).isEqualTo(5);
    assertThat(channel.getBytesWritten()).isEqualTo(5L);
    verify(mockInternalChannel).write(buffer);
  }

  @Test
  void testWriteWhenClosedThrowsException() throws Exception {
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, blobInfo, writeOptions);
    when(mockInternalChannel.isOpen()).thenReturn(false);

    assertThrows(
        ClosedChannelException.class, () -> channel.write(ByteBuffer.wrap(new byte[] {1, 2, 3})));
  }

  @Test
  void testWriteWithFakeStorage() throws Exception {
    Storage fakeStorage = FakeGcsClientImpl.storage;
    BlobId blobId = BlobId.of("test-bucket", "test-write-object");
    BlobInfo bInfo = BlobInfo.newBuilder(blobId).build();
    GcsFileSystem fakeGcsFileSystem = mockGcsFileSystem;
    Telemetry realTelemetry = new Telemetry(ImmutableList.of());
    when(fakeGcsFileSystem.getTelemetry()).thenReturn(realTelemetry);

    // Tell our mock file system to return the Fake SDK channel for this specific test
    when(fakeGcsFileSystem.create(eq(bInfo), eq(writeOptions)))
        .thenReturn(fakeStorage.blobWriteSession(bInfo).open());

    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(fakeGcsFileSystem, bInfo, writeOptions);
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

    when(fakeGcsFileSystem.create(eq(bInfo), eq(writeOptions)))
        .thenReturn(fakeStorage.blobWriteSession(bInfo).open());

    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(fakeGcsFileSystem, bInfo, writeOptions);
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
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e403 = new StorageException(403, "Forbidden");

    when(mockInternalChannel.write(any(ByteBuffer.class))).thenThrow(e403);

    assertThrows(AccessDeniedException.class, () -> channel.write(buffer));
  }

  @Test
  void testExceptionTranslation_NotFoundOnWrite() throws Exception {
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e404 = new StorageException(404, "Not Found");

    when(mockInternalChannel.write(any(ByteBuffer.class))).thenThrow(e404);

    assertThrows(FileNotFoundException.class, () -> channel.write(buffer));
  }

  @Test
  void testExceptionTranslation_NotFoundOnClose() throws Exception {
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, blobInfo, writeOptions);
    StorageException e404 = new StorageException(404, "Not Found");
    doThrow(e404).when(mockInternalChannel).close();

    assertThrows(FileNotFoundException.class, channel::close);
  }

  @Test
  void testWrite_GenericStorageException() throws Exception {
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});

    StorageException e500 = new StorageException(500, "Internal Server Error");
    when(mockInternalChannel.write(any(ByteBuffer.class))).thenThrow(e500);

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));
    assertThat(thrown).hasMessageThat().contains("Error writing to GCS");
  }

  @Test
  void testClose_GenericStorageException() throws Exception {
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, blobInfo, writeOptions);
    StorageException e500 = new StorageException(500, "Internal Server Error");
    doThrow(e500).when(mockInternalChannel).close();

    IOException thrown = assertThrows(IOException.class, channel::close);
    assertThat(thrown).hasMessageThat().contains("Upload failed for");
  }

  @Test
  void testClose_IOException() throws Exception {
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, blobInfo, writeOptions);
    doThrow(new IOException("Generic Close Error")).when(mockInternalChannel).close();

    IOException thrown = assertThrows(IOException.class, channel::close);

    assertThat(thrown).hasMessageThat().contains("Upload failed for");
  }

  @Test
  void testWrite_IOException() throws Exception {
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    when(mockInternalChannel.write(any(ByteBuffer.class)))
        .thenThrow(new IOException("Generic I/O Error"));

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown).hasMessageThat().contains("Error writing to GCS");
  }

  @Test
  void testClose_AlreadyClosed() throws Exception {
    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(mockGcsFileSystem, blobInfo, writeOptions);

    channel.close(); // First close executes normally
    channel.close(); // Second close should trigger the early return branch

    verify(mockInternalChannel, times(1)).close();
  }
}
