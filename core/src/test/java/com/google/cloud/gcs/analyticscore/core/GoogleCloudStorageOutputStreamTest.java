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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cloud.gcs.analyticscore.client.FakeGcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsWriteChannel;
import com.google.cloud.gcs.analyticscore.client.GcsWriteOptions;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class GoogleCloudStorageOutputStreamTest {

  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_OBJECT = "test-object";
  private final BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT)).build();
  private final GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();

  @Mock private GcsFileSystem mockFileSystem;
  @Mock private WritableByteChannel mockChannel;
  @Mock private GcsWriteChannel mockGcsWriteChannel;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  void create_initializesWriteSessionAndReturnsStream() throws IOException {
    when(mockFileSystem.create(eq(blobInfo), eq(writeOptions))).thenReturn(mockChannel);

    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(mockFileSystem, blobInfo, writeOptions);

    assertThat(stream).isNotNull();
    verify(mockFileSystem).create(blobInfo, writeOptions);
  }

  @Test
  void create_nullFileSystem_throwsIllegalStateException() {
    var exception =
        assertThrows(
            IllegalStateException.class,
            () -> GoogleCloudStorageOutputStream.create(null, blobInfo, writeOptions));

    assertThat(exception).hasMessageThat().isEqualTo("GcsFileSystem shouldn't be null");
  }

  @Test
  void create_nullBlobInfo_throwsIllegalStateException() {
    var exception =
        assertThrows(
            IllegalStateException.class,
            () -> GoogleCloudStorageOutputStream.create(mockFileSystem, null, writeOptions));

    assertThat(exception).hasMessageThat().isEqualTo("BlobInfo shouldn't be null");
  }

  @Test
  void create_nullWriteOptions_throwsIllegalStateException() {
    var exception =
        assertThrows(
            IllegalStateException.class,
            () -> GoogleCloudStorageOutputStream.create(mockFileSystem, blobInfo, null));

    assertThat(exception).hasMessageThat().isEqualTo("GcsWriteOptions shouldn't be null");
  }

  @Test
  void write_singleByte_writesToChannel() throws IOException {
    when(mockFileSystem.create(eq(blobInfo), eq(writeOptions))).thenReturn(mockChannel);
    when(mockChannel.write(any(ByteBuffer.class))).thenReturn(1);
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(mockFileSystem, blobInfo, writeOptions);

    stream.write(65); // 'A'

    ArgumentCaptor<ByteBuffer> bufferCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
    verify(mockChannel).write(bufferCaptor.capture());
    ByteBuffer capturedBuffer = bufferCaptor.getValue();
    assertThat(capturedBuffer.limit()).isEqualTo(1);
    assertThat(capturedBuffer.get(0)).isEqualTo((byte) 65);
  }

  @Test
  void write_byteArray_writesToChannel() throws IOException {
    when(mockFileSystem.create(eq(blobInfo), eq(writeOptions))).thenReturn(mockChannel);
    when(mockChannel.write(any(ByteBuffer.class))).thenReturn(5);
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(mockFileSystem, blobInfo, writeOptions);

    byte[] data = new byte[] {1, 2, 3, 4, 5};
    stream.write(data, 1, 3); // Writes {2, 3, 4}

    ArgumentCaptor<ByteBuffer> bufferCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
    verify(mockChannel).write(bufferCaptor.capture());
    ByteBuffer capturedBuffer = bufferCaptor.getValue();
    assertThat(capturedBuffer.position()).isEqualTo(1);
    assertThat(capturedBuffer.limit()).isEqualTo(4);
    assertThat(capturedBuffer.get(1)).isEqualTo((byte) 2);
    assertThat(capturedBuffer.get(2)).isEqualTo((byte) 3);
    assertThat(capturedBuffer.get(3)).isEqualTo((byte) 4);
  }

  @Test
  void write_byteArray_invalidArgs_throwsExceptions() throws IOException {
    when(mockFileSystem.create(eq(blobInfo), eq(writeOptions))).thenReturn(mockChannel);
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(mockFileSystem, blobInfo, writeOptions);
    byte[] data = new byte[] {1, 2, 3};

    assertThrows(NullPointerException.class, () -> stream.write(null, 0, 1));
    assertThrows(IndexOutOfBoundsException.class, () -> stream.write(data, -1, 2));
    assertThrows(IndexOutOfBoundsException.class, () -> stream.write(data, 0, -1));
    assertThrows(IndexOutOfBoundsException.class, () -> stream.write(data, 2, 2));
  }

  @Test
  void write_byteArray_zeroLength_doesNothing() throws IOException {
    when(mockFileSystem.create(eq(blobInfo), eq(writeOptions))).thenReturn(mockChannel);
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(mockFileSystem, blobInfo, writeOptions);
    byte[] data = new byte[] {1, 2, 3};

    stream.write(data, 0, 0);

    verify(mockChannel, never()).write(any(ByteBuffer.class));
  }

  @Test
  void close_closesChannel() throws IOException {
    when(mockFileSystem.create(eq(blobInfo), eq(writeOptions))).thenReturn(mockChannel);
    when(mockChannel.isOpen()).thenReturn(true);
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(mockFileSystem, blobInfo, writeOptions);

    stream.close();

    verify(mockChannel).close();
  }

  @Test
  void close_whenChannelAlreadyClosed_isIdempotent() throws IOException {
    when(mockFileSystem.create(eq(blobInfo), eq(writeOptions))).thenReturn(mockChannel);
    when(mockChannel.isOpen()).thenReturn(false);

    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(mockFileSystem, blobInfo, writeOptions);
    stream.close();

    verify(mockChannel, never()).close();
  }

  @Test
  void getBytesWritten_delegatesToGcsWriteChannel() throws IOException {
    when(mockFileSystem.create(eq(blobInfo), eq(writeOptions))).thenReturn(mockGcsWriteChannel);
    when(mockGcsWriteChannel.getBytesWritten()).thenReturn(100L);

    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(mockFileSystem, blobInfo, writeOptions);

    assertThat(stream.getBytesWritten()).isEqualTo(100L);
    verify(mockGcsWriteChannel).getBytesWritten();
  }

  @Test
  void getBytesWritten_nonGcsWriteChannel_returnsZero() throws IOException {
    when(mockFileSystem.create(eq(blobInfo), eq(writeOptions))).thenReturn(mockChannel);

    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(mockFileSystem, blobInfo, writeOptions);

    assertThat(stream.getBytesWritten()).isEqualTo(0L);
  }

  @Test
  void write_withFakeGcsFileSystem_writesDataCorrectly() throws IOException {
    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(Map.of(), "");
    FakeGcsFileSystemImpl fakeGcsFileSystem = new FakeGcsFileSystemImpl(options);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(itemId.getBucketName(), itemId.getObjectName().get())).build();

    try (GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeGcsFileSystem, blobInfo, writeOptions)) {
      stream.write("hello fake world".getBytes(UTF_8));
    }

    // Verify the data is present in the fake file system
    GcsFileInfo fileInfo = fakeGcsFileSystem.getFileInfo(itemId);
    assertThat(fileInfo).isNotNull();
    assertThat(fileInfo.getItemInfo().getSize()).isEqualTo("hello fake world".length());

    // Read the data back using GoogleCloudStorageInputStream to verify
    try (GoogleCloudStorageInputStream inputStream =
        GoogleCloudStorageInputStream.create(fakeGcsFileSystem, itemId)) {
      byte[] buffer = new byte[50];
      int read = inputStream.read(buffer, 0, buffer.length);
      assertThat(new String(buffer, 0, read, UTF_8))
          .isEqualTo("hello fake world");
    }
  }

  @Test
  void write_multipleChunksAndSingleBytes_withFakeGcsFileSystem() throws IOException {
    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(Map.of(), "");
    FakeGcsFileSystemImpl fakeGcsFileSystem = new FakeGcsFileSystemImpl(options);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(itemId.getBucketName(), itemId.getObjectName().get())).build();
    int valA = 65; // 'A'
    byte[] chunk1 = "hello".getBytes(UTF_8);
    int valB = 66; // 'B'
    byte[] chunk2 = "world".getBytes(UTF_8);
    int chunk2Offset = 0;
    int chunk2Length = 3; // "wor"

    ByteArrayOutputStream expectedStream = new ByteArrayOutputStream();
    expectedStream.write(valA);
    expectedStream.write(chunk1, 0, chunk1.length);
    expectedStream.write(valB);
    expectedStream.write(chunk2, chunk2Offset, chunk2Length);
    byte[] expectedBytes = expectedStream.toByteArray();

    try (GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeGcsFileSystem, blobInfo, writeOptions)) {
      stream.write(valA);
      stream.write(chunk1);
      stream.write(valB);
      stream.write(chunk2, chunk2Offset, chunk2Length);
    }

    // Verify the data is present in the fake file system
    GcsFileInfo fileInfo = fakeGcsFileSystem.getFileInfo(itemId);
    assertThat(fileInfo).isNotNull();
    assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(expectedBytes.length);

    try (GoogleCloudStorageInputStream inputStream =
        GoogleCloudStorageInputStream.create(fakeGcsFileSystem, itemId)) {
      byte[] buffer = new byte[expectedBytes.length + 10];
      int read = inputStream.read(buffer, 0, buffer.length);
      assertThat(read).isEqualTo(expectedBytes.length);
      byte[] actualBytes = Arrays.copyOf(buffer, read);
      assertThat(actualBytes).isEqualTo(expectedBytes);
    }
  }
}
