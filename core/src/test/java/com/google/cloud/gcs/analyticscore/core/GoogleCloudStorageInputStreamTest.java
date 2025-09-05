/*
 * Copyright 2025 Google LLC
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
import static org.mockito.Mockito.*;

import com.google.cloud.gcs.analyticscore.client.*;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class GoogleCloudStorageInputStreamTest {

  private final URI testUri = URI.create("gs://test-bucket/test-object");
  @Mock private VectoredSeekableByteChannel mockChannel;
  @Mock private GcsFileSystem mockFileSystem;
  @Mock private GcsFileSystemOptions mockFileSystemOptions;
  @Mock private GcsClientOptions mockClientOptions;
  @Mock private GcsFileInfo mockGcsFileInfo;
  private GoogleCloudStorageInputStream googleCloudStorageInputStream;

  @BeforeEach
  void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    when(mockFileSystem.getFileSystemOptions()).thenReturn(mockFileSystemOptions);
    when(mockFileSystemOptions.getGcsClientOptions()).thenReturn(mockClientOptions);
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.getFileInfo(testUri)).thenReturn(mockGcsFileInfo);
    when(mockGcsFileInfo.getUri()).thenReturn(testUri);
    when(mockFileSystem.open(mockGcsFileInfo, readOptions)).thenReturn(mockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
  }

  @Test
  void create_usesFileSystemOptions_callsGetFileInfoAndOpen() throws IOException {
    GcsReadOptions readOptions = GcsReadOptions.builder().build();

    verify(mockFileSystem).getFileInfo(testUri);
    verify(mockFileSystem).open(mockGcsFileInfo, readOptions);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void create_nullFileSystem_throwsIllegalStateException() {
    var exception =
        assertThrows(
            IllegalStateException.class, () -> GoogleCloudStorageInputStream.create(null, testUri));

    assertThat(exception).hasMessageThat().isEqualTo("GcsFileSystem shouldn't be null");
  }

  @Test
  void create_whenGetFileInfoFails_propagatesException() throws IOException {
    IOException simulatedException = new IOException("Object does not exist");
    when(mockFileSystem.getFileInfo(testUri)).thenThrow(simulatedException);

    IOException thrown =
        assertThrows(
            IOException.class, () -> GoogleCloudStorageInputStream.create(mockFileSystem, testUri));
    assertThat(thrown).isSameInstanceAs(simulatedException);
  }

  @Test
  void getPos_onNewStream_returnsInitialPosition() throws IOException {
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void seek_updatesPositionAndUnderlyingChannel() throws IOException {
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

    googleCloudStorageInputStream.seek(123L);

    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(123L);
    verify(mockChannel).position(123L);
  }

  @Test
  void seek_withNegativePosition_throwsIllegalArgumentException() {
    var exception =
        assertThrows(IllegalArgumentException.class, () -> googleCloudStorageInputStream.seek(-1L));

    assertThat(exception).hasMessageThat().contains("position can't be negative: -1");
  }

  @Test
  void seek_afterClose_throwsIOException() throws IOException {
    googleCloudStorageInputStream.close();

    var exception = assertThrows(IOException.class, () -> googleCloudStorageInputStream.seek(10));

    assertThat(exception).hasMessageThat().isEqualTo(testUri + ": Cannot seek: already closed");
  }

  @Test
  void seek_whenChannelThrowsError_propagatesException() throws IOException {
    doThrow(new IOException("Simulated channel position error")).when(mockChannel).position(100);

    var exception = assertThrows(IOException.class, () -> googleCloudStorageInputStream.seek(100));
    assertThat(exception).hasMessageThat().isEqualTo("Simulated channel position error");
  }

  @Test
  void read_singleByte_readsFromChannelAndUpdatesPosition() throws IOException {
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            inv -> {
              inv.<ByteBuffer>getArgument(0).put((byte) 77);
              return 1;
            });

    int result = googleCloudStorageInputStream.read();

    assertThat(result).isEqualTo(77);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(1);
  }

  @Test
  void read_singleByteAtEOF_returnsMinusOneAndDoesNotUpdatePosition() throws IOException {
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(-1);

    int result = googleCloudStorageInputStream.read();

    assertThat(result).isEqualTo(-1);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void read_singleByteAfterClose_throwsIOException() throws IOException {
    googleCloudStorageInputStream.close();

    var exception = assertThrows(IOException.class, () -> googleCloudStorageInputStream.read());

    assertThat(exception).hasMessageThat().isEqualTo(testUri + ": Cannot read: already closed");
  }

  @Test
  void read_byteArray_readsFromChannelAndUpdatesPosition() throws IOException {
    byte[] data = "test-data".getBytes();
    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            inv -> {
              inv.<ByteBuffer>getArgument(0).put(data);
              return data.length;
            });
    byte[] buffer = new byte[20];

    int bytesRead = googleCloudStorageInputStream.read(buffer, 0, buffer.length);

    assertThat(bytesRead).isEqualTo(data.length);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(data.length);
  }

  @Test
  void read_byteArrayAtEOF_returnsMinusOneAndDoesNotUpdatePosition() throws IOException {
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(-1);
    byte[] buffer = new byte[20];

    int bytesRead = googleCloudStorageInputStream.read(buffer, 0, buffer.length);

    assertThat(bytesRead).isEqualTo(-1);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void read_byteArrayWithNegativeLength_returnsIndexOutOfBound() throws IOException {
    byte[] buffer = new byte[20];
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> googleCloudStorageInputStream.read(buffer, 0, -1 * buffer.length));
  }

  @Test
  void read_byteArrayWithNegativeOffset_returnsIndexOutOfBound() throws IOException {
    byte[] buffer = new byte[20];
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> googleCloudStorageInputStream.read(buffer, -1, buffer.length));
  }

  @Test
  void read_postEndOfBuffer_returnsIndexOutOfBound() throws IOException {
    byte[] buffer = new byte[20];
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> googleCloudStorageInputStream.read(buffer, 15, buffer.length / 2));
  }

  @Test
  void read_zeroLength_returnsZeroBytes() throws IOException {
    byte[] buffer = new byte[20];

    int bytesRead = googleCloudStorageInputStream.read(buffer, 0, 0);

    assertThat(bytesRead).isEqualTo(0);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void read_afterClose_throwsIOException() throws IOException {
    googleCloudStorageInputStream.close();
    byte[] buffer = new byte[20];

    var exception =
        assertThrows(
            IOException.class, () -> googleCloudStorageInputStream.read(buffer, 0, buffer.length));

    assertThat(exception).hasMessageThat().isEqualTo(testUri + ": Cannot read: already closed");
  }

  @Test
  void close_closesUnderlyingChannel() throws IOException {
    googleCloudStorageInputStream.close();
    verify(mockChannel).close();
  }

  @Test
  void close_isIdempotent() throws IOException {
    googleCloudStorageInputStream.close();
    googleCloudStorageInputStream.close();
    verify(mockChannel, times(1)).close();
  }

  @Test
  void close_nullChannel() throws IOException {
    GcsFileSystemOptions mockFileSystemOptions = mock(GcsFileSystemOptions.class);
    GcsClientOptions mockClientOptions = mock(GcsClientOptions.class);
    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    when(mockFileSystem.getFileSystemOptions()).thenReturn(mockFileSystemOptions);
    when(mockFileSystemOptions.getGcsClientOptions()).thenReturn(mockClientOptions);
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(mockGcsFileInfo, readOptions)).thenReturn(null);

    GoogleCloudStorageInputStream googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

    googleCloudStorageInputStream.close();
    verify(mockChannel, times(0)).close();
  }

  @Test
  void readFully_throwsUnsupported() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> googleCloudStorageInputStream.readFully(0, new byte[1], 0, 1));
  }

  @Test
  void readTail_throwsUnsupported() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> googleCloudStorageInputStream.readTail(new byte[1], 0, 1));
  }

  @Test
  void readVectored_throwsUnsupported() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> googleCloudStorageInputStream.readVectored(Collections.emptyList(), size -> null));
  }
}
