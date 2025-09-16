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
import java.io.EOFException;
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
  private GoogleCloudStorageInputStream googleCloudStorageInputStream;

  @BeforeEach
  void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    when(mockFileSystem.getFileSystemOptions()).thenReturn(mockFileSystemOptions);
    when(mockFileSystemOptions.getGcsClientOptions()).thenReturn(mockClientOptions);
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(testUri, readOptions)).thenReturn(mockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
  }

  @Test
  void create_usesFileSystemOptions_openChannelAndCallsConstructor() throws IOException {
    GcsReadOptions readOptions = GcsReadOptions.builder().build();

    verify(mockFileSystem).open(testUri, readOptions);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void create_nullFileSystem_throwsIllegalStateException() throws IOException {
    var exception =
        assertThrows(
            IllegalStateException.class, () -> GoogleCloudStorageInputStream.create(null, testUri));

    assertThat(exception).hasMessageThat().isEqualTo("GcsFileSystem shouldn't be null");
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
    when(mockFileSystem.open(testUri, readOptions)).thenReturn(null);

    GoogleCloudStorageInputStream googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

    googleCloudStorageInputStream.close();
    verify(mockChannel, times(0)).close();
  }

  @Test
  void readFully_validArgs_readsDataFromNewChannel() throws IOException {
    byte[] data = "test-data".getBytes();
    byte[] buffer = new byte[data.length];
    long readPosition = 100L;
    GcsReadOptions readOptions = mockClientOptions.getGcsReadOptions();
    VectoredSeekableByteChannel newMockChannel = mock(VectoredSeekableByteChannel.class);
    when(mockFileSystem.open(testUri, readOptions)).thenReturn(newMockChannel);
    when(newMockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            inv -> {
              inv.<ByteBuffer>getArgument(0).put(data);
              return data.length;
            });
    long initialStreamPosition = googleCloudStorageInputStream.getPos();

    googleCloudStorageInputStream.readFully(readPosition, buffer, 0, buffer.length);

    assertThat(buffer).isEqualTo(data);
    verify(newMockChannel).position(readPosition);
    verify(newMockChannel).read(any(ByteBuffer.class));
    verify(newMockChannel).close();
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(initialStreamPosition);
  }

  @Test
  void readFully_whenReadIsShort_throwsEofException() throws IOException {
    byte[] buffer = new byte[20];
    long readPosition = 100L;
    int bytesToRead = buffer.length;
    int actualBytesRead = 10;
    GcsReadOptions readOptions = mockClientOptions.getGcsReadOptions();
    VectoredSeekableByteChannel newMockChannel = mock(VectoredSeekableByteChannel.class);
    when(mockFileSystem.open(testUri, readOptions)).thenReturn(newMockChannel);
    when(newMockChannel.read(any(ByteBuffer.class))).thenReturn(actualBytesRead);

    var exception =
        assertThrows(
            EOFException.class,
            () -> googleCloudStorageInputStream.readFully(readPosition, buffer, 0, bytesToRead));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Reached the end of stream with "
                + (bytesToRead - actualBytesRead)
                + " bytes left to read");
    verify(newMockChannel).close();
  }

  @Test
  void readFully_withInvalidBufferArgs_throwsIndexOutOfBoundsException() {
    byte[] buffer = new byte[10];

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> googleCloudStorageInputStream.readFully(0, buffer, -1, buffer.length));
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> googleCloudStorageInputStream.readFully(0, buffer, 0, -1));
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> googleCloudStorageInputStream.readFully(0, buffer, 1, buffer.length));
  }

  @Test
  void readTail_validArgs_readsDataFromNewChannel() throws IOException {
    byte[] data = "test-data".getBytes();
    int length = data.length;
    byte[] buffer = new byte[20]; // larger buffer
    byte[] readData = new byte[length];
    int offset = 5;
    long fileSize = 1024L;
    long expectedPosition = fileSize - offset;
    GcsReadOptions readOptions = mockClientOptions.getGcsReadOptions();
    VectoredSeekableByteChannel newMockChannel = mock(VectoredSeekableByteChannel.class);
    GcsFileInfo mockFileInfo = mock(GcsFileInfo.class);
    GcsItemInfo mockItemInfo = mock(GcsItemInfo.class);
    when(mockFileSystem.open(testUri, readOptions)).thenReturn(newMockChannel);
    when(mockFileSystem.getFileInfo(testUri)).thenReturn(mockFileInfo);
    when(mockFileInfo.getItemInfo()).thenReturn(mockItemInfo);
    when(mockItemInfo.getSize()).thenReturn(fileSize);
    when(newMockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            inv -> {
              inv.<ByteBuffer>getArgument(0).put(data);
              return data.length;
            });
    long initialStreamPosition = googleCloudStorageInputStream.getPos();

    int bytesRead = googleCloudStorageInputStream.readTail(buffer, offset, length);
    System.arraycopy(buffer, offset, readData, 0, length);

    assertThat(bytesRead).isEqualTo(data.length);
    assertThat(readData).isEqualTo(data);
    verify(newMockChannel).position(expectedPosition);
    verify(newMockChannel).read(any(ByteBuffer.class));
    verify(newMockChannel).close();
    // readTail should not affect the stream's position
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(initialStreamPosition);
  }

  @Test
  void readTail_zeroLength_returnsZero() throws IOException {
    byte[] buffer = new byte[20];
    GcsReadOptions readOptions = mockClientOptions.getGcsReadOptions();
    VectoredSeekableByteChannel newMockChannel = mock(VectoredSeekableByteChannel.class);
    GcsFileInfo mockFileInfo = mock(GcsFileInfo.class);
    GcsItemInfo mockItemInfo = mock(GcsItemInfo.class);

    when(mockFileSystem.open(testUri, readOptions)).thenReturn(newMockChannel);
    when(mockFileSystem.getFileInfo(testUri)).thenReturn(mockFileInfo);
    when(mockFileInfo.getItemInfo()).thenReturn(mockItemInfo);
    when(mockItemInfo.getSize()).thenReturn(1024L);

    int bytesRead = googleCloudStorageInputStream.readTail(buffer, 0, 0);

    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void readVectored_delegatesToReadChannelAndDoesNotChangeState() throws IOException {
    long positionBeforeVectoredRead = mockChannel.position();
    googleCloudStorageInputStream.readVectored(any(), any());

    verify(mockChannel).readVectored(any(), any());
    assertThat(mockChannel.position()).isEqualTo(positionBeforeVectoredRead);
  }
}
