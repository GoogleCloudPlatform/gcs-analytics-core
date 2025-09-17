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

  private final long fileSize = 1000L;
  private final int prefetchSize = 10;
  private final URI testUri = URI.create("gs://test-bucket/test-object");
  @Mock private VectoredSeekableByteChannel mockChannel;
  @Mock private VectoredSeekableByteChannel mockPrefetchChannel;
  @Mock private GcsFileSystem mockFileSystem;
  @Mock private GcsFileSystemOptions mockFileSystemOptions;
  @Mock private GcsClientOptions mockClientOptions;
  @Mock private GcsFileInfo mockGcsFileInfo;
  private GoogleCloudStorageInputStream googleCloudStorageInputStream;

  @BeforeEach
  void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    when(mockFileSystem.getFileSystemOptions()).thenReturn(mockFileSystemOptions);
    when(mockFileSystemOptions.getGcsClientOptions()).thenReturn(mockClientOptions);
    when(mockFileSystem.getFileInfo(testUri)).thenReturn(mockGcsFileInfo);
    when(mockGcsFileInfo.getUri()).thenReturn(testUri);
  }

  @Test
  void create_usesFileSystemOptions_openMainChannelAndCallsConstructor() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSize(prefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    // Main channel is just returned, only upon call to read second channel is returned.
    when(mockFileSystem.open(eq(mockGcsFileInfo), any(GcsReadOptions.class)))
        .thenReturn(mockChannel);

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

    verify(mockFileSystem).open(mockGcsFileInfo, readOptions);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void create_withGcsFileInfo_opensChannelAndReturnsStream() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSize(prefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), any(GcsReadOptions.class)))
        .thenReturn(mockChannel);

    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(mockFileSystem, mockGcsFileInfo);

    verify(mockFileSystem).open(mockGcsFileInfo, readOptions);
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
    when(mockClientOptions.getGcsReadOptions()).thenReturn(GcsReadOptions.builder().build());
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(GcsReadOptions.builder().build())))
        .thenReturn(mockChannel);

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void seek_updatesPositionAndUnderlyingChannel() throws IOException {
    when(mockClientOptions.getGcsReadOptions()).thenReturn(GcsReadOptions.builder().build());
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(GcsReadOptions.builder().build())))
        .thenReturn(mockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

    googleCloudStorageInputStream.seek(123L);

    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(123L);
    verify(mockChannel).position(123L);
  }

  @Test
  void seek_withNegativePosition_throwsIllegalArgumentException() throws IOException {
    when(mockClientOptions.getGcsReadOptions()).thenReturn(GcsReadOptions.builder().build());
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(GcsReadOptions.builder().build())))
        .thenReturn(mockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

    var exception =
        assertThrows(IllegalArgumentException.class, () -> googleCloudStorageInputStream.seek(-1L));

    assertThat(exception).hasMessageThat().contains("position can't be negative: -1");
  }

  @Test
  void seek_afterClose_throwsIOException() throws IOException {
    when(mockClientOptions.getGcsReadOptions()).thenReturn(GcsReadOptions.builder().build());
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(GcsReadOptions.builder().build())))
        .thenReturn(mockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    googleCloudStorageInputStream.close();

    var exception = assertThrows(IOException.class, () -> googleCloudStorageInputStream.seek(10));

    assertThat(exception).hasMessageThat().isEqualTo(testUri + ": Cannot seek: already closed");
  }

  @Test
  void seek_whenChannelThrowsError_propagatesException() throws IOException {
    when(mockClientOptions.getGcsReadOptions()).thenReturn(GcsReadOptions.builder().build());
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(GcsReadOptions.builder().build())))
        .thenReturn(mockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    doThrow(new IOException("Simulated channel position error")).when(mockChannel).position(100);

    var exception = assertThrows(IOException.class, () -> googleCloudStorageInputStream.seek(100));

    assertThat(exception).hasMessageThat().isEqualTo("Simulated channel position error");
  }

  @Test
  void read_singleByte_notFromCache_useMainChannelAndUpdatesPosition() throws IOException {
    when(mockClientOptions.getGcsReadOptions())
        .thenReturn(GcsReadOptions.builder().setFooterPrefetchSize(2097152).build());
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(GcsReadOptions.builder().build())))
        .thenReturn(mockChannel);
    when(mockChannel.size()).thenReturn(1000L);
    // Prefetch size is greater than file size, hence no prefetch.
    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            inv -> {
              inv.<ByteBuffer>getArgument(0).put((byte) 77);
              return 1;
            });

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    int result = googleCloudStorageInputStream.read();

    assertThat(result).isEqualTo(77);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(1);
  }

  @Test
  void read_singleByte_fromFooter_servesFromCache() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSize(prefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    // As we prefetch footer we open two channels main and prefetch.
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions)))
        .thenReturn(mockChannel)
        .thenReturn(mockPrefetchChannel);
    when(mockChannel.size()).thenReturn(fileSize);

    byte[] footerData = new byte[] {50, 51, 52, 53, 54, 55, 56, 57, 58, 59};
    when(mockPrefetchChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put(footerData);
              return prefetchSize;
            });

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    googleCloudStorageInputStream.seek(995L);
    int result = googleCloudStorageInputStream.read();

    assertThat(result).isEqualTo(55);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(996L);
    verify(mockPrefetchChannel).read(any(ByteBuffer.class));
    verify(mockChannel, never()).read(any(ByteBuffer.class));
  }

  @Test
  void read_byteArray_fromCache_succeeds() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSize(prefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions)))
        .thenReturn(mockChannel)
        .thenReturn(mockPrefetchChannel);
    when(mockChannel.size()).thenReturn(fileSize);

    byte[] footerData = new byte[] {50, 51, 52, 53, 54, 55, 56, 57, 58, 59};
    when(mockPrefetchChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put(footerData);
              return prefetchSize;
            });

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    googleCloudStorageInputStream.seek(992L);
    byte[] readBuffer = new byte[4];
    int bytesRead = googleCloudStorageInputStream.read(readBuffer, 0, readBuffer.length);

    assertThat(bytesRead).isEqualTo(4);
    assertThat(readBuffer).isEqualTo(new byte[] {52, 53, 54, 55});
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(992L + 4);
    verify(mockPrefetchChannel).read(any(ByteBuffer.class));
    verify(mockChannel, never()).read(any(ByteBuffer.class));
  }

  @Test
  void read_fromCacheTwice_usesCacheOnSecondRead() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSize(prefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockChannel.size()).thenReturn(fileSize);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions)))
        .thenReturn(mockChannel)
        .thenReturn(mockPrefetchChannel);
    // Mock the data that the prefetch channel will return.
    byte[] footerData = new byte[] {50, 51, 52, 53, 54, 55, 56, 57, 58, 59};
    when(mockPrefetchChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put(footerData);
              return prefetchSize;
            });

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

    // First Read (triggers caching)
    googleCloudStorageInputStream.seek(992L);
    byte[] readBuffer1 = new byte[2];
    int bytesRead1 = googleCloudStorageInputStream.read(readBuffer1, 0, 2);

    assertThat(bytesRead1).isEqualTo(2);
    assertThat(readBuffer1).isEqualTo(new byte[] {52, 53});
    verify(mockPrefetchChannel, times(1)).read(any(ByteBuffer.class));
    verify(mockPrefetchChannel, times(1)).position(fileSize - prefetchSize);
    verify(mockPrefetchChannel).close();

    // Second Read (should use existing cache)
    googleCloudStorageInputStream.seek(995L);
    int bytesRead2 = googleCloudStorageInputStream.read(new byte[3], 0, 3);

    assertThat(bytesRead2).isEqualTo(3);
    // Verify that the prefetch channel(and main channel) was not called again.
    verifyNoMoreInteractions(mockPrefetchChannel);
    verify(mockChannel, never()).read(any(ByteBuffer.class));
  }

  @Test
  void read_byteArray_fromFooterWhenCacheFails_fallsBackToMainChannel() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSize(prefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockChannel.size()).thenReturn(fileSize);
    // Opening Prefetch channel throws error.
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions)))
        .thenReturn(mockChannel)
        .thenThrow(new IOException("Simulated cache channel open failure"));
    byte[] fallbackData = new byte[] {95, 96, 97, 98};
    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put(fallbackData);
              return fallbackData.length;
            });

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    googleCloudStorageInputStream.seek(995L);
    byte[] readBuffer = new byte[4];
    int bytesRead = googleCloudStorageInputStream.read(readBuffer, 0, readBuffer.length);

    assertThat(bytesRead).isEqualTo(4);
    assertThat(readBuffer).isEqualTo(fallbackData);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(995L + 4);
    verify(mockChannel).read(any(ByteBuffer.class));
  }

  @Test
  void read_fromFooterWhenCacheReadFails_fallsBackToMainChannel() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSize(prefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockChannel.size()).thenReturn(fileSize);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions)))
        .thenReturn(mockChannel)
        .thenReturn(mockPrefetchChannel);
    // Prefetch channel throws an error upon read, leads to using Main channel.
    when(mockPrefetchChannel.read(any(ByteBuffer.class)))
        .thenThrow(new IOException("Simulated cache read failure"));
    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put((byte) 99); // Fallback data
              return 1;
            });

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    googleCloudStorageInputStream.seek(995L);
    int result = googleCloudStorageInputStream.read();

    assertThat(result).isEqualTo(99);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(996L);
    // Verify fallback read occurred.
    verify(mockChannel).read(any(ByteBuffer.class));
  }

  @Test
  void read_singleByteAtEOF_returnsMinusOneAndDoesNotUpdatePosition() throws IOException {
    when(mockClientOptions.getGcsReadOptions()).thenReturn(GcsReadOptions.builder().build());
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(GcsReadOptions.builder().build())))
        .thenReturn(mockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(-1);

    int result = googleCloudStorageInputStream.read();

    assertThat(result).isEqualTo(-1);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void read_singleByteAfterClose_throwsIOException() throws IOException {
    when(mockClientOptions.getGcsReadOptions()).thenReturn(GcsReadOptions.builder().build());
    when(mockFileSystem.open(eq(mockGcsFileInfo), any(GcsReadOptions.class)))
        .thenReturn(mockChannel);

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    googleCloudStorageInputStream.close();

    var exception = assertThrows(IOException.class, () -> googleCloudStorageInputStream.read());

    assertThat(exception).hasMessageThat().isEqualTo(testUri + ": Cannot read: already closed");
  }

  @Test
  void read_fromFooterWithUnexpectedEof_readsPartialCache() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSize(prefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockChannel.size()).thenReturn(fileSize);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions)))
        .thenReturn(mockChannel)
        .thenReturn(mockPrefetchChannel);

    byte[] partialFooterData = new byte[] {50, 51, 52, 53, 54};
    when(mockPrefetchChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put(partialFooterData);
              return partialFooterData.length;
            })
        .thenReturn(-1);

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

    googleCloudStorageInputStream.seek(fileSize - prefetchSize);
    byte[] readBuffer = new byte[prefetchSize];
    int bytesRead = googleCloudStorageInputStream.read(readBuffer, 0, readBuffer.length);

    assertThat(bytesRead).isEqualTo(partialFooterData.length);
    verify(mockPrefetchChannel, times(2)).read(any(ByteBuffer.class));
    verify(mockPrefetchChannel).position(fileSize - prefetchSize);
    verify(mockPrefetchChannel).close();
  }

  @Test
  void read_fromFooterWhenChannelSizeFails_fallsBackToMainChannel() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSize(prefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions))).thenReturn(mockChannel);
    when(mockChannel.size()).thenThrow(new IOException("Simulated size() failure"));

    byte[] fallbackData = new byte[] {99};
    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put(fallbackData);
              return fallbackData.length;
            });

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    googleCloudStorageInputStream.seek(995L);
    int result = googleCloudStorageInputStream.read();

    assertThat(result).isEqualTo(99);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(996L);
    verify(mockChannel).read(any(ByteBuffer.class));
  }

  @Test
  void read_byteArrayAtEOF_returnsMinusOneAndDoesNotUpdatePosition() throws IOException {
    when(mockClientOptions.getGcsReadOptions()).thenReturn(GcsReadOptions.builder().build());
    when(mockFileSystem.open(eq(mockGcsFileInfo), any(GcsReadOptions.class)))
        .thenReturn(mockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(-1);
    byte[] buffer = new byte[20];

    int bytesRead = googleCloudStorageInputStream.read(buffer, 0, buffer.length);

    assertThat(bytesRead).isEqualTo(-1);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void read_byteArrayWithNegativeLength_returnsIndexOutOfBound() throws IOException {
    when(mockClientOptions.getGcsReadOptions()).thenReturn(GcsReadOptions.builder().build());
    when(mockFileSystem.open(eq(mockGcsFileInfo), any(GcsReadOptions.class)))
        .thenReturn(mockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    byte[] buffer = new byte[20];

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> googleCloudStorageInputStream.read(buffer, 0, -1 * buffer.length));
  }

  @Test
  void read_byteArrayWithNegativeOffset_returnsIndexOutOfBound() throws IOException {
    when(mockClientOptions.getGcsReadOptions()).thenReturn(GcsReadOptions.builder().build());
    when(mockFileSystem.open(eq(mockGcsFileInfo), any(GcsReadOptions.class)))
        .thenReturn(mockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    byte[] buffer = new byte[20];

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> googleCloudStorageInputStream.read(buffer, -1, buffer.length));
  }

  @Test
  void read_postEndOfBuffer_returnsIndexOutOfBound() throws IOException {
    when(mockClientOptions.getGcsReadOptions()).thenReturn(GcsReadOptions.builder().build());
    when(mockFileSystem.open(eq(mockGcsFileInfo), any(GcsReadOptions.class)))
        .thenReturn(mockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    byte[] buffer = new byte[20];

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> googleCloudStorageInputStream.read(buffer, 15, buffer.length / 2));
  }

  @Test
  void read_zeroLength_returnsZeroBytes() throws IOException {
    when(mockClientOptions.getGcsReadOptions()).thenReturn(GcsReadOptions.builder().build());
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(GcsReadOptions.builder().build())))
        .thenReturn(mockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    byte[] buffer = new byte[20];

    int bytesRead = googleCloudStorageInputStream.read(buffer, 0, 0);

    assertThat(bytesRead).isEqualTo(0);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void read_afterClose_throwsIOException() throws IOException {
    when(mockClientOptions.getGcsReadOptions()).thenReturn(GcsReadOptions.builder().build());
    when(mockFileSystem.open(eq(mockGcsFileInfo), any(GcsReadOptions.class)))
        .thenReturn(mockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    googleCloudStorageInputStream.close();
    byte[] buffer = new byte[20];

    var exception =
        assertThrows(
            IOException.class, () -> googleCloudStorageInputStream.read(buffer, 0, buffer.length));

    assertThat(exception).hasMessageThat().isEqualTo(testUri + ": Cannot read: already closed");
  }

  @Test
  void close_closesUnderlyingChannel() throws IOException {
    when(mockClientOptions.getGcsReadOptions()).thenReturn(GcsReadOptions.builder().build());
    when(mockFileSystem.open(eq(mockGcsFileInfo), any(GcsReadOptions.class)))
        .thenReturn(mockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

    googleCloudStorageInputStream.close();

    verify(mockChannel).close();
  }

  @Test
  void close_isIdempotent() throws IOException {
    when(mockClientOptions.getGcsReadOptions()).thenReturn(GcsReadOptions.builder().build());
    when(mockFileSystem.open(eq(mockGcsFileInfo), any(GcsReadOptions.class)))
        .thenReturn(mockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
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
  void readFully_validArgs_readsDataFromNewChannel() throws IOException {
    byte[] data = "test-data".getBytes();
    byte[] buffer = new byte[data.length];
    long readPosition = 100L;
    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    VectoredSeekableByteChannel newMockChannel = mock(VectoredSeekableByteChannel.class);
    when(mockFileSystem.open(mockGcsFileInfo, readOptions)).thenReturn(newMockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
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
    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    VectoredSeekableByteChannel newMockChannel = mock(VectoredSeekableByteChannel.class);
    when(mockFileSystem.open(mockGcsFileInfo, readOptions)).thenReturn(newMockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
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
  void readFully_withInvalidBufferArgs_throwsIndexOutOfBoundsException() throws IOException {
    byte[] buffer = new byte[10];
    when(mockClientOptions.getGcsReadOptions()).thenReturn(GcsReadOptions.builder().build());
    when(mockFileSystem.open(eq(mockGcsFileInfo), any(GcsReadOptions.class)))
        .thenReturn(mockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

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
    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    VectoredSeekableByteChannel newMockChannel = mock(VectoredSeekableByteChannel.class);
    GcsItemInfo mockItemInfo = mock(GcsItemInfo.class);
    when(mockFileSystem.open(mockGcsFileInfo, readOptions)).thenReturn(newMockChannel);
    when(mockGcsFileInfo.getItemInfo()).thenReturn(mockItemInfo);
    when(mockItemInfo.getSize()).thenReturn(fileSize);
    when(newMockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            inv -> {
              inv.<ByteBuffer>getArgument(0).put(data);
              return data.length;
            });
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
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
    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    VectoredSeekableByteChannel newMockChannel = mock(VectoredSeekableByteChannel.class);
    GcsItemInfo mockItemInfo = mock(GcsItemInfo.class);

    when(mockFileSystem.open(mockGcsFileInfo, readOptions)).thenReturn(newMockChannel);
    when(mockGcsFileInfo.getItemInfo()).thenReturn(mockItemInfo);
    when(mockItemInfo.getSize()).thenReturn(1024L);

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    int bytesRead = googleCloudStorageInputStream.readTail(buffer, 0, 0);

    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void readVectored_throwsUnsupported() throws IOException {
    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(mockGcsFileInfo, readOptions)).thenReturn(mockChannel);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    assertThrows(
        UnsupportedOperationException.class,
        () -> googleCloudStorageInputStream.readVectored(Collections.emptyList(), size -> null));
  }

  @Test
  void read_byteArray_seekFromNonCacheToCache_usesChannelCorrectly() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSize(prefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), any(GcsReadOptions.class)))
        .thenReturn(mockChannel)
        .thenReturn(mockPrefetchChannel);
    when(mockChannel.size()).thenReturn(fileSize);

    // First read from non-cache position.
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    googleCloudStorageInputStream.seek(0);
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(20);
    int bytesReadFromChannel = googleCloudStorageInputStream.read(new byte[20], 0, 20);

    assertThat(bytesReadFromChannel).isEqualTo(20);
    verify(mockChannel, times(1)).read(any(ByteBuffer.class));
    verify(mockPrefetchChannel, never()).read(any(ByteBuffer.class));

    // Second read from cache position.
    byte[] footerData = new byte[] {50, 51, 52, 53, 54, 55, 56, 57, 58, 59};
    when(mockPrefetchChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put(footerData);
              return prefetchSize;
            });

    googleCloudStorageInputStream.seek(fileSize - prefetchSize + 2);
    byte[] readBuffer = new byte[4];
    int bytesReadFromCache = googleCloudStorageInputStream.read(readBuffer, 0, 4);

    assertThat(bytesReadFromCache).isEqualTo(4);
    assertThat(readBuffer).isEqualTo(new byte[] {52, 53, 54, 55});
    verify(mockPrefetchChannel, times(1)).read(any(ByteBuffer.class));
  }

  @Test
  void read_byteArray_seekFromCacheToNonCache_usesChannelCorrectly() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSize(prefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions)))
        .thenReturn(mockChannel)
        .thenReturn(mockPrefetchChannel);
    when(mockChannel.size()).thenReturn(fileSize);

    // Mock prefetch channel to return data for caching
    byte[] footerData = new byte[] {50, 51, 52, 53, 54, 55, 56, 57, 58, 59};
    when(mockPrefetchChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put(footerData);
              return prefetchSize;
            });
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

    // First read from cache position.
    googleCloudStorageInputStream.seek(fileSize - prefetchSize + 2); // Seek to 992
    byte[] readBuffer = new byte[4];
    int bytesRead = googleCloudStorageInputStream.read(readBuffer, 0, readBuffer.length);

    assertThat(bytesRead).isEqualTo(4);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(fileSize - prefetchSize + 2 + 4);
    assertThat(readBuffer).isEqualTo(new byte[] {52, 53, 54, 55});

    verify(mockPrefetchChannel, times(1)).read(any(ByteBuffer.class));
    verify(mockPrefetchChannel, times(1)).position(fileSize - prefetchSize);
    verify(mockPrefetchChannel).close();
    verify(mockChannel, never()).read(any(ByteBuffer.class));

    // Second read from non-cached position.
    googleCloudStorageInputStream.seek(0);
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(20);
    int bytesReadFromChannel = googleCloudStorageInputStream.read(new byte[20], 0, 20);

    verifyNoMoreInteractions(mockPrefetchChannel);
    assertThat(bytesReadFromChannel).isEqualTo(20);
    verify(mockChannel, times(1)).read(any(ByteBuffer.class)); // This is the new read
  }
}
