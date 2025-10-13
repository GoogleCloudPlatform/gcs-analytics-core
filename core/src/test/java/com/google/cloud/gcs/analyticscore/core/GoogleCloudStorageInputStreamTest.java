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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

class GoogleCloudStorageInputStreamTest {

  private final long fileSize = 1000L;
  private final int prefetchSize = 10;
  private final URI testUri = URI.create("gs://test-bucket/test-object");
  private final GcsItemId testGcsItemId =
      GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();

  @Mock private VectoredSeekableByteChannel mockChannel;
  @Mock private GcsFileSystem mockFileSystem;
  @Mock private GcsFileSystemOptions mockFileSystemOptions;
  @Mock private GcsClientOptions mockClientOptions;
  @Mock private GcsFileInfo mockGcsFileInfo;
  @Mock private GcsItemInfo mockGcsItemInfo;
  private GoogleCloudStorageInputStream googleCloudStorageInputStream;

  @BeforeEach
  void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    when(mockFileSystem.getFileSystemOptions()).thenReturn(mockFileSystemOptions);
    when(mockFileSystemOptions.getGcsClientOptions()).thenReturn(mockClientOptions);
    when(mockFileSystem.getFileInfo(testUri)).thenReturn(mockGcsFileInfo);
    when(mockGcsFileInfo.getUri()).thenReturn(testUri);
    when(mockGcsFileInfo.getItemInfo()).thenReturn(mockGcsItemInfo);
    when(mockGcsItemInfo.getSize()).thenReturn(fileSize);
  }

  GoogleCloudStorageInputStream defaultGcsInputStream() throws IOException {
    when(mockClientOptions.getGcsReadOptions()).thenReturn(GcsReadOptions.builder().build());
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(GcsReadOptions.builder().build())))
        .thenReturn(mockChannel);
    return GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
  }

  @Test
  void create_usesFileSystemOptions_callsGetFileInfoAndOpen() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSizeSmallFile(prefetchSize).build();
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
        GcsReadOptions.builder().setFooterPrefetchSizeSmallFile(prefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), any(GcsReadOptions.class)))
        .thenReturn(mockChannel);

    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(mockFileSystem, mockGcsFileInfo);

    verify(mockFileSystem).open(mockGcsFileInfo, readOptions);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void create_whenGetFileInfoReturnsNull_throwsIllegalStateException() throws IOException {
    when(mockFileSystem.getFileInfo(testUri)).thenReturn(null);

    var exception =
        assertThrows(
            IllegalStateException.class,
            () -> GoogleCloudStorageInputStream.create(mockFileSystem, testUri));

    assertThat(exception).hasMessageThat().isEqualTo("GcsFileInfo shouldn't be null");
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
    googleCloudStorageInputStream = defaultGcsInputStream();

    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void seek_updatesPositionAndUnderlyingChannel() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();

    googleCloudStorageInputStream.seek(123L);

    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(123L);
    verify(mockChannel).position(123L);
  }

  @Test
  void seek_withNegativePosition_throwsIllegalArgumentException() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();

    var exception =
        assertThrows(IllegalArgumentException.class, () -> googleCloudStorageInputStream.seek(-1L));

    assertThat(exception).hasMessageThat().contains("position can't be negative: -1");
  }

  @Test
  void seek_afterClose_throwsIOException() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    googleCloudStorageInputStream.close();

    var exception = assertThrows(IOException.class, () -> googleCloudStorageInputStream.seek(10));

    assertThat(exception).hasMessageThat().isEqualTo(testUri + ": Cannot seek: already closed");
  }

  @Test
  void seek_whenChannelThrowsError_propagatesException() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    doThrow(new IOException("Simulated channel position error")).when(mockChannel).position(100);

    var exception = assertThrows(IOException.class, () -> googleCloudStorageInputStream.seek(100));

    assertThat(exception).hasMessageThat().isEqualTo("Simulated channel position error");
  }

  @Test
  void read_singleByte_fromCache_servesFromCache() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchSizeSmallFile(prefetchSize)
            .setSmallObjectCacheSize(0)
            .build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions))).thenReturn(mockChannel);

    byte[] footerData = new byte[] {50, 51, 52, 53, 54, 55, 56, 57, 58, 59};
    when(mockChannel.read(any(ByteBuffer.class)))
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
    // Verify caching read and seeks
    verify(mockChannel, times(1)).read(any(ByteBuffer.class));
    verify(mockChannel).position(fileSize - prefetchSize);
    verify(mockChannel, times(2)).position(995L);
  }

  @Test
  void read_byteArray_fromCache_succeeds() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchSizeSmallFile(prefetchSize)
            .setSmallObjectCacheSize(0)
            .build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions))).thenReturn(mockChannel);

    byte[] footerData = new byte[] {50, 51, 52, 53, 54, 55, 56, 57, 58, 59};
    when(mockChannel.read(any(ByteBuffer.class)))
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
    // Verify caching read and seeks
    verify(mockChannel, times(1)).read(any(ByteBuffer.class));
    verify(mockChannel).position(fileSize - prefetchSize);
    verify(mockChannel, times(2)).position(992L);
  }

  @Test
  void read_fromCacheTwice_usesCacheOnSecondRead() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchSizeSmallFile(prefetchSize)
            .setSmallObjectCacheSize(0)
            .build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions))).thenReturn(mockChannel);
    // Mock the data that the prefetch channel will return.
    byte[] footerData = new byte[] {50, 51, 52, 53, 54, 55, 56, 57, 58, 59};
    when(mockChannel.read(any(ByteBuffer.class)))
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
    verify(mockChannel, times(1)).read(any(ByteBuffer.class));
    verify(mockChannel, times(1)).position(fileSize - prefetchSize);
    verify(mockChannel, times(2)).position(992L);

    // Second Read (should use existing cache)
    googleCloudStorageInputStream.seek(995L);
    int bytesRead2 = googleCloudStorageInputStream.read(new byte[3], 0, 3);

    assertThat(bytesRead2).isEqualTo(3);
  }

  @Test
  void read_atEndOfCache_fallsBackToMainChannelForEof() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchSizeSmallFile(prefetchSize)
            .setSmallObjectCacheSize(0)
            .build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions))).thenReturn(mockChannel);
    when(mockChannel.size()).thenReturn(fileSize);
    when(mockChannel.position()).thenReturn(1000L);

    byte[] footerData = new byte[prefetchSize];
    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put(footerData);
              return prefetchSize;
            });

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

    googleCloudStorageInputStream.seek(fileSize - prefetchSize);
    googleCloudStorageInputStream.read(new byte[1], 0, 1);

    googleCloudStorageInputStream.seek(fileSize);
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(-1);
    int bytesRead = googleCloudStorageInputStream.read(new byte[10], 0, 10);

    assertThat(bytesRead).isEqualTo(-1);
    verify(mockChannel, times(1)).position(fileSize);
  }

  @Test
  void read_fromCacheWhenCacheReadFails_fallsBackToMainChannel() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSizeSmallFile(prefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockChannel.size()).thenReturn(fileSize);
    when(mockChannel.position()).thenReturn(995L);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions))).thenReturn(mockChannel);
    // Mock channel to fail during the caching read.
    when(mockChannel.read(any(ByteBuffer.class)))
        .thenThrow(new IOException("Simulated cache read failure"))
        // Subsequent call for fallback read
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
    verify(mockChannel, times(2)).read(any(ByteBuffer.class));
  }

  @Test
  void read_singleByteAtEOF_returnsMinusOneAndDoesNotUpdatePosition() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(-1);

    int result = googleCloudStorageInputStream.read();

    assertThat(result).isEqualTo(-1);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void read_singleByteAfterClose_throwsIOException() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    googleCloudStorageInputStream.close();

    var exception = assertThrows(IOException.class, () -> googleCloudStorageInputStream.read());

    assertThat(exception).hasMessageThat().isEqualTo(testUri + ": Cannot read: already closed");
  }

  @Test
  void read_cachingEncountersUnexpectedEof_doesNotUseCache() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchSizeSmallFile(prefetchSize)
            .setSmallObjectCacheSize(0)
            .build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockChannel.size()).thenReturn(fileSize);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions))).thenReturn(mockChannel);
    when(mockChannel.position()).thenReturn(fileSize - prefetchSize);
    byte[] partialFooterData = new byte[] {50, 51, 52, 53, 54};
    when(mockChannel.read(any(ByteBuffer.class)))
        // Partial data for caching
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put(partialFooterData);
              return partialFooterData.length;
            })
        // Unexpected EOF for caching
        .thenReturn(-1)
        // Return full buffer read for fallback read
        .thenAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              int size = buffer.remaining();
              while (buffer.hasRemaining()) {
                buffer.put((byte) 99);
              }
              return size;
            });

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

    googleCloudStorageInputStream.seek(fileSize - prefetchSize);
    byte[] readBuffer = new byte[prefetchSize];
    int bytesRead = googleCloudStorageInputStream.read(readBuffer, 0, readBuffer.length);

    assertThat(bytesRead).isEqualTo(readBuffer.length);
    verify(mockChannel, times(3)).read(any(ByteBuffer.class));
    verify(mockChannel, times(3)).position(fileSize - prefetchSize);
  }

  @Test
  void read_outsideOfCache_withSimulatedPositionError() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchSizeSmallFile(prefetchSize)
            .setSmallObjectCacheSize(0)
            .build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockChannel.size()).thenReturn(fileSize);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions))).thenReturn(mockChannel);

    byte[] footerData = new byte[] {50, 51, 52, 53, 54, 55, 56, 57, 58, 59};
    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put(footerData);
              return footerData.length;
            });

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

    googleCloudStorageInputStream.seek((fileSize - prefetchSize) - 1);
    byte[] readBuffer = new byte[prefetchSize];
    when(mockChannel.position()).thenReturn(0L);
    var exception =
        assertThrows(
            IllegalStateException.class,
            () -> googleCloudStorageInputStream.read(readBuffer, 0, readBuffer.length));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            String.format("Channel position (0) and stream position (989) should be the same"));
  }

  @Test
  void read_byteArrayAtEOF_returnsMinusOneAndDoesNotUpdatePosition() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(-1);
    byte[] buffer = new byte[20];

    int bytesRead = googleCloudStorageInputStream.read(buffer, 0, buffer.length);

    assertThat(bytesRead).isEqualTo(-1);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void read_byteArrayWithNegativeLength_returnsIndexOutOfBound() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    byte[] buffer = new byte[20];

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> googleCloudStorageInputStream.read(buffer, 0, -1 * buffer.length));
  }

  @Test
  void read_byteArrayWithNegativeOffset_returnsIndexOutOfBound() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    byte[] buffer = new byte[20];

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> googleCloudStorageInputStream.read(buffer, -1, buffer.length));
  }

  @Test
  void read_postEndOfBuffer_returnsIndexOutOfBound() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    byte[] buffer = new byte[20];

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> googleCloudStorageInputStream.read(buffer, 15, buffer.length / 2));
  }

  @Test
  void read_zeroLength_returnsZeroBytes() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    byte[] buffer = new byte[20];

    int bytesRead = googleCloudStorageInputStream.read(buffer, 0, 0);

    assertThat(bytesRead).isEqualTo(0);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void read_afterClose_throwsIOException() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    googleCloudStorageInputStream.close();
    byte[] buffer = new byte[20];

    var exception =
        assertThrows(
            IOException.class, () -> googleCloudStorageInputStream.read(buffer, 0, buffer.length));

    assertThat(exception).hasMessageThat().isEqualTo(testUri + ": Cannot read: already closed");
  }

  @Test
  void close_closesUnderlyingChannel() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();

    googleCloudStorageInputStream.close();

    verify(mockChannel).close();
  }

  @Test
  void close_isIdempotent() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
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
    googleCloudStorageInputStream = defaultGcsInputStream();

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
    when(mockGcsItemInfo.getSize()).thenReturn(fileSize);
    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            inv -> {
              inv.<ByteBuffer>getArgument(0).put(data);
              return data.length;
            });
    googleCloudStorageInputStream = defaultGcsInputStream();
    long initialStreamPosition = googleCloudStorageInputStream.getPos();

    int bytesRead = googleCloudStorageInputStream.readTail(buffer, offset, length);
    System.arraycopy(buffer, offset, readData, 0, length);

    assertThat(bytesRead).isEqualTo(data.length);
    assertThat(readData).isEqualTo(data);
    verify(mockChannel).position(expectedPosition);
    verify(mockChannel).read(any(ByteBuffer.class));
    verify(mockChannel).close();
    // readTail should not affect the stream's position
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(initialStreamPosition);
  }

  @Test
  void readTail_zeroLength_returnsZero() throws IOException {
    byte[] buffer = new byte[20];

    when(mockGcsFileInfo.getItemInfo()).thenReturn(mockGcsItemInfo);
    when(mockGcsItemInfo.getSize()).thenReturn(1024L);

    googleCloudStorageInputStream = defaultGcsInputStream();

    int bytesRead = googleCloudStorageInputStream.readTail(buffer, 0, 0);

    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void readVectored_delegatesToReadChannelAndDoesNotChangeState() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    long positionBeforeVectoredRead = googleCloudStorageInputStream.getPos();
    googleCloudStorageInputStream.readVectored(any(), any());

    verify(mockChannel).readVectored(any(), any());
    assertThat(mockChannel.position()).isEqualTo(positionBeforeVectoredRead);
  }

  void readVectored_fullObjectCached_servesFromCache() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setSmallObjectCacheSize(204800) // 200KB
            .setFooterPrefetchSizeSmallFile(102400)
            .build();
    when(mockGcsItemInfo.getSize()).thenReturn(202400L);
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions))).thenReturn(mockChannel);
    byte[] fileContent = TestDataGenerator.generateSeededRandomBytes(204800, /* seed= */ 1);
    mockChannelReadToWriteBytes(mockChannel, fileContent);
  }

  @Test
  void read_byteArray_seekFromNonCacheToCache_usesChannelCorrectly() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchSizeSmallFile(prefetchSize)
            .setSmallObjectCacheSize(0)
            .build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), any(GcsReadOptions.class)))
        .thenReturn(mockChannel);
    when(mockChannel.size()).thenReturn(fileSize);
    when(mockFileSystem.getFileInfo(any())).thenReturn(mockGcsFileInfo);
    // First read from non-cache position.
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    googleCloudStorageInputStream.seek(0);
    byte[] dummyBytes = TestDataGenerator.generateSeededRandomBytes(20, /* seed= */ 1);
    mockChannelReadToWriteBytes(mockChannel, dummyBytes);

    int bytesReadFromChannel = googleCloudStorageInputStream.read(new byte[20], 0, 20);
    assertThat(bytesReadFromChannel).isEqualTo(20);
    verify(mockChannel, times(1)).read(any(ByteBuffer.class));

    // Second read from cache position.
    byte[] footerData = {50, 51, 52, 53, 54, 55, 56, 57, 58, 59};
    when(mockChannel.read(any(ByteBuffer.class)))
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
    // Caching read + fallback read
    verify(mockChannel, times(2)).read(any(ByteBuffer.class));
  }

  @Test
  void read_byteArray_seekFromCacheToNonCache_usesChannelCorrectly() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchSizeSmallFile(prefetchSize)
            .setSmallObjectCacheSize(0)
            .build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions))).thenReturn(mockChannel);
    when(mockChannel.size()).thenReturn(fileSize);

    byte[] footerData = new byte[] {50, 51, 52, 53, 54, 55, 56, 57, 58, 59};
    when(mockChannel.read(any(ByteBuffer.class)))
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

    verify(mockChannel, times(1)).read(any(ByteBuffer.class));
    verify(mockChannel).position(fileSize - prefetchSize);
    verify(mockChannel, times(2)).position(fileSize - prefetchSize + 2);

    // Second read from non-cached position.
    reset(mockChannel); // Reset mock to verify only the next interaction
    googleCloudStorageInputStream.seek(0);
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(20);
    int bytesReadFromChannel = googleCloudStorageInputStream.read(new byte[20], 0, 20);

    assertThat(bytesReadFromChannel).isEqualTo(20);
    verify(mockChannel, times(1)).read(any(ByteBuffer.class)); // This is the new read
  }

  @Test
  void cache_whenRestorePositionFails_propagatesException() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSizeSmallFile(prefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions))).thenReturn(mockChannel);
    when(mockChannel.size()).thenReturn(fileSize);
    byte[] footerData = new byte[prefetchSize];
    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put(footerData);
              return prefetchSize;
            });

    long seekPosition = 992L;
    long footerStartPosition = fileSize - prefetchSize;
    when(mockChannel.position(footerStartPosition)).thenReturn(mockChannel);
    when(mockChannel.position(seekPosition))
        .thenReturn(mockChannel)
        .thenThrow(new IOException("Simulated restore failure"));

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    googleCloudStorageInputStream.seek(seekPosition);

    IOException exception =
        assertThrows(
            IOException.class, () -> googleCloudStorageInputStream.read(new byte[4], 0, 4));

    assertThat(exception).hasMessageThat().isEqualTo("Simulated restore failure");
  }

  @Test
  void read_forLargeFile_usesLargeFilePrefetchSize() throws IOException {
    long largeFileSize = 2L * 1024 * 1024 * 1024;
    int largeFilePrefetchSize = 20;
    when(mockGcsItemInfo.getSize()).thenReturn(largeFileSize);

    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSizeLargeFile(largeFilePrefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions))).thenReturn(mockChannel);

    byte[] footerData = new byte[largeFilePrefetchSize];
    for (int i = 0; i < largeFilePrefetchSize; i++) {
      footerData[i] = (byte) (100 + i);
    }
    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put(footerData);
              return largeFilePrefetchSize;
            });

    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(mockFileSystem, mockGcsFileInfo);
    long seekPosition = largeFileSize - 5;
    googleCloudStorageInputStream.seek(seekPosition);
    int result = googleCloudStorageInputStream.read();

    assertThat(result).isEqualTo(115); // 100 + (20 - 5)
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(seekPosition + 1);
    verify(mockChannel).position(largeFileSize - largeFilePrefetchSize);
  }

  @Test
  void read_whenFooterPrefetchIsDisabled_smallObjectCacheDisabled_doesNotPrefetch()
      throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchEnabled(false).setSmallObjectCacheSize(0).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions))).thenReturn(mockChannel);
    when(mockChannel.position()).thenAnswer(invocation -> googleCloudStorageInputStream.getPos());

    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put((byte) 88);
              return 1;
            })
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put((byte) 99);
              return 1;
            });

    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(mockFileSystem, mockGcsFileInfo);
    long seekPosition = fileSize - 5;
    googleCloudStorageInputStream.seek(seekPosition);

    int result1 = googleCloudStorageInputStream.read();
    assertThat(result1).isEqualTo(88);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(seekPosition + 1);

    int result2 = googleCloudStorageInputStream.read();
    assertThat(result2).isEqualTo(99);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(seekPosition + 2);
    verify(mockChannel, times(2)).read(any(ByteBuffer.class));
    verify(mockChannel, never()).position(fileSize - prefetchSize);
  }

  @Test
  void read_whenFileSizeIsLessThanPrefetchSize_cachesFullObject() throws IOException {
    long smallFileSize = prefetchSize - 1;
    when(mockGcsItemInfo.getSize()).thenReturn(smallFileSize);
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSizeSmallFile(prefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions))).thenReturn(mockChannel);
    byte[] fileContent = new byte[(int) smallFileSize];
    for (int i = 0; i < smallFileSize; i++) {
      fileContent[i] = (byte) i;
    }

    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put(fileContent);
              return (int) smallFileSize;
            });

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    // First read, should trigger caching
    int firstByte = googleCloudStorageInputStream.read();
    assertThat(firstByte).isEqualTo(fileContent[0]);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(1);

    // Verify that the whole file was read into the small object cache
    verify(mockChannel).read(any(ByteBuffer.class));
    verify(mockChannel, times(2)).position(0L);

    // Second read, should be served from cache and position gets updated.
    int secondByte = googleCloudStorageInputStream.read();
    assertThat(secondByte).isEqualTo(fileContent[1]);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(2);
    // Verify read() was not called on the channel again
    verify(mockChannel, times(1)).read(any(ByteBuffer.class));
  }

  @Test
  void read_whenFileSizeIsEqualToPrefetchSize_cachesFullObject() throws IOException {
    long smallFileSize = prefetchSize;
    when(mockGcsItemInfo.getSize()).thenReturn(smallFileSize);
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSizeSmallFile(prefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions))).thenReturn(mockChannel);

    byte[] fileContent = new byte[(int) smallFileSize];
    for (int i = 0; i < smallFileSize; i++) {
      fileContent[i] = (byte) i;
    }

    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put(fileContent);
              return (int) smallFileSize;
            });

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

    // Read to trigger caching
    byte[] readBuffer = new byte[5];
    int bytesRead = googleCloudStorageInputStream.read(readBuffer, 0, 5);
    assertThat(bytesRead).isEqualTo(5);
    for (int i = 0; i < 5; i++) {
      assertThat(readBuffer[i]).isEqualTo(fileContent[i]);
    }
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(5);

    // Verify that the whole file was read into the cache
    verify(mockChannel).read(any(ByteBuffer.class));
    verify(mockChannel, times(2)).position(0L);

    // Read again, should be served from cache
    bytesRead = googleCloudStorageInputStream.read(readBuffer, 0, 5);
    assertThat(bytesRead).isEqualTo(5);
    for (int i = 0; i < 5; i++) {
      assertThat(readBuffer[i]).isEqualTo(fileContent[i + 5]);
    }
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(10);
    verify(mockChannel, times(1)).read(any(ByteBuffer.class));
  }

  @Test
  void seek_inFullObjectCache_readsCorrectData() throws IOException {
    long smallFileSize = prefetchSize;
    when(mockGcsItemInfo.getSize()).thenReturn(smallFileSize);
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSizeSmallFile(prefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions))).thenReturn(mockChannel);

    byte[] fileContent = new byte[(int) smallFileSize];
    for (int i = 0; i < smallFileSize; i++) {
      fileContent[i] = (byte) i;
    }

    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put(fileContent);
              return (int) smallFileSize;
            });

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);
    googleCloudStorageInputStream.read();

    googleCloudStorageInputStream.seek(5);
    int byteRead = googleCloudStorageInputStream.read();
    assertThat(byteRead).isEqualTo(fileContent[5]);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(6);
    verify(mockChannel, times(1)).read(any(ByteBuffer.class));
  }

  @Test
  void read_pastEndOfFullObjectCache_returnsEof() throws IOException {
    long smallFileSize = prefetchSize;
    when(mockGcsItemInfo.getSize()).thenReturn(smallFileSize);
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSizeSmallFile(prefetchSize).build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions))).thenReturn(mockChannel);

    byte[] fileContent = new byte[(int) smallFileSize];
    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              invocation.<ByteBuffer>getArgument(0).put(fileContent);
              return (int) smallFileSize;
            });

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

    // Read to trigger caching
    googleCloudStorageInputStream.read(new byte[(int) smallFileSize], 0, (int) smallFileSize);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(smallFileSize);

    // Read at EOF
    int result = googleCloudStorageInputStream.read();
    assertThat(result).isEqualTo(-1);

    // Read with buffer at EOF
    int bytesRead = googleCloudStorageInputStream.read(new byte[1], 0, 1);
    assertThat(bytesRead).isEqualTo(-1);
  }

  @Test
  void read_smallObjectCachingIsDisabled_footerPrefetchingDisabled_doesNotCache()
      throws IOException {
    // 1. Setup: A file small enough for caching, but the option is disabled.
    long smallFileSize = prefetchSize - 1;
    when(mockGcsItemInfo.getSize()).thenReturn(smallFileSize);
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setSmallObjectCacheSize(0) // Disable the cache
            .setFooterPrefetchSizeSmallFile(0) // Disable footer prefetch for small file
            .build();
    when(mockClientOptions.getGcsReadOptions()).thenReturn(readOptions);
    when(mockFileSystem.open(eq(mockGcsFileInfo), eq(readOptions))).thenReturn(mockChannel);

    // Mock channel reads and positions.
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(1);
    when(mockChannel.position()).thenReturn(0L).thenReturn(1L);

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(mockFileSystem, testUri);

    // 2. Execution: Read from the stream twice.
    googleCloudStorageInputStream.read();
    googleCloudStorageInputStream.read();

    // 3. Verification:
    // Verify that the underlying channel was read from twice, proving no cache was used.
    verify(mockChannel, times(2)).read(any(ByteBuffer.class));
    // Crucially, verify that no incorrect attempt was made to position for a footer cache.
    verify(mockChannel, never()).position(smallFileSize - prefetchSize);
  }

  @Test
  void readVectored_smallObjectCached_readsFromCache()
      throws IOException, ExecutionException, InterruptedException {
    GcsFileSystemOptions options =
        GcsFileSystemOptions.createFromOptions(
            Map.of("analytics-core.small-file.cache.threshold-bytes", "1024"), "");
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    byte data[] = TestDataGenerator.createGcsData(itemId, 1024);
    FakeGcsFileSystemImpl fakeGcsFileSystem = new FakeGcsFileSystemImpl(options);
    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(
            fakeGcsFileSystem, URI.create("gs://test-bucket/test-object"));
    GcsObjectRange range1 = createGcsObjectRange(/* offset= */ 200, /* length= */ 100);
    GcsObjectRange range2 = createGcsObjectRange(/* offset= */ 600, /* length= */ 100);
    googleCloudStorageInputStream.read(); // caches the object
    long positon = googleCloudStorageInputStream.getPos();

    googleCloudStorageInputStream.readVectored(
        List.of(range1, range2), (size) -> ByteBuffer.allocate(size));
    ByteBuffer range1Result = range1.getByteBufferFuture().get();
    ByteBuffer range2Result = range2.getByteBufferFuture().get();

    assertTargetByteBufferPresentAtOffset(data, range1Result, range1.getOffset());
    assertTargetByteBufferPresentAtOffset(data, range2Result, range2.getOffset());
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(positon);
  }

  @Test
  void readVectored_smallObjectCached_partialRead_throws()
          throws IOException, ExecutionException, InterruptedException {
    GcsFileSystemOptions options =
            GcsFileSystemOptions.createFromOptions(
                    Map.of("analytics-core.small-file.cache.threshold-bytes", "1024"), "");
    GcsItemId itemId =
            GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    TestDataGenerator.createGcsData(itemId, 1024);
    FakeGcsFileSystemImpl fakeGcsFileSystem = new FakeGcsFileSystemImpl(options);
    googleCloudStorageInputStream =
            GoogleCloudStorageInputStream.create(
                    fakeGcsFileSystem, URI.create("gs://test-bucket/test-object"));
    GcsObjectRange range1 = createGcsObjectRange(/* offset= */ 1000, /* length= */ 100);
    googleCloudStorageInputStream.read(); // caches the object

    googleCloudStorageInputStream.readVectored(
            List.of(range1), (size) -> ByteBuffer.allocate(size));

    assertThrows(ExecutionException.class, () -> range1.getByteBufferFuture().get());
  }

  @Test
  void readVectored_cacheNotAvailable_readsFromChannels()
      throws IOException, ExecutionException, InterruptedException {
    GcsFileSystemOptions options =
        GcsFileSystemOptions.createFromOptions(
            Map.of(
                "analytics-core.small-file.cache.threshold-bytes", "1024",
                "analytics-core.read.vectored.min.range.seek.size", "100"),
            "");
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    byte data[] = TestDataGenerator.createGcsData(itemId, 2024);
    FakeGcsFileSystemImpl fakeGcsFileSystem = new FakeGcsFileSystemImpl(options);
    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(
            fakeGcsFileSystem, URI.create("gs://test-bucket/test-object"));
    GcsObjectRange range1 = createGcsObjectRange(/* offset= */ 200, /* length= */ 100);
    GcsObjectRange range2 = createGcsObjectRange(/* offset= */ 600, /* length= */ 100);
    long position = googleCloudStorageInputStream.getPos();

    googleCloudStorageInputStream.readVectored(
        List.of(range1, range2), (size) -> ByteBuffer.allocate(size));
    ByteBuffer range1Result = range1.getByteBufferFuture().get();
    ByteBuffer range2Result = range2.getByteBufferFuture().get();

    assertTargetByteBufferPresentAtOffset(data, range1Result, range1.getOffset());
    assertTargetByteBufferPresentAtOffset(data, range2Result, range2.getOffset());
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(position);
  }

  @Test
  void readVectored_cacheNotAvailable_partialRead_throws() throws IOException {
    GcsFileSystemOptions options =
            GcsFileSystemOptions.createFromOptions(
                    Map.of("analytics-core.small-file.cache.threshold-bytes", "100"), "");
    GcsItemId itemId =
            GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    TestDataGenerator.createGcsData(itemId, 1024);
    FakeGcsFileSystemImpl fakeGcsFileSystem = new FakeGcsFileSystemImpl(options);
    googleCloudStorageInputStream =
            GoogleCloudStorageInputStream.create(
                    fakeGcsFileSystem, URI.create("gs://test-bucket/test-object"));
    GcsObjectRange range1 = createGcsObjectRange(/* offset= */ 1000, /* length= */ 100);

    googleCloudStorageInputStream.readVectored(
            List.of(range1), (size) -> ByteBuffer.allocate(size));

    assertThrows(ExecutionException.class, () -> range1.getByteBufferFuture().get());
  }

  @Test
  void read_fromHead_smallObjectCachingEnabled_objectSmall_caches() throws IOException {
    GcsFileSystemOptions options =
        GcsFileSystemOptions.createFromOptions(
            Map.of("analytics-core.small-file.cache.threshold-bytes", "1024"), "");
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    byte data[] = TestDataGenerator.createGcsData(itemId, 1024);
    FakeGcsFileSystemImpl fakeGcsFileSystem = new FakeGcsFileSystemImpl(options);
    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(
            fakeGcsFileSystem, URI.create("gs://test-bucket/test-object"));

    byte read = (byte) googleCloudStorageInputStream.read();
    assertThat(read).isEqualTo(data[0]);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(1);
    read = (byte) googleCloudStorageInputStream.read();
    assertThat(read).isEqualTo(data[1]);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(2);
  }

  private void mockChannelReadToWriteBytes(VectoredSeekableByteChannel mockChannel, byte[] data)
      throws IOException {
    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            (Answer<Integer>)
                invocation -> {
                  ByteBuffer buffer = invocation.getArgument(0);
                  int bytesToWrite = Math.min(buffer.remaining(), data.length);
                  for (int i = 0; i < bytesToWrite; i++) {
                    buffer.put(data[i]);
                  }
                  return bytesToWrite;
                });
  }

  private void assertTargetByteBufferPresentAtOffset(
      byte[] source, ByteBuffer target, long offset) {
    ByteBuffer sourceSlice =
        ByteBuffer.wrap(source)
            .position(Math.toIntExact(offset))
            .limit(Math.toIntExact(offset + target.limit()));
    assertThat(sourceSlice.equals(target)).isTrue();
  }

  private GcsObjectRange createGcsObjectRange(long offset, int length) {
    return GcsObjectRange.builder()
        .setOffset(offset)
        .setLength(length)
        .setByteBufferFuture(new CompletableFuture<>())
        .build();
  }
}
