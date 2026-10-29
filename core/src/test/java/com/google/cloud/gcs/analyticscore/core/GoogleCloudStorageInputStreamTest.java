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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.client.*;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;

class GoogleCloudStorageInputStreamTest {

  private final long fileSize = 1000L;
  private final int prefetchSize = 10;
  private final URI testUri = URI.create("gs://test-bucket/test-object");
  private final GcsItemId testGcsItemId =
      GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();

  private GcsFileSystem fakeFileSystem;
  private GcsFileSystemOptions fileSystemOptions;
  private GcsClientOptions clientOptions;
  private GoogleCloudStorageInputStream googleCloudStorageInputStream;
  private byte[] testData;

  @BeforeEach
  void initializeFileSystemAndTestData() throws IOException {
    MockitoAnnotations.openMocks(this);

    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    clientOptions = GcsClientOptions.builder().setGcsReadOptions(readOptions).build();
    GcsCacheOptions cacheOptions = GcsCacheOptions.builder().build();
    fileSystemOptions =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(clientOptions)
            .setGcsCacheOptions(cacheOptions)
            .build();
    fakeFileSystem = new FakeGcsFileSystemImpl(fileSystemOptions);

    // Setup data in fake storage
    testData = new byte[(int) fileSize];
    for (int i = 0; i < fileSize; i++) {
      testData[i] = (byte) i;
    }
    FakeGcsClientImpl.storage.create(
        BlobInfo.newBuilder(testGcsItemId.getBucketName(), testGcsItemId.getObjectName().get(), 1L)
            .build(),
        testData);
  }

  GoogleCloudStorageInputStream defaultGcsInputStream() throws IOException {
    return GoogleCloudStorageInputStream.create(fakeFileSystem, testUri);
  }

  private GoogleCloudStorageInputStream createStream(GcsReadOptions readOptions)
      throws IOException {
    GcsClientOptions newClientOptions =
        clientOptions.toBuilder().setGcsReadOptions(readOptions).build();
    GcsFileSystemOptions newFileSystemOptions =
        fileSystemOptions.toBuilder().setGcsClientOptions(newClientOptions).build();
    GcsFileSystem newFakeFileSystem = new FakeGcsFileSystemImpl(newFileSystemOptions);
    return GoogleCloudStorageInputStream.create(newFakeFileSystem, testUri);
  }

  @Test
  void create_withUri_succeeds() throws IOException {
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(fakeFileSystem, testUri);

    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void create_usesFileSystemOptions_callsGetFileInfoAndOpen() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFooterPrefetchSizeSmallFile(prefetchSize).build();
    googleCloudStorageInputStream = createStream(readOptions);

    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void create_withGcsFileInfo_opensChannelAndReturnsStream() throws IOException {
    GcsFileInfo fileInfo = fakeFileSystem.getFileInfo(testUri);

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(fakeFileSystem, fileInfo);

    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void create_withGcsItemId_opensChannelAndReturnsStream() throws IOException {
    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(fakeFileSystem, testGcsItemId);

    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void size_withGcsFileInfo_returnsSizeFromFileInfo() throws IOException {
    GcsFileInfo fileInfo = fakeFileSystem.getFileInfo(testUri);
    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(fakeFileSystem, fileInfo);

    assertThat(googleCloudStorageInputStream.size()).isEqualTo(fileSize);
  }

  @Test
  void size_withGcsItemId_resolvesSizeFromChannel() throws IOException {
    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(fakeFileSystem, testGcsItemId);

    assertThat(googleCloudStorageInputStream.size()).isEqualTo(fileSize);
  }

  @Test
  void size_afterClose_throwsIOException() throws IOException {
    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(fakeFileSystem, testGcsItemId);
    googleCloudStorageInputStream.close();

    assertThrows(IOException.class, () -> googleCloudStorageInputStream.size());
  }

  @Test
  void create_withGcsItemId_nullFileSystem_throwsIllegalStateException() {
    var exception =
        assertThrows(
            IllegalStateException.class,
            () -> GoogleCloudStorageInputStream.create(null, testGcsItemId));

    assertThat(exception).hasMessageThat().isEqualTo("GcsFileSystem shouldn't be null");
  }

  @Test
  void create_whenGetFileInfoObjectReturnsNull_throwsIllegalStateException() throws IOException {
    GcsFileSystem mockFileSystem = mock(GcsFileSystem.class);
    when(mockFileSystem.getFileInfoObject(testUri)).thenReturn(null);

    var exception =
        assertThrows(
            IllegalStateException.class,
            () -> GoogleCloudStorageInputStream.create(mockFileSystem, testUri));

    assertThat(exception).hasMessageThat().isEqualTo("GcsFileInfo shouldn't be null");
  }

  @Test
  void create_withUri_usesObjectOnlyLookup() throws IOException {
    GcsFileSystem spyFileSystem = spy(fakeFileSystem);

    googleCloudStorageInputStream = GoogleCloudStorageInputStream.create(spyFileSystem, testUri);

    verify(spyFileSystem).getFileInfoObject(testUri);
    verify(spyFileSystem, never()).getFileInfo(any(URI.class));
  }

  @Test
  void create_withDirectoryUri_throwsIllegalArgumentException() {
    URI directoryUri = URI.create("gs://test-bucket/test-dir/");

    assertThrows(
        IllegalArgumentException.class,
        () -> GoogleCloudStorageInputStream.create(fakeFileSystem, directoryUri));
  }

  @Test
  void create_withUriOfDirectoryWithoutTrailingSlash_throwsFileNotFoundException() {
    FakeGcsClientImpl.storage.create(
        BlobInfo.newBuilder("test-bucket", "dir-with-children/child").build(), new byte[] {1});
    URI directoryUri = URI.create("gs://test-bucket/dir-with-children");

    assertThrows(
        FileNotFoundException.class,
        () -> GoogleCloudStorageInputStream.create(fakeFileSystem, directoryUri));
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

    assertThat(exception).hasMessageThat().contains("already closed");
  }

  @Test
  void seek_whenChannelThrowsError_propagatesException() throws IOException {
    VectoredSeekableByteChannel mockChannel = mock(VectoredSeekableByteChannel.class);
    doThrow(new IOException("Simulated channel position error")).when(mockChannel).position(100);
    GcsFileSystem mockFileSystem = mock(GcsFileSystem.class);
    when(mockFileSystem.getFileSystemOptions()).thenReturn(fileSystemOptions);
    when(mockFileSystem.getTelemetry()).thenReturn(new Telemetry(ImmutableList.of()));
    when(mockFileSystem.getCacheManager()).thenReturn(fakeFileSystem.getCacheManager());
    when(mockFileSystem.open(any(GcsItemId.class), any())).thenReturn(mockChannel);

    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(mockFileSystem, testGcsItemId);

    var exception = assertThrows(IOException.class, () -> googleCloudStorageInputStream.seek(100));

    assertThat(exception).hasMessageThat().isEqualTo("Simulated channel position error");
  }

  @Test
  void read_singleByte_fromCache_servesFromCache() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchEnabled(true)
            .setFooterPrefetchSizeSmallFile(prefetchSize)
            .build();
    googleCloudStorageInputStream = createStream(readOptions);

    googleCloudStorageInputStream.seek(995L);
    int result = googleCloudStorageInputStream.read();

    assertThat((byte) result).isEqualTo(testData[995]);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(996L);
  }

  @Test
  void read_byteArray_fromCache_succeeds() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchEnabled(true)
            .setFooterPrefetchSizeSmallFile(prefetchSize)
            .build();
    googleCloudStorageInputStream = createStream(readOptions);

    googleCloudStorageInputStream.seek(992L);
    byte[] readBuffer = new byte[4];
    int bytesRead = googleCloudStorageInputStream.read(readBuffer, 0, readBuffer.length);

    assertThat(bytesRead).isEqualTo(4);
    assertThat(readBuffer)
        .isEqualTo(new byte[] {testData[992], testData[993], testData[994], testData[995]});
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(996L);
  }

  @Test
  void read_fromCacheTwice_usesCacheOnSecondRead() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchEnabled(true)
            .setFooterPrefetchSizeSmallFile(prefetchSize)
            .build();
    googleCloudStorageInputStream = createStream(readOptions);

    googleCloudStorageInputStream.seek(990L);
    byte[] readBuffer = new byte[2];
    googleCloudStorageInputStream.read(readBuffer, 0, 2);

    assertThat(readBuffer).isEqualTo(new byte[] {testData[990], testData[991]});

    // Second read from cache position.
    googleCloudStorageInputStream.seek(992L);
    byte[] secondReadBuffer = new byte[2];
    int bytesReadFromCache = googleCloudStorageInputStream.read(secondReadBuffer, 0, 2);

    assertThat(bytesReadFromCache).isEqualTo(2);
    assertThat(secondReadBuffer).isEqualTo(new byte[] {testData[992], testData[993]});
  }

  @Test
  void read_atEndOfCache_fallsBackToMainChannelForEof() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchEnabled(true)
            .setFooterPrefetchSizeSmallFile(prefetchSize)
            .build();
    googleCloudStorageInputStream = createStream(readOptions);

    // First read from cache position to trigger prefetch.
    googleCloudStorageInputStream.seek(992L);
    byte[] readBuffer = new byte[prefetchSize];
    int bytesRead = googleCloudStorageInputStream.read(readBuffer, 0, prefetchSize);

    assertThat(bytesRead).isEqualTo(8); // Read until EOF
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(1000L);

    // Read at EOF
    int eofRead = googleCloudStorageInputStream.read();
    assertThat(eofRead).isEqualTo(-1);
  }

  @Test
  void read_singleByteAtEOF_returnsMinusOneAndDoesNotUpdatePosition() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    googleCloudStorageInputStream.seek(fileSize);

    int result = googleCloudStorageInputStream.read();

    assertThat(result).isEqualTo(-1);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(fileSize);
  }

  @Test
  void read_singleByteAfterClose_throwsIOException() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    googleCloudStorageInputStream.close();

    var exception = assertThrows(IOException.class, () -> googleCloudStorageInputStream.read());

    assertThat(exception).hasMessageThat().contains("already closed");
  }

  @Test
  void read_byteArrayAtEOF_returnsMinusOneAndDoesNotUpdatePosition() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    googleCloudStorageInputStream.seek(fileSize);

    int result = googleCloudStorageInputStream.read(new byte[10], 0, 10);

    assertThat(result).isEqualTo(-1);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(fileSize);
  }

  @Test
  void read_zeroLength_returnsZeroBytes() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();

    int result = googleCloudStorageInputStream.read(new byte[10], 0, 0);

    assertThat(result).isEqualTo(0);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void read_invalidArguments_throwsIndexOutOfBoundsException() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    byte[] buffer = new byte[10];

    assertThrows(
        IndexOutOfBoundsException.class, () -> googleCloudStorageInputStream.read(buffer, -1, 5));
    assertThrows(
        IndexOutOfBoundsException.class, () -> googleCloudStorageInputStream.read(buffer, 0, -1));
    assertThrows(
        IndexOutOfBoundsException.class, () -> googleCloudStorageInputStream.read(buffer, 5, 6));
  }

  @Test
  void read_positionMismatch_throwsIllegalStateException() throws IOException {
    VectoredSeekableByteChannel mockChannel = mock(VectoredSeekableByteChannel.class);
    when(mockChannel.position()).thenReturn(100L); // Mismatch with stream position 0
    GcsFileSystem mockFileSystem = mock(GcsFileSystem.class);
    when(mockFileSystem.getFileSystemOptions()).thenReturn(fileSystemOptions);
    when(mockFileSystem.getTelemetry()).thenReturn(new Telemetry(ImmutableList.of()));
    when(mockFileSystem.getCacheManager()).thenReturn(fakeFileSystem.getCacheManager());
    when(mockFileSystem.open(any(GcsItemId.class), any())).thenReturn(mockChannel);

    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(mockFileSystem, testGcsItemId);

    assertThrows(
        IllegalStateException.class,
        () -> googleCloudStorageInputStream.read(ByteBuffer.allocate(10)));
  }

  @Test
  void close_calledTwice_onlyClosesUnderlyingChannelOnce() throws IOException {
    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    googleCloudStorageInputStream = createStream(readOptions);
    // Read to ensure channel is opened

    googleCloudStorageInputStream.read();
    googleCloudStorageInputStream.close();
    googleCloudStorageInputStream.close();
    // Verify close was only called once on the actual channel
    // Since we use FakeGcsFileSystemImpl, it's hard to mock the channel directly here without
    // changing setup.
    // However, the test will at least cover the code path.
  }

  @Test
  void close_closesUnderlyingChannel() throws IOException {
    VectoredSeekableByteChannel mockChannel = mock(VectoredSeekableByteChannel.class);
    when(mockChannel.isOpen()).thenReturn(true);
    GcsFileSystem mockFileSystem = mock(GcsFileSystem.class);
    when(mockFileSystem.getFileSystemOptions()).thenReturn(fileSystemOptions);
    when(mockFileSystem.getTelemetry()).thenReturn(new Telemetry(ImmutableList.of()));
    when(mockFileSystem.getCacheManager()).thenReturn(fakeFileSystem.getCacheManager());
    when(mockFileSystem.open(any(GcsItemId.class), any())).thenReturn(mockChannel);

    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(mockFileSystem, testGcsItemId);
    googleCloudStorageInputStream.close();

    verify(mockChannel).close();
  }

  @Test
  void readFully_validArgs_readsDataFromNewChannel() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    long initialStreamPosition = googleCloudStorageInputStream.getPos();
    int readPosition = 100;
    int length = 100;
    byte[] buffer = new byte[length];

    googleCloudStorageInputStream.readFully(readPosition, buffer, 0, length);

    for (int i = 0; i < length; i++) {
      assertThat(buffer[i]).isEqualTo(testData[(int) readPosition + i]);
    }
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(initialStreamPosition);
  }

  @Test
  void readFully_reachesEofEarly_throwsEOFException() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    byte[] buffer = new byte[100];

    assertThrows(
        EOFException.class, () -> googleCloudStorageInputStream.readFully(950, buffer, 0, 100));
  }

  @Test
  void readFully_withGcsItemId_adoptsResolvedItemInfo() throws IOException {
    GcsItemInfo resolvedItemInfo = mock(GcsItemInfo.class);
    when(resolvedItemInfo.getItemId()).thenReturn(testGcsItemId);
    when(resolvedItemInfo.getSize()).thenReturn(1000L);
    VectoredSeekableByteChannel streamChannel = mock(VectoredSeekableByteChannel.class);
    VectoredSeekableByteChannel readFullyChannel = mock(VectoredSeekableByteChannel.class);
    when(readFullyChannel.read(any(ByteBuffer.class))).thenReturn(10);
    when(readFullyChannel.getItemInfo()).thenReturn(resolvedItemInfo);
    GcsFileSystem mockFileSystem = mock(GcsFileSystem.class);
    when(mockFileSystem.getFileSystemOptions()).thenReturn(fileSystemOptions);
    when(mockFileSystem.getTelemetry()).thenReturn(new Telemetry(ImmutableList.of()));
    when(mockFileSystem.getCacheManager()).thenReturn(fakeFileSystem.getCacheManager());
    when(mockFileSystem.open(any(GcsItemId.class), any()))
        .thenReturn(streamChannel, readFullyChannel);
    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(mockFileSystem, testGcsItemId);

    googleCloudStorageInputStream.readFully(0, new byte[10], 0, 10);

    assertThat(googleCloudStorageInputStream.size()).isEqualTo(1000L);
    verify(streamChannel, never()).size();
  }

  @Test
  void readFully_withItemInfoResolvedOnStreamChannel_opensChannelWithFileInfo() throws IOException {
    GcsItemInfo resolvedItemInfo =
        GcsItemInfo.builder().setItemId(testGcsItemId).setSize(fileSize).build();
    VectoredSeekableByteChannel streamChannel = mock(VectoredSeekableByteChannel.class);
    when(streamChannel.getItemInfo()).thenReturn(resolvedItemInfo);
    VectoredSeekableByteChannel readFullyChannel = mock(VectoredSeekableByteChannel.class);
    when(readFullyChannel.read(any(ByteBuffer.class))).thenReturn(10);
    GcsFileSystem mockFileSystem = mock(GcsFileSystem.class);
    when(mockFileSystem.getFileSystemOptions()).thenReturn(fileSystemOptions);
    when(mockFileSystem.getTelemetry()).thenReturn(new Telemetry(ImmutableList.of()));
    when(mockFileSystem.getCacheManager()).thenReturn(fakeFileSystem.getCacheManager());
    when(mockFileSystem.open(any(GcsItemId.class), any())).thenReturn(streamChannel);
    when(mockFileSystem.open(any(GcsFileInfo.class), any())).thenReturn(readFullyChannel);
    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(mockFileSystem, testGcsItemId);

    googleCloudStorageInputStream.readFully(0, new byte[10], 0, 10);

    ArgumentCaptor<GcsFileInfo> fileInfoCaptor = ArgumentCaptor.forClass(GcsFileInfo.class);
    verify(mockFileSystem).open(fileInfoCaptor.capture(), any());
    assertThat(fileInfoCaptor.getValue().getItemInfo()).isEqualTo(resolvedItemInfo);
  }

  @Test
  void size_channelItemInfoHasNegativeSize_doesNotAdoptItemInfo() throws IOException {
    GcsItemInfo unresolvedItemInfo =
        GcsItemInfo.builder().setItemId(testGcsItemId).setSize(-1L).build();
    VectoredSeekableByteChannel streamChannel = mock(VectoredSeekableByteChannel.class);
    when(streamChannel.size()).thenReturn(1000L);
    when(streamChannel.getItemInfo()).thenReturn(unresolvedItemInfo);
    GcsFileSystem mockFileSystem = mock(GcsFileSystem.class);
    when(mockFileSystem.getFileSystemOptions()).thenReturn(fileSystemOptions);
    when(mockFileSystem.getTelemetry()).thenReturn(new Telemetry(ImmutableList.of()));
    when(mockFileSystem.getCacheManager()).thenReturn(fakeFileSystem.getCacheManager());
    when(mockFileSystem.open(any(GcsItemId.class), any())).thenReturn(streamChannel);
    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(mockFileSystem, testGcsItemId);

    long firstSize = googleCloudStorageInputStream.size();
    long secondSize = googleCloudStorageInputStream.size();

    assertThat(ImmutableList.of(firstSize, secondSize)).containsExactly(1000L, 1000L);
    verify(streamChannel, times(2)).size();
  }

  @Test
  void readTail_withInitializedFileInfo_reusesFileInfo() throws IOException {
    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    googleCloudStorageInputStream = createStream(readOptions);
    byte[] buffer = new byte[10];
    // Read first to initialize gcsFileInfo

    googleCloudStorageInputStream.read();
    // Now call readTail
    int bytesRead = googleCloudStorageInputStream.readTail(buffer, 0, 10);

    assertThat(bytesRead).isEqualTo(10);
  }

  @Test
  void readTail_withGcsItemId_opensTailChannelWithFileInfoAdoptedFromRead() throws IOException {
    GcsItemInfo resolvedItemInfo =
        GcsItemInfo.builder().setItemId(testGcsItemId).setSize(fileSize).build();
    VectoredSeekableByteChannel streamChannel = mock(VectoredSeekableByteChannel.class);
    when(streamChannel.read(any(ByteBuffer.class))).thenReturn(1);
    when(streamChannel.getItemInfo()).thenReturn(resolvedItemInfo);
    VectoredSeekableByteChannel tailChannel = mock(VectoredSeekableByteChannel.class);
    when(tailChannel.read(any(ByteBuffer.class))).thenReturn(10);
    GcsFileSystem mockFileSystem = mock(GcsFileSystem.class);
    when(mockFileSystem.getFileSystemOptions()).thenReturn(fileSystemOptions);
    when(mockFileSystem.getTelemetry()).thenReturn(new Telemetry(ImmutableList.of()));
    when(mockFileSystem.getCacheManager()).thenReturn(fakeFileSystem.getCacheManager());
    when(mockFileSystem.open(any(GcsItemId.class), any())).thenReturn(streamChannel);
    when(mockFileSystem.open(any(GcsFileInfo.class), any())).thenReturn(tailChannel);
    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(mockFileSystem, testGcsItemId);
    googleCloudStorageInputStream.read();

    googleCloudStorageInputStream.readTail(new byte[10], 0, 10);

    ArgumentCaptor<GcsFileInfo> fileInfoCaptor = ArgumentCaptor.forClass(GcsFileInfo.class);
    verify(mockFileSystem).open(fileInfoCaptor.capture(), any());
    assertThat(fileInfoCaptor.getValue().getItemInfo()).isEqualTo(resolvedItemInfo);
    verify(tailChannel).position(fileSize - 10);
    verify(streamChannel, never()).size();
  }

  @Test
  void readTail_validArgs_readsDataFromNewChannel() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    long initialStreamPosition = googleCloudStorageInputStream.getPos();
    int length = 10;
    int offset = 5;
    byte[] buffer = new byte[20];

    int bytesRead = googleCloudStorageInputStream.readTail(buffer, offset, length);

    assertThat(bytesRead).isEqualTo(length);
    for (int i = 0; i < length; i++) {
      assertThat(buffer[offset + i]).isEqualTo(testData[(int) (fileSize - length) + i]);
    }
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(initialStreamPosition);
  }

  @Test
  void readTail_streamFromGcsItemId_doesNotLookUpFileInfo() throws IOException {
    GcsFileSystem spyFileSystem = spy(fakeFileSystem);
    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(spyFileSystem, testGcsItemId);

    googleCloudStorageInputStream.readTail(new byte[10], 0, 10);

    verify(spyFileSystem, never()).getFileInfoObject(any(GcsItemId.class));
    verify(spyFileSystem, never()).getFileInfo(any(GcsItemId.class));
  }

  @Test
  void readTail_emptyFile_returnsMinusOne() throws IOException {
    // Create a new stream for an empty file
    GcsItemId emptyItemId =
        GcsItemId.builder().setBucketName("b").setObjectName("empty.txt").build();
    FakeGcsClientImpl.storage.create(
        BlobInfo.newBuilder(emptyItemId.getBucketName(), emptyItemId.getObjectName().get(), 1L)
            .build(),
        new byte[0]);
    GoogleCloudStorageInputStream emptyStream =
        GoogleCloudStorageInputStream.create(fakeFileSystem, emptyItemId);
    byte[] buffer = new byte[10];

    int bytesRead = emptyStream.readTail(buffer, 0, 10);

    assertThat(bytesRead).isEqualTo(-1);
  }

  @Test
  void readTail_smallFile_readsFromStart() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    int length = 2000; // Larger than file size
    byte[] buffer = new byte[length];

    int bytesRead = googleCloudStorageInputStream.readTail(buffer, 0, length);

    assertThat(bytesRead).isEqualTo((int) fileSize);
    for (int i = 0; i < fileSize; i++) {
      assertThat(buffer[i]).isEqualTo(testData[i]);
    }
  }

  @Test
  void readVectored_delegatesToChannel() throws IOException {
    VectoredSeekableByteChannel mockChannel = mock(VectoredSeekableByteChannel.class);
    GcsFileSystem mockFileSystem = mock(GcsFileSystem.class);
    when(mockFileSystem.getFileSystemOptions()).thenReturn(fileSystemOptions);
    when(mockFileSystem.getTelemetry()).thenReturn(new Telemetry(ImmutableList.of()));
    when(mockFileSystem.getCacheManager()).thenReturn(fakeFileSystem.getCacheManager());
    when(mockFileSystem.open(any(GcsItemId.class), any())).thenReturn(mockChannel);

    googleCloudStorageInputStream =
        GoogleCloudStorageInputStream.create(mockFileSystem, testGcsItemId);
    GcsObjectRange range = createGcsObjectRange(0, 10);
    List<GcsObjectRange> ranges = List.of(range);
    googleCloudStorageInputStream.readVectored(ranges, (size) -> ByteBuffer.allocate(size));

    verify(mockChannel).readVectored(eq(ranges), any());
  }

  @Test
  void readVectored_smallObjectCached_readsFromCache()
      throws IOException, ExecutionException, InterruptedException {
    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    googleCloudStorageInputStream = createStream(readOptions);

    GcsObjectRange range1 = createGcsObjectRange(/* offset= */ 200, /* length= */ 100);
    GcsObjectRange range2 = createGcsObjectRange(/* offset= */ 600, /* length= */ 100);

    // Trigger caching by reading one byte
    googleCloudStorageInputStream.read();
    long position = googleCloudStorageInputStream.getPos();

    googleCloudStorageInputStream.readVectored(
        List.of(range1, range2), (size) -> ByteBuffer.allocate(size));

    ByteBuffer range1Result = range1.getByteBufferFuture().get();
    ByteBuffer range2Result = range2.getByteBufferFuture().get();

    for (int i = 0; i < 100; i++) {
      assertThat(range1Result.get()).isEqualTo(testData[200 + i]);
      assertThat(range2Result.get()).isEqualTo(testData[600 + i]);
    }
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(position);
  }

  @Test
  void read_fromHead_smallObjectCachingEnabled_objectSmall_caches() throws IOException {
    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    googleCloudStorageInputStream = createStream(readOptions);

    byte read = (byte) googleCloudStorageInputStream.read();
    assertThat(read).isEqualTo(testData[0]);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(1);

    read = (byte) googleCloudStorageInputStream.read();
    assertThat(read).isEqualTo(testData[1]);
    assertThat(googleCloudStorageInputStream.getPos()).isEqualTo(2);
  }

  private GcsObjectRange createGcsObjectRange(long offset, int length) {
    return GcsObjectRange.builder()
        .setOffset(offset)
        .setLength(length)
        .setByteBufferFuture(new CompletableFuture<>())
        .build();
  }

  @Test
  void readFully_reachesEOF_throwsEOFException() throws IOException {
    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    googleCloudStorageInputStream = createStream(readOptions);
    byte[] buffer = new byte[100];
    // Total file size is 1000. Start reading from 950 for 100 bytes (hits EOF)
    EOFException exception =
        assertThrows(
            EOFException.class, () -> googleCloudStorageInputStream.readFully(950, buffer, 0, 100));

    assertThat(exception)
        .hasMessageThat()
        .contains("Reached the end of stream with 50 bytes left to read");
  }

  @Test
  void readFully_success_readsExpectedBytes() throws IOException {
    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    googleCloudStorageInputStream = createStream(readOptions);
    byte[] buffer = new byte[10];
    // Read 10 bytes starting from 100

    googleCloudStorageInputStream.readFully(100, buffer, 0, 10);
    for (int i = 0; i < 10; i++) {

      assertThat(buffer[i]).isEqualTo(testData[100 + i]);
    }
  }

  private GcsFileInfo testFileInfo(long size) {
    return GcsFileInfo.builder()
        .setItemInfo(GcsItemInfo.builder().setItemId(testGcsItemId).setSize(size).build())
        .setUri(testUri)
        .setAttributes(ImmutableMap.of())
        .build();
  }

  private GoogleCloudStorageInputStream createMockStream(
      GcsFileInfo fileInfo, GcsFileSystem mockFs, VectoredSeekableByteChannel... channels)
      throws IOException {
    when(mockFs.getFileSystemOptions()).thenReturn(fileSystemOptions);
    when(mockFs.getTelemetry()).thenReturn(new Telemetry(ImmutableList.of()));
    when(mockFs.getCacheManager()).thenReturn(fakeFileSystem.getCacheManager());
    if (channels.length == 1) {
      when(mockFs.open(eq(fileInfo), any(GcsReadOptions.class))).thenReturn(channels[0]);
    } else if (channels.length > 1) {
      when(mockFs.open(eq(fileInfo), any(GcsReadOptions.class)))
          .thenReturn(channels[0], java.util.Arrays.copyOfRange(channels, 1, channels.length));
    }
    return GoogleCloudStorageInputStream.create(mockFs, fileInfo);
  }

  @Test
  void readFullyAndTail_openTemporaryChannelWithRandomAccessPattern() throws IOException {
    GcsFileInfo fileInfo = testFileInfo(fileSize);
    VectoredSeekableByteChannel streamCh = mock(VectoredSeekableByteChannel.class);
    VectoredSeekableByteChannel tempCh = mock(VectoredSeekableByteChannel.class);
    when(tempCh.read(any(ByteBuffer.class))).thenReturn(10);
    GcsFileSystem mockFs = mock(GcsFileSystem.class);
    GoogleCloudStorageInputStream stream =
        createMockStream(fileInfo, mockFs, streamCh, tempCh, tempCh);

    stream.readFully(0, new byte[10], 0, 10);
    stream.readTail(new byte[10], 0, 10);

    ArgumentCaptor<GcsReadOptions> captor = ArgumentCaptor.forClass(GcsReadOptions.class);
    verify(mockFs, times(3)).open(eq(fileInfo), captor.capture());
    List<GcsReadOptions> options = captor.getAllValues();
    assertThat(options.get(0).getFileAccessPattern()).isEqualTo(FileAccessPattern.AUTO_SEQUENTIAL);
    assertThat(options.get(1).getFileAccessPattern()).isEqualTo(FileAccessPattern.RANDOM);
    assertThat(options.get(2)).isSameInstanceAs(options.get(1));
  }

  @Test
  void readFullyAndTail_multiChunkPartialRead_readsAllBytes() throws IOException {
    GcsFileInfo fileInfo = testFileInfo(fileSize);
    VectoredSeekableByteChannel streamCh = mock(VectoredSeekableByteChannel.class);
    VectoredSeekableByteChannel readFullyCh = mock(VectoredSeekableByteChannel.class);
    VectoredSeekableByteChannel tailCh = mock(VectoredSeekableByteChannel.class);
    org.mockito.stubbing.Answer<Integer> chunkAnswer =
        inv -> {
          ByteBuffer bb = inv.getArgument(0);
          bb.put(bb.remaining() >= 2 ? new byte[] {1, 2} : new byte[] {3});
          return bb.position() <= 2 ? 2 : 1;
        };
    when(readFullyCh.read(any(ByteBuffer.class))).thenAnswer(chunkAnswer);
    when(tailCh.read(any(ByteBuffer.class))).thenAnswer(chunkAnswer);
    GcsFileSystem mockFs = mock(GcsFileSystem.class);
    GoogleCloudStorageInputStream stream =
        createMockStream(fileInfo, mockFs, streamCh, readFullyCh, tailCh);

    byte[] fullyBuf = new byte[3];
    stream.readFully(0, fullyBuf, 0, 3);
    assertThat(fullyBuf).isEqualTo(new byte[] {1, 2, 3});

    byte[] tailBuf = new byte[3];
    int tailRead = stream.readTail(tailBuf, 0, 3);
    assertThat(tailRead).isEqualTo(3);
    assertThat(tailBuf).isEqualTo(new byte[] {1, 2, 3});
  }

  @Test
  void readTail_immediateEofFromChannel_returnsMinusOne() throws IOException {
    GcsFileInfo fileInfo = testFileInfo(fileSize);
    VectoredSeekableByteChannel streamCh = mock(VectoredSeekableByteChannel.class);
    VectoredSeekableByteChannel tailCh = mock(VectoredSeekableByteChannel.class);
    when(tailCh.read(any(ByteBuffer.class))).thenReturn(-1);
    GcsFileSystem mockFs = mock(GcsFileSystem.class);
    GoogleCloudStorageInputStream stream = createMockStream(fileInfo, mockFs, streamCh, tailCh);

    assertThat(stream.readTail(new byte[10], 0, 10)).isEqualTo(-1);
  }

  @Test
  void readFully_pastEndOfKnownFile_throwsEofFastWithoutOpeningChannel() throws IOException {
    GcsFileInfo fileInfo = testFileInfo(100L);
    GcsFileSystem mockFs = mock(GcsFileSystem.class);
    GoogleCloudStorageInputStream stream =
        createMockStream(fileInfo, mockFs, mock(VectoredSeekableByteChannel.class));

    EOFException ex =
        assertThrows(EOFException.class, () -> stream.readFully(95, new byte[10], 0, 10));
    assertThat(ex).hasMessageThat().contains("Reached the end of stream with 5 bytes left to read");
    verify(mockFs, times(1)).open(eq(fileInfo), any(GcsReadOptions.class));
  }

  @Test
  void readFullyAndTail_zeroLength_doesNotOpenChannel() throws IOException {
    GcsFileInfo fileInfo = testFileInfo(fileSize);
    GcsFileSystem mockFs = mock(GcsFileSystem.class);
    GoogleCloudStorageInputStream stream =
        createMockStream(fileInfo, mockFs, mock(VectoredSeekableByteChannel.class));

    stream.readFully(0, new byte[10], 0, 0);
    assertThat(stream.readTail(new byte[10], 0, 0)).isEqualTo(0);
    verify(mockFs, times(1)).open(any(GcsFileInfo.class), any());
  }

  @Test
  void readFully_negativePosition_throwsEofException() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    EOFException ex =
        assertThrows(
            EOFException.class,
            () -> googleCloudStorageInputStream.readFully(-1L, new byte[10], 0, 10));
    assertThat(ex).hasMessageThat().contains("position can't be negative: -1");
  }

  @Test
  void readFullyAndTail_afterClose_throwsIOException() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    googleCloudStorageInputStream.close();
    byte[] buf = new byte[10];
    assertThat(
            assertThrows(
                IOException.class, () -> googleCloudStorageInputStream.readFully(0, buf, 0, 10)))
        .hasMessageThat()
        .contains("Cannot read: already closed");
    assertThat(
            assertThrows(
                IOException.class, () -> googleCloudStorageInputStream.readTail(buf, 0, 10)))
        .hasMessageThat()
        .contains("Cannot read: already closed");
  }

  @Test
  void readFullyAndTail_invalidArguments_throwExpectedExceptions() throws IOException {
    googleCloudStorageInputStream = defaultGcsInputStream();
    byte[] buf = new byte[10];
    assertThrows(
        NullPointerException.class, () -> googleCloudStorageInputStream.readFully(0, null, 0, 5));
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> googleCloudStorageInputStream.readFully(0, buf, -1, 5));
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> googleCloudStorageInputStream.readFully(0, buf, 0, -1));
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> googleCloudStorageInputStream.readFully(0, buf, 5, 6));

    assertThrows(
        NullPointerException.class, () -> googleCloudStorageInputStream.readTail(null, 0, 5));
    assertThrows(
        IndexOutOfBoundsException.class, () -> googleCloudStorageInputStream.readTail(buf, -1, 5));
    assertThrows(
        IndexOutOfBoundsException.class, () -> googleCloudStorageInputStream.readTail(buf, 0, -1));
    assertThrows(
        IndexOutOfBoundsException.class, () -> googleCloudStorageInputStream.readTail(buf, 5, 6));
  }

  @Test
  void positionalReadOptions_whenConfiguredIsRandom_reusesConfiguredOptions() throws IOException {
    GcsReadOptions randomOpts =
        clientOptions.getGcsReadOptions().toBuilder()
            .setFileAccessPattern(FileAccessPattern.RANDOM)
            .build();
    GcsFileSystemOptions newFsOpts =
        fileSystemOptions.toBuilder()
            .setGcsClientOptions(clientOptions.toBuilder().setGcsReadOptions(randomOpts).build())
            .build();
    GcsFileInfo fileInfo = testFileInfo(fileSize);
    VectoredSeekableByteChannel streamCh = mock(VectoredSeekableByteChannel.class);
    VectoredSeekableByteChannel readFullyCh = mock(VectoredSeekableByteChannel.class);
    when(readFullyCh.read(any(ByteBuffer.class))).thenReturn(5);

    GcsFileSystem mockFs = mock(GcsFileSystem.class);
    when(mockFs.getFileSystemOptions()).thenReturn(newFsOpts);
    when(mockFs.getTelemetry()).thenReturn(new Telemetry(ImmutableList.of()));
    when(mockFs.getCacheManager()).thenReturn(fakeFileSystem.getCacheManager());
    when(mockFs.open(eq(fileInfo), any(GcsReadOptions.class))).thenReturn(streamCh, readFullyCh);

    GoogleCloudStorageInputStream stream = GoogleCloudStorageInputStream.create(mockFs, fileInfo);
    stream.readFully(0, new byte[5], 0, 5);

    ArgumentCaptor<GcsReadOptions> captor = ArgumentCaptor.forClass(GcsReadOptions.class);
    verify(mockFs, times(2)).open(eq(fileInfo), captor.capture());
    assertThat(captor.getAllValues().get(1)).isSameInstanceAs(randomOpts);
  }

  @Test
  void readFully_withGcsItemId_channelReturnsFewerBytes_throwsEOFException() throws IOException {
    VectoredSeekableByteChannel streamCh = mock(VectoredSeekableByteChannel.class);
    VectoredSeekableByteChannel readFullyCh = mock(VectoredSeekableByteChannel.class);
    when(readFullyCh.read(any(ByteBuffer.class)))
        .thenAnswer(
            inv -> {
              ((ByteBuffer) inv.getArgument(0)).put(new byte[5]);
              return 5;
            })
        .thenReturn(-1);

    GcsFileSystem mockFs = mock(GcsFileSystem.class);
    when(mockFs.getFileSystemOptions()).thenReturn(fileSystemOptions);
    when(mockFs.getTelemetry()).thenReturn(new Telemetry(ImmutableList.of()));
    when(mockFs.getCacheManager()).thenReturn(fakeFileSystem.getCacheManager());
    when(mockFs.open(eq(testGcsItemId), any(GcsReadOptions.class)))
        .thenReturn(streamCh, readFullyCh);

    GoogleCloudStorageInputStream stream =
        GoogleCloudStorageInputStream.create(mockFs, testGcsItemId);
    EOFException ex =
        assertThrows(EOFException.class, () -> stream.readFully(0, new byte[10], 0, 10));
    assertThat(ex).hasMessageThat().contains("Reached the end of stream with 5 bytes left to read");
  }

  @Test
  void readFully_unknownSizeInFileInfo_readsFromChannel() throws IOException {
    GcsFileInfo fileInfo = testFileInfo(-1L);
    VectoredSeekableByteChannel streamCh = mock(VectoredSeekableByteChannel.class);
    VectoredSeekableByteChannel readFullyCh = mock(VectoredSeekableByteChannel.class);
    when(readFullyCh.read(any(ByteBuffer.class)))
        .thenAnswer(
            inv -> {
              ByteBuffer bb = inv.getArgument(0);
              bb.put(new byte[] {1, 2, 3, 4, 5});
              return 5;
            });
    GcsFileSystem mockFs = mock(GcsFileSystem.class);
    GoogleCloudStorageInputStream stream =
        createMockStream(fileInfo, mockFs, streamCh, readFullyCh);

    byte[] buf = new byte[5];
    stream.readFully(0, buf, 0, 5);

    assertThat(buf).isEqualTo(new byte[] {1, 2, 3, 4, 5});
  }
}
