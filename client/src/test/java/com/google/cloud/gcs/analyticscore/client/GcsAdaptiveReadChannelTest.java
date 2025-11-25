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
package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

class GcsAdaptiveReadChannelTest {

  private static final String TEST_PROJECT_ID = "test-project-id";
  private static GcsReadOptions TEST_GCS_READ_OPTIONS =
      GcsReadOptions.builder().setUserProjectId(TEST_PROJECT_ID).build();

  private final Supplier<ExecutorService> executorServiceSupplier =
      Suppliers.memoize(() -> Executors.newFixedThreadPool(30));
  private final Storage storage = Mockito.spy(LocalStorageHelper.getOptions().getService());

  @Test
  void read_inChunks_fillsBuffersAndAdvancesPosition() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
    ByteBuffer buffer1 = ByteBuffer.allocate(5);
    ByteBuffer buffer2 = ByteBuffer.allocate(6);

    int bytesRead1 = gcsReadChannel.read(buffer1);
    int bytesRead2 = gcsReadChannel.read(buffer2);

    assertThat(bytesRead1).isEqualTo(5);
    assertThat(new String(buffer1.array(), StandardCharsets.UTF_8)).isEqualTo("hello");
    assertThat(bytesRead2).isEqualTo(6);
    assertThat(new String(buffer2.array(), StandardCharsets.UTF_8)).isEqualTo(" world");
    assertThat(gcsReadChannel.position()).isEqualTo(11);
  }

  @Test
  void read_fullObject_fillEntireObjectIntoBuffer() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
    ByteBuffer buffer = ByteBuffer.allocate(objectData.length());

    int bytesRead = gcsReadChannel.read(buffer);

    assertThat(bytesRead).isEqualTo(objectData.length());
    assertThat(new String(buffer.array(), StandardCharsets.UTF_8)).isEqualTo(objectData);
    assertThat(gcsReadChannel.position()).isEqualTo(objectData.length());
  }

  @Test
  void read_withSeek_advancesPositionAndReadsIntoBuffer() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
    gcsReadChannel.position(6);
    ByteBuffer buffer = ByteBuffer.allocate(5);

    int bytesRead = gcsReadChannel.read(buffer);

    assertThat(bytesRead).isEqualTo(5);
    assertThat(new String(buffer.array(), StandardCharsets.UTF_8)).isEqualTo("world");
    assertThat(gcsReadChannel.position()).isEqualTo(11L);
  }

  @Test
  void isOpen_forUnClosedChannel_returnsTrue() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);

    assertThat(gcsReadChannel.isOpen()).isTrue();
  }

  @Test
  void isOpen_forClosedChannel_returnsFalse() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
    gcsReadChannel.close();

    assertThat(gcsReadChannel.isOpen()).isFalse();
  }

  @Test
  void read_withSequentialFileAccessPattern_readsUntilEnd() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world this is a test string for sequential access";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsReadOptions options =
        TEST_GCS_READ_OPTIONS.toBuilder()
            .setFileAccessPattern(FileAccessPattern.SEQUENTIAL)
            .setMinRangeRequestSize(5)
            .build();
    List<ReadChannel> capturedChannels = Lists.newArrayList();
    Mockito.doAnswer(
            invocation -> {
              ReadChannel realChannel = (ReadChannel) invocation.callRealMethod();
              ReadChannel spyChannel = Mockito.spy(realChannel);
              capturedChannels.add(spyChannel);
              return spyChannel;
            })
        .when(storage)
        .reader(Mockito.any(BlobId.class), Mockito.any());
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(storage, itemInfo, options, executorServiceSupplier);
    ByteBuffer buffer = ByteBuffer.allocate(5);

    gcsReadChannel.read(buffer);

    assertThat(capturedChannels).hasSize(1);
    Mockito.verify(capturedChannels.get(0)).limit(objectData.length());
  }

  @Test
  void read_withSequentialFileAccessPattern_doesNotSwitchToRandomOnBackwardSeek()
      throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "0123456789";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsReadOptions options =
        TEST_GCS_READ_OPTIONS.toBuilder()
            .setFileAccessPattern(FileAccessPattern.SEQUENTIAL)
            .setMinRangeRequestSize(2)
            .build();
    List<ReadChannel> capturedChannels = Lists.newArrayList();
    Mockito.doAnswer(
            invocation -> {
              ReadChannel realChannel = (ReadChannel) invocation.callRealMethod();
              ReadChannel spyChannel = Mockito.spy(realChannel);
              capturedChannels.add(spyChannel);
              return spyChannel;
            })
        .when(storage)
        .reader(Mockito.any(BlobId.class), Mockito.any());
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(storage, itemInfo, options, executorServiceSupplier);

    gcsReadChannel.read(ByteBuffer.allocate(2));
    gcsReadChannel.position(0);
    gcsReadChannel.read(ByteBuffer.allocate(2));

    assertThat(capturedChannels).hasSize(2);
    Mockito.verify(capturedChannels.get(0)).limit(objectData.length());
    Mockito.verify(capturedChannels.get(1)).limit(objectData.length());
  }

  @Test
  void read_withAutoFileAccessPattern_startsSequential() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsReadOptions options =
        TEST_GCS_READ_OPTIONS.toBuilder()
            .setFileAccessPattern(FileAccessPattern.AUTO)
            .setMinRangeRequestSize(2)
            .build();
    List<ReadChannel> capturedChannels = Lists.newArrayList();
    Mockito.doAnswer(
            invocation -> {
              ReadChannel realChannel = (ReadChannel) invocation.callRealMethod();
              ReadChannel spyChannel = Mockito.spy(realChannel);
              capturedChannels.add(spyChannel);
              return spyChannel;
            })
        .when(storage)
        .reader(Mockito.any(BlobId.class), Mockito.any());
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(storage, itemInfo, options, executorServiceSupplier);

    gcsReadChannel.read(ByteBuffer.allocate(2));

    assertThat(capturedChannels).hasSize(1);
    Mockito.verify(capturedChannels.get(0)).limit(objectData.length());
  }

  @Test
  void read_withAutoFileAccessPattern_switchesToRandomOnLargeForwardSeek() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "01234567890123456789"; // 20 bytes
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsReadOptions options =
        TEST_GCS_READ_OPTIONS.toBuilder()
            .setFileAccessPattern(FileAccessPattern.AUTO)
            .setMinRangeRequestSize(2)
            .setInplaceSeekLimit(2)
            .build();
    List<ReadChannel> capturedChannels = Lists.newArrayList();
    Mockito.doAnswer(
            invocation -> {
              ReadChannel realChannel = (ReadChannel) invocation.callRealMethod();
              ReadChannel spyChannel = Mockito.spy(realChannel);
              capturedChannels.add(spyChannel);
              return spyChannel;
            })
        .when(storage)
        .reader(Mockito.any(BlobId.class), Mockito.any());
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(storage, itemInfo, options, executorServiceSupplier);

    gcsReadChannel.read(ByteBuffer.allocate(2));
    gcsReadChannel.position(10);
    gcsReadChannel.read(ByteBuffer.allocate(2));

    assertThat(capturedChannels).hasSize(2);
    Mockito.verify(capturedChannels.get(0)).limit(objectData.length());
    Mockito.verify(capturedChannels.get(1)).limit(12);
  }

  @Test
  void read_withAutoFileAccessPattern_staysRandomAfterSwitch() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "0123456789";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsReadOptions options =
        TEST_GCS_READ_OPTIONS.toBuilder()
            .setFileAccessPattern(FileAccessPattern.AUTO)
            .setMinRangeRequestSize(2)
            .build();
    List<ReadChannel> capturedChannels = Lists.newArrayList();
    Mockito.doAnswer(
            invocation -> {
              ReadChannel realChannel = (ReadChannel) invocation.callRealMethod();
              ReadChannel spyChannel = Mockito.spy(realChannel);
              capturedChannels.add(spyChannel);
              return spyChannel;
            })
        .when(storage)
        .reader(Mockito.any(BlobId.class), Mockito.any());
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(storage, itemInfo, options, executorServiceSupplier);

    gcsReadChannel.read(ByteBuffer.allocate(2));
    gcsReadChannel.position(0);
    gcsReadChannel.read(ByteBuffer.allocate(2));
    gcsReadChannel.read(ByteBuffer.allocate(2));

    assertThat(capturedChannels).hasSize(3);
    Mockito.verify(capturedChannels.get(0)).limit(objectData.length());
    Mockito.verify(capturedChannels.get(1)).limit(2);
    Mockito.verify(capturedChannels.get(2)).limit(4);
  }

  private GcsObjectRange createRange(long offset, int length) {
    return GcsObjectRange.builder()
        .setOffset(offset)
        .setLength(length)
        .setByteBufferFuture(new CompletableFuture<>())
        .build();
  }

  private ImmutableList<GcsObjectRange> createRanges(
      ImmutableMap<Long, Integer> offsetToLengthMap) {
    return offsetToLengthMap.entrySet().stream()
        .map(entry -> createRange(entry.getKey(), entry.getValue()))
        .collect(ImmutableList.toImmutableList());
  }

  private void createBlobInStorage(BlobId blobId, String blobContent) {
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, blobContent.getBytes(StandardCharsets.UTF_8));
  }

  private String getGcsObjectRangeData(GcsObjectRange range)
      throws ExecutionException, InterruptedException {
    return StandardCharsets.UTF_8.decode(range.getByteBufferFuture().get()).toString();
  }

  @Test
  void read_zeroRemaining_returnsZero() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(10).setContentGeneration(0L).build();
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
    ByteBuffer buffer = ByteBuffer.allocate(0);

    int bytesRead = gcsReadChannel.read(buffer);

    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void read_atEndOfFile_returnsMinusOne() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(10).setContentGeneration(0L).build();
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
    gcsReadChannel.position(10);
    ByteBuffer buffer = ByteBuffer.allocate(1);

    int bytesRead = gcsReadChannel.read(buffer);

    assertThat(bytesRead).isEqualTo(-1);
  }

  @Test
  void read_closedChannel_throwsClosedChannelException() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(10).setContentGeneration(0L).build();
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
    gcsReadChannel.close();
    ByteBuffer buffer = ByteBuffer.allocate(1);

    assertThrows(ClosedChannelException.class, () -> gcsReadChannel.read(buffer));
  }

  @Test
  void position_samePosition_noOp() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(10).setContentGeneration(0L).build();
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);

    gcsReadChannel.position(0);

    assertThat(gcsReadChannel.position()).isEqualTo(0);
  }

  @Test
  void position_closedChannel_throwsClosedChannelException() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(10).setContentGeneration(0L).build();
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
    gcsReadChannel.close();

    assertThrows(ClosedChannelException.class, () -> gcsReadChannel.position(0));
  }

  @Test
  void readContent_unexpectedEOF_throwsIOException() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(100).setContentGeneration(0L).build();
    Storage mockStorage = Mockito.mock(Storage.class);
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(
            mockStorage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
    ReadChannel mockChannel = Mockito.mock(ReadChannel.class);
    Mockito.when(mockStorage.reader(Mockito.any(BlobId.class), Mockito.any()))
        .thenReturn(mockChannel);
    Mockito.when(mockChannel.read(Mockito.any(ByteBuffer.class))).thenReturn(-1);

    IOException e =
        assertThrows(IOException.class, () -> gcsReadChannel.read(ByteBuffer.allocate(10)));
    assertThat(e)
        .hasMessageThat()
        .contains("Received end of stream result before all requestedBytes were received");
  }

  @Test
  void openByteChannel_exception_throwsIOException() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(100).setContentGeneration(0L).build();
    Storage mockStorage = Mockito.mock(Storage.class);
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(
            mockStorage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
    ReadChannel mockChannel = Mockito.mock(ReadChannel.class);
    Mockito.when(mockStorage.reader(Mockito.any(BlobId.class), Mockito.any()))
        .thenReturn(mockChannel);
    Mockito.doThrow(new RuntimeException("Seek failed")).when(mockChannel).seek(Mockito.anyLong());

    IOException e =
        assertThrows(IOException.class, () -> gcsReadChannel.read(ByteBuffer.allocate(10)));
    assertThat(e)
        .hasMessageThat()
        .contains("Unable to update the boundaries/Range of contentChannel");
  }

  @Test
  void skipInPlace_eof_closesChannel() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(100).setContentGeneration(0L).build();
    GcsReadOptions options = TEST_GCS_READ_OPTIONS.toBuilder().setInplaceSeekLimit(100).build();
    Storage mockStorage = Mockito.mock(Storage.class);
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(mockStorage, itemInfo, options, executorServiceSupplier);
    ReadChannel mockChannel = Mockito.mock(ReadChannel.class);
    Mockito.when(mockStorage.reader(Mockito.any(BlobId.class), Mockito.any()))
        .thenReturn(mockChannel);
    Mockito.when(mockChannel.read(Mockito.any(ByteBuffer.class)))
        .thenAnswer(answerFillBuffer())
        .thenReturn(-1) // For skipInPlace
        .thenAnswer(answerFillBuffer()); // For subsequent read

    gcsReadChannel.read(ByteBuffer.allocate(10));
    gcsReadChannel.position(20);
    gcsReadChannel.read(ByteBuffer.allocate(10));

    Mockito.verify(mockChannel).close();
  }

  @Test
  void skipInPlace_exception_closesChannel() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(100).setContentGeneration(0L).build();
    GcsReadOptions options = TEST_GCS_READ_OPTIONS.toBuilder().setInplaceSeekLimit(100).build();
    Storage mockStorage = Mockito.mock(Storage.class);
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(mockStorage, itemInfo, options, executorServiceSupplier);
    ReadChannel mockChannel = Mockito.mock(ReadChannel.class);
    Mockito.when(mockStorage.reader(Mockito.any(BlobId.class), Mockito.any()))
        .thenReturn(mockChannel);
    Mockito.when(mockChannel.read(Mockito.any(ByteBuffer.class)))
        .thenAnswer(answerFillBuffer())
        .thenThrow(new IOException("Skip failed"));

    gcsReadChannel.read(ByteBuffer.allocate(10));
    gcsReadChannel.position(20);

    assertThrows(IOException.class, () -> gcsReadChannel.read(ByteBuffer.allocate(10)));
    Mockito.verify(mockChannel, Mockito.atLeast(1)).close();
  }

  @Test
  void closeContentChannel_exception_ignores() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(100).setContentGeneration(0L).build();
    Storage mockStorage = Mockito.mock(Storage.class);
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(
            mockStorage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
    ReadChannel mockChannel = Mockito.mock(ReadChannel.class);
    Mockito.when(mockStorage.reader(Mockito.any(BlobId.class), Mockito.any()))
        .thenReturn(mockChannel);
    Mockito.when(mockChannel.read(Mockito.any(ByteBuffer.class))).thenAnswer(answerFillBuffer());
    gcsReadChannel.read(ByteBuffer.allocate(10));
    Mockito.doThrow(new RuntimeException("Close failed")).when(mockChannel).close();

    gcsReadChannel.close();

    Mockito.verify(mockChannel).close();
    assertThat(gcsReadChannel.isOpen()).isFalse();
  }

  @Test
  void read_exception_closesContentChannel() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(100).setContentGeneration(0L).build();
    Storage mockStorage = Mockito.mock(Storage.class);
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(
            mockStorage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
    ReadChannel mockChannel = Mockito.mock(ReadChannel.class);
    Mockito.when(mockStorage.reader(Mockito.any(BlobId.class), Mockito.any()))
        .thenReturn(mockChannel);
    Mockito.when(mockChannel.read(Mockito.any(ByteBuffer.class)))
        .thenThrow(new IOException("Read failed"));

    assertThrows(IOException.class, () -> gcsReadChannel.read(ByteBuffer.allocate(10)));

    Mockito.verify(mockChannel).close();
  }

  private static Answer<Integer> answerFillBuffer() {
    return invocation -> {
      ByteBuffer buffer = invocation.getArgument(0);
      int remaining = buffer.remaining();
      buffer.position(buffer.position() + remaining);
      return remaining;
    };
  }

  @Test
  void skipInPlace_successfulSkip_updatesPosition() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(100).setContentGeneration(0L).build();
    GcsReadOptions options = TEST_GCS_READ_OPTIONS.toBuilder().setInplaceSeekLimit(100).build();
    Storage mockStorage = Mockito.mock(Storage.class);
    GcsAdaptiveReadChannel gcsReadChannel =
        new GcsAdaptiveReadChannel(mockStorage, itemInfo, options, executorServiceSupplier);
    ReadChannel mockChannel = Mockito.mock(ReadChannel.class);
    Mockito.when(mockStorage.reader(Mockito.any(BlobId.class), Mockito.any()))
        .thenReturn(mockChannel);
    Mockito.when(mockChannel.read(Mockito.any(ByteBuffer.class))).thenAnswer(answerFillBuffer());

    gcsReadChannel.read(ByteBuffer.allocate(10));
    gcsReadChannel.position(15);
    gcsReadChannel.read(ByteBuffer.allocate(10));

    ArgumentCaptor<ByteBuffer> captor = ArgumentCaptor.forClass(ByteBuffer.class);
    Mockito.verify(mockChannel, Mockito.times(3)).read(captor.capture());
    List<ByteBuffer> buffers = captor.getAllValues();
    assertThat(buffers.get(0).capacity()).isEqualTo(10);
    assertThat(buffers.get(1).limit()).isEqualTo(5);
    assertThat(buffers.get(2).capacity()).isEqualTo(10);
  }
}
