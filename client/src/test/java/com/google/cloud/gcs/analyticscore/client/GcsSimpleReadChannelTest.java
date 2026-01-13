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

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class GcsSimpleReadChannelTest {

  private static final String TEST_PROJECT_ID = "test-project-id";
  private static GcsReadOptions TEST_GCS_READ_OPTIONS =
      GcsReadOptions.builder().setUserProjectId(TEST_PROJECT_ID).build();

  private final Supplier<ExecutorService> executorServiceSupplier =
      Suppliers.memoize(() -> Executors.newFixedThreadPool(30));
  private final Storage storage = Mockito.spy(LocalStorageHelper.getOptions().getService());

  @Test
  void constructor_withChunkSize_setsChunkSizeOnReadChannel() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(100).setContentGeneration(0L).build();
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setUserProjectId(TEST_PROJECT_ID).setChunkSize(1024).build();
    Storage mockStorage = Mockito.mock(Storage.class);
    ReadChannel mockReadChannel = Mockito.mock(ReadChannel.class);
    Mockito.when(
            mockStorage.reader(
                Mockito.any(BlobId.class), Mockito.any(Storage.BlobSourceOption[].class)))
        .thenReturn(mockReadChannel);
    Mockito.when(mockReadChannel.isOpen()).thenReturn(true);

    new GcsSimpleReadChannel(mockStorage, itemInfo, readOptions, executorServiceSupplier);

    Mockito.verify(mockReadChannel).setChunkSize(1024);
  }

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
    GcsSimpleReadChannel gcsReadChannel =
        new GcsSimpleReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
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
    GcsSimpleReadChannel gcsReadChannel =
        new GcsSimpleReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
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
    GcsSimpleReadChannel gcsReadChannel =
        new GcsSimpleReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
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
    GcsSimpleReadChannel gcsReadChannel =
        new GcsSimpleReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);

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
    GcsSimpleReadChannel gcsReadChannel =
        new GcsSimpleReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
    gcsReadChannel.close();

    assertThat(gcsReadChannel.isOpen()).isFalse();
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
}
