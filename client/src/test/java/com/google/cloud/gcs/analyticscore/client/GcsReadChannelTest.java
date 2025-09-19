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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.IntFunction;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class GcsReadChannelTest {

  private static final String TEST_PROJECT_ID = "test-project-id";
  private static GcsReadOptions TEST_GCS_READ_OPTIONS =
      GcsReadOptions.builder().setProjectId(TEST_PROJECT_ID).build();

  private final Supplier<ExecutorService> executorServiceSupplier =
      Suppliers.memoize(() -> Executors.newFixedThreadPool(30));
  private final Storage storage = Mockito.spy(LocalStorageHelper.getOptions().getService());

  @Test
  void constructor_nullStorage_throwsNullPointerException() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();

    NullPointerException e =
        assertThrows(
            NullPointerException.class,
            () ->
                new GcsReadChannel(null, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier));

    assertThat(e).hasMessageThat().isEqualTo("Storage instance cannot be null");
  }

  @Test
  void constructor_itemInfoDoesNotPointToObject_throws() {
    GcsItemId itemId = GcsItemId.builder().setBucketName("test-bucket").build();
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setContentGeneration(0L).build();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new GcsReadChannel(
                    storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier));

    assertThat(e).hasMessageThat().isEqualTo("Expected Gcs Object but got " + itemInfo.getItemId());
  }

  @Test
  void constructor_nullItemInfo_throwsNullPointerException() {
    NullPointerException e =
        assertThrows(
            NullPointerException.class,
            () ->
                new GcsReadChannel(storage, null, TEST_GCS_READ_OPTIONS, executorServiceSupplier));

    assertThat(e).hasMessageThat().isEqualTo("Item info cannot be null");
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
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
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
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
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
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
    gcsReadChannel.position(6);
    ByteBuffer buffer = ByteBuffer.allocate(5);

    int bytesRead = gcsReadChannel.read(buffer);

    assertThat(bytesRead).isEqualTo(5);
    assertThat(new String(buffer.array(), StandardCharsets.UTF_8)).isEqualTo("world");
    assertThat(gcsReadChannel.position()).isEqualTo(11L);
  }

  @Test
  void position_negative_throwsEOFException() throws IOException {
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
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);

    EOFException e = assertThrows(EOFException.class, () -> gcsReadChannel.position(-1L));

    assertThat(e)
        .hasMessageThat()
        .contains("Invalid seek offset: position value (-1) must be >= 0");
  }

  @Test
  void position_greaterThanSize_throwsEOFException() throws IOException {
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
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
    long size = objectData.length();

    EOFException e = assertThrows(EOFException.class, () -> gcsReadChannel.position(size + 1));

    assertThat(e)
        .hasMessageThat()
        .contains(
            String.format(
                "Invalid seek offset: position value (%d) must be " + "between 0 and %d",
                size + 1, size));
  }

  @Test
  void write_throwsUnsupportedOperationException() throws IOException {
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
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
    ByteBuffer src = ByteBuffer.allocate(10);

    assertThrows(UnsupportedOperationException.class, () -> gcsReadChannel.write(src));
  }

  @Test
  void truncate_throwsUnsupportedOperationException() throws IOException {
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
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);

    assertThrows(UnsupportedOperationException.class, () -> gcsReadChannel.truncate(5L));
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
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);

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
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
    gcsReadChannel.close();

    assertThat(gcsReadChannel.isOpen()).isFalse();
  }

  @Test
  void size_returnsBlobContentLength() throws IOException {
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
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);

    assertThat(gcsReadChannel.size()).isEqualTo(objectData.length());
  }

  @Test
  void readVectored_nullThreadPool_throwsNullPointerException()
      throws IOException, ExecutionException, InterruptedException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(100).setContentGeneration(0L).build();
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS, Suppliers.memoize(() -> null));

    NullPointerException e =
        assertThrows(
            NullPointerException.class,
            () -> gcsReadChannel.readVectored(null, ByteBuffer::allocate));

    assertThat(e).hasMessageThat().isEqualTo("Thread pool must not be null");
  }

  @Test
  void readVectored_rangesNotEligibleForMerging_readsRanges()
      throws IOException, ExecutionException, InterruptedException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world,this is a test string for vectored read.";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsVectoredReadOptions vectoredReadOptions =
        GcsVectoredReadOptions.builder().setMaxMergeGap(1).setMaxMergeSize(1).build();
    GcsReadOptions readOptions =
        TEST_GCS_READ_OPTIONS.builder().setGcsVectoredReadOptions(vectoredReadOptions).build();
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(storage, itemInfo, readOptions, executorServiceSupplier);
    List<Storage.BlobSourceOption> sourceOptions = Lists.newArrayList();
    BlobId blobId = BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L);
    // "hello", "this", "test string"
    ImmutableList<GcsObjectRange> ranges = createRanges(ImmutableMap.of(0L, 5, 12L, 4, 22L, 11));

    gcsReadChannel.readVectored(ranges, ByteBuffer::allocate);

    assertThat(getGcsObjectRangeData(ranges.get(0))).isEqualTo("hello");
    assertThat(getGcsObjectRangeData(ranges.get(1))).isEqualTo("this");
    assertThat(getGcsObjectRangeData(ranges.get(2))).isEqualTo("test string");
    Mockito.verify(storage, Mockito.times(4))
        .reader(blobId, sourceOptions.toArray(new Storage.BlobSourceOption[0]));
  }

  @Test
  void readVectored_rangesCanBeMerged_readsRanges()
      throws IOException, ExecutionException, InterruptedException {
    GcsVectoredReadOptions vectoredReadOptions =
        GcsVectoredReadOptions.builder().setMaxMergeGap(10).build();
    List<Storage.BlobSourceOption> sourceOptions = Lists.newArrayList();
    GcsReadOptions readOptions =
        TEST_GCS_READ_OPTIONS.builder().setGcsVectoredReadOptions(vectoredReadOptions).build();
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world,this is a test string for vectored read."; // length 55
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    BlobId blobId = BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L);
    createBlobInStorage(blobId, objectData);
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(storage, itemInfo, readOptions, executorServiceSupplier);
    // "hello", "world", "this", "string", "vectored"
    ImmutableList<GcsObjectRange> ranges =
        createRanges(ImmutableMap.of(0L, 5, 6L, 5, 12L, 4, 27L, 6, 38L, 8));

    gcsReadChannel.readVectored(ranges, ByteBuffer::allocate);

    assertThat(getGcsObjectRangeData(ranges.get(0))).isEqualTo("hello");
    assertThat(getGcsObjectRangeData(ranges.get(1))).isEqualTo("world");
    assertThat(getGcsObjectRangeData(ranges.get(2))).isEqualTo("this");
    assertThat(getGcsObjectRangeData(ranges.get(3))).isEqualTo("string");
    assertThat(getGcsObjectRangeData(ranges.get(4))).isEqualTo("vectored");
    Mockito.verify(storage, Mockito.times(3))
        .reader(blobId, sourceOptions.toArray(new Storage.BlobSourceOption[0]));
  }

  @Test
  void readVectored_allocationError_completesFuturesExceptionally() throws IOException {
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
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier);
    ImmutableList<GcsObjectRange> ranges = createRanges(ImmutableMap.of(0L, 5));
    IntFunction<ByteBuffer> badAllocator =
        size -> {
          throw new RuntimeException("Allocation failed");
        };

    gcsReadChannel.readVectored(ranges, badAllocator);

    ExecutionException e =
        assertThrows(ExecutionException.class, () -> ranges.get(0).getByteBufferFuture().get());
    assertThat(e).hasCauseThat().isInstanceOf(IOException.class);
    assertThat(e).hasCauseThat().hasMessageThat().contains("Error while populating childRange");
    assertThat(e.getCause().getCause()).isInstanceOf(RuntimeException.class);
    assertThat(e.getCause().getCause()).hasMessageThat().isEqualTo("Allocation failed");
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
