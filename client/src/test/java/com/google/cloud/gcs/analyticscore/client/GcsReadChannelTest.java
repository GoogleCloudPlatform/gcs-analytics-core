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
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.MetricKey;
import com.google.cloud.gcs.analyticscore.common.telemetry.Operation;
import com.google.cloud.gcs.analyticscore.common.telemetry.OperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
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
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class GcsReadChannelTest {

  private static final String TEST_PROJECT_ID = "test-project-id";
  private static GcsReadOptions TEST_GCS_READ_OPTIONS =
      GcsReadOptions.builder().setUserProjectId(TEST_PROJECT_ID).build();

  private static final Supplier<ExecutorService> executorServiceSupplier =
      Suppliers.memoize(() -> Executors.newFixedThreadPool(30));
  private final Storage storage = Mockito.spy(LocalStorageHelper.getOptions().getService());
  private final Telemetry telemetry = new Telemetry(ImmutableList.of());

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
                new GcsReadChannel(
                    null, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry));

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
                    storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry));

    assertThat(e).hasMessageThat().isEqualTo("Expected Gcs Object but got " + itemInfo.getItemId());
  }

  @Test
  void constructor_itemId_nullStorage_throwsNullPointerException() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();

    NullPointerException e =
        assertThrows(
            NullPointerException.class,
            () ->
                new GcsReadChannel(
                    null, itemId, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry));

    assertThat(e).hasMessageThat().isEqualTo("Storage instance cannot be null");
  }

  @Test
  void constructor_itemId_itemInfoDoesNotPointToObject_throws() {
    GcsItemId itemId = GcsItemId.builder().setBucketName("test-bucket").build();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new GcsReadChannel(
                    storage, itemId, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry));

    assertThat(e).hasMessageThat().isEqualTo("Expected Gcs Object but got " + itemId);
  }

  @Test
  void constructor_nullItemId_throwsNullPointerException() {
    NullPointerException e =
        assertThrows(
            NullPointerException.class,
            () ->
                new GcsReadChannel(
                    storage,
                    (GcsItemId) null,
                    TEST_GCS_READ_OPTIONS,
                    executorServiceSupplier,
                    telemetry));

    assertThat(e).hasMessageThat().isEqualTo("Item id cannot be null");
  }

  @Test
  void constructor_nullItemInfo_throwsNullPointerException() {
    NullPointerException e =
        assertThrows(
            NullPointerException.class,
            () ->
                new GcsReadChannel(
                    storage,
                    (GcsItemInfo) null,
                    TEST_GCS_READ_OPTIONS,
                    executorServiceSupplier,
                    telemetry));

    assertThat(e).hasMessageThat().isEqualTo("Item info cannot be null");
  }

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

    new GcsReadChannel(mockStorage, itemInfo, readOptions, executorServiceSupplier, telemetry);

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
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry);
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
        new GcsReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry);
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
        new GcsReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry);
    gcsReadChannel.position(6);
    ByteBuffer buffer = ByteBuffer.allocate(5);

    int bytesRead = gcsReadChannel.read(buffer);

    assertThat(bytesRead).isEqualTo(5);
    assertThat(new String(buffer.array(), StandardCharsets.UTF_8)).isEqualTo("world");
    assertThat(gcsReadChannel.position()).isEqualTo(11L);
  }

  @Test
  void read_emptyBuffer_returnsZero() throws IOException {
    GcsItemInfo itemInfo = createItemInfoWith(100);
    try (GcsReadChannel gcsReadChannel =
        new GcsReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry)) {

      ByteBuffer emptyBuffer = ByteBuffer.allocate(0);
      int bytesRead = gcsReadChannel.read(emptyBuffer);

      assertThat(bytesRead).isEqualTo(0);
    }
  }

  @Test
  void read_eof_doesNotAdvancePosition() throws IOException {
    GcsItemInfo itemInfo = createItemInfoWith(100);
    createBlobInStorage(
        BlobId.of(
            itemInfo.getItemId().getBucketName(), itemInfo.getItemId().getObjectName().get(), 0L),
        "a".repeat(100));

    try (GcsReadChannel gcsReadChannel =
        new GcsReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry)) {
      gcsReadChannel.position(100);
      ByteBuffer buffer = ByteBuffer.allocate(10);

      int bytesRead = gcsReadChannel.read(buffer);

      assertThat(bytesRead).isEqualTo(-1);
      assertThat(gcsReadChannel.position()).isEqualTo(100L);
    }
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
        new GcsReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry);

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
        new GcsReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry);
    long size = objectData.length();

    gcsReadChannel.position(size + 1);

    assertThat(gcsReadChannel.position()).isEqualTo(size + 1);
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
        new GcsReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry);
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
        new GcsReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry);

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
        new GcsReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry);

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
        new GcsReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry);
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
        new GcsReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry);

    assertThat(gcsReadChannel.size()).isEqualTo(objectData.length());
  }

  @Test
  void size_whenMetadataNotLoaded_throwsIOException() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    // Do not create the blob in storage to ensure metadata is not loaded
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(
            storage, itemId, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry);

    IOException e = assertThrows(IOException.class, () -> gcsReadChannel.size());
    assertThat(e).hasMessageThat().isEqualTo("Object metadata not initialized");
  }

  @Test
  void readVectored_nullThreadPool_throwsNullPointerException() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(100).setContentGeneration(0L).build();
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, Suppliers.memoize(() -> null), telemetry);

    NullPointerException e =
        assertThrows(
            NullPointerException.class,
            () -> gcsReadChannel.readVectored(null, ByteBuffer::allocate));

    assertThat(e).hasMessageThat().isEqualTo("Thread pool must not be null");
  }

  @Test
  void readVectored_rangesNotEligibleForMerging_readsRanges() throws Exception {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world,this is a test string for vectored read.";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(objectData.length()).build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get()), objectData);
    GcsVectoredReadOptions vectoredReadOptions =
        GcsVectoredReadOptions.builder().setMaxMergeGap(1).setMaxMergeSize(1).build();
    GcsReadOptions readOptions =
        TEST_GCS_READ_OPTIONS.builder().setGcsVectoredReadOptions(vectoredReadOptions).build();
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);
    List<Storage.BlobSourceOption> sourceOptions = Lists.newArrayList();
    BlobId blobId = BlobId.of(itemId.getBucketName(), itemId.getObjectName().get());
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

    AtomicLong totalBytesReadFromMetrics = new AtomicLong(0L);
    GcsVectoredReadOptions vectoredReadOptions =
        GcsVectoredReadOptions.builder().setMaxMergeGap(10).build();
    List<Storage.BlobSourceOption> sourceOptions = Lists.newArrayList();
    GcsReadOptions readOptions =
        TEST_GCS_READ_OPTIONS.builder().setGcsVectoredReadOptions(vectoredReadOptions).build();
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName("test-bucket")
            .setObjectName("test-object")
            .setContentGeneration(0L)
            .build();
    String objectData = "hello world,this is a test string for vectored read."; // length 55
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    BlobId blobId = BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L);
    createBlobInStorage(blobId, objectData);
    OperationListener listener =
        new OperationListener() {
          @Override
          public void onOperationStart(Operation operation) {}

          @Override
          public void onOperationEnd(Operation operation, Map<MetricKey, Long> metrics) {
            if (operation.getName().equals("VECTORED_READ")) {
              totalBytesReadFromMetrics.addAndGet(
                  metrics.get(MetricKey.builder().setMetric(Metric.READ_BYTES).build()));
            }
          }
        };
    Telemetry telemetry = new Telemetry(Collections.singletonList(listener));
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);
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
    assertThat(totalBytesReadFromMetrics.get()).isEqualTo(35L);

    // Clean up.

    gcsReadChannel.close();
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
        new GcsReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry);
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

  @Test
  void readVectored_combinedRange_partialFirstRead_readsFully() throws Exception {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "abcdefghijklmnopqrstuvwxyz";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);

    Storage mockStorage = Mockito.mock(Storage.class);
    ReadChannel mockReadChannel = Mockito.mock(ReadChannel.class);
    Mockito.when(
            mockStorage.reader(
                Mockito.any(BlobId.class), Mockito.any(Storage.BlobSourceOption[].class)))
        .thenReturn(mockReadChannel);
    Mockito.when(mockReadChannel.isOpen()).thenReturn(true);

    byte[] dataBytes = objectData.getBytes(StandardCharsets.UTF_8);
    var ref =
        new Object() {
          int callCount = 0;
        };

    Mockito.when(mockReadChannel.read(Mockito.any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              if (ref.callCount == 0) {
                // Return 1 byte on the first call
                buffer.put(dataBytes, 0, 1);
                ref.callCount++;
                return 1;
              } else {
                // Return the rest on the second call
                int remaining = buffer.remaining();
                buffer.put(dataBytes, 1, remaining);
                return remaining;
              }
            });

    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(
            mockStorage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry);
    GcsObjectRange range1 = createRange(0, 10);
    ImmutableList<GcsObjectRange> ranges = ImmutableList.of(range1);
    gcsReadChannel.readVectored(ranges, ByteBuffer::allocate);

    assertThat(getGcsObjectRangeData(range1)).isEqualTo("abcdefghij");
  }

  @Test
  void readVectored_eofReachedBeforeFullyRead_completesExceptionally() throws Exception {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "abcde";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    Storage mockStorage = Mockito.mock(Storage.class);
    ReadChannel mockReadChannel = Mockito.mock(ReadChannel.class);
    Mockito.when(
            mockStorage.reader(
                Mockito.any(BlobId.class), Mockito.any(Storage.BlobSourceOption[].class)))
        .thenReturn(mockReadChannel);
    Mockito.when(mockReadChannel.isOpen()).thenReturn(true);
    byte[] dataBytes = objectData.getBytes(StandardCharsets.UTF_8);
    AtomicInteger callCount = new AtomicInteger(0);
    Mockito.when(mockReadChannel.read(Mockito.any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              if (callCount.get() == 0) {
                // Return 5 bytes on the first call
                buffer.put(dataBytes, 0, 5);
                callCount.incrementAndGet();
                return 5;
              } else {
                // Return EOF on the second call
                return -1;
              }
            });
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(
            mockStorage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry);
    // Request 10 bytes, but only 5 are returned before EOF
    GcsObjectRange range1 = createRange(0, 10);
    ImmutableList<GcsObjectRange> ranges = ImmutableList.of(range1);

    gcsReadChannel.readVectored(ranges, ByteBuffer::allocate);
    gcsReadChannel.close();

    ExecutionException e =
        assertThrows(ExecutionException.class, () -> range1.getByteBufferFuture().get());
    assertThat(e).hasCauseThat().isInstanceOf(IOException.class);
    assertThat(e).hasCauseThat().hasMessageThat().contains("Error while populating childRange");
    assertThat(e.getCause().getCause()).isInstanceOf(EOFException.class);
    assertThat(e.getCause().getCause())
        .hasMessageThat()
        .contains("EOF reached while reading combinedObjectRange");
  }

  @Test
  void position_doesNotSeekImmediately() throws IOException {
    GcsItemInfo itemInfo = createItemInfoWith(100);
    Storage mockStorage = Mockito.mock(Storage.class);
    ReadChannel mockSdkReadChannel = Mockito.mock(ReadChannel.class);
    Mockito.when(
            mockStorage.reader(
                Mockito.any(BlobId.class), Mockito.any(Storage.BlobSourceOption[].class)))
        .thenReturn(mockSdkReadChannel);
    Mockito.when(mockSdkReadChannel.isOpen()).thenReturn(true);

    try (GcsReadChannel gcsReadChannel =
        new GcsReadChannel(
            mockStorage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry)) {
      gcsReadChannel.position(10);

      Mockito.verify(mockSdkReadChannel, Mockito.never()).seek(Mockito.anyLong());
    }
  }

  @Test
  void close_calledTwice_closesOnlyOnce() throws IOException {
    GcsItemInfo itemInfo = createItemInfoWith(100);
    createBlobInStorage(
        BlobId.of(
            itemInfo.getItemId().getBucketName(), itemInfo.getItemId().getObjectName().get(), 0L),
        "a".repeat(100));
    FakeGcsReadChannel gcsReadChannel =
        new FakeGcsReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry);
    TrackingReadStrategy strategy = gcsReadChannel.getTrackingReadStrategy();

    gcsReadChannel.close();
    gcsReadChannel.close();

    assertThat(strategy.getCloseCalls()).isEqualTo(1);
  }

  @Test
  void position_onClosedChannel_throwsClosedChannelException() throws IOException {
    GcsItemInfo itemInfo = createItemInfoWith(100);
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry);
    gcsReadChannel.close();

    assertThrows(ClosedChannelException.class, () -> gcsReadChannel.position());
  }

  @Test
  void positionLong_onClosedChannel_throwsClosedChannelException() throws IOException {
    GcsItemInfo itemInfo = createItemInfoWith(100);
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry);
    gcsReadChannel.close();

    assertThrows(ClosedChannelException.class, () -> gcsReadChannel.position(10));
  }

  @Test
  void read_onClosedChannel_throwsClosedChannelException() throws IOException {
    GcsItemInfo itemInfo = createItemInfoWith(100);
    GcsReadChannel gcsReadChannel =
        new GcsReadChannel(
            storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry);
    gcsReadChannel.close();
    ByteBuffer buffer = ByteBuffer.allocate(10);

    ClosedChannelException e =
        assertThrows(ClosedChannelException.class, () -> gcsReadChannel.read(buffer));

    assertThat(e).isInstanceOf(ClosedChannelException.class);
  }

  @Test
  void read_unexpectedEof_throwsIOException() throws IOException {
    GcsItemInfo itemInfo = createItemInfoWith(100);
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setUserProjectId(TEST_PROJECT_ID)
            .setFileAccessPattern(FileAccessPattern.RANDOM)
            .build();
    try (FakeGcsReadChannel gcsReadChannel =
        new FakeGcsReadChannel(
            storage, itemInfo, readOptions, executorServiceSupplier, telemetry)) {
      gcsReadChannel.setDefaultEofAtCall(1);
      ByteBuffer buffer = ByteBuffer.allocate(50);

      IOException e = assertThrows(IOException.class, () -> gcsReadChannel.read(buffer));

      assertThat(e)
          .hasMessageThat()
          .contains("Received end of stream signal before all requestedBytes were received");
    }
  }

  @Test
  void read_unexpectedEof_nullItemInfo_throwsIOException() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setUserProjectId(TEST_PROJECT_ID)
            .setFileAccessPattern(FileAccessPattern.RANDOM)
            .build();
    ByteBuffer buffer = ByteBuffer.allocate(50);
    IOException e;

    try (FakeGcsReadChannel gcsReadChannel =
        new FakeGcsReadChannel(storage, itemId, readOptions, executorServiceSupplier, telemetry)) {
      gcsReadChannel.setDefaultEofAtCall(1);

      e = assertThrows(IOException.class, () -> gcsReadChannel.read(buffer));
    }

    assertThat(e)
        .hasMessageThat()
        .contains("Received end of stream signal before all requestedBytes were received");
    assertThat(e).hasMessageThat().contains("size: -1");
  }

  @Test
  void constructor_createReadStrategyThrowsIOException_propagatesException() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(100).setContentGeneration(0L).build();

    RuntimeException e =
        assertThrows(
            RuntimeException.class,
            () ->
                new GcsReadChannel(
                    storage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry) {
                  @Override
                  protected ReadStrategy createReadStrategy(
                      Storage storage,
                      GcsItemId itemId,
                      GcsReadOptions readOptions,
                      GcsItemInfo itemInfo,
                      long position) {
                    throw new RuntimeException("Simulated IO error");
                  }
                });

    assertThat(e).hasMessageThat().isEqualTo("Simulated IO error");
  }

  @Test
  void createReadStrategy_randomAccessPattern_returnsRandomReadStrategy() throws IOException {
    GcsItemInfo itemInfo = createItemInfoWith(100);
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setUserProjectId(TEST_PROJECT_ID)
            .setFileAccessPattern(FileAccessPattern.RANDOM)
            .build();

    try (FakeGcsReadChannel gcsReadChannel =
        new FakeGcsReadChannel(
            storage, itemInfo, readOptions, executorServiceSupplier, telemetry)) {
      assertThat(gcsReadChannel.getTrackingReadStrategy().getDelegate())
          .isInstanceOf(RandomReadStrategy.class);
    }
  }

  @Test
  void createReadStrategy_sequentialAccessPattern_returnsSequentialReadStrategy()
      throws IOException {
    GcsItemInfo itemInfo = createItemInfoWith(100);
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setUserProjectId(TEST_PROJECT_ID)
            .setFileAccessPattern(FileAccessPattern.SEQUENTIAL)
            .build();

    try (FakeGcsReadChannel gcsReadChannel =
        new FakeGcsReadChannel(
            storage, itemInfo, readOptions, executorServiceSupplier, telemetry)) {
      assertThat(gcsReadChannel.getTrackingReadStrategy().getDelegate())
          .isInstanceOf(SequentialReadStrategy.class);
    }
  }

  @Test
  void read_unexpectedEof_withItemInfo_verifiesSizeInMessage() throws IOException {
    GcsItemInfo itemInfo = createItemInfoWith(100);
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setUserProjectId(TEST_PROJECT_ID)
            .setFileAccessPattern(FileAccessPattern.RANDOM)
            .build();
    ByteBuffer buffer = ByteBuffer.allocate(50);

    try (FakeGcsReadChannel gcsReadChannel =
        new FakeGcsReadChannel(
            storage, itemInfo, readOptions, executorServiceSupplier, telemetry)) {
      gcsReadChannel.setDefaultEofAtCall(1);

      IOException e = assertThrows(IOException.class, () -> gcsReadChannel.read(buffer));

      assertThat(e).hasMessageThat().contains("size: 100");
    }
  }

  @Test
  void read_onClosedChannel_throwsClosedChannelException_verified() throws IOException {
    GcsItemInfo itemInfo = createItemInfoWith(100);
    Storage localStorage = LocalStorageHelper.getOptions().getService();
    ByteBuffer buffer = ByteBuffer.allocate(10);

    try (GcsReadChannel gcsReadChannel =
        new GcsReadChannel(
            localStorage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry)) {
      gcsReadChannel.close();

      assertThrows(ClosedChannelException.class, () -> gcsReadChannel.read(buffer));
    }
  }

  @Test
  void position_onClosedChannel_throwsClosedChannelException_verified() throws IOException {
    GcsItemInfo itemInfo = createItemInfoWith(100);
    Storage localStorage = LocalStorageHelper.getOptions().getService();

    try (GcsReadChannel gcsReadChannel =
        new GcsReadChannel(
            localStorage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry)) {
      gcsReadChannel.close();

      assertThrows(ClosedChannelException.class, () -> gcsReadChannel.position());
    }
  }

  @Test
  void positionLong_onClosedChannel_throwsClosedChannelException_verified() throws IOException {
    GcsItemInfo itemInfo = createItemInfoWith(100);
    Storage localStorage = LocalStorageHelper.getOptions().getService();

    try (GcsReadChannel gcsReadChannel =
        new GcsReadChannel(
            localStorage, itemInfo, TEST_GCS_READ_OPTIONS, executorServiceSupplier, telemetry)) {
      gcsReadChannel.close();

      assertThrows(ClosedChannelException.class, () -> gcsReadChannel.position(10));
    }
  }

  private GcsItemInfo createItemInfoWith(long size) {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    return GcsItemInfo.builder().setItemId(itemId).setSize(size).setContentGeneration(0L).build();
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
