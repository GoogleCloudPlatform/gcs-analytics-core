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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.*;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class GcsClientImplTest {

  private static final String TEST_PROJECT = "test-project";
  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_OBJECT = "test-object";
  private static final String TEST_BUCKET_NAME = "test-bucket-name";
  private static final String TEST_BUCKET_ID = "test-bucket-id";
  private static final String TEST_OBJECT_ID = "test-object-id";
  private static final String TEST_WRITE_OBJECT = "test-write-object";
  private static final String TEST_NULL_OPTIONS_OBJECT = "test-null-options";
  private static final String TEST_NON_EXISTENT_OBJECT = "non-existent";
  private static final String TEST_OBJECT_NAME = "test-object-name";
  private static final String BLOB_WRITE_SESSION_CONFIG_FIELD = "blobWriteSessionConfig";
  private static final int MB = 1024 * 1024;

  private static final GcsClientOptions TEST_GCS_CLIENT_OPTIONS =
      GcsClientOptions.builder().setProjectId(TEST_PROJECT).build();
  private static final GcsItemId TEST_ITEM_ID =
      GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();
  private static final BlobInfo TEST_BLOB_INFO =
      BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT)).build();
  private static final GcsWriteOptions DEFAULT_WRITE_OPTIONS = GcsWriteOptions.builder().build();

  private final Storage storage = LocalStorageHelper.getOptions().getService();
  private final Supplier<ExecutorService> executorServiceSupplier =
      Suppliers.memoize(() -> Executors.newFixedThreadPool(30));
  private final Telemetry telemetry = new Telemetry(ImmutableList.of());

  private GcsClient gcsClient;

  @BeforeEach
  void setUp() throws IOException {
    gcsClient =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry) {
          @Override
          protected Storage createStorage(Optional<Credentials> credentials) {
            return GcsClientImplTest.this.storage;
          }
        };
  }

  @Test
  void getGcsItemInfo_itemIdPointsToDirectory_throwsUnsupportedOperationException() {
    GcsItemId directoryItemId = GcsItemId.builder().setBucketName(TEST_BUCKET_ID).build();

    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class, () -> gcsClient.getGcsItemInfo(directoryItemId));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(String.format("Expected gcs object but got %s", directoryItemId));
  }

  @Test
  void getGcsItemInfo_gcsObjectExists_returnsItemInfo() throws IOException {
    String objectData = "hello world";
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_ID).setObjectName(TEST_OBJECT_ID).build();
    StorageTestUtils.createBlobInStorage(
        storage, BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);

    GcsItemInfo itemInfo = gcsClient.getGcsItemInfo(itemId);

    GcsItemId expectedItemId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET_ID)
            .setObjectName(TEST_OBJECT_ID)
            .setContentGeneration(itemInfo.getContentGeneration().get())
            .build();
    assertThat(itemInfo.getItemId()).isEqualTo(expectedItemId);
    assertThat(itemInfo.getSize()).isEqualTo(objectData.length());
    assertThat(itemInfo.getContentGeneration().get()).isEqualTo(0L);
  }

  @Test
  void getGcsItemInfo_nonExistentBlob_throwsIOException() {
    GcsItemId nonExistentItemId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET_NAME)
            .setObjectName(TEST_NON_EXISTENT_OBJECT)
            .build();

    IOException e =
        assertThrows(IOException.class, () -> gcsClient.getGcsItemInfo(nonExistentItemId));

    assertThat(e).hasMessageThat().contains("Object not found:" + nonExistentItemId);
  }

  @Test
  void openReadChannel_gcsObjectExists_returnsChannelWithCorrectSizeAndContent()
      throws IOException {
    String objectData = "hello world";
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId("test-project").build();
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT_NAME).build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    StorageTestUtils.createBlobInStorage(
        storage, BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    ByteBuffer buffer = ByteBuffer.allocate(objectData.length());

    SeekableByteChannel channel = gcsClient.openReadChannel(itemInfo, readOptions);
    int bytesRead = channel.read(buffer);

    assertThat(channel.size()).isEqualTo(objectData.length());
    assertThat(bytesRead).isEqualTo(objectData.length());
    assertThat(new String(buffer.array(), UTF_8)).isEqualTo(objectData);
  }

  @Test
  void openReadChannel_itemId_gcsObjectExists_returnsChannelWithCorrectSizeAndContent()
      throws IOException {
    String objectData = "hello world";
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId("test-project").build();
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT_NAME).build();
    StorageTestUtils.createBlobInStorage(
        storage, BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    ByteBuffer buffer = ByteBuffer.allocate(objectData.length());

    SeekableByteChannel channel = gcsClient.openReadChannel(itemId, readOptions);
    int bytesRead = channel.read(buffer);

    assertThat(channel.size()).isEqualTo(objectData.length());
    assertThat(channel.size())
        .isEqualTo(objectData.length()); // Call twice to cover cached size branch
    assertThat(bytesRead).isEqualTo(objectData.length());
    assertThat(new String(buffer.array(), UTF_8)).isEqualTo(objectData);
  }

  @Test
  void openReadChannel_nullItemId_throwsNullPointerException() {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setUserProjectId("test-project-id").build();

    NullPointerException e =
        assertThrows(
            NullPointerException.class,
            () -> gcsClient.openReadChannel((GcsItemId) null, readOptions));
    assertThat(e).hasMessageThat().isEqualTo("gcsItemId should not be null");
  }

  @Test
  void openReadChannel_nullItemInfo_throwsNullPointerException() {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setUserProjectId("test-project-id").build();

    NullPointerException e =
        assertThrows(
            NullPointerException.class,
            () -> gcsClient.openReadChannel((GcsItemInfo) null, readOptions));
    assertThat(e).hasMessageThat().isEqualTo("itemInfo should not be null");
  }

  @Test
  void openReadChannel_nullReadOptions_throwsNullPointerException() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT).build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(0L).setContentGeneration(0L).build();

    NullPointerException e =
        assertThrows(NullPointerException.class, () -> gcsClient.openReadChannel(itemInfo, null));
    assertThat(e).hasMessageThat().isEqualTo("readOptions should not be null");
  }

  @Test
  void openReadChannel_itemInfoPointsToDirectory_throwsIllegalArgumentException() {
    GcsItemId directoryItemId = GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).build();
    GcsItemInfo directoryItemInfo =
        GcsItemInfo.builder()
            .setItemId(directoryItemId)
            .setSize(0L)
            .setContentGeneration(-1L)
            .build();
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setUserProjectId("test-project-id").build();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> gcsClient.openReadChannel(directoryItemInfo, readOptions));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Expected GCS object to be provided. But got: " + directoryItemId);
  }

  @Test
  void getUserAgent_noOptionalUserAgent() throws Exception {
    GcsClientImpl client =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry);
    String userAgent = client.getUserAgent();
    assertThat(userAgent).isEqualTo("gcs-analytics-core/" + VersionHelper.VERSION);
  }

  @Test
  void getUserAgent_withOptionalUserAgent() throws Exception {
    GcsClientOptions options =
        GcsClientOptions.builder()
            .setProjectId("test-project")
            .setUserAgent("custom-app/1.0")
            .build();
    GcsClientImpl client = new GcsClientImpl(options, executorServiceSupplier, telemetry);
    String userAgent = client.getUserAgent();
    assertThat(userAgent)
        .isEqualTo("gcs-analytics-core/" + VersionHelper.VERSION + " custom-app/1.0");
  }

  @Test
  void createStore_withCredentials_usesProvidedCredentials() throws IOException {
    GcsClientImpl client =
        new GcsClientImpl(
            NoCredentials.getInstance(),
            TEST_GCS_CLIENT_OPTIONS,
            executorServiceSupplier,
            telemetry);
    assertThat(client.storage.getOptions().getCredentials()).isEqualTo(NoCredentials.getInstance());
  }

  @Test
  void create_withLocalStorage_writesSuccessfully() throws Exception {
    GcsClientImpl client =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry) {
          @Override
          protected Storage createStorage(Optional<Credentials> credentials) {
            return GcsClientImplTest.this.storage;
          }
        };
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_WRITE_OBJECT).build();
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_WRITE_OBJECT)).build();
    byte[] data = "hello write world".getBytes(StandardCharsets.UTF_8);

    try (WritableByteChannel channel = client.create(itemId, DEFAULT_WRITE_OPTIONS)) {
      int bytesWritten = channel.write(ByteBuffer.wrap(data));
      assertThat(bytesWritten).isEqualTo(data.length);
    }

    assertThat(new String(storage.readAllBytes(blobInfo.getBlobId()), StandardCharsets.UTF_8))
        .isEqualTo("hello write world");
  }

  @Test
  void create_whenAccessDenied_throwsAccessDeniedException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    StorageException e403 = new StorageException(403, "Forbidden");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e403);

    assertThrows(
        AccessDeniedException.class,
        () -> clientWithMock.create(TEST_ITEM_ID, DEFAULT_WRITE_OPTIONS));
  }

  @Test
  void create_whenFileExists_throwsFileAlreadyExistsException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    StorageException e409 = new StorageException(409, "Conflict");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e409);

    assertThrows(
        FileAlreadyExistsException.class,
        () -> clientWithMock.create(TEST_ITEM_ID, DEFAULT_WRITE_OPTIONS));
  }

  @Test
  void create_whenPreconditionFailedAndNoOverwrite_throwsFileAlreadyExistsException()
      throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().setOverwriteExisting(false).build();
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e412);

    FileAlreadyExistsException exception =
        assertThrows(
            FileAlreadyExistsException.class,
            () -> clientWithMock.create(TEST_ITEM_ID, writeOptions));

    assertThat(exception).hasCauseThat().isSameInstanceAs(e412);
  }

  @Test
  void create_whenGenerationMismatch_throwsIOException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId itemIdWithGen =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_OBJECT)
            .setContentGeneration(12345L)
            .build();
    BlobInfo blobInfoWithGen =
        BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT, 12345L)).build();
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockStorage.blobWriteSession(eq(blobInfoWithGen), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e412);

    IOException exception =
        assertThrows(
            IOException.class, () -> clientWithMock.create(itemIdWithGen, DEFAULT_WRITE_OPTIONS));

    assertThat(exception).hasMessageThat().contains("Generation mismatch for object");
    assertThat(exception).hasCauseThat().isSameInstanceAs(e412);
  }

  @Test
  void create_whenBucketOrObjectNotFound_throwsFileNotFoundException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName("non-existent-bucket")
            .setObjectName("test-object")
            .build();
    BlobInfo blobInfo =
        BlobInfo.newBuilder(BlobId.of("non-existent-bucket", "test-object")).build();
    StorageException e404 = new StorageException(404, "Not Found");
    when(mockStorage.blobWriteSession(eq(blobInfo), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e404);

    FileNotFoundException exception =
        assertThrows(
            FileNotFoundException.class,
            () -> clientWithMock.create(itemId, DEFAULT_WRITE_OPTIONS));

    assertThat(exception).hasCauseThat().isSameInstanceAs(e404);
  }

  @Test
  void create_whenStorageExceptionOccurs_throwsIOException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    StorageException e500 = new StorageException(500, "Internal Server Error");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e500);

    IOException thrown =
        assertThrows(
            IOException.class, () -> clientWithMock.create(TEST_ITEM_ID, DEFAULT_WRITE_OPTIONS));

    assertThat(thrown).hasMessageThat().contains("Error during initialization to GCS");
  }

  @Test
  void create_whenOpenThrowsIOExceptionWrappingStorageException_translatesStorageException()
      throws Exception {
    Storage mockStorage = mock(Storage.class);
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    StorageException nestedStorageException = new StorageException(404, "Not Found");
    IOException wrappingException = new IOException(nestedStorageException);
    when(mockSession.open()).thenThrow(wrappingException);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    IOException exception =
        assertThrows(IOException.class, () -> clientWithMock.create(TEST_ITEM_ID, null));

    assertThat(exception).isInstanceOf(FileNotFoundException.class);
    assertThat(exception).hasCauseThat().isSameInstanceAs(nestedStorageException);
  }

  @Test
  void create_nullWriteOptions_usesDefaultWriteOptions() throws Exception {
    Storage mockStorage = mock(Storage.class);
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    WritableByteChannel mockChannel = mock(WritableByteChannel.class);
    when(mockSession.open()).thenReturn(mockChannel);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    WritableByteChannel returnedChannel = clientWithMock.create(TEST_ITEM_ID, null);

    assertThat(returnedChannel).isInstanceOf(GcsWriteChannel.class);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
    when(mockChannel.isOpen()).thenReturn(true);
    returnedChannel.write(buffer);
    verify(mockChannel).write(buffer);
  }

  @Test
  void create_allWriteOptionsEnabled_generatesCorrectBlobWriteOptions() throws Exception {
    Storage mockStorage = mock(Storage.class);
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    when(mockSession.open()).thenReturn(mock(WritableByteChannel.class));
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsWriteOptions allOptions =
        GcsWriteOptions.builder()
            .setChecksumValidationEnabled(true)
            .setDisableGzipContent(true)
            .setOverwriteExisting(false)
            .setKmsKeyName("kms-key")
            .setEncryptionKey("MDEyMzQ1Njc4OUFCQ0RFRkdISUpLTE1OT1BRUlNUVVU=")
            .setUserProject("user-project")
            .build();

    clientWithMock.create(TEST_ITEM_ID, allOptions);

    String capturedOptionsString = captureBlobWriteOptions(mockStorage, TEST_BLOB_INFO);
    assertThat(capturedOptionsString).contains("Crc32cMatchExtractor");
    assertThat(capturedOptionsString).contains("IF_GENERATION_MATCH");
    assertThat(capturedOptionsString).contains("KMS_KEY_NAME");
    assertThat(capturedOptionsString).contains("CUSTOMER_SUPPLIED_KEY");
  }

  @Test
  void create_withDisableGzipContentFalse_doesNotAddDisableGzipOption() throws Exception {
    Storage mockStorage = mock(Storage.class);
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    when(mockSession.open()).thenReturn(mock(WritableByteChannel.class));
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().setDisableGzipContent(false).build();

    clientWithMock.create(TEST_ITEM_ID, writeOptions);

    String capturedOptionsString = captureBlobWriteOptions(mockStorage, TEST_BLOB_INFO);
    assertThat(capturedOptionsString).doesNotContain("disableGzipContent");
  }

  @Test
  void create_withGenerationId_generatesGenerationMatchOption() throws Exception {
    Storage mockStorage = mock(Storage.class);
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    when(mockSession.open()).thenReturn(mock(WritableByteChannel.class));
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId itemIdWithGen =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_OBJECT)
            .setContentGeneration(12345L)
            .build();
    BlobInfo blobInfoWithGen =
        BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT, 12345L)).build();

    clientWithMock.create(itemIdWithGen, DEFAULT_WRITE_OPTIONS);

    String capturedOptionsString = captureBlobWriteOptions(mockStorage, blobInfoWithGen);
    assertThat(capturedOptionsString).contains("GenerationMatchExtractor");
  }

  @Test
  void createStorage_withParallelCompositeUpload_setsPcuSessionConfig() throws Exception {
    GcsWriteOptions writeOptions =
        GcsWriteOptions.builder()
            .setUploadType(GcsWriteOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD)
            .setPcuBufferCount(5)
            .setPcuBufferCapacity(128 * MB)
            .setPcuPartFileCleanupType(GcsWriteOptions.PartFileCleanupType.NEVER)
            .setPcuPartFileNamePrefix("custom-prefix-")
            .build();

    assertPcuSessionConfig(writeOptions);
  }

  @Test
  void createStorage_withWriteToDiskThenUpload_setsBufferToDiskSessionConfig() throws Exception {
    GcsWriteOptions writeOptions =
        GcsWriteOptions.builder()
            .setUploadType(GcsWriteOptions.UploadType.WRITE_TO_DISK_THEN_UPLOAD)
            .setTemporaryPaths(ImmutableList.of("/tmp/path1"))
            .build();

    GcsClientImpl client = createClientWithWriteOptions(writeOptions);

    assertThat(getBlobWriteSessionConfig(client.storage.getOptions()))
        .isInstanceOf(BufferToDiskThenUpload.class);
  }

  @Test
  void createStorage_withJournalingOnHttp_throwsUnsupportedOperationException() {
    GcsWriteOptions writeOptions =
        GcsWriteOptions.builder()
            .setUploadType(GcsWriteOptions.UploadType.JOURNALING)
            .setTemporaryPaths(ImmutableList.of("/tmp/path1"))
            .build();

    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class, () -> createClientWithWriteOptions(writeOptions));

    assertThat(e).hasMessageThat().contains("JOURNALING upload type is not supported");
  }

  @Test
  void getBlob_whenStorageThrowsException_throwsIOException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();
    when(mockStorage.get(any(BlobId.class), any(Storage.BlobGetOption[].class)))
        .thenThrow(new StorageException(500, "Internal Server Error"));

    IOException e = assertThrows(IOException.class, () -> clientWithMock.getGcsItemInfo(itemId));

    assertThat(e).hasMessageThat().contains("Unable to access blob");
  }

  @Test
  void close_whenStorageThrowsException_handledSilently() throws Exception {
    Storage mockStorage = mock(Storage.class);
    doThrow(new RuntimeException("close failed")).when(mockStorage).close();
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    clientWithMock.close();

    verify(mockStorage).close();
  }

  @Test
  void close_whenCalled_closesStorage() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    clientWithMock.close();

    verify(mockStorage).close();
  }

  @Test
  void create_whenOpenThrowsIOException_propagatesIOException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    IOException ioException = new IOException("Open failed");
    when(mockSession.open()).thenThrow(ioException);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    IOException thrown =
        assertThrows(
            IOException.class, () -> clientWithMock.create(TEST_ITEM_ID, DEFAULT_WRITE_OPTIONS));

    assertThat(thrown).isSameInstanceAs(ioException);
  }

  @Test
  void getBlob_whenBucketNameIsNull_throwsNullPointerException() throws Exception {
    GcsItemId itemId = mock(GcsItemId.class);
    when(itemId.getBucketName()).thenReturn(null);
    when(itemId.getObjectName()).thenReturn(Optional.of(TEST_OBJECT));
    when(itemId.isGcsObject()).thenReturn(true);

    GcsClientImpl client =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry);

    assertThrows(NullPointerException.class, () -> client.getGcsItemInfo(itemId));
  }

  @Test
  void getGcsItemInfo_whenBucketItemIdProvided_throwsUnsupportedOperationException()
      throws Exception {
    GcsItemId bucketItemId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    GcsClientImpl client =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry);

    UnsupportedOperationException thrown =
        assertThrows(
            UnsupportedOperationException.class, () -> client.getGcsItemInfo(bucketItemId));

    assertThat(thrown).hasMessageThat().contains("Expected gcs object but got");
  }

  @Test
  void create_withNullWriteOptions_success() throws Exception {
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_NULL_OPTIONS_OBJECT)
            .build();
    try (WritableByteChannel channel = gcsClient.create(itemId, null)) {
      channel.write(ByteBuffer.wrap("null options write".getBytes(UTF_8)));
    }
    assertThat(
            new String(
                storage.readAllBytes(BlobId.of(TEST_BUCKET, TEST_NULL_OPTIONS_OBJECT)), UTF_8))
        .isEqualTo("null options write");
  }

  @Test
  void createStorage_withWriteToDiskAndNoTempPaths_setsBufferToTempDirSessionConfig()
      throws Exception {
    GcsWriteOptions writeOptions =
        GcsWriteOptions.builder()
            .setUploadType(GcsWriteOptions.UploadType.WRITE_TO_DISK_THEN_UPLOAD)
            .setTemporaryPaths(ImmutableList.of()) // Empty paths
            .build();

    GcsClientImpl client = createClientWithWriteOptions(writeOptions);

    assertThat(getBlobWriteSessionConfig(client.storage.getOptions()))
        .isInstanceOf(BufferToDiskThenUpload.class);
  }

  @Test
  void createStorage_withParallelCompositeUploadAndOnSuccessCleanup_setsPcuSessionConfig()
      throws Exception {
    GcsWriteOptions writeOptions =
        GcsWriteOptions.builder()
            .setUploadType(GcsWriteOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD)
            .setPcuPartFileCleanupType(GcsWriteOptions.PartFileCleanupType.ON_SUCCESS)
            .build();

    assertPcuSessionConfig(writeOptions);
  }

  @Test
  void createStorage_withParallelCompositeUploadAndAlwaysCleanup_setsPcuSessionConfig()
      throws Exception {
    GcsWriteOptions writeOptions =
        GcsWriteOptions.builder()
            .setUploadType(GcsWriteOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD)
            .setPcuPartFileCleanupType(GcsWriteOptions.PartFileCleanupType.ALWAYS)
            .build();

    assertPcuSessionConfig(writeOptions);
  }

  @Test
  void createStorage_withChunkUpload_setsDefaultSessionConfig() throws Exception {
    int customChunkSize = 2 * MB;
    GcsWriteOptions writeOptions =
        GcsWriteOptions.builder()
            .setUploadType(GcsWriteOptions.UploadType.CHUNK_UPLOAD)
            .setUploadChunkSize(customChunkSize)
            .build();

    GcsClientImpl client = createClientWithWriteOptions(writeOptions);

    BlobWriteSessionConfig config = getBlobWriteSessionConfig(client.storage.getOptions());
    assertThat(config).isInstanceOf(DefaultBlobWriteSessionConfig.class);
    assertThat(((DefaultBlobWriteSessionConfig) config).getChunkSize()).isEqualTo(customChunkSize);
  }

  @Test
  void create_whenPreconditionFailedAndNullWriteOptions_throwsIOException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e412);

    IOException exception =
        assertThrows(IOException.class, () -> clientWithMock.create(TEST_ITEM_ID, null));

    assertThat(exception).hasCauseThat().isSameInstanceAs(e412);
  }

  @Test
  void create_whenPreconditionFailedAndDefaultOverwrite_throwsIOException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().setOverwriteExisting(true).build();
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e412);

    IOException exception =
        assertThrows(IOException.class, () -> clientWithMock.create(TEST_ITEM_ID, writeOptions));

    assertThat(exception).hasCauseThat().isSameInstanceAs(e412);
  }

  @Test
  void create_whenRuntimeExceptionOccurs_directlyPropagates() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    RuntimeException runtimeException = new NullPointerException("mock null pointer exception");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(runtimeException);

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> clientWithMock.create(TEST_ITEM_ID, DEFAULT_WRITE_OPTIONS));

    assertThat(exception).isSameInstanceAs(runtimeException);
  }

  private BlobWriteSessionConfig getBlobWriteSessionConfig(StorageOptions options) {
    Class<?> clazz = options.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(BLOB_WRITE_SESSION_CONFIG_FIELD);
        field.setAccessible(true);
        return (BlobWriteSessionConfig) field.get(options);
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    throw new RuntimeException(
        new NoSuchFieldException(
            "Field " + BLOB_WRITE_SESSION_CONFIG_FIELD + " not found in options hierarchy"));
  }

  @Test
  void generateSessionConfig_withJournalingAndNonHttpStorageOptions_returnsJournalingConfig()
      throws Exception {
    GcsClientImpl client =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry);
    GcsWriteOptions writeOptions =
        GcsWriteOptions.builder()
            .setUploadType(GcsWriteOptions.UploadType.JOURNALING)
            .setTemporaryPaths(ImmutableList.of("/tmp/journal1", "/tmp/journal2"))
            .build();
    Method method =
        GcsClientImpl.class.getDeclaredMethod(
            "generateSessionConfig", GcsWriteOptions.class, boolean.class);
    method.setAccessible(true);

    Object result = method.invoke(client, writeOptions, false);

    assertThat(result).isNotNull();
    assertThat(result.getClass().getSimpleName()).contains("Journaling");
  }

  @Test
  void
      getJournalingSessionConfig_withNonHttpStorageOptionsAndEmptyTemporaryPaths_throwsIllegalArgumentException()
          throws Exception {
    GcsClientImpl client =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry);
    GcsWriteOptions writeOptions =
        GcsWriteOptions.builder()
            .setUploadType(GcsWriteOptions.UploadType.JOURNALING)
            .setTemporaryPaths(ImmutableList.of())
            .build();
    Method method =
        GcsClientImpl.class.getDeclaredMethod(
            "getJournalingSessionConfig", GcsWriteOptions.class, boolean.class);
    method.setAccessible(true);

    InvocationTargetException exception =
        assertThrows(
            InvocationTargetException.class, () -> method.invoke(client, writeOptions, false));

    assertThat(exception.getCause()).isInstanceOf(IllegalArgumentException.class);
    assertThat(exception.getCause())
        .hasMessageThat()
        .contains("Temporary paths must be configured for JOURNALING upload type");
  }

  private void assertPcuSessionConfig(GcsWriteOptions writeOptions) throws Exception {
    GcsClientImpl client = createClientWithWriteOptions(writeOptions);

    assertThat(getBlobWriteSessionConfig(client.storage.getOptions()))
        .isInstanceOf(ParallelCompositeUploadBlobWriteSessionConfig.class);
  }

  private GcsClientImpl createClientWithMockStorage(Storage mockStorage) throws IOException {
    GcsClientImpl client =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry);
    client.storage = mockStorage;
    return client;
  }

  private GcsClientImpl createClientWithWriteOptions(GcsWriteOptions writeOptions)
      throws IOException {
    GcsClientOptions clientOptions =
        GcsClientOptions.builder()
            .setProjectId(TEST_PROJECT)
            .setGcsWriteOptions(writeOptions)
            .build();
    return new GcsClientImpl(clientOptions, executorServiceSupplier, telemetry);
  }

  private BlobWriteSession mockBlobWriteSession(Storage mockStorage) {
    BlobWriteSession mockSession = mock(BlobWriteSession.class);
    when(mockStorage.blobWriteSession(any(BlobInfo.class), any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockSession);
    return mockSession;
  }

  private String captureBlobWriteOptions(Storage mockStorage, BlobInfo blobInfo) {
    ArgumentCaptor<Storage.BlobWriteOption[]> optionsCaptor =
        ArgumentCaptor.forClass(Storage.BlobWriteOption[].class);
    verify(mockStorage).blobWriteSession(eq(blobInfo), optionsCaptor.capture());
    return Arrays.toString(optionsCaptor.getValue());
  }

  @Test
  void createStore_bidiDisabled_usesHttpTransport() throws IOException {
    GcsClientImpl client =
        new GcsClientImpl(
            NoCredentials.getInstance(),
            TEST_GCS_CLIENT_OPTIONS,
            executorServiceSupplier,
            telemetry);
    assertThat(client.storage.getOptions()).isInstanceOf(HttpStorageOptions.class);
  }

  @Test
  void createStore_bidiEnabled_usesGrpcTransport() throws IOException {
    GcsClientOptions options =
        GcsClientOptions.builder()
            .setProjectId("test-project")
            .setGcsReadOptions(GcsReadOptions.builder().setBidiReadEnabled(true).build())
            .build();
    GcsClientImpl client =
        new GcsClientImpl(NoCredentials.getInstance(), options, executorServiceSupplier, telemetry);
    assertThat(client.storage.getOptions()).isInstanceOf(GrpcStorageOptions.class);
  }

  @Test
  void openReadChannel_bidiEnabled_returnsGcsBidiReadChannel() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setUserProjectId("test-project").setBidiReadEnabled(true).build();
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName("test-bucket-name")
            .setObjectName("test-object-name")
            .build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(100L).setContentGeneration(0L).build();

    Storage mockStorage = mock(Storage.class);
    ApiFuture<BlobReadSession> mockSessionFuture = mock(ApiFuture.class);
    when(mockStorage.blobReadSession(any(BlobId.class))).thenReturn(mockSessionFuture);

    GcsClient bidiClient =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry) {
          @Override
          protected Storage createStorage(Optional<Credentials> credentials) {
            return mockStorage;
          }
        };

    VectoredSeekableByteChannel channel = bidiClient.openReadChannel(itemInfo, readOptions);
    assertThat(channel).isInstanceOf(GcsBidiReadChannel.class);
  }

  @Test
  void openReadChannel_itemId_bidiEnabled_returnsGcsBidiReadChannel() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setUserProjectId("test-project").setBidiReadEnabled(true).build();
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName("test-bucket-name")
            .setObjectName("test-object-name")
            .build();

    Storage mockStorage = mock(Storage.class);
    ApiFuture<BlobReadSession> mockSessionFuture = mock(ApiFuture.class);
    when(mockStorage.blobReadSession(any(BlobId.class))).thenReturn(mockSessionFuture);

    GcsClient bidiClient =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry) {
          @Override
          protected Storage createStorage(Optional<Credentials> credentials) {
            return mockStorage;
          }
        };

    VectoredSeekableByteChannel channel = bidiClient.openReadChannel(itemId, readOptions);
    assertThat(channel).isInstanceOf(GcsBidiReadChannel.class);
  }
}
