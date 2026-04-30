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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.*;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class GcsClientImplTest {

  private static GcsClientOptions TEST_GCS_CLIENT_OPTIONS =
      GcsClientOptions.builder().setProjectId("test-project").build();

  private final Storage storage = LocalStorageHelper.getOptions().getService();
  private final Supplier<ExecutorService> executorServiceSupplier =
      Suppliers.memoize(() -> Executors.newFixedThreadPool(30));
  private final Telemetry telemetry = new Telemetry(ImmutableList.of());

  private GcsClient gcsClient;
  private Storage tempMockStorage;

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
    GcsItemId directoryItemId = GcsItemId.builder().setBucketName("test-bucket-id").build();

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
        GcsItemId.builder().setBucketName("test-bucket-id").setObjectName("test-object-id").build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);

    GcsItemInfo itemInfo = gcsClient.getGcsItemInfo(itemId);

    GcsItemId expectedItemId =
        GcsItemId.builder()
            .setBucketName("test-bucket-id")
            .setObjectName("test-object-id")
            .setContentGeneration(itemInfo.getContentGeneration().get())
            .build();
    assertThat(itemInfo.getItemId()).isEqualTo(expectedItemId);
    assertThat(itemInfo.getSize()).isEqualTo(objectData.length());
    assertThat(itemInfo.getContentGeneration().get()).isEqualTo(0L);
  }

  @Test
  void getGcsItemInfo_nonExistentBlob_throwsIOException() {
    GcsItemId nonExistentItemId =
        GcsItemId.builder().setBucketName("test-bucket-name").setObjectName("non-existent").build();

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
        GcsItemId.builder()
            .setBucketName("test-bucket-name")
            .setObjectName("test-object-name")
            .build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
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
        GcsItemId.builder()
            .setBucketName("test-bucket-name")
            .setObjectName("test-object-name")
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    ByteBuffer buffer = ByteBuffer.allocate(objectData.length());

    SeekableByteChannel channel = gcsClient.openReadChannel(itemId, readOptions);
    int bytesRead = channel.read(buffer);

    assertThat(channel.size()).isEqualTo(objectData.length());
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
        GcsItemId.builder().setBucketName("test-bucket-name").setObjectName("test-object").build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(0L).setContentGeneration(0L).build();

    NullPointerException e =
        assertThrows(NullPointerException.class, () -> gcsClient.openReadChannel(itemInfo, null));
    assertThat(e).hasMessageThat().isEqualTo("readOptions should not be null");
  }

  @Test
  void openReadChannel_itemInfoPointsToDirectory_throwsIllegalArgumentException() {
    GcsItemId directoryItemId = GcsItemId.builder().setBucketName("test-bucket-name").build();
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
  void getUserAgent_noOptionalUserAgent() {
    GcsClientImpl client =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry);
    String userAgent = client.getUserAgent();
    assertThat(userAgent).isEqualTo("gcs-analytics-core/" + VersionHelper.VERSION);
  }

  @Test
  void getUserAgent_withOptionalUserAgent() {
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

  private void createBlobInStorage(BlobId blobId, String blobContent) {
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, blobContent.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void create_writeAndClose_successWithLocalStorage() throws Exception {
    GcsClientImpl client =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry) {
          @Override
          protected Storage createStorage(Optional<Credentials> credentials) {
            return GcsClientImplTest.this.storage;
          }
        };
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of("test-bucket", "test-write-object")).build();
    GcsWriteOptions options = GcsWriteOptions.builder().build();
    WritableByteChannel channel = client.create(blobInfo, options);

    byte[] data = "hello write world".getBytes(StandardCharsets.UTF_8);
    int bytesWritten = channel.write(ByteBuffer.wrap(data));
    channel.close();

    assertThat(bytesWritten).isEqualTo(data.length);
    byte[] readData = storage.readAllBytes(blobInfo.getBlobId());
    assertThat(new String(readData, StandardCharsets.UTF_8)).isEqualTo("hello write world");
  }

  @Test
  void create_TranslatesAccessDeniedException() throws Exception {
    tempMockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry);
    clientWithMock.storage = tempMockStorage;
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of("test-bucket", "test-object")).build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
    StorageException e403 = new StorageException(403, "Forbidden");
    when(tempMockStorage.blobWriteSession(eq(blobInfo), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e403);

    assertThrows(AccessDeniedException.class, () -> clientWithMock.create(blobInfo, writeOptions));
  }

  @Test
  void create_GenericStorageException() throws Exception {
    tempMockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry);
    clientWithMock.storage = tempMockStorage;
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of("test-bucket", "test-object")).build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
    StorageException e500 = new StorageException(500, "Internal Server Error");
    when(tempMockStorage.blobWriteSession(eq(blobInfo), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e500);

    IOException thrown =
        assertThrows(IOException.class, () -> clientWithMock.create(blobInfo, writeOptions));

    assertThat(thrown).hasMessageThat().contains("Failed to initialize BlobWriteSession");
  }

  @Test
  void create_GenerateWriteOptions_NullOptions() throws Exception {
    Storage mockStorage = mock(Storage.class);
    BlobWriteSession mockSession = mock(BlobWriteSession.class);
    WritableByteChannel mockChannel = mock(WritableByteChannel.class);
    when(mockStorage.blobWriteSession(any(BlobInfo.class), any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockSession);
    when(mockSession.open()).thenReturn(mockChannel);
    GcsClientImpl clientWithMock =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry);
    clientWithMock.storage = mockStorage;
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of("test-bucket", "test-object")).build();

    WritableByteChannel returnedChannel = clientWithMock.create(blobInfo, null);

    assertThat(returnedChannel).isEqualTo(mockChannel);
  }

  @Test
  void create_GenerateWriteOptions_AllOptionsEnabled() throws Exception {
    Storage mockStorage = mock(Storage.class);
    BlobWriteSession mockSession = mock(BlobWriteSession.class);
    when(mockStorage.blobWriteSession(any(BlobInfo.class), any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockSession);
    when(mockSession.open()).thenReturn(mock(WritableByteChannel.class));
    GcsClientImpl clientWithMock =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry);
    clientWithMock.storage = mockStorage;
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of("test-bucket", "test-object")).build();
    GcsWriteOptions allOptions =
        GcsWriteOptions.builder()
            .setChecksumValidationEnabled(true)
            .setDisableGzipContent(true)
            .setOverwriteExisting(false)
            .setKmsKeyName("kms-key")
            .setEncryptionKey("MDEyMzQ1Njc4OUFCQ0RFRkdISUpLTE1OT1BRUlNUVVU=")
            .setUserProject("user-project")
            .build();

    clientWithMock.create(blobInfo, allOptions);

    ArgumentCaptor<Storage.BlobWriteOption[]> optionsCaptor =
        ArgumentCaptor.forClass(Storage.BlobWriteOption[].class);
    verify(mockStorage).blobWriteSession(eq(blobInfo), optionsCaptor.capture());
    String capturedOptionsString = Arrays.toString(optionsCaptor.getValue());
    assertThat(capturedOptionsString).contains("Crc32cMatchExtractor");
    assertThat(capturedOptionsString).contains("IF_GENERATION_MATCH");
    assertThat(capturedOptionsString).contains("KMS_KEY_NAME");
    assertThat(capturedOptionsString).contains("CUSTOMER_SUPPLIED_KEY");
  }

  @Test
  void create_GenerateWriteOptions_WithGenerationId() throws Exception {
    Storage mockStorage = mock(Storage.class);
    BlobWriteSession mockSession = mock(BlobWriteSession.class);
    when(mockStorage.blobWriteSession(any(BlobInfo.class), any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockSession);
    when(mockSession.open()).thenReturn(mock(WritableByteChannel.class));
    GcsClientImpl clientWithMock =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry);
    clientWithMock.storage = mockStorage;
    BlobInfo blobInfoWithGen =
        BlobInfo.newBuilder(BlobId.of("test-bucket", "test-object", 12345L)).build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();

    clientWithMock.create(blobInfoWithGen, writeOptions);

    ArgumentCaptor<Storage.BlobWriteOption[]> optionsCaptor =
        ArgumentCaptor.forClass(Storage.BlobWriteOption[].class);
    verify(mockStorage).blobWriteSession(eq(blobInfoWithGen), optionsCaptor.capture());
    assertThat(Arrays.toString(optionsCaptor.getValue())).contains("GenerationMatchExtractor");
  }
}
