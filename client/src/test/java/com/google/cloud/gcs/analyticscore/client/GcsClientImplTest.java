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

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.*;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GcsClientImplTest {

  private static GcsClientOptions TEST_GCS_CLIENT_OPTIONS =
      GcsClientOptions.builder().setProjectId("test-project").build();

  private final Storage storage = LocalStorageHelper.getOptions().getService();
  private final Supplier<ExecutorService> executorServiceSupplier =
      Suppliers.memoize(() -> Executors.newFixedThreadPool(30));

  private GcsClient gcsClient;

  @BeforeEach
  void setUp() throws IOException {
    gcsClient =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier) {
          @Override
          Storage createStorage(Optional<Credentials> credentials) {
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

    assertThat(itemInfo.getItemId()).isEqualTo(itemId);
    assertThat(itemInfo.getSize()).isEqualTo(objectData.length());
    assertThat(itemInfo.getContentGeneration()).isEqualTo(0L);
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
    GcsReadOptions readOptions = GcsReadOptions.builder().setProjectId("test-project").build();
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
  void openReadChannel_nonExistentBlob_throwsIOException() {
    GcsItemId nonExistentItemId =
        GcsItemId.builder().setBucketName("test-bucket-name").setObjectName("non-existent").build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setProjectId("test-project-id").build();

    IOException e =
        assertThrows(
            IOException.class, () -> gcsClient.openReadChannel(nonExistentItemId, readOptions));

    assertThat(e).hasMessageThat().contains("Object not found:" + nonExistentItemId);
  }

  @Test
  void openReadChannel_itemIdPointsToDirectory_throwsIllegalArgumentException() {
    GcsItemId directoryItemId = GcsItemId.builder().setBucketName("test-bucket-name").build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setProjectId("test-project-id").build();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> gcsClient.openReadChannel(directoryItemId, readOptions));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Expected GCS object to be provided. But got: " + directoryItemId);
  }


  void createStore_withCredentials_usesProvidedCredentials() throws IOException {
    GcsClientImpl client =
        new GcsClientImpl(
            NoCredentials.getInstance(), TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier);

    assertThat(client.storage.getOptions().getCredentials()).isEqualTo(NoCredentials.getInstance());
  }

//  @Test
//  void createStorage_configuresStorageOptionsCorrectly() {
//    String projectId = "test-project-id";
//    String clientLibToken = LocalStorageHelper.getOptions().getClientLibToken();
//    String serviceHost = "http://test-host";
//    Credentials testCredentials = LocalStorageHelper.getOptions().getCredentials();
//    GcsClientOptions options =
//        GcsClientOptions.builder()
//            .setProjectId(projectId)
//            .setClientLibToken(clientLibToken)
//            .setServiceHost(serviceHost)
//            .build();
//
//    GcsClientImpl client = new GcsClientImpl(testCredentials, options, executorServiceSupplier);
//    StorageOptions storageOptions = client.storage.getOptions();
//
//    assertThat(storageOptions.getProjectId()).isEqualTo(projectId);
//    assertThat(storageOptions.getHost()).isEqualTo(serviceHost);
//    assertThat(storageOptions.getClientLibToken()).isEqualTo(clientLibToken);
//    assertThat(storageOptions.getCredentials()).isSameInstanceAs(testCredentials);
//  }

  private void createBlobInStorage(BlobId blobId, String blobContent) {
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, blobContent.getBytes(StandardCharsets.UTF_8));
  }
}
