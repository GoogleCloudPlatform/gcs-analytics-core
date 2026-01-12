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
  void openReadChannel_adaptiveRangeReadEnabled_returnsGcsAdaptiveReadChannel() throws IOException {
    GcsClientOptions options = GcsClientOptions.builder().setProjectId("test-project").build();
    GcsClient client =
        new GcsClientImpl(options, executorServiceSupplier) {
          @Override
          protected Storage createStorage(Optional<Credentials> credentials) {
            return GcsClientImplTest.this.storage;
          }
        };
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName("test-bucket-name")
            .setObjectName("test-object-name")
            .build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(100L).setContentGeneration(0L).build();
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setUserProjectId("test-project")
            .setAdaptiveRangeReadEnabled(true)
            .build();

    try (SeekableByteChannel channel = client.openReadChannel(itemInfo, readOptions)) {
      assertThat(channel).isInstanceOf(GcsAdaptiveReadChannel.class);
    }
  }

  @Test
  void openReadChannel_adaptiveRangeReadDisabled_returnsGcsReadChannel() throws IOException {
    GcsClientOptions options = GcsClientOptions.builder().setProjectId("test-project").build();
    GcsClient client =
        new GcsClientImpl(options, executorServiceSupplier) {
          @Override
          protected Storage createStorage(Optional<Credentials> credentials) {
            return GcsClientImplTest.this.storage;
          }
        };
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName("test-bucket-name")
            .setObjectName("test-object-name")
            .build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(100L).setContentGeneration(0L).build();
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setUserProjectId("test-project")
            .setAdaptiveRangeReadEnabled(false)
            .build();

    try (SeekableByteChannel channel = client.openReadChannel(itemInfo, readOptions)) {
      assertThat(channel).isInstanceOf(GcsReadChannel.class);
      assertThat(channel).isNotInstanceOf(GcsAdaptiveReadChannel.class);
    }
  }

  @Test
  void createStore_withCredentials_usesProvidedCredentials() throws IOException {
    GcsClientImpl client =
        new GcsClientImpl(
            NoCredentials.getInstance(), TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier);

    assertThat(client.storage.getOptions().getCredentials()).isEqualTo(NoCredentials.getInstance());
  }

  private void createBlobInStorage(BlobId blobId, String blobContent) {
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, blobContent.getBytes(StandardCharsets.UTF_8));
  }
}
