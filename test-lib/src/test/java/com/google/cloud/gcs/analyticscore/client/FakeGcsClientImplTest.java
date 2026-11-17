/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobId;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FakeGcsClientImplTest {

  private FakeGcsClientImpl fakeGcsClient;

  @BeforeEach
  void setUp() {
    GcsFileSystemOptions options = GcsFileSystemOptions.builder().build();
    fakeGcsClient =
        new FakeGcsClientImpl(
            options.getGcsClientOptions(),
            Suppliers.ofInstance(Executors.newSingleThreadExecutor()),
            new Telemetry(ImmutableList.of()),
            new AnalyticsCacheManager(options.getGcsCacheOptions()).getBucketPropertiesCache());
    FakeGcsClientImpl.resetCounts();
  }

  @Test
  void openReadChannel_incrementsOpenReadChannelCount() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(100L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    TestDataGenerator.createGcsData(itemId, 100);

    VectoredSeekableByteChannel channel = fakeGcsClient.openReadChannel(itemInfo, readOptions);

    assertNotNull(channel);
    assertEquals(1, FakeGcsClientImpl.getOpenReadChannelCount());
  }

  @Test
  void getGcsItemInfo_objectExists_pass() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    TestDataGenerator.createGcsData(itemId, 100);

    GcsItemInfo itemInfo = fakeGcsClient.getGcsItemInfo(itemId);

    assertNotNull(itemInfo);
    GcsItemId expectedItemId =
        GcsItemId.builder()
            .setBucketName("test-bucket")
            .setObjectName("test-object")
            .setContentGeneration(itemInfo.getContentGeneration().get())
            .build();
    assertThat(itemInfo.getItemId()).isEqualTo(expectedItemId);
    assertEquals(100L, itemInfo.getSize());
  }

  @Test
  void getGcsItemInfo_objectDoesNotExists_throwsFileNotFoundException() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("not-exists").build();

    assertThrows(FileNotFoundException.class, () -> fakeGcsClient.getGcsItemInfo(itemId));
  }

  @Test
  void getGcsItemInfo_itemIsNotObject_throws() {
    GcsItemId itemId = GcsItemId.builder().setBucketName("test-bucket").build(); // No object name

    assertThrows(UnsupportedOperationException.class, () -> fakeGcsClient.getGcsItemInfo(itemId));
  }

  @Test
  void close_incrementsCloseCount() {
    fakeGcsClient.close();
    assertEquals(1, FakeGcsClientImpl.getCloseCount());
  }

  @Test
  void lazyGetGrpcStorage_returnsLocalStorageInstance() throws IOException {
    assertThat(fakeGcsClient.lazyGetGrpcStorage()).isSameInstanceAs(FakeGcsClientImpl.storage);
  }

  @Test
  void moveObject_existingSource_movesObjectToDestination() throws IOException {
    GcsItemId srcId = moveObjectId("move-src");
    GcsItemId dstId = moveObjectId("move-dst");
    createMoveObjectBlob("move-src");

    fakeGcsClient.moveObject(srcId, dstId);

    assertThrows(FileNotFoundException.class, () -> fakeGcsClient.getGcsItemInfo(srcId));
    GcsItemInfo dstInfo = fakeGcsClient.getGcsItemInfo(dstId);
    assertThat(dstInfo.getItemId().getObjectName()).hasValue("move-dst");
    assertThat(dstInfo.getSize()).isEqualTo(10L);
  }

  @Test
  void moveObject_missingSource_throwsFileNotFoundException() {
    FileNotFoundException e =
        assertThrows(
            FileNotFoundException.class,
            () -> fakeGcsClient.moveObject(moveObjectId("missing-src"), moveObjectId("move-dst")));

    assertThat(e).hasMessageThat().contains("gs://move-bucket/missing-src");
  }

  @Test
  void moveObject_staleSourceGeneration_throwsIOExceptionAndKeepsSource() throws IOException {
    GcsItemId srcId = moveObjectId("stale-src");
    createMoveObjectBlob("stale-src");
    long generation = fakeGcsClient.getGcsItemInfo(srcId).getContentGeneration().get();
    GcsItemId staleSrcId =
        GcsItemId.builder()
            .setBucketName("move-bucket")
            .setObjectName("stale-src")
            .setContentGeneration(generation + 1)
            .build();

    IOException e =
        assertThrows(
            IOException.class,
            () -> fakeGcsClient.moveObject(staleSrcId, moveObjectId("stale-dst")));

    assertThat(e.getClass()).isEqualTo(IOException.class);
    assertThat(fakeGcsClient.getGcsItemInfo(srcId).getSize()).isEqualTo(10L);
  }

  @Test
  void moveObject_existingDestinationWithZeroGeneration_throwsFileAlreadyExistsAndKeepsBoth()
      throws IOException {
    GcsItemId srcId = moveObjectId("taken-src");
    GcsItemId dstId = moveObjectId("taken-dst");
    createMoveObjectBlob("taken-src");
    StorageTestUtils.createBlobInStorage(
        FakeGcsClientImpl.storage, BlobId.of("move-bucket", "taken-dst", 1L), "dst");
    GcsItemId mustNotExistDstId =
        GcsItemId.builder()
            .setBucketName("move-bucket")
            .setObjectName("taken-dst")
            .setContentGeneration(0L)
            .build();

    FileAlreadyExistsException e =
        assertThrows(
            FileAlreadyExistsException.class,
            () -> fakeGcsClient.moveObject(srcId, mustNotExistDstId));

    assertThat(e.getFile()).isEqualTo("gs://move-bucket/taken-dst");
    assertThat(fakeGcsClient.getGcsItemInfo(srcId).getSize()).isEqualTo(10L);
    assertThat(fakeGcsClient.getGcsItemInfo(dstId).getSize()).isEqualTo(3L);
  }

  private static GcsItemId moveObjectId(String objectName) {
    return GcsItemId.builder().setBucketName("move-bucket").setObjectName(objectName).build();
  }

  private static void createMoveObjectBlob(String objectName) {
    StorageTestUtils.createBlobInStorage(
        FakeGcsClientImpl.storage, BlobId.of("move-bucket", objectName, 1L), "0123456789");
  }
}
