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

import com.google.api.gax.rpc.FailedPreconditionException;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
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
  void createGetAndDeleteFolder_roundTripsHermeticallyInMemory() throws IOException {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName("hns-fake-bucket").setObjectName("a/b/c").build();

    fakeGcsClient.createFolder(folderId, true);

    GcsItemInfo folderInfo = fakeGcsClient.getFolderInfo(folderId);
    assertThat(folderInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.NATIVE_FOLDER);
    assertThat(folderInfo.getItemId())
        .isEqualTo(
            GcsItemId.builder().setBucketName("hns-fake-bucket").setObjectName("a/b/c/").build());
    assertThat(folderInfo.getMetaGeneration()).isEqualTo(1L);

    fakeGcsClient.deleteFolder(folderId);

    assertThrows(FileNotFoundException.class, () -> fakeGcsClient.getFolderInfo(folderId));
  }

  @Test
  void createFolder_duplicateFolder_throwsFileAlreadyExistsException() throws IOException {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName("hns-fake-bucket").setObjectName("dup/").build();
    fakeGcsClient.createFolder(folderId, false);

    assertThrows(
        FileAlreadyExistsException.class, () -> fakeGcsClient.createFolder(folderId, false));
  }

  @Test
  void createFolder_nonRecursiveWithoutParent_throwsIOExceptionWithFailedPrecondition() {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName("hns-fake-bucket").setObjectName("missing/child").build();

    IOException e =
        assertThrows(IOException.class, () -> fakeGcsClient.createFolder(folderId, false));

    assertThat(e).hasCauseThat().isInstanceOf(FailedPreconditionException.class);
  }

  @Test
  void deleteFolder_missingFolder_throwsFileNotFoundException() {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName("hns-fake-bucket").setObjectName("missing/").build();

    assertThrows(FileNotFoundException.class, () -> fakeGcsClient.deleteFolder(folderId));
  }

  @Test
  void deleteFolder_nonEmptyChildFolder_throwsDirectoryNotEmptyExceptionWithFailedPrecondition()
      throws IOException {
    GcsItemId parentId =
        GcsItemId.builder().setBucketName("hns-fake-bucket").setObjectName("parent/").build();
    GcsItemId childId =
        GcsItemId.builder().setBucketName("hns-fake-bucket").setObjectName("parent/child/").build();
    fakeGcsClient.createFolder(childId, true);

    DirectoryNotEmptyException e =
        assertThrows(DirectoryNotEmptyException.class, () -> fakeGcsClient.deleteFolder(parentId));

    assertThat(e.getFile()).isEqualTo("gs://hns-fake-bucket/parent/");
    assertThat(e).hasCauseThat().isInstanceOf(FailedPreconditionException.class);
  }

  @Test
  void deleteFolder_nonEmptyChildObject_throwsDirectoryNotEmptyExceptionWithFailedPrecondition()
      throws IOException {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName("hns-object-bucket").setObjectName("dir/").build();
    GcsItemId objectId =
        GcsItemId.builder()
            .setBucketName("hns-object-bucket")
            .setObjectName("dir/child.parquet")
            .build();
    fakeGcsClient.createFolder(folderId, false);
    TestDataGenerator.createGcsData(objectId, 16);

    DirectoryNotEmptyException e =
        assertThrows(DirectoryNotEmptyException.class, () -> fakeGcsClient.deleteFolder(folderId));

    assertThat(e.getFile()).isEqualTo("gs://hns-object-bucket/dir/");
    assertThat(e).hasCauseThat().isInstanceOf(FailedPreconditionException.class);
  }

  @Test
  void deleteFolder_afterClose_throwsStorageControlClientClosedException() {
    fakeGcsClient.close();
    GcsItemId folderId =
        GcsItemId.builder().setBucketName("hns-fake-bucket").setObjectName("dir/").build();

    IOException e = assertThrows(IOException.class, () -> fakeGcsClient.deleteFolder(folderId));

    assertThat(e).hasMessageThat().isEqualTo("StorageControlClient is closed");
  }
}
