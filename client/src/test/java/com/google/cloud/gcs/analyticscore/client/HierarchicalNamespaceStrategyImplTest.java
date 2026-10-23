/*
 * Copyright 2026 Google LLC
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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HierarchicalNamespaceStrategyImplTest {

  private static final String TEST_BUCKET = "test-hns-bucket";
  private static final String TEST_FOLDER = "my-folder";

  private GcsClient mockClient;
  private HierarchicalNamespaceStrategyImpl strategy;

  @BeforeEach
  void setUp() {
    mockClient = mock(GcsClient.class);
    strategy = new HierarchicalNamespaceStrategyImpl(mockClient);
  }

  @Test
  void createDirectory_validItemIdWithTrailingSlash_createsFolderWithoutTrailingSlash()
      throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir/").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir").build();

    strategy.createDirectory(itemId);

    verify(mockClient).createFolder(expectedItemId, true);
  }

  @Test
  void createDirectory_validItemIdWithoutTrailingSlash_createsFolderWithoutTrailingSlash()
      throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir").build();

    strategy.createDirectory(itemId);

    verify(mockClient).createFolder(expectedItemId, true);
  }

  @Test
  void createDirectory_alreadyExists_suppressesException() throws IOException {
    GcsItemId itemId = GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir").build();
    doThrow(new FileAlreadyExistsException("dir"))
        .when(mockClient)
        .createFolder(expectedItemId, true);

    assertDoesNotThrow(() -> strategy.createDirectory(itemId));
  }

  @Test
  void createDirectory_ioException_propagatesException() throws IOException {
    GcsItemId itemId = GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir").build();
    doThrow(new IOException("Network failure")).when(mockClient).createFolder(expectedItemId, true);

    assertThat(assertThrows(IOException.class, () -> strategy.createDirectory(itemId)))
        .hasMessageThat()
        .isEqualTo("Network failure");
  }

  @Test
  void createDirectory_nullOrNonObjectItemId_throwsException() {
    GcsItemId bucketItemId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();

    assertThrows(NullPointerException.class, () -> strategy.createDirectory(null));
    assertThrows(IllegalArgumentException.class, () -> strategy.createDirectory(bucketItemId));
  }

  @Test
  void getDirectoryInfo_nullItemId_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> strategy.getDirectoryInfo(null));
  }

  @Test
  void getDirectoryInfo_folderExists_returnsFolderItemInfo() throws IOException {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_FOLDER).build();
    GcsItemId folderDirId = folderId.toDirectoryId();
    GcsItemInfo folderInfo =
        GcsItemInfo.builder()
            .setItemId(folderDirId)
            .setSize(0L)
            .setItemType(GcsItemInfo.ItemType.NATIVE_FOLDER)
            .build();
    when(mockClient.getFolderInfo(eq(folderId))).thenReturn(folderInfo);

    GcsItemInfo result = strategy.getDirectoryInfo(folderId);

    assertThat(result).isSameInstanceAs(folderInfo);
  }

  @Test
  void getDirectoryInfo_folderNotFound_throwsFileNotFoundException() throws IOException {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_FOLDER).build();
    when(mockClient.getFolderInfo(eq(folderId)))
        .thenThrow(GcsExceptionUtil.createFileNotFoundException(folderId));

    FileNotFoundException e =
        assertThrows(FileNotFoundException.class, () -> strategy.getDirectoryInfo(folderId));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(
            "Location does not exist or generation not found: gs://"
                + TEST_BUCKET
                + "/"
                + TEST_FOLDER);
    verify(mockClient).getFolderInfo(folderId);
  }

  @Test
  void getDirectoryInfo_getFolderInfoThrowsIOException_throwsIOException() throws IOException {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_FOLDER).build();
    when(mockClient.getFolderInfo(eq(folderId)))
        .thenThrow(new IOException("GCS folder lookup failure"));

    IOException e = assertThrows(IOException.class, () -> strategy.getDirectoryInfo(folderId));

    assertThat(e).hasMessageThat().contains("GCS folder lookup failure");
  }

  @Test
  void getDirectoryInfo_inferImplicitDirectoriesFalse_stillQueriesNativeFolder()
      throws IOException {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_FOLDER).build();
    GcsItemInfo folderInfo =
        GcsItemInfo.builder()
            .setItemId(folderId.toDirectoryId())
            .setSize(0L)
            .setItemType(GcsItemInfo.ItemType.NATIVE_FOLDER)
            .build();
    when(mockClient.getFolderInfo(eq(folderId))).thenReturn(folderInfo);

    GcsItemInfo result = strategy.getDirectoryInfo(folderId, /* inferImplicitDirectories= */ false);

    assertThat(result).isSameInstanceAs(folderInfo);
    verify(mockClient).getFolderInfo(folderId);
  }
}
