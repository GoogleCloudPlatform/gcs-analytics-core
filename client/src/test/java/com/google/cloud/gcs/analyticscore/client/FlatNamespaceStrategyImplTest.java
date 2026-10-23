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

import com.google.cloud.storage.StorageException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FlatNamespaceStrategyImplTest {

  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_DIR = "my-dir";

  private GcsClient mockClient;
  private FlatNamespaceStrategyImpl strategy;

  @BeforeEach
  void setUp() {
    mockClient = mock(GcsClient.class);
    strategy = new FlatNamespaceStrategyImpl(mockClient);
  }

  @Test
  void createDirectory_validItemIdWithoutTrailingSlash_createsEmptyObjectWithTrailingSlash()
      throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir/").build();

    strategy.createDirectory(itemId);

    verify(mockClient).createEmptyObject(expectedItemId);
  }

  @Test
  void createDirectory_validItemIdWithTrailingSlash_createsEmptyObjectWithTrailingSlash()
      throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir/").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir/").build();

    strategy.createDirectory(itemId);

    verify(mockClient).createEmptyObject(expectedItemId);
  }

  @Test
  void createDirectory_alreadyExists_suppressesException() throws IOException {
    GcsItemId itemId = GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/").build();
    doThrow(new FileAlreadyExistsException("dir/"))
        .when(mockClient)
        .createEmptyObject(expectedItemId);

    assertDoesNotThrow(() -> strategy.createDirectory(itemId));
  }

  @Test
  void createDirectory_ioException_propagatesException() throws IOException {
    GcsItemId itemId = GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/").build();
    doThrow(new IOException("Network failure")).when(mockClient).createEmptyObject(expectedItemId);

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
  void getDirectoryInfo_placeholderObjectListed_returnsPlaceholderObjectMetadata()
      throws IOException {
    GcsItemId dirId = createGcsItemId(TEST_DIR);
    GcsItemId prefixId = dirId.toDirectoryId();
    GcsItemInfo placeholderItem =
        GcsItemInfo.builder()
            .setItemId(
                GcsItemId.builder()
                    .setBucketName(TEST_BUCKET)
                    .setObjectName(TEST_DIR + "/")
                    .setContentGeneration(123L)
                    .build())
            .setSize(0L)
            .setContentGeneration(123L)
            .setItemType(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY)
            .build();
    when(mockClient.listFirstObjectWithPrefix(eq(prefixId)))
        .thenReturn(Optional.of(placeholderItem));

    GcsItemInfo result = strategy.getDirectoryInfo(dirId);

    assertThat(result.getItemType()).isEqualTo(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY);
    assertThat(result.getItemId().getBucketName()).isEqualTo(TEST_BUCKET);
    assertThat(result.getItemId().getObjectName()).hasValue(TEST_DIR + "/");
    assertThat(result.getItemId().getContentGeneration()).hasValue(123L);
    assertThat(result.getContentGeneration()).hasValue(123L);
    assertThat(result.getSize()).isEqualTo(0L);
  }

  @Test
  void getDirectoryInfo_childObjectListed_returnsInferredDirectoryWithoutGeneration()
      throws IOException {
    GcsItemId dirIdWithGeneration =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_DIR)
            .setContentGeneration(42L)
            .build();
    GcsItemId prefixId = dirIdWithGeneration.toDirectoryId();
    GcsItemInfo childItem = createGcsItemInfo(TEST_DIR + "/file.txt", 100L);
    when(mockClient.listFirstObjectWithPrefix(eq(prefixId))).thenReturn(Optional.of(childItem));

    GcsItemInfo result = strategy.getDirectoryInfo(dirIdWithGeneration);

    assertThat(result.getItemType()).isEqualTo(GcsItemInfo.ItemType.INFERRED_DIRECTORY);
    assertThat(result.getItemId()).isEqualTo(prefixId);
    assertThat(result.getItemId().getContentGeneration()).isEmpty();
    assertThat(result.getContentGeneration()).isEmpty();
    assertThat(result.getSize()).isEqualTo(0L);
  }

  @Test
  void getDirectoryInfo_implicitDirectoryDoesNotExist_throwsFileNotFoundException()
      throws IOException {
    GcsItemId dirId = createGcsItemId(TEST_DIR);
    GcsItemId prefixId = dirId.toDirectoryId();
    when(mockClient.listFirstObjectWithPrefix(eq(prefixId))).thenReturn(Optional.empty());

    FileNotFoundException e =
        assertThrows(FileNotFoundException.class, () -> strategy.getDirectoryInfo(dirId));

    assertThat(e)
        .hasMessageThat()
        .contains(
            "Location does not exist or generation not found: gs://"
                + TEST_BUCKET
                + "/"
                + TEST_DIR);
    assertThat(e.getCause()).isInstanceOf(StorageException.class);
    assertThat(((StorageException) e.getCause()).getCode()).isEqualTo(404);
    verify(mockClient).listFirstObjectWithPrefix(prefixId);
  }

  @Test
  void getDirectoryInfo_listFirstObjectWithPrefixThrowsIOException_throwsIOException()
      throws IOException {
    GcsItemId dirId = createGcsItemId(TEST_DIR);
    GcsItemId prefixId = dirId.toDirectoryId();
    when(mockClient.listFirstObjectWithPrefix(eq(prefixId)))
        .thenThrow(new IOException("GCS listing failure"));

    IOException e = assertThrows(IOException.class, () -> strategy.getDirectoryInfo(dirId));

    assertThat(e).hasMessageThat().contains("GCS listing failure");
    verify(mockClient).listFirstObjectWithPrefix(prefixId);
  }

  @Test
  void getDirectoryInfo_inferImplicitDirectoriesFalse_placeholderNotFound_preservesInputIdInError()
      throws IOException {
    GcsItemId dirIdWithoutSlash = createGcsItemId(TEST_DIR);
    GcsItemId prefixId = dirIdWithoutSlash.toDirectoryId();
    when(mockClient.getGcsItemInfo(eq(prefixId)))
        .thenThrow(GcsExceptionUtil.createFileNotFoundException(prefixId));

    FileNotFoundException e =
        assertThrows(
            FileNotFoundException.class,
            () ->
                strategy.getDirectoryInfo(
                    dirIdWithoutSlash, /* inferImplicitDirectories= */ false));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(
            "Location does not exist or generation not found: gs://"
                + TEST_BUCKET
                + "/"
                + TEST_DIR);
    verify(mockClient).getGcsItemInfo(prefixId);
  }

  private static GcsItemId createGcsItemId(String objectName) {
    return GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(objectName).build();
  }

  private static GcsItemInfo createGcsItemInfo(String objectName, long size) {
    return GcsItemInfo.builder().setItemId(createGcsItemId(objectName)).setSize(size).build();
  }
}
