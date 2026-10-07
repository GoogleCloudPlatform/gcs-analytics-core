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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.StorageException;
import java.io.FileNotFoundException;
import java.io.IOException;
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

  private static GcsItemId createGcsItemId(String objectName) {
    return GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(objectName).build();
  }

  private static GcsItemInfo createGcsItemInfo(String objectName, long size) {
    return GcsItemInfo.builder().setItemId(createGcsItemId(objectName)).setSize(size).build();
  }
}
