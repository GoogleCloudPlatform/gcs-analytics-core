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

import java.io.FileNotFoundException;
import java.io.IOException;
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
  void getDirectoryInfo_nullItemId_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> strategy.getDirectoryInfo(null));
  }

  @Test
  void getDirectoryInfo_folderExists_returnsFolderItemInfo() throws IOException {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_FOLDER).build();
    GcsItemInfo folderInfo =
        GcsItemInfo.builder()
            .setItemId(folderId)
            .setSize(0L)
            .setItemType(GcsItemInfo.ItemType.NATIVE_FOLDER)
            .build();
    when(mockClient.getFolderInfo(eq(folderId))).thenReturn(folderInfo);

    GcsItemInfo result = strategy.getDirectoryInfo(folderId);

    assertThat(result).isNotNull();
    assertThat(result.getItemId()).isEqualTo(folderId);
    assertThat(result.getItemType()).isEqualTo(GcsItemInfo.ItemType.NATIVE_FOLDER);
    verify(mockClient).getFolderInfo(folderId);
  }

  @Test
  void getDirectoryInfo_folderNotFound_throwsFileNotFoundException() throws IOException {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_FOLDER).build();
    when(mockClient.getFolderInfo(eq(folderId)))
        .thenThrow(new FileNotFoundException("Folder not found: " + folderId));

    FileNotFoundException e =
        assertThrows(FileNotFoundException.class, () -> strategy.getDirectoryInfo(folderId));

    assertThat(e).hasMessageThat().contains("Folder not found: " + folderId);
    verify(mockClient).getFolderInfo(folderId);
  }
}
