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

import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
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
  void getDirectoryInfo_implicitDirectoryExists_returnsInferredDirectory() throws IOException {
    GcsItemId dirId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_DIR).build();
    GcsItemId prefixId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_DIR + "/").build();
    GcsItemInfo childItem =
        GcsItemInfo.builder()
            .setItemId(
                GcsItemId.builder()
                    .setBucketName(TEST_BUCKET)
                    .setObjectName(TEST_DIR + "/file.txt")
                    .build())
            .setSize(100L)
            .build();
    when(mockClient.listFirstObjectWithPrefix(eq(prefixId)))
        .thenReturn(ImmutableList.of(childItem));

    GcsItemInfo result = strategy.getDirectoryInfo(dirId);

    assertThat(result).isNotNull();
    assertThat(result.getItemId()).isEqualTo(dirId);
    assertThat(result.getItemType()).isEqualTo(GcsItemInfo.ItemType.INFERRED_DIRECTORY);
    assertThat(result.isInferredDirectory()).isTrue();
    verify(mockClient).listFirstObjectWithPrefix(prefixId);
  }

  @Test
  void getDirectoryInfo_implicitDirectoryDoesNotExist_throwsFileNotFoundException()
      throws IOException {
    GcsItemId dirId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_DIR).build();
    GcsItemId prefixId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_DIR + "/").build();
    when(mockClient.listFirstObjectWithPrefix(eq(prefixId))).thenReturn(ImmutableList.of());

    FileNotFoundException e =
        assertThrows(FileNotFoundException.class, () -> strategy.getDirectoryInfo(dirId));

    assertThat(e).hasMessageThat().contains("Directory not found: " + dirId);
    verify(mockClient).listFirstObjectWithPrefix(prefixId);
  }

  @Test
  void getDirectoryInfo_listFirstObjectWithPrefixThrowsIOException_throwsFileNotFoundException()
      throws IOException {
    GcsItemId dirId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_DIR).build();
    GcsItemId prefixId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_DIR + "/").build();
    when(mockClient.listFirstObjectWithPrefix(eq(prefixId)))
        .thenThrow(new IOException("GCS listing failure"));

    FileNotFoundException e =
        assertThrows(FileNotFoundException.class, () -> strategy.getDirectoryInfo(dirId));

    assertThat(e).hasMessageThat().contains("Directory not found: " + dirId);
    verify(mockClient).listFirstObjectWithPrefix(prefixId);
  }
}
