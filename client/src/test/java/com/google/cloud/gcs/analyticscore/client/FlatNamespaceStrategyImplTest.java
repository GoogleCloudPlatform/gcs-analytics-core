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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import org.junit.jupiter.api.Test;

class FlatNamespaceStrategyImplTest {

  private static final String BUCKET = "test-bucket";

  private final GcsClient mockGcsClient = mock(GcsClient.class);
  private final FlatNamespaceStrategyImpl strategy = new FlatNamespaceStrategyImpl(mockGcsClient);

  @Test
  void createDirectory_validItemIdWithoutTrailingSlash_createsEmptyObjectWithTrailingSlash()
      throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(BUCKET).setObjectName("dir/subdir").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(BUCKET).setObjectName("dir/subdir/").build();

    strategy.createDirectory(itemId);

    verify(mockGcsClient).createEmptyObject(expectedItemId);
  }

  @Test
  void createDirectory_validItemIdWithTrailingSlash_createsEmptyObjectWithTrailingSlash()
      throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(BUCKET).setObjectName("dir/subdir/").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(BUCKET).setObjectName("dir/subdir/").build();

    strategy.createDirectory(itemId);

    verify(mockGcsClient).createEmptyObject(expectedItemId);
  }

  @Test
  void createDirectory_alreadyExists_suppressesException() throws IOException {
    GcsItemId itemId = GcsItemId.builder().setBucketName(BUCKET).setObjectName("dir").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(BUCKET).setObjectName("dir/").build();
    doThrow(new FileAlreadyExistsException("dir/"))
        .when(mockGcsClient)
        .createEmptyObject(expectedItemId);

    assertDoesNotThrow(() -> strategy.createDirectory(itemId));
  }

  @Test
  void createDirectory_ioException_propagatesException() throws IOException {
    GcsItemId itemId = GcsItemId.builder().setBucketName(BUCKET).setObjectName("dir").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(BUCKET).setObjectName("dir/").build();
    doThrow(new IOException("Network failure"))
        .when(mockGcsClient)
        .createEmptyObject(expectedItemId);

    assertThat(assertThrows(IOException.class, () -> strategy.createDirectory(itemId)))
        .hasMessageThat()
        .isEqualTo("Network failure");
  }

  @Test
  void createDirectory_nullOrNonObjectItemId_throwsException() {
    GcsItemId bucketItemId = GcsItemId.builder().setBucketName(BUCKET).build();

    assertThrows(NullPointerException.class, () -> strategy.createDirectory(null));
    assertThrows(IllegalArgumentException.class, () -> strategy.createDirectory(bucketItemId));
  }
}
