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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class FakeGcsFileSystemImplTest {

  @Test
  void constructor_initializedWithFakeGcsClient() {
    GcsFileSystemOptions options = GcsFileSystemOptions.builder().build();

    FakeGcsFileSystemImpl fakeFileSystem = new FakeGcsFileSystemImpl(options);

    assertNotNull(fakeFileSystem);
    assertTrue(fakeFileSystem.getGcsClient() instanceof FakeGcsClientImpl);
  }

  @Test
  void listFileInfo_directory_returnsImmediateChildrenSortedByUri() throws IOException {
    String bucket = newBucketName();
    putBlob(bucket, "dir/");
    putBlob(bucket, "dir/file.txt");
    putBlob(bucket, "dir/implicit/nested.txt");
    putBlob(bucket, "dir/sub/");
    putBlob(bucket, "dir/sub/nested.txt");
    putBlob(bucket, "other/file.txt");

    try (FakeGcsFileSystemImpl fakeFileSystem = newFakeFileSystem()) {
      List<GcsFileInfo> listed =
          fakeFileSystem.listFileInfo(URI.create("gs://" + bucket + "/dir/"));

      assertThat(describeListed(listed))
          .containsExactly(
              "gs://" + bucket + "/dir/sub/ PLACEHOLDER_DIRECTORY",
              "gs://" + bucket + "/dir/file.txt OBJECT",
              "gs://" + bucket + "/dir/implicit/ INFERRED_DIRECTORY")
          .inOrder();
    }
  }

  @Test
  void listFileInfo_objectPath_returnsOnlyThatObject() throws IOException {
    String bucket = newBucketName();
    putBlob(bucket, "dir/file.txt");
    putBlob(bucket, "dir/other.txt");

    try (FakeGcsFileSystemImpl fakeFileSystem = newFakeFileSystem()) {
      List<GcsFileInfo> listed =
          fakeFileSystem.listFileInfo(URI.create("gs://" + bucket + "/dir/file.txt"));

      assertThat(describeListed(listed)).containsExactly("gs://" + bucket + "/dir/file.txt OBJECT");
    }
  }

  @Test
  void listFileInfo_directoryPathWithoutTrailingSlash_returnsChildren() throws IOException {
    String bucket = newBucketName();
    putBlob(bucket, "dir/file.txt");

    try (FakeGcsFileSystemImpl fakeFileSystem = newFakeFileSystem()) {
      List<GcsFileInfo> listed = fakeFileSystem.listFileInfo(URI.create("gs://" + bucket + "/dir"));

      assertThat(describeListed(listed)).containsExactly("gs://" + bucket + "/dir/file.txt OBJECT");
    }
  }

  @Test
  void listFileInfo_emptyDirectoryWithPlaceholder_returnsEmptyList() throws IOException {
    String bucket = newBucketName();
    putBlob(bucket, "empty/");

    try (FakeGcsFileSystemImpl fakeFileSystem = newFakeFileSystem()) {
      List<GcsFileInfo> listed =
          fakeFileSystem.listFileInfo(URI.create("gs://" + bucket + "/empty/"));

      assertThat(listed).isEmpty();
    }
  }

  @Test
  void listFileInfo_missingDirectory_throwsFileNotFoundException() {
    String bucket = newBucketName();
    putBlob(bucket, "dir/file.txt");

    try (FakeGcsFileSystemImpl fakeFileSystem = newFakeFileSystem()) {
      assertThrows(
          FileNotFoundException.class,
          () -> fakeFileSystem.listFileInfo(URI.create("gs://" + bucket + "/missing/")));
    }
  }

  private static FakeGcsFileSystemImpl newFakeFileSystem() {
    return new FakeGcsFileSystemImpl(GcsFileSystemOptions.builder().build());
  }

  private static String newBucketName() {
    return "fake-fs-list-" + UUID.randomUUID();
  }

  private static void putBlob(String bucket, String name) {
    FakeGcsClientImpl.storage.create(
        BlobInfo.newBuilder(BlobId.of(bucket, name)).build(),
        name.endsWith("/") ? new byte[0] : "data".getBytes(StandardCharsets.UTF_8));
  }

  /** Returns each listed item as its URI followed by its item type. */
  private static List<String> describeListed(List<GcsFileInfo> listed) {
    return listed.stream()
        .map(fileInfo -> fileInfo.getUri() + " " + fileInfo.getItemInfo().getItemType())
        .collect(Collectors.toList());
  }
}
