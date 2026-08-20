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

import org.junit.jupiter.api.Test;

class PathTypeTest {

  private static final String BUCKET_NAME = "test-bucket";

  @Test
  void resolve_root_returnsRoot() {
    GcsItemId itemId = GcsItemId.builder().setBucketName("").setObjectName("").build();

    PathType result = PathType.resolve(itemId);

    assertThat(result).isEqualTo(PathType.ROOT);
  }

  @Test
  void resolve_nullItemId_throwsNullPointerException() {
    NullPointerException e = assertThrows(NullPointerException.class, () -> PathType.resolve(null));

    assertThat(e).hasMessageThat().contains("itemId cannot be null");
  }

  @Test
  void resolve_bucket_returnsBucket() {
    GcsItemId itemId = GcsItemId.builder().setBucketName(BUCKET_NAME).setObjectName("").build();

    PathType result = PathType.resolve(itemId);

    assertThat(result).isEqualTo(PathType.BUCKET);
  }

  @Test
  void resolve_directory_returnsDirectory() {
    GcsItemId itemId = GcsItemId.builder().setBucketName(BUCKET_NAME).setObjectName("foo/").build();

    PathType result = PathType.resolve(itemId);

    assertThat(result).isEqualTo(PathType.DIRECTORY);
  }

  @Test
  void resolve_knownExtension_returnsFile() {
    for (String ext : new String[] {".parquet", ".csv", ".json", ".avro", ".orc", ".txt"}) {
      GcsItemId itemId =
          GcsItemId.builder().setBucketName(BUCKET_NAME).setObjectName("foo" + ext).build();

      PathType result = PathType.resolve(itemId);

      assertThat(result).isEqualTo(PathType.FILE);
    }
  }

  @Test
  void resolve_nullBucketName_returnsRoot() {
    GcsItemId itemId = GcsItemId.ROOT;

    PathType result = PathType.resolve(itemId);

    assertThat(result).isEqualTo(PathType.ROOT);
  }

  @Test
  void resolve_unknownExtension_returnsUnknown() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(BUCKET_NAME).setObjectName("foo.bar").build();

    PathType result = PathType.resolve(itemId);

    assertThat(result).isEqualTo(PathType.UNKNOWN);
  }

  @Test
  void resolve_noExtension_returnsUnknown() {
    GcsItemId itemId = GcsItemId.builder().setBucketName(BUCKET_NAME).setObjectName("foo").build();

    PathType result = PathType.resolve(itemId);

    assertThat(result).isEqualTo(PathType.UNKNOWN);
  }
}
