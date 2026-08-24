/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class UriUtilTest {

  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_OBJECT = "test-object";
  private static final String TEST_PATH = "dir/subdir";

  @ParameterizedTest
  @ValueSource(strings = {"gs://" + TEST_BUCKET, "gs://" + TEST_BUCKET + "/"})
  void getItemIdFromString_gcsBucket_succeeds(String gcsBucketName) {
    GcsItemId itemId = UriUtil.getItemIdFromString(gcsBucketName);

    assertThat(itemId.getBucketName()).isEqualTo(TEST_BUCKET);
    assertThat(itemId.getObjectName()).isEmpty();
  }

  @Test
  void getItemIdFromString_gcsObject_succeeds() {
    String gcsObjectName = "gs://" + TEST_BUCKET + "/" + TEST_OBJECT;

    GcsItemId itemId = UriUtil.getItemIdFromString(gcsObjectName);

    assertThat(itemId.getBucketName()).isEqualTo(TEST_BUCKET);
    assertThat(itemId.getObjectName()).hasValue(TEST_OBJECT);
  }

  @ParameterizedTest
  @ValueSource(strings = {"gs:/", "gs://"})
  void getItemIdFromString_rootPath_returnsRootItemId(String rootPath) {
    GcsItemId itemId = UriUtil.getItemIdFromString(rootPath);

    assertThat(itemId).isEqualTo(GcsItemId.ROOT);
  }

  @Test
  void getItemIdFromString_nullPath_throwsIllegalArgumentException() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> UriUtil.getItemIdFromString(null));

    assertThat(e).hasMessageThat().isEqualTo("path should not be null");
  }

  @ParameterizedTest
  @ValueSource(strings = {"http://test-bucket/test-object", "gs:///", "gs:/test-bucket", ""})
  void getItemIdFromString_invalidPath_throwsIllegalArgumentException(String invalidPath) {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> UriUtil.getItemIdFromString(invalidPath));

    assertThat(e).hasMessageThat().isEqualTo("Invalid GCS path: " + invalidPath);
  }

  @Test
  void getItemIdFromString_consecutiveSlashes_throwsIllegalArgumentException() {
    String consecutiveSlashPath = "gs://test-bucket/dir//test-object";

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> UriUtil.getItemIdFromString(consecutiveSlashPath));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo("GCS path must not have consecutive '/' characters: " + consecutiveSlashPath);
  }

  @Test
  void removeTrailingSlash_withTrailingSlash_removesSlash() {
    String result = UriUtil.removeTrailingSlash(TEST_PATH + "/");

    assertThat(result).isEqualTo(TEST_PATH);
  }

  @Test
  void removeTrailingSlash_withoutTrailingSlash_returnsSame() {
    String result = UriUtil.removeTrailingSlash(TEST_PATH);

    assertThat(result).isEqualTo(TEST_PATH);
  }

  @Test
  void removeTrailingSlash_nullPath_returnsNull() {
    String result = UriUtil.removeTrailingSlash(null);

    assertThat(result).isNull();
  }

  @Test
  void ensureTrailingSlash_withoutTrailingSlash_appendsSlash() {
    String result = UriUtil.ensureTrailingSlash(TEST_PATH);

    assertThat(result).isEqualTo(TEST_PATH + "/");
  }

  @Test
  void ensureTrailingSlash_withTrailingSlash_returnsSame() {
    String result = UriUtil.ensureTrailingSlash(TEST_PATH + "/");

    assertThat(result).isEqualTo(TEST_PATH + "/");
  }

  @Test
  void ensureTrailingSlash_nullPath_returnsNull() {
    String result = UriUtil.ensureTrailingSlash(null);

    assertThat(result).isNull();
  }
}
