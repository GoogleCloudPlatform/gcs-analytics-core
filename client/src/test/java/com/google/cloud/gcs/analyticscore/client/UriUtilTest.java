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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
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
  void getItemIdFromUri_validUri_succeeds() {
    URI uri = URI.create("gs://" + TEST_BUCKET + "/" + TEST_OBJECT);

    GcsItemId itemId = UriUtil.getItemIdFromUri(uri);

    assertThat(itemId.getBucketName()).isEqualTo(TEST_BUCKET);
    assertThat(itemId.getObjectName()).hasValue(TEST_OBJECT);
  }

  @Test
  void getItemIdFromUri_nullUri_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> UriUtil.getItemIdFromUri(null));
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
  void toDirectoryPath_withoutTrailingSlash_appendsSlash() {
    String result = UriUtil.toDirectoryPath(TEST_PATH);

    assertThat(result).isEqualTo(TEST_PATH + "/");
  }

  @Test
  void toDirectoryPath_withTrailingSlash_returnsOriginal() {
    String result = UriUtil.toDirectoryPath(TEST_PATH + "/");

    assertThat(result).isEqualTo(TEST_PATH + "/");
  }

  @Test
  void toDirectoryPath_nullPath_returnsNull() {
    String result = UriUtil.toDirectoryPath(null);

    assertThat(result).isNull();
  }

  @Test
  void toDirectoryPath_emptyPath_returnsEmpty() {
    String result = UriUtil.toDirectoryPath("");

    assertThat(result).isEmpty();
  }

  @Test
  void getStringPath_gcsObject_returnsObjectPath() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();
    String result = UriUtil.getStringPath(itemId);
    assertThat(result).isEqualTo("gs://" + TEST_BUCKET + "/" + TEST_OBJECT);
  }

  @Test
  void getStringPath_bucket_returnsBucketPath() {
    GcsItemId itemId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    String result = UriUtil.getStringPath(itemId);
    assertThat(result).isEqualTo("gs://" + TEST_BUCKET);
  }

  @Test
  void getStringPath_gcsObjectWithGeneration_omitsGeneration() {
    // The result is passed to URI.create by GcsFileInfo.createNotFound, where a '#generation'
    // suffix would be parsed as a fragment rather than part of the object name.
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_OBJECT)
            .setContentGeneration(1234L)
            .build();

    String result = UriUtil.getStringPath(itemId);

    assertThat(result).isEqualTo("gs://" + TEST_BUCKET + "/" + TEST_OBJECT);
  }

  @Test
  void getStringPath_root_returnsRootPath() {
    String result = UriUtil.getStringPath(GcsItemId.ROOT);
    assertThat(result).isEqualTo("gs:/");
  }

  @Test
  void getStringPath_nullItemId_throwsIllegalArgumentException() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> UriUtil.getStringPath(null));
    assertThat(e).hasMessageThat().isEqualTo("itemId should not be null");
  }

  @ParameterizedTest
  @ValueSource(strings = {TEST_OBJECT, TEST_OBJECT + "/", TEST_PATH, TEST_PATH + "/"})
  void validateDirectoryItemId_validObjectName_doesNotThrow(String objectName) {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(objectName).build();

    assertDoesNotThrow(() -> UriUtil.validateDirectoryItemId(itemId));
  }

  @ParameterizedTest
  @ValueSource(strings = {"/", "//", "/dir/subdir", "dir//subdir", "dir/subdir//"})
  void validateDirectoryItemId_leadingOrConsecutiveSlashes_throwsIllegalArgumentException(
      String objectName) {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(objectName).build();

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> UriUtil.validateDirectoryItemId(itemId));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Object name cannot start with '/' or contain consecutive '/': " + itemId);
  }

  @Test
  void validateDirectoryItemId_nonObjectItemId_throwsIllegalArgumentException() {
    GcsItemId bucketItemId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    GcsItemId emptyBucketItemId =
        GcsItemId.builder().setBucketName("").setObjectName(TEST_PATH).build();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> UriUtil.validateDirectoryItemId(bucketItemId));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Expected a directory itemId within a bucket but got: " + bucketItemId);
    assertThrows(
        IllegalArgumentException.class, () -> UriUtil.validateDirectoryItemId(GcsItemId.ROOT));
    assertThrows(
        IllegalArgumentException.class, () -> UriUtil.validateDirectoryItemId(emptyBucketItemId));
  }

  @Test
  void validateDirectoryItemId_nullItemId_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> UriUtil.validateDirectoryItemId(null));
  }
}
