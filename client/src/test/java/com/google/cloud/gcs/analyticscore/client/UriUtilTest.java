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

public final class UriUtilTest {

  @Test
  public void getItemIdFromString_gcsBucket_succeeds() {
    String gcsBucketName = "gs://test-bucket";

    GcsItemId itemId = UriUtil.getItemIdFromString(gcsBucketName);

    assertThat(itemId.getBucketName()).isEqualTo("test-bucket");
    assertThat(itemId.getObjectName().isEmpty()).isTrue();
  }

  @Test
  public void getItemIdFromString_gcsObject_succeeds() {
    String gcsObjectName = "gs://test-bucket/test-object";

    GcsItemId itemId = UriUtil.getItemIdFromString(gcsObjectName);

    assertThat(itemId.getBucketName()).isEqualTo("test-bucket");
    assertThat(itemId.getObjectName().isPresent()).isTrue();
    assertThat(itemId.getObjectName().get()).isEqualTo("test-object");
  }

  @Test
  public void getItemIdFromString_gsOnly_throwsIllegalArgumentException() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> UriUtil.getItemIdFromString("gs://"));

    assertThat(e).hasMessageThat().isEqualTo("GCS path must include a bucket name: gs://");
  }

  @Test
  public void getItemIdFromString_nullPath_throwsIllegalArgumentException() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> UriUtil.getItemIdFromString(null));

    assertThat(e).hasMessageThat().isEqualTo("path should not be null");
  }

  @Test
  public void getItemIdFromString_invalidPath_throwsIllegalArgumentException() {
    String invalidPath = "http://test-bucket/test-pbject";
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> UriUtil.getItemIdFromString(invalidPath));

    assertThat(e).hasMessageThat().isEqualTo("Invalid GCS path: " + invalidPath);
  }

  @Test
  public void removeTrailingSlash_withTrailingSlash_removesSlash() {
    String path = "dir/subdir/";

    String result = UriUtil.removeTrailingSlash(path);

    assertThat(result).isEqualTo("dir/subdir");
  }

  @Test
  public void removeTrailingSlash_withoutTrailingSlash_returnsSame() {
    String path = "dir/subdir";

    String result = UriUtil.removeTrailingSlash(path);

    assertThat(result).isEqualTo("dir/subdir");
  }

  @Test
  public void removeTrailingSlash_nullPath_returnsNull() {
    String result = UriUtil.removeTrailingSlash(null);

    assertThat(result).isNull();
  }

  @Test
  public void ensureTrailingSlash_withoutTrailingSlash_appendsSlash() {
    String path = "dir/subdir";

    String result = UriUtil.ensureTrailingSlash(path);

    assertThat(result).isEqualTo("dir/subdir/");
  }

  @Test
  public void ensureTrailingSlash_withTrailingSlash_returnsSame() {
    String path = "dir/subdir/";

    String result = UriUtil.ensureTrailingSlash(path);

    assertThat(result).isEqualTo("dir/subdir/");
  }

  @Test
  public void ensureTrailingSlash_nullPath_returnsNull() {
    String result = UriUtil.ensureTrailingSlash(null);

    assertThat(result).isNull();
  }
}
