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

import org.junit.jupiter.api.Test;

class GcsItemIdTest {

  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_OBJECT = "test-object";

  @Test
  void build_gcsObject_succeeds() {
    GcsItemId gcsItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();

    assertThat(gcsItemId.getBucketName()).isEqualTo(TEST_BUCKET);
    assertThat(gcsItemId.getObjectName()).hasValue(TEST_OBJECT);
  }

  @Test
  void build_gcsBucket_succeeds() {
    GcsItemId gcsItemId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();

    assertThat(gcsItemId.getBucketName()).isEqualTo(TEST_BUCKET);
    assertThat(gcsItemId.getObjectName()).isEmpty();
  }

  @Test
  void isGcsObject_itemIdPointsToGcsObject_returnsTrue() {
    GcsItemId gcsItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();

    assertThat(gcsItemId.isGcsObject()).isTrue();
  }

  @Test
  void isGcsObject_itemIdPointsToBucket_returnsFalse() {
    GcsItemId gcsItemId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();

    assertThat(gcsItemId.isGcsObject()).isFalse();
  }

  @Test
  void isGcsObject_emptyBucketWithObject_returnsFalse() {
    GcsItemId itemId = GcsItemId.builder().setBucketName("").setObjectName(TEST_OBJECT).build();

    assertThat(itemId.isGcsObject()).isFalse();
  }

  @Test
  void isBucket_withBucketOnly_returnsTrue() {
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();

    assertThat(bucketId.isBucket()).isTrue();
    assertThat(bucketId.isGcsObject()).isFalse();
  }

  @Test
  void isBucket_withObject_returnsFalse() {
    GcsItemId objectId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();

    assertThat(objectId.isBucket()).isFalse();
  }

  @Test
  void root_hasEmptyBucketAndObjectName() {
    GcsItemId root = GcsItemId.ROOT;

    assertThat(root.getBucketName()).isEmpty();
    assertThat(root.getObjectName()).hasValue("");
    assertThat(root.isBucket()).isFalse();
    assertThat(root.isGcsObject()).isFalse();
  }
}
