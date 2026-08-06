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

  @Test
  void build_gcsObject_succeeds() {
    GcsItemId gcsItemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();

    assertThat(gcsItemId.getBucketName()).isEqualTo("test-bucket");
    assertThat(gcsItemId.getObjectName().get()).isEqualTo("test-object");
  }

  @Test
  void build_gcsBucket_succeeds() {
    GcsItemId gcsItemId = GcsItemId.builder().setBucketName("test-bucket").build();

    assertThat(gcsItemId.getBucketName()).isEqualTo("test-bucket");
    assertThat(gcsItemId.getObjectName().isEmpty()).isTrue();
  }

  @Test
  void isGcsObject_itemIdPointsToGcsObject_returnsTrue() {
    GcsItemId gcsItemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();

    assertThat(gcsItemId.isGcsObject()).isTrue();
  }

  @Test
  void isGcsObject_itemIdPointsToDirectory_returnsFalse() {
    GcsItemId gcsItemId = GcsItemId.builder().setBucketName("test-bucket").build();

    assertThat(gcsItemId.isGcsObject()).isFalse();
  }

  @Test
  void root_hasEmptyBucketAndObjectName() {
    GcsItemId root = GcsItemId.ROOT;

    assertThat(root.getBucketName()).isEmpty();
    assertThat(root.getObjectName().isPresent()).isTrue();
    assertThat(root.getObjectName().get()).isEmpty();
  }
}
