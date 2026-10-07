/*
 * Copyright 2026 Google LLC
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

import java.time.Duration;
import org.junit.jupiter.api.Test;

class GcsCreateBucketOptionsTest {

  @Test
  void defaultOptions_hasExpectedDefaults() {
    GcsCreateBucketOptions options = GcsCreateBucketOptions.DEFAULT;

    assertThat(options.getLocation()).isEmpty();
    assertThat(options.getStorageClass()).isEmpty();
    assertThat(options.isHierarchicalNamespaceEnabled()).isFalse();
    assertThat(options.getTtl()).isEmpty();
    assertThat(options.getZonalPlacement()).isEmpty();
  }

  @Test
  void builder_setsAllFields() {
    GcsCreateBucketOptions options =
        GcsCreateBucketOptions.builder()
            .setLocation("us-central1")
            .setStorageClass("RAPID")
            .setHierarchicalNamespaceEnabled(true)
            .setTtl(Duration.ofDays(7))
            .setZonalPlacement("us-central1-a")
            .build();

    assertThat(options.getLocation()).hasValue("us-central1");
    assertThat(options.getStorageClass()).hasValue("RAPID");
    assertThat(options.isHierarchicalNamespaceEnabled()).isTrue();
    assertThat(options.getTtl()).hasValue(Duration.ofDays(7));
    assertThat(options.getZonalPlacement()).hasValue("us-central1-a");
  }

  @Test
  void toBuilder_preservesAndOverridesFields() {
    GcsCreateBucketOptions original =
        GcsCreateBucketOptions.builder()
            .setLocation("us-east1")
            .setHierarchicalNamespaceEnabled(true)
            .build();

    GcsCreateBucketOptions modified = original.toBuilder().setStorageClass("STANDARD").build();

    assertThat(modified.getLocation()).hasValue("us-east1");
    assertThat(modified.isHierarchicalNamespaceEnabled()).isTrue();
    assertThat(modified.getStorageClass()).hasValue("STANDARD");
  }
}
