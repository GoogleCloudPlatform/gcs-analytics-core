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

import org.junit.jupiter.api.Test;

class BucketPropertiesTest {

  @Test
  void create_singleArg_defaultsRapidToFalse() {
    BucketProperties properties = BucketProperties.create(true);

    assertThat(properties.isHnsEnabled()).isTrue();
    assertThat(properties.isRapid()).isFalse();
  }

  @Test
  void create_twoArgs_setsBothProperties() {
    BucketProperties properties = BucketProperties.create(true, true);

    assertThat(properties.isHnsEnabled()).isTrue();
    assertThat(properties.isRapid()).isTrue();
  }

  @Test
  void builder_defaultsToFalse() {
    BucketProperties properties = BucketProperties.builder().build();

    assertThat(properties.isHnsEnabled()).isFalse();
    assertThat(properties.isRapid()).isFalse();
  }

  @Test
  void toBuilder_updatesProperties() {
    BucketProperties initial = BucketProperties.create(true);

    BucketProperties updated = initial.toBuilder().setRapid(true).build();

    assertThat(updated.isHnsEnabled()).isTrue();
    assertThat(updated.isRapid()).isTrue();
  }
}
