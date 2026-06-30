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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class GcsCacheOptionsTest {

  private static final long KB = 1024L;
  private static final long MB = 1024L * KB;

  @Test
  void build_defaultValues_succeeds() {
    GcsCacheOptions options = GcsCacheOptions.builder().build();

    assertThat(options.isFooterCacheEnabled()).isFalse();
    assertThat(options.getFooterCacheMaxSizeBytes()).isEqualTo(100 * MB);
    assertThat(options.isSmallObjectCacheEnabled()).isFalse();
    assertThat(options.getSmallObjectCacheMaxSizeBytes()).isEqualTo(200 * MB);
  }

  @Test
  void build_disabledCacheNonPositiveSizeBytes_succeeds() {
    GcsCacheOptions options =
        GcsCacheOptions.builder()
            .setFooterCacheEnabled(false)
            .setFooterCacheMaxSizeBytes(0)
            .setSmallObjectCacheEnabled(false)
            .setSmallObjectCacheMaxSizeBytes(0)
            .build();

    assertThat(options.isFooterCacheEnabled()).isFalse();
    assertThat(options.getFooterCacheMaxSizeBytes()).isEqualTo(0);
    assertThat(options.isSmallObjectCacheEnabled()).isFalse();
    assertThat(options.getSmallObjectCacheMaxSizeBytes()).isEqualTo(0);
  }

  @Test
  void build_enabledCacheZeroSizeBytes_throwsException() {
    GcsCacheOptions.Builder builder =
        GcsCacheOptions.builder().setFooterCacheEnabled(true).setFooterCacheMaxSizeBytes(0);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_enabledCacheNegativeSizeBytes_throwsException() {
    GcsCacheOptions.Builder builder =
        GcsCacheOptions.builder().setFooterCacheEnabled(true).setFooterCacheMaxSizeBytes(-1);

    assertThrows(IllegalArgumentException.class, builder::build);
  }
}
