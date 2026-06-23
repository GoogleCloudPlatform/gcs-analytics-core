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

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GcsCacheOptionsTest {

  @Test
  void build_defaultValues_succeeds() {
    GcsCacheOptions options = GcsCacheOptions.builder().build();

    assertThat(options.isFooterCacheEnabled()).isTrue();
    assertThat(options.getFooterCacheMaxEntries()).isEqualTo(100);
    assertThat(options.getBucketPropertiesCacheMaxEntryAgeMinutes()).isEqualTo(10);
  }

  @Test
  void build_disabledFooterCacheNonPositiveEntries_succeeds() {
    GcsCacheOptions options =
        GcsCacheOptions.builder().setFooterCacheEnabled(false).setFooterCacheMaxEntries(0).build();

    assertThat(options.isFooterCacheEnabled()).isFalse();
    assertThat(options.getFooterCacheMaxEntries()).isEqualTo(0);
  }

  @Test
  void build_enabledFooterCacheZeroEntries_throwsException() {
    GcsCacheOptions.Builder builder =
        GcsCacheOptions.builder().setFooterCacheEnabled(true).setFooterCacheMaxEntries(0);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_enabledFooterCacheNegativeEntries_throwsException() {
    GcsCacheOptions.Builder builder =
        GcsCacheOptions.builder().setFooterCacheEnabled(true).setFooterCacheMaxEntries(-1);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_invalidBucketPropertiesMaxEntryAgeMinutes_throwsException() {
    GcsCacheOptions.Builder builder =
        GcsCacheOptions.builder().setBucketPropertiesCacheMaxEntryAgeMinutes(-1);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_BucketPropertiesMaxEntryAgeMinutesZero_succeeds() {
    GcsCacheOptions options =
        GcsCacheOptions.builder().setBucketPropertiesCacheMaxEntryAgeMinutes(0).build();

    assertThat(options.getBucketPropertiesCacheMaxEntryAgeMinutes()).isEqualTo(0);
  }

  @Test
  void createFromOptions_withAllOptions_succeeds() {
    Map<String, String> map = new HashMap<>();
    map.put("gcs." + GcsCacheOptions.FOOTER_CACHE_ENABLED_KEY, "false");
    map.put("gcs." + GcsCacheOptions.FOOTER_CACHE_MAX_ENTRIES_KEY, "50");
    map.put("gcs." + GcsCacheOptions.BUCKET_PROPERTIES_CACHE_MAX_ENTRY_AGE_MINUTES_KEY, "20");

    GcsCacheOptions options = GcsCacheOptions.createFromOptions(map, "gcs.");

    assertThat(options.isFooterCacheEnabled()).isFalse();
    assertThat(options.getFooterCacheMaxEntries()).isEqualTo(50);
    assertThat(options.getBucketPropertiesCacheMaxEntryAgeMinutes()).isEqualTo(20);
  }

  @Test
  void createFromOptions_withEmptyOptions_returnsDefaults() {
    Map<String, String> map = new HashMap<>();

    GcsCacheOptions options = GcsCacheOptions.createFromOptions(map, "gcs.");

    assertThat(options.isFooterCacheEnabled()).isTrue();
    assertThat(options.getFooterCacheMaxEntries()).isEqualTo(100);
    assertThat(options.getBucketPropertiesCacheMaxEntryAgeMinutes()).isEqualTo(10);
  }

  @Test
  void createFromOptions_malformedInteger_throwsNumberFormatException() {
    Map<String, String> map = new HashMap<>();
    map.put("gcs." + GcsCacheOptions.FOOTER_CACHE_MAX_ENTRIES_KEY, "not-a-number");

    assertThrows(NumberFormatException.class, () -> GcsCacheOptions.createFromOptions(map, "gcs."));
  }
}
