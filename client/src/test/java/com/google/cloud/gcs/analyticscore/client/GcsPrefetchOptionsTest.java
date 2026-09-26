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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GcsPrefetchOptionsTest {

  private static final String PREFIX = "gcs.";

  @Test
  void build_defaultValues_returnsDisabled() {
    GcsPrefetchOptions options = GcsPrefetchOptions.builder().build();

    assertThat(options.isEnabled()).isFalse();
  }

  @Test
  void isEnabled_setEnabledTrue_returnsTrue() {
    GcsPrefetchOptions options = GcsPrefetchOptions.builder().setEnabled(true).build();

    assertThat(options.isEnabled()).isTrue();
  }

  @Test
  void toBuilder_enabledOptions_roundTripsInstance() {
    GcsPrefetchOptions options = GcsPrefetchOptions.builder().setEnabled(true).build();

    GcsPrefetchOptions roundTripped = options.toBuilder().build();

    assertThat(roundTripped).isEqualTo(options);
  }

  @Test
  void build_defaultValues_returnsDefaultBufferCacheMaxSizeBytes() {
    GcsPrefetchOptions options = GcsPrefetchOptions.builder().build();

    assertThat(options.getBufferCacheMaxSizeBytes()).isEqualTo(2L * 1024 * 1024 * 1024);
  }

  @Test
  void build_defaultValues_returnsDefaultBufferCacheTtlSeconds() {
    GcsPrefetchOptions options = GcsPrefetchOptions.builder().build();

    assertThat(options.getBufferCacheTtlSeconds()).isEqualTo(60);
  }

  @Test
  void build_enabledWithZeroBufferCacheMaxSizeBytes_throwsIllegalArgumentException() {
    GcsPrefetchOptions.Builder builder =
        GcsPrefetchOptions.builder().setEnabled(true).setBufferCacheMaxSizeBytes(0);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_enabledWithNegativeBufferCacheTtlSeconds_throwsIllegalArgumentException() {
    GcsPrefetchOptions.Builder builder =
        GcsPrefetchOptions.builder().setEnabled(true).setBufferCacheTtlSeconds(-1);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_disabledWithZeroBufferCacheMaxSizeBytes_throwsIllegalArgumentException() {
    GcsPrefetchOptions.Builder builder =
        GcsPrefetchOptions.builder().setEnabled(false).setBufferCacheMaxSizeBytes(0);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_disabledWithZeroBufferCacheTtlSeconds_throwsIllegalArgumentException() {
    GcsPrefetchOptions.Builder builder =
        GcsPrefetchOptions.builder().setEnabled(false).setBufferCacheTtlSeconds(0);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void createFromOptions_withEmptyOptions_returnsDefaults() {
    Map<String, String> map = ImmutableMap.of();

    GcsPrefetchOptions options = GcsPrefetchOptions.createFromOptions(map, PREFIX);

    assertThat(options).isEqualTo(GcsPrefetchOptions.builder().build());
  }

  @Test
  void createFromOptions_mapWithEnabledTrue_parsesEnabled() {
    Map<String, String> map = ImmutableMap.of(PREFIX + GcsPrefetchOptions.ENABLED_KEY, "true");

    GcsPrefetchOptions options = GcsPrefetchOptions.createFromOptions(map, PREFIX);

    assertThat(options.isEnabled()).isTrue();
  }

  @Test
  void createFromOptions_keysResolvedUnderNonEmptyPrefix_parsesEnabled() {
    Map<String, String> map = ImmutableMap.of("custom." + GcsPrefetchOptions.ENABLED_KEY, "true");

    GcsPrefetchOptions options = GcsPrefetchOptions.createFromOptions(map, "custom.");

    assertThat(options.isEnabled()).isTrue();
  }

  @Test
  void createFromOptions_keyUnderDifferentPrefix_isIgnored() {
    Map<String, String> map = ImmutableMap.of("other." + GcsPrefetchOptions.ENABLED_KEY, "true");

    GcsPrefetchOptions options = GcsPrefetchOptions.createFromOptions(map, PREFIX);

    assertThat(options.isEnabled()).isFalse();
  }

  @Test
  void createFromOptions_mapWithBufferCacheMaxSizeBytes_parsesMaxSizeBytes() {
    Map<String, String> map =
        ImmutableMap.of(PREFIX + GcsPrefetchOptions.BUFFER_CACHE_MAX_SIZE_BYTES_KEY, "1024");

    GcsPrefetchOptions options = GcsPrefetchOptions.createFromOptions(map, PREFIX);

    assertThat(options.getBufferCacheMaxSizeBytes()).isEqualTo(1024);
  }

  @Test
  void createFromOptions_mapWithBufferCacheTtlSeconds_parsesTtlSeconds() {
    Map<String, String> map =
        ImmutableMap.of(PREFIX + GcsPrefetchOptions.BUFFER_CACHE_TTL_SECONDS_KEY, "30");

    GcsPrefetchOptions options = GcsPrefetchOptions.createFromOptions(map, PREFIX);

    assertThat(options.getBufferCacheTtlSeconds()).isEqualTo(30);
  }
}
