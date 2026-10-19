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

import com.google.api.gax.retrying.RetrySettings;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class GcsRetryOptionsTest {

  private static final String PREFIX = "gcs.";

  private static final RetrySettings BASE =
      RetrySettings.newBuilder()
          .setMaxAttempts(6)
          .setInitialRetryDelayDuration(Duration.ofSeconds(1))
          .setRetryDelayMultiplier(2.0)
          .setMaxRetryDelayDuration(Duration.ofSeconds(32))
          .setTotalTimeoutDuration(Duration.ofSeconds(50))
          .build();

  @Test
  void builder_noValues_hasNoOverrides() {
    GcsRetryOptions options = GcsRetryOptions.builder().build();

    assertThat(options.hasOverrides()).isFalse();
  }

  @Test
  void builder_maxAttemptsSet_hasOverrides() {
    GcsRetryOptions options = GcsRetryOptions.builder().setMaxAttempts(3).build();

    assertThat(options.hasOverrides()).isTrue();
  }

  @Test
  void createFromOptions_noKeys_hasNoOverrides() {
    GcsRetryOptions options = GcsRetryOptions.createFromOptions(ImmutableMap.of(), PREFIX);

    assertThat(options.hasOverrides()).isFalse();
  }

  @Test
  void createFromOptions_blankValue_isIgnored() {
    GcsRetryOptions options =
        GcsRetryOptions.createFromOptions(
            ImmutableMap.of(PREFIX + GcsRetryOptions.MAX_ATTEMPTS_KEY, "  "), PREFIX);

    assertThat(options.hasOverrides()).isFalse();
  }

  @Test
  void createFromOptions_allKeysSet_parsesAllValues() {
    Map<String, String> raw =
        ImmutableMap.of(
            PREFIX + GcsRetryOptions.MAX_ATTEMPTS_KEY, "11",
            PREFIX + GcsRetryOptions.INITIAL_DELAY_KEY, " 200 ",
            PREFIX + GcsRetryOptions.DELAY_MULTIPLIER_KEY, "1.5",
            PREFIX + GcsRetryOptions.MAX_DELAY_KEY, "10000",
            PREFIX + GcsRetryOptions.TOTAL_TIMEOUT_KEY, "60000");

    GcsRetryOptions options = GcsRetryOptions.createFromOptions(raw, PREFIX);

    assertThat(options)
        .isEqualTo(
            GcsRetryOptions.builder()
                .setMaxAttempts(11)
                .setInitialRetryDelay(Duration.ofMillis(200))
                .setRetryDelayMultiplier(1.5)
                .setMaxRetryDelay(Duration.ofMillis(10000))
                .setTotalTimeout(Duration.ofMillis(60000))
                .build());
  }

  @Test
  void createFromOptions_unprefixedKey_isIgnored() {
    GcsRetryOptions options =
        GcsRetryOptions.createFromOptions(
            ImmutableMap.of(GcsRetryOptions.MAX_ATTEMPTS_KEY, "3"), PREFIX);

    assertThat(options.hasOverrides()).isFalse();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        GcsRetryOptions.MAX_ATTEMPTS_KEY,
        GcsRetryOptions.INITIAL_DELAY_KEY,
        GcsRetryOptions.DELAY_MULTIPLIER_KEY,
        GcsRetryOptions.MAX_DELAY_KEY,
        GcsRetryOptions.TOTAL_TIMEOUT_KEY
      })
  void createFromOptions_nonNumericValue_throwsIllegalArgumentException(String key) {
    Map<String, String> raw = ImmutableMap.of(PREFIX + key, "abc");

    assertThrows(
        IllegalArgumentException.class, () -> GcsRetryOptions.createFromOptions(raw, PREFIX));
  }

  @Test
  void createFromOptions_overflowingMaxAttempts_throwsIllegalArgumentException() {
    Map<String, String> raw =
        ImmutableMap.of(PREFIX + GcsRetryOptions.MAX_ATTEMPTS_KEY, "3000000000");

    assertThrows(
        IllegalArgumentException.class, () -> GcsRetryOptions.createFromOptions(raw, PREFIX));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1})
  void build_maxAttemptsBelowOne_throwsIllegalArgumentException(int attempts) {
    GcsRetryOptions.Builder builder = GcsRetryOptions.builder().setMaxAttempts(attempts);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_maxAttemptsOne_isAllowed() {
    GcsRetryOptions options = GcsRetryOptions.builder().setMaxAttempts(1).build();

    assertThat(options.getMaxAttempts()).hasValue(1);
  }

  @Test
  void build_multiplierBelowOne_throwsIllegalArgumentException() {
    GcsRetryOptions.Builder builder = GcsRetryOptions.builder().setRetryDelayMultiplier(0.5);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @ParameterizedTest
  @ValueSource(doubles = {Double.POSITIVE_INFINITY, Double.NaN})
  void build_nonFiniteMultiplier_throwsIllegalArgumentException(double multiplier) {
    GcsRetryOptions.Builder builder = GcsRetryOptions.builder().setRetryDelayMultiplier(multiplier);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void createFromOptions_infinityMultiplier_throwsIllegalArgumentException() {
    Map<String, String> raw =
        ImmutableMap.of(PREFIX + GcsRetryOptions.DELAY_MULTIPLIER_KEY, "Infinity");

    assertThrows(
        IllegalArgumentException.class, () -> GcsRetryOptions.createFromOptions(raw, PREFIX));
  }

  @Test
  void createFromOptions_nonNumericMaxAttempts_reportsPrefixedKey() {
    Map<String, String> raw = ImmutableMap.of(PREFIX + GcsRetryOptions.MAX_ATTEMPTS_KEY, "abc");

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> GcsRetryOptions.createFromOptions(raw, PREFIX));

    assertThat(e).hasMessageThat().contains(PREFIX + GcsRetryOptions.MAX_ATTEMPTS_KEY + "=abc");
  }

  @Test
  void createFromOptions_invalidMaxAttempts_reportsPrefixedKey() {
    Map<String, String> raw = ImmutableMap.of(PREFIX + GcsRetryOptions.MAX_ATTEMPTS_KEY, "0");

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> GcsRetryOptions.createFromOptions(raw, PREFIX));

    assertThat(e).hasMessageThat().contains(PREFIX + GcsRetryOptions.MAX_ATTEMPTS_KEY + "=0");
  }

  @Test
  void build_invalidMaxAttempts_reportsUnprefixedKey() {
    GcsRetryOptions.Builder builder = GcsRetryOptions.builder().setMaxAttempts(0);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e).hasMessageThat().startsWith(GcsRetryOptions.MAX_ATTEMPTS_KEY + "=0");
  }

  @Test
  void build_zeroInitialDelay_throwsIllegalArgumentException() {
    GcsRetryOptions.Builder builder = GcsRetryOptions.builder().setInitialRetryDelay(Duration.ZERO);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_negativeMaxDelay_throwsIllegalArgumentException() {
    GcsRetryOptions.Builder builder =
        GcsRetryOptions.builder().setMaxRetryDelay(Duration.ofMillis(-1));

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_zeroTotalTimeout_throwsIllegalArgumentException() {
    GcsRetryOptions.Builder builder = GcsRetryOptions.builder().setTotalTimeout(Duration.ZERO);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_initialDelayGreaterThanMaxDelay_throwsIllegalArgumentException() {
    GcsRetryOptions.Builder builder =
        GcsRetryOptions.builder()
            .setInitialRetryDelay(Duration.ofSeconds(5))
            .setMaxRetryDelay(Duration.ofSeconds(1));

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void applyTo_noOverrides_returnsEquivalentSettings() {
    GcsRetryOptions options = GcsRetryOptions.builder().build();

    assertThat(options.applyTo(BASE)).isEqualTo(BASE);
  }

  @Test
  void applyTo_allOverrides_replacesAllFields() {
    GcsRetryOptions options =
        GcsRetryOptions.builder()
            .setMaxAttempts(11)
            .setInitialRetryDelay(Duration.ofMillis(200))
            .setRetryDelayMultiplier(1.5)
            .setMaxRetryDelay(Duration.ofSeconds(10))
            .setTotalTimeout(Duration.ofMinutes(1))
            .build();

    RetrySettings result = options.applyTo(BASE);

    assertThat(result)
        .isEqualTo(
            BASE.toBuilder()
                .setMaxAttempts(11)
                .setInitialRetryDelayDuration(Duration.ofMillis(200))
                .setRetryDelayMultiplier(1.5)
                .setMaxRetryDelayDuration(Duration.ofSeconds(10))
                .setTotalTimeoutDuration(Duration.ofMinutes(1))
                .build());
  }

  @Test
  void applyTo_partialOverrides_preservesUnsetBaseFields() {
    GcsRetryOptions options = GcsRetryOptions.builder().setMaxAttempts(2).build();

    RetrySettings result = options.applyTo(BASE);

    assertThat(result).isEqualTo(BASE.toBuilder().setMaxAttempts(2).build());
  }
}
