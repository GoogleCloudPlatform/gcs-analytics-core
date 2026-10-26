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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auto.value.AutoValue;
import com.google.cloud.gcs.analyticscore.common.ConfigurationUtil;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * Retry configuration applied to the underlying Cloud Storage client.
 *
 * <p>Every field is optional. Fields that are not set keep the defaults of the {@code
 * google-cloud-storage} library, so an instance without overrides leaves retry behavior unchanged.
 * These options only tune the retry budget (attempts, backoff and total time); which errors are
 * retried is still decided by the library's idempotency-aware retry strategy.
 *
 * <p>Note that {@link #getMaxAttempts()} counts the initial attempt, unlike hadoop-connectors'
 * {@code fs.gs.http.max.retry}, which counts retries only. A value of {@code 1} disables retries.
 */
@AutoValue
public abstract class GcsRetryOptions {

  static final String MAX_ATTEMPTS_KEY = "analytics-core.retry.max-attempts";
  static final String INITIAL_DELAY_KEY = "analytics-core.retry.initial-delay-ms";
  static final String DELAY_MULTIPLIER_KEY = "analytics-core.retry.delay-multiplier";
  static final String MAX_DELAY_KEY = "analytics-core.retry.max-delay-ms";
  static final String TOTAL_TIMEOUT_KEY = "analytics-core.retry.total-timeout-ms";

  /** Maximum number of attempts, including the initial one. */
  public abstract Optional<Integer> getMaxAttempts();

  /** Delay before the first retry. */
  public abstract Optional<Duration> getInitialRetryDelay();

  /** Factor by which the retry delay grows after each attempt. */
  public abstract Optional<Double> getRetryDelayMultiplier();

  /** Upper bound on the delay between two attempts. */
  public abstract Optional<Duration> getMaxRetryDelay();

  /** Total time budget across all attempts of a single operation. */
  public abstract Optional<Duration> getTotalTimeout();

  /** Returns whether at least one retry setting overrides the library default. */
  public boolean hasOverrides() {
    return getMaxAttempts().isPresent()
        || getInitialRetryDelay().isPresent()
        || getRetryDelayMultiplier().isPresent()
        || getMaxRetryDelay().isPresent()
        || getTotalTimeout().isPresent();
  }

  /**
   * Applies the configured overrides on top of {@code base}, leaving unset fields untouched.
   *
   * <p>When only one of the initial and maximum retry delays is overridden, the other is inherited
   * from {@code base}. The effective pair is validated here so that a single-sided override that
   * conflicts with the inherited value fails with a message naming the configuration keys, rather
   * than with an opaque error from the retry library.
   *
   * @param base The retry settings to start from, typically the library defaults.
   * @return A new {@link RetrySettings} with the overrides applied.
   * @throws IllegalArgumentException If the effective initial delay exceeds the effective maximum
   *     delay.
   */
  public RetrySettings applyTo(RetrySettings base) {
    checkNotNull(base, "base cannot be null");
    checkEffectiveDelayOrdering(base);
    RetrySettings.Builder builder = base.toBuilder();
    getMaxAttempts().ifPresent(builder::setMaxAttempts);
    getInitialRetryDelay().ifPresent(builder::setInitialRetryDelayDuration);
    getRetryDelayMultiplier().ifPresent(builder::setRetryDelayMultiplier);
    getMaxRetryDelay().ifPresent(builder::setMaxRetryDelayDuration);
    getTotalTimeout().ifPresent(builder::setTotalTimeoutDuration);
    return builder.build();
  }

  private void checkEffectiveDelayOrdering(RetrySettings base) {
    Duration initial = getInitialRetryDelay().orElse(base.getInitialRetryDelayDuration());
    Duration max = getMaxRetryDelay().orElse(base.getMaxRetryDelayDuration());
    checkArgument(
        initial.compareTo(max) <= 0,
        "Effective %s=%s must not exceed effective %s=%s; set both keys when overriding one"
            + " would conflict with the inherited default",
        INITIAL_DELAY_KEY,
        initial.toMillis(),
        MAX_DELAY_KEY,
        max.toMillis());
  }

  public abstract Builder toBuilder();

  /** Returns a builder with no overrides set. */
  public static Builder builder() {
    return new AutoValue_GcsRetryOptions.Builder();
  }

  /**
   * Creates retry options from a configuration map.
   *
   * @param analyticsCoreOptions The configuration properties map.
   * @param prefix The prefix prepended to every configuration key.
   * @return The parsed retry options.
   * @throws IllegalArgumentException If a value cannot be parsed or is out of range.
   */
  public static GcsRetryOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    Builder builder = builder();
    ConfigurationUtil.getTrimmedValue(analyticsCoreOptions, prefix + MAX_ATTEMPTS_KEY)
        .map(value -> parseInt(prefix + MAX_ATTEMPTS_KEY, value))
        .ifPresent(builder::setMaxAttempts);
    ConfigurationUtil.getTrimmedValue(analyticsCoreOptions, prefix + INITIAL_DELAY_KEY)
        .map(value -> parseMillis(prefix + INITIAL_DELAY_KEY, value))
        .ifPresent(builder::setInitialRetryDelay);
    ConfigurationUtil.getTrimmedValue(analyticsCoreOptions, prefix + DELAY_MULTIPLIER_KEY)
        .map(value -> parseDouble(prefix + DELAY_MULTIPLIER_KEY, value))
        .ifPresent(builder::setRetryDelayMultiplier);
    ConfigurationUtil.getTrimmedValue(analyticsCoreOptions, prefix + MAX_DELAY_KEY)
        .map(value -> parseMillis(prefix + MAX_DELAY_KEY, value))
        .ifPresent(builder::setMaxRetryDelay);
    ConfigurationUtil.getTrimmedValue(analyticsCoreOptions, prefix + TOTAL_TIMEOUT_KEY)
        .map(value -> parseMillis(prefix + TOTAL_TIMEOUT_KEY, value))
        .ifPresent(builder::setTotalTimeout);
    return builder.build(prefix);
  }

  private static int parseInt(String key, String value) {
    try {
      return ConfigurationUtil.safeParseInteger(key, value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("%s=%s must be a whole number", key, value), e);
    }
  }

  private static Duration parseMillis(String key, String value) {
    try {
      return Duration.ofMillis(Long.parseLong(value));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("%s=%s must be a whole number of milliseconds", key, value), e);
    }
  }

  private static double parseDouble(String key, String value) {
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(String.format("%s=%s must be a number", key, value), e);
    }
  }

  /** Builder for {@link GcsRetryOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setMaxAttempts(int maxAttempts);

    public abstract Builder setInitialRetryDelay(Duration initialRetryDelay);

    public abstract Builder setRetryDelayMultiplier(double retryDelayMultiplier);

    public abstract Builder setMaxRetryDelay(Duration maxRetryDelay);

    public abstract Builder setTotalTimeout(Duration totalTimeout);

    abstract GcsRetryOptions autoBuild();

    /**
     * Builds and validates the retry options.
     *
     * @throws IllegalArgumentException If a value is out of range.
     */
    public GcsRetryOptions build() {
      return build("");
    }

    /**
     * Builds and validates the retry options, reporting violations against the prefixed
     * configuration key that caused them.
     */
    GcsRetryOptions build(String prefix) {
      GcsRetryOptions options = autoBuild();
      options.getMaxAttempts().ifPresent(attempts -> checkMaxAttempts(prefix, attempts));
      options
          .getRetryDelayMultiplier()
          .ifPresent(multiplier -> checkMultiplier(prefix, multiplier));
      options
          .getInitialRetryDelay()
          .ifPresent(delay -> checkPositive(prefix + INITIAL_DELAY_KEY, delay));
      options.getMaxRetryDelay().ifPresent(delay -> checkPositive(prefix + MAX_DELAY_KEY, delay));
      options
          .getTotalTimeout()
          .ifPresent(timeout -> checkPositive(prefix + TOTAL_TIMEOUT_KEY, timeout));
      validateDelayOrdering(options, prefix);
      return options;
    }

    private static void checkMaxAttempts(String prefix, int attempts) {
      checkArgument(attempts >= 1, "%s=%s must be at least 1", prefix + MAX_ATTEMPTS_KEY, attempts);
    }

    private static void checkMultiplier(String prefix, double multiplier) {
      checkArgument(
          Double.isFinite(multiplier) && multiplier >= 1.0,
          "%s=%s must be a finite number of at least 1.0",
          prefix + DELAY_MULTIPLIER_KEY,
          multiplier);
    }

    private static void checkPositive(String key, Duration value) {
      checkArgument(
          !value.isNegative() && !value.isZero(), "%s=%s must be positive", key, value.toMillis());
    }

    private static void validateDelayOrdering(GcsRetryOptions options, String prefix) {
      if (options.getInitialRetryDelay().isPresent() && options.getMaxRetryDelay().isPresent()) {
        Duration initial = options.getInitialRetryDelay().get();
        Duration max = options.getMaxRetryDelay().get();
        checkArgument(
            initial.compareTo(max) <= 0,
            "%s=%s must not exceed %s=%s",
            prefix + INITIAL_DELAY_KEY,
            initial.toMillis(),
            prefix + MAX_DELAY_KEY,
            max.toMillis());
      }
    }
  }
}
