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

package com.google.cloud.gcs.analyticscore.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Optional;

/** Utility methods for parsing configuration options. */
public final class ConfigurationUtil {

  private ConfigurationUtil() {}

  /**
   * Parses a string configuration value into an integer, failing if it overflows {@code int}.
   *
   * @param key The configuration property key, used in error messages.
   * @param valueStr The string value to parse.
   * @return The parsed integer value.
   * @throws IllegalArgumentException If the value overflows {@code int} or is not a valid integer.
   */
  public static int safeParseInteger(String key, String valueStr) {
    try {
      return Math.toIntExact(Long.parseLong(valueStr));
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          String.format(
              "%s=%s cannot be greater than Integer.MAX_VALUE (%d)",
              key, valueStr, Integer.MAX_VALUE),
          e);
    }
  }

  /**
   * Parses a string configuration value from a map into an integer, failing if it overflows {@code
   * int}.
   *
   * @param options The configuration properties map.
   * @param key The configuration property key to read.
   * @return The parsed integer value.
   * @throws IllegalArgumentException If the value overflows {@code int} or is not a valid integer.
   */
  public static int safeParseInteger(Map<String, String> options, String key) {
    return safeParseInteger(key, options.get(key));
  }

  /**
   * Reads a property from a configuration map, trimming surrounding whitespace and treating blank
   * values as absent.
   *
   * @param options The configuration properties map.
   * @param key The configuration property key to read.
   * @return An {@link Optional} containing the trimmed value, or empty if absent or blank.
   */
  public static Optional<String> getTrimmedValue(Map<String, String> options, String key) {
    checkNotNull(options, "options cannot be null");
    checkNotNull(key, "key cannot be null");
    return Optional.ofNullable(options.get(key))
        .map(String::trim)
        .filter(value -> !value.isEmpty());
  }
}
