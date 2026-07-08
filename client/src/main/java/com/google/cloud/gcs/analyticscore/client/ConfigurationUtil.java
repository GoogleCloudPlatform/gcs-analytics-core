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

import java.util.Map;

/** Utility methods for configuration options. */
final class ConfigurationUtil {

  private ConfigurationUtil() {}

  static int safeParseInteger(String key, String valueStr) {
    long value = Long.parseLong(valueStr);
    if (value > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format(
              "%s=%d cannot be greater than Integer.MAX_VALUE (%d)",
              key, value, Integer.MAX_VALUE));
    }
    return (int) value;
  }

  static int safeParseInteger(Map<String, String> options, String key) {
    return safeParseInteger(key, options.get(key));
  }
}
