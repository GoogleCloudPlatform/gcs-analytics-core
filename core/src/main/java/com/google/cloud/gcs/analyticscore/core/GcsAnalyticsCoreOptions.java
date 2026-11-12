/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gcs.analyticscore.core;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/** Configuration options for GCS Analytics Core. */
public class GcsAnalyticsCoreOptions {

  private final String prefix;
  private final ImmutableMap<String, String> analyticsCoreOptions;

  /**
   * Constructs a new {@code GcsAnalyticsCoreOptions} instance.
   *
   * @param prefix the configuration key prefix
   * @param analyticsCoreOptions configuration options map
   * @throws NullPointerException if {@code prefix} or {@code analyticsCoreOptions} is {@code null}
   *     or if {@code analyticsCoreOptions} contains {@code null} keys or values
   */
  public GcsAnalyticsCoreOptions(String prefix, Map<String, String> analyticsCoreOptions) {
    this.prefix = checkNotNull(prefix, "prefix cannot be null");
    checkNotNull(analyticsCoreOptions, "analyticsCoreOptions cannot be null");
    this.analyticsCoreOptions = ImmutableMap.copyOf(analyticsCoreOptions);
  }

  /**
   * Returns {@link GcsFileSystemOptions} created from the configuration options and prefix.
   *
   * @return the file system options
   */
  public GcsFileSystemOptions getGcsFileSystemOptions() {
    return GcsFileSystemOptions.createFromOptions(analyticsCoreOptions, prefix);
  }
}
