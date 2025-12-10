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
package com.google.cloud.gcs.analyticscore.common.telemetry;

public final class GcsAnalyticsCoreMetrics {

  public static final String GCS_API_TIME = "gcs.analytics-core.GCS_API_TIME";

  public static final String GCS_API_REQUEST_COUNT = "gcs.analytics-core.GCS_API_REQUEST_COUNT";

  public static final String GCS_API_EXCEPTIONS = "gcs.analytics-core.GCS_API_EXCEPTIONS";

  public static final String GCS_READ_CHANNEL_OPEN_COUNT =
      "gcs.analytics-core.GCS_READ_CHANNEL_OPEN_COUNT";

  public static final String GCS_BYTES_READ = "gcs.analytics-core.GCS_BYTES_READ";

  public static final String GCS_READ_DURATION = "gcs.analytics-core.GCS_READ_DURATION";

  public static final String GCS_CLIENT_EXCEPTIONS = "gcs.analytics-core.GCS_CLIENT_EXCEPTIONS";

  public static final String GCS_READ_CACHE_HIT = "gcs.analytics-core.GCS_READ_CACHE_HIT";

  public static final String GCS_READ_CACHE_MISS = "gcs.analytics-core.GCS_READ_CACHE_MISS";

  private GcsAnalyticsCoreMetrics() {}
}
