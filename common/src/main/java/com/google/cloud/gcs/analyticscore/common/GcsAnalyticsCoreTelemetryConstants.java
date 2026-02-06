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
package com.google.cloud.gcs.analyticscore.common;

public class GcsAnalyticsCoreTelemetryConstants {
  public enum Attribute {
    CLASS_NAME,
    READ_LENGTH,
    READ_OFFSET;
  }

  public enum Metric {
    SEEK_DISTANCE,
    SEEK_DURATION,
    READ_BYTES,
    READ_DURATION,
    OPEN_DURATION,
    READ_CACHE_HIT,
    READ_CACHE_MISS,
    CLOSE_DURATION,
    GCS_CLIENT_CREATE_DURATION;
  }

  public enum Operation {
    SEEK,
    READ,
    CLOSE,
    READ_FULLY,
    READ_TAIL,
    OPEN,
    VECTORED_READ,
    GCS_CLIENT_CREATE;
  }
}
