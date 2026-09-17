/*
 * Copyright 2026 Google LLC
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

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Locale;

/** Supported client types for interacting with Google Cloud Storage. */
public enum ClientType {
  /** Standard HTTP / JSON client. */
  JSON,
  /** gRPC client using standard read channels. */
  GRPC,
  /** gRPC client using bidirectional streaming read channels. */
  BIDI;

  /**
   * Parses the given string into a {@link ClientType}.
   *
   * @param value the string value to parse
   * @return the corresponding {@link ClientType}
   * @throws IllegalArgumentException if the value cannot be parsed
   */
  public static ClientType fromString(String value) {
    checkNotNull(value, "clientType must not be null");
    switch (value.trim().toUpperCase(Locale.ROOT)) {
      case "JSON":
        return JSON;
      case "GRPC":
        return GRPC;
      case "BIDI":
        return BIDI;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Invalid client type: '%s'. Supported values are 'JSON', 'GRPC', 'BIDI'.", value));
    }
  }
}
