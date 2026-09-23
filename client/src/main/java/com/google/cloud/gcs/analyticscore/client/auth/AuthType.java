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

package com.google.cloud.gcs.analyticscore.client.auth;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.Locale;

/** Enumeration of supported Google Cloud authentication types. */
public enum AuthType {
  /** Configures Application Default Credentials authentication. */
  APPLICATION_DEFAULT,

  /** Configures Google Compute Engine service account authentication. */
  COMPUTE_ENGINE,

  /** Configures JSON keyfile service account authentication. */
  SERVICE_ACCOUNT_JSON_KEYFILE,

  /** Configures Workload Identity Federation (external account credentials) authentication. */
  WORKLOAD_IDENTITY_FEDERATION,

  /** Configures OAuth2 user credentials authentication. */
  USER_CREDENTIALS,

  /** Configures unauthenticated access. */
  UNAUTHENTICATED;

  /**
   * Parses an {@link AuthType} from a string in a case-insensitive manner, replacing hyphens with
   * underscores.
   *
   * @param authTypeStr The string representation of the auth type.
   * @return The corresponding {@link AuthType}.
   * @throws IllegalArgumentException if {@code authTypeStr} does not match any auth type.
   */
  public static AuthType fromString(String authTypeStr) {
    checkNotNull(authTypeStr, "authTypeStr cannot be null");
    String normalizedAuthType = authTypeStr.trim().replace('-', '_').toUpperCase(Locale.ROOT);
    try {
      return valueOf(normalizedAuthType);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported auth type: '%s'. Supported values are %s.",
              authTypeStr, Arrays.toString(values())),
          e);
    }
  }
}
