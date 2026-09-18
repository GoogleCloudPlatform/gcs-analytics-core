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

/**
 * The certificate trust store that a transport validates TLS certificates against.
 *
 * <p>This is a per-transport decision rather than a configuration option, because a single
 * deployment can need both: OAuth2 tokens are always fetched from a Google endpoint, while the data
 * plane may be pointed at a custom endpoint or a non-default universe domain whose certificate
 * authority is not in Google's bundle.
 */
enum TrustStoreSource {

  /**
   * Validates against the certificate trust store bundled with the Google API client.
   *
   * <p>This is the appropriate choice for Google endpoints, because it does not inherit whatever
   * certificate authorities the host JVM happens to trust.
   */
  GOOGLE_BUNDLED,

  /**
   * Validates against the JVM's default trust store.
   *
   * <p>This is required for endpoints whose certificate authority is absent from Google's bundle,
   * such as a custom endpoint or a non-default universe domain.
   */
  SYSTEM_DEFAULT;

  /** Returns whether TLS certificates are validated against the JVM's default trust store. */
  boolean isSystemDefault() {
    return this == SYSTEM_DEFAULT;
  }
}
