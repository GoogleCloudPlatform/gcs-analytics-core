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
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.client.http.HttpTransport;
import com.google.auth.http.HttpTransportFactory;
import com.google.cloud.http.HttpTransportOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.io.IOException;
import java.io.UncheckedIOException;
import javax.annotation.Nullable;

/**
 * Factory for the {@link HttpTransportOptions} used by the GCS JSON API client.
 *
 * <p>The connect and read timeouts always apply. The client's default transport is replaced only
 * when a proxy is configured, so without a proxy requests keep the JVM's trust store and share its
 * connection pool. The proxy transport trusts Google's bundled certificates, unless the client
 * talks to a custom endpoint or universe domain, whose certificates may only be in the JVM's trust
 * store.
 */
public final class GcsTransportOptionsFactory {

  private static final String GOOGLE_DEFAULT_UNIVERSE_DOMAIN = "googleapis.com";

  private GcsTransportOptionsFactory() {}

  /**
   * Creates the transport options for GCS JSON API requests.
   *
   * @param options The authentication options supplying the proxy and timeouts.
   * @param storageEndpoint The configured storage endpoint, or {@code null} for the default one.
   * @param universeDomain The configured universe domain, or {@code null} for the default one.
   * @return The transport options for the storage client.
   */
  public static HttpTransportOptions createHttpTransportOptions(
      GcsAuthOptions options, @Nullable String storageEndpoint, @Nullable String universeDomain) {
    checkNotNull(options, "options cannot be null");
    HttpTransportOptions.Builder builder =
        HttpTransportOptions.newBuilder()
            .setConnectTimeout(
                GcsHttpTransportFactory.toTimeoutMillis(
                    options.getHttpConnectTimeout(), "connectTimeout"))
            .setReadTimeout(
                GcsHttpTransportFactory.toReadTimeoutMillis(options.getHttpReadTimeout()));
    if (options.getProxyAddress().isPresent()) {
      TrustStoreSource trustStoreSource =
          usesNonGoogleEndpoint(storageEndpoint, universeDomain)
              ? TrustStoreSource.SYSTEM_DEFAULT
              : TrustStoreSource.GOOGLE_BUNDLED;
      builder.setHttpTransportFactory(createProxyTransportFactory(options, trustStoreSource));
    }
    return builder.build();
  }

  /**
   * Returns whether requests go to a custom endpoint or a non-default universe domain, whose
   * certificates may not be in Google's bundled trust store.
   */
  @VisibleForTesting
  static boolean usesNonGoogleEndpoint(
      @Nullable String storageEndpoint, @Nullable String universeDomain) {
    if (!isNullOrEmpty(storageEndpoint)) {
      return true;
    }
    return !isNullOrEmpty(universeDomain)
        && !GOOGLE_DEFAULT_UNIVERSE_DOMAIN.equalsIgnoreCase(universeDomain.trim());
  }

  /** Returns a factory that creates the proxy transport once, on first use. */
  private static HttpTransportFactory createProxyTransportFactory(
      GcsAuthOptions options, TrustStoreSource trustStoreSource) {
    Supplier<HttpTransport> transport =
        Suppliers.memoize(
            () -> {
              try {
                return GcsHttpTransportFactory.createHttpTransport(options, trustStoreSource);
              } catch (IOException e) {
                throw new UncheckedIOException("Failed to create the proxy HTTP transport", e);
              }
            });
    return transport::get;
  }
}
