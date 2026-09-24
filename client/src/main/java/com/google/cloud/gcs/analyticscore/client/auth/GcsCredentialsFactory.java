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

import com.google.api.client.http.HttpTransport;
import com.google.auth.Credentials;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.NoCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating Google Cloud {@link Credentials} from {@link GcsAuthOptions}.
 *
 * <p>Applies the {@code cloud-platform} scope, optional custom token server URI, and optional
 * service account impersonation.
 */
public final class GcsCredentialsFactory {

  private static final Logger LOG = LoggerFactory.getLogger(GcsCredentialsFactory.class);

  private static final ImmutableList<String> CLOUD_PLATFORM_SCOPES =
      ImmutableList.of("https://www.googleapis.com/auth/cloud-platform");

  /** Auth types whose credentials can use a custom token server URI. */
  private static final ImmutableSet<AuthType> TOKEN_SERVER_URI_AUTH_TYPES =
      Sets.immutableEnumSet(
          AuthType.APPLICATION_DEFAULT,
          AuthType.SERVICE_ACCOUNT_JSON_KEYFILE,
          AuthType.USER_CREDENTIALS);

  private GcsCredentialsFactory() {}

  /**
   * Creates a {@link Credentials} instance according to the specified {@link GcsAuthOptions}.
   *
   * @param options The authentication options.
   * @return A configured {@link Credentials} instance, or {@link NoCredentials} if unauthenticated.
   * @throws IOException If credential resolution, key file reading, or transport creation fails.
   */
  public static Credentials createCredentials(GcsAuthOptions options) throws IOException {
    checkNotNull(options, "options cannot be null");
    if (options.getAuthType() == AuthType.UNAUTHENTICATED) {
      return NoCredentials.getInstance();
    }

    HttpTransport tokenTransport =
        GcsHttpTransportFactory.createHttpTransport(options, tokenTrustStoreSource(options));
    HttpTransport impersonationTransport = createImpersonationTransport(options, tokenTransport);

    return createCredentials(options, () -> tokenTransport, () -> impersonationTransport);
  }

  /**
   * Returns the HTTP transport for IAM Credentials requests, reusing {@code tokenTransport} when it
   * already uses the bundled Google trust store.
   */
  @VisibleForTesting
  static HttpTransport createImpersonationTransport(
      GcsAuthOptions options, HttpTransport tokenTransport) throws IOException {
    if (options.getImpersonationServiceAccount().isEmpty()
        || tokenTrustStoreSource(options) == TrustStoreSource.GOOGLE_BUNDLED) {
      return tokenTransport;
    }
    return GcsHttpTransportFactory.createHttpTransport(options, TrustStoreSource.GOOGLE_BUNDLED);
  }

  /**
   * Returns {@link TrustStoreSource#SYSTEM_DEFAULT} when a custom token server URI is configured
   * for an auth type that can use it, or {@link TrustStoreSource#GOOGLE_BUNDLED} otherwise.
   */
  @VisibleForTesting
  static TrustStoreSource tokenTrustStoreSource(GcsAuthOptions options) {
    return options.getTokenServerUri().isPresent()
            && TOKEN_SERVER_URI_AUTH_TYPES.contains(options.getAuthType())
        ? TrustStoreSource.SYSTEM_DEFAULT
        : TrustStoreSource.GOOGLE_BUNDLED;
  }

  @VisibleForTesting
  static Credentials createCredentials(
      GcsAuthOptions options,
      HttpTransportFactory tokenTransportFactory,
      HttpTransportFactory impersonationTransportFactory)
      throws IOException {
    GoogleCredentials credentials = createCredentialsForAuthType(options, tokenTransportFactory);
    GoogleCredentials scopedCredentials =
        options
            .getTokenServerUri()
            .map(tokenServerUri -> applyTokenServerUri(credentials, tokenServerUri))
            .orElse(credentials)
            .createScoped(CLOUD_PLATFORM_SCOPES);

    return options
        .getImpersonationServiceAccount()
        .map(principal -> impersonate(scopedCredentials, principal, impersonationTransportFactory))
        .orElse(scopedCredentials);
  }

  private static GoogleCredentials createCredentialsForAuthType(
      GcsAuthOptions options, HttpTransportFactory transportFactory) throws IOException {
    switch (options.getAuthType()) {
      case APPLICATION_DEFAULT:
        return GoogleCredentials.getApplicationDefault(transportFactory);
      case COMPUTE_ENGINE:
        return ComputeEngineCredentials.newBuilder()
            .setHttpTransportFactory(transportFactory)
            .build();
      case SERVICE_ACCOUNT_JSON_KEYFILE:
        return createServiceAccountCredentials(options, transportFactory);
      case WORKLOAD_IDENTITY_FEDERATION:
        return createExternalAccountCredentials(options, transportFactory);
      case USER_CREDENTIALS:
        return createUserCredentials(options, transportFactory);
      case UNAUTHENTICATED:
        break;
    }
    throw new VerifyException(
        "Unauthenticated access is resolved before an auth type is looked up: "
            + options.getAuthType());
  }

  private static GoogleCredentials createServiceAccountCredentials(
      GcsAuthOptions options, HttpTransportFactory transportFactory) throws IOException {
    try (InputStream keyFileStream =
        Files.newInputStream(Paths.get(options.getServiceAccountJsonKeyfile().get()))) {
      return ServiceAccountCredentials.fromStream(keyFileStream, transportFactory);
    }
  }

  private static GoogleCredentials createExternalAccountCredentials(
      GcsAuthOptions options, HttpTransportFactory transportFactory) throws IOException {
    try (InputStream configFileStream =
        Files.newInputStream(Paths.get(options.getWorkloadIdentityCredentialConfigFile().get()))) {
      return ExternalAccountCredentials.fromStream(configFileStream, transportFactory);
    }
  }

  private static GoogleCredentials createUserCredentials(
      GcsAuthOptions options, HttpTransportFactory transportFactory) {
    return UserCredentials.newBuilder()
        .setClientId(options.getClientId().get())
        .setClientSecret(options.getClientSecret().get().value())
        .setRefreshToken(options.getRefreshToken().get().value())
        .setHttpTransportFactory(transportFactory)
        .build();
  }

  private static GoogleCredentials applyTokenServerUri(
      GoogleCredentials credentials, URI tokenServerUri) {
    if (credentials instanceof ServiceAccountCredentials) {
      return ((ServiceAccountCredentials) credentials)
          .toBuilder().setTokenServerUri(tokenServerUri).build();
    }
    if (credentials instanceof UserCredentials) {
      return ((UserCredentials) credentials).toBuilder().setTokenServerUri(tokenServerUri).build();
    }
    LOG.warn(
        "Ignoring configured token server URI {}; credential type {} does not support it.",
        tokenServerUri,
        credentials.getClass().getSimpleName());
    return credentials;
  }

  private static GoogleCredentials impersonate(
      GoogleCredentials sourceCredentials,
      String targetPrincipal,
      HttpTransportFactory transportFactory) {
    LOG.debug("Configuring static impersonation for target service account: {}", targetPrincipal);
    return ImpersonatedCredentials.newBuilder()
        .setSourceCredentials(sourceCredentials)
        .setTargetPrincipal(targetPrincipal)
        .setScopes(CLOUD_PLATFORM_SCOPES)
        .setHttpTransportFactory(transportFactory)
        .build();
  }
}
