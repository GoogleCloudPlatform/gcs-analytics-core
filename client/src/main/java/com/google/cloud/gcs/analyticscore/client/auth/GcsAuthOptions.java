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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.cloud.gcs.analyticscore.common.RedactedString;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/** Configuration options for Google Cloud Storage authentication and network proxy. */
@AutoValue
public abstract class GcsAuthOptions {

  private static final String AUTH_TYPE_KEY = "auth.type";
  private static final String SERVICE_ACCOUNT_JSON_KEYFILE_KEY =
      "auth.service-account-json-keyfile";
  private static final String WORKLOAD_IDENTITY_CREDENTIAL_CONFIG_FILE_KEY =
      "auth.workload-identity-federation.credential-config-file";
  private static final String CLIENT_ID_KEY = "auth.client-id";
  private static final String CLIENT_SECRET_KEY = "auth.client-secret";
  private static final String REFRESH_TOKEN_KEY = "auth.refresh-token";
  private static final String IMPERSONATION_SERVICE_ACCOUNT_KEY =
      "auth.impersonation-service-account";
  private static final String TOKEN_SERVER_URL_KEY = "auth.token-server-url";
  private static final String PROXY_ADDRESS_KEY = "auth.proxy.address";
  private static final String PROXY_USERNAME_KEY = "auth.proxy.username";
  private static final String PROXY_PASSWORD_KEY = "auth.proxy.password";
  private static final String HTTP_CONNECT_TIMEOUT_KEY = "auth.http.connect-timeout-ms";
  private static final String HTTP_READ_TIMEOUT_KEY = "auth.http.read-timeout-ms";

  private static final Duration DEFAULT_HTTP_CONNECT_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration DEFAULT_HTTP_READ_TIMEOUT = Duration.ofSeconds(5);

  public abstract AuthType getAuthType();

  public abstract Optional<String> getServiceAccountJsonKeyfile();

  public abstract Optional<String> getWorkloadIdentityCredentialConfigFile();

  public abstract Optional<String> getClientId();

  public abstract Optional<RedactedString> getClientSecret();

  public abstract Optional<RedactedString> getRefreshToken();

  public abstract Optional<String> getImpersonationServiceAccount();

  public abstract Optional<URI> getTokenServerUri();

  public abstract Optional<String> getProxyAddress();

  public abstract Optional<RedactedString> getProxyUsername();

  public abstract Optional<RedactedString> getProxyPassword();

  public abstract Duration getHttpConnectTimeout();

  public abstract Duration getHttpReadTimeout();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_GcsAuthOptions.Builder()
        .setAuthType(AuthType.APPLICATION_DEFAULT)
        .setHttpConnectTimeout(DEFAULT_HTTP_CONNECT_TIMEOUT)
        .setHttpReadTimeout(DEFAULT_HTTP_READ_TIMEOUT);
  }

  /**
   * Creates a {@link GcsAuthOptions} instance from a configuration properties map.
   *
   * <p>Values are trimmed, and blank values are treated as absent.
   *
   * @param options The map containing configuration properties.
   * @param prefix The prefix prepended to configuration property keys, including any trailing
   *     separator.
   * @return A configured {@link GcsAuthOptions} instance.
   */
  public static GcsAuthOptions createFromOptions(Map<String, String> options, String prefix) {
    checkNotNull(options, "options cannot be null");
    checkNotNull(prefix, "prefix cannot be null");

    Builder optionsBuilder = builder();
    getTrimmedValue(options, prefix + AUTH_TYPE_KEY)
        .ifPresent(value -> optionsBuilder.setAuthType(AuthType.fromString(value)));
    getTrimmedValue(options, prefix + SERVICE_ACCOUNT_JSON_KEYFILE_KEY)
        .ifPresent(optionsBuilder::setServiceAccountJsonKeyfile);
    getTrimmedValue(options, prefix + WORKLOAD_IDENTITY_CREDENTIAL_CONFIG_FILE_KEY)
        .ifPresent(optionsBuilder::setWorkloadIdentityCredentialConfigFile);
    getTrimmedValue(options, prefix + CLIENT_ID_KEY).ifPresent(optionsBuilder::setClientId);
    getTrimmedValue(options, prefix + CLIENT_SECRET_KEY)
        .map(RedactedString::create)
        .ifPresent(optionsBuilder::setClientSecret);
    getTrimmedValue(options, prefix + REFRESH_TOKEN_KEY)
        .map(RedactedString::create)
        .ifPresent(optionsBuilder::setRefreshToken);
    getTrimmedValue(options, prefix + IMPERSONATION_SERVICE_ACCOUNT_KEY)
        .ifPresent(optionsBuilder::setImpersonationServiceAccount);
    getTrimmedValue(options, prefix + TOKEN_SERVER_URL_KEY)
        .map(value -> parseUri(prefix + TOKEN_SERVER_URL_KEY, value))
        .ifPresent(optionsBuilder::setTokenServerUri);
    getTrimmedValue(options, prefix + PROXY_ADDRESS_KEY).ifPresent(optionsBuilder::setProxyAddress);
    getTrimmedValue(options, prefix + PROXY_USERNAME_KEY)
        .map(RedactedString::create)
        .ifPresent(optionsBuilder::setProxyUsername);
    getTrimmedValue(options, prefix + PROXY_PASSWORD_KEY)
        .map(RedactedString::create)
        .ifPresent(optionsBuilder::setProxyPassword);
    getTrimmedValue(options, prefix + HTTP_CONNECT_TIMEOUT_KEY)
        .map(value -> parseTimeout(prefix + HTTP_CONNECT_TIMEOUT_KEY, value))
        .ifPresent(optionsBuilder::setHttpConnectTimeout);
    getTrimmedValue(options, prefix + HTTP_READ_TIMEOUT_KEY)
        .map(value -> parseTimeout(prefix + HTTP_READ_TIMEOUT_KEY, value))
        .ifPresent(optionsBuilder::setHttpReadTimeout);

    return optionsBuilder.build();
  }

  private static Optional<String> getTrimmedValue(Map<String, String> options, String key) {
    return Optional.ofNullable(options.get(key))
        .map(String::trim)
        .filter(value -> !value.isEmpty());
  }

  private static URI parseUri(String key, String value) {
    try {
      return new URI(value);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(String.format("%s=%s is not a valid URI", key, value), e);
    }
  }

  private static Duration parseTimeout(String key, String value) {
    long timeoutMillis;
    try {
      timeoutMillis = Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("%s=%s is not a valid number of milliseconds", key, value), e);
    }
    checkArgument(timeoutMillis > 0, "%s=%s must be positive", key, value);
    return Duration.ofMillis(timeoutMillis);
  }

  /** Builder for {@link GcsAuthOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setAuthType(AuthType authType);

    public abstract Builder setServiceAccountJsonKeyfile(
        @Nullable String serviceAccountJsonKeyfile);

    public abstract Builder setWorkloadIdentityCredentialConfigFile(
        @Nullable String workloadIdentityCredentialConfigFile);

    public abstract Builder setClientId(@Nullable String clientId);

    public abstract Builder setClientSecret(@Nullable RedactedString clientSecret);

    public abstract Builder setRefreshToken(@Nullable RedactedString refreshToken);

    public abstract Builder setImpersonationServiceAccount(
        @Nullable String impersonationServiceAccount);

    public abstract Builder setTokenServerUri(@Nullable URI tokenServerUri);

    public abstract Builder setProxyAddress(@Nullable String proxyAddress);

    public abstract Builder setProxyUsername(@Nullable RedactedString proxyUsername);

    public abstract Builder setProxyPassword(@Nullable RedactedString proxyPassword);

    public abstract Builder setHttpConnectTimeout(Duration httpConnectTimeout);

    public abstract Builder setHttpReadTimeout(Duration httpReadTimeout);

    public GcsAuthOptions build() {
      GcsAuthOptions options = autoBuild();
      validateRequiredFields(options);
      return options;
    }

    abstract GcsAuthOptions autoBuild();

    private static void validateRequiredFields(GcsAuthOptions options) {
      switch (options.getAuthType()) {
        case SERVICE_ACCOUNT_JSON_KEYFILE:
          checkRequiredField(
              options.getServiceAccountJsonKeyfile(),
              SERVICE_ACCOUNT_JSON_KEYFILE_KEY,
              options.getAuthType());
          break;
        case WORKLOAD_IDENTITY_FEDERATION:
          checkRequiredField(
              options.getWorkloadIdentityCredentialConfigFile(),
              WORKLOAD_IDENTITY_CREDENTIAL_CONFIG_FILE_KEY,
              options.getAuthType());
          break;
        case USER_CREDENTIALS:
          checkRequiredField(options.getClientId(), CLIENT_ID_KEY, options.getAuthType());
          checkRequiredField(options.getClientSecret(), CLIENT_SECRET_KEY, options.getAuthType());
          checkRequiredField(options.getRefreshToken(), REFRESH_TOKEN_KEY, options.getAuthType());
          break;
        default:
          break;
      }
    }

    private static void checkRequiredField(
        Optional<?> value, String requiredKey, AuthType authType) {
      checkState(
          value.isPresent(), "%s is required when %s is %s", requiredKey, AUTH_TYPE_KEY, authType);
    }
  }
}
