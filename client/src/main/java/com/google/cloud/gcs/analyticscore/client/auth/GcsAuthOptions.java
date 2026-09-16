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

import static com.google.cloud.gcs.analyticscore.common.ConfigurationUtil.getTrimmedValue;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.auto.value.AutoValue;
import com.google.cloud.gcs.analyticscore.common.RedactedString;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Configuration options for Google Cloud Storage authentication and network proxy.
 *
 * <p>Keys are namespaced under {@code analytics-core.auth.} because embedders supply a prefix of
 * their own ({@code fs.gs.} for the Hadoop connector) and typically pass their entire configuration
 * through. A bare {@code auth.} namespace would make keys such as {@code fs.gs.auth.type} mean one
 * thing to the embedder and another here, and the embedder has no way to tell the two apart.
 */
@AutoValue
public abstract class GcsAuthOptions {

  private static final String AUTH_TYPE_KEY = "analytics-core.auth.type";
  private static final String SERVICE_ACCOUNT_JSON_KEYFILE_KEY =
      "analytics-core.auth.service-account-json-keyfile";
  private static final String WORKLOAD_IDENTITY_CREDENTIAL_CONFIG_FILE_KEY =
      "analytics-core.auth.workload-identity-federation.credential-config-file";
  private static final String CLIENT_ID_KEY = "analytics-core.auth.client-id";
  private static final String CLIENT_SECRET_KEY = "analytics-core.auth.client-secret";
  private static final String REFRESH_TOKEN_KEY = "analytics-core.auth.refresh-token";
  private static final String IMPERSONATION_SERVICE_ACCOUNT_KEY =
      "analytics-core.auth.impersonation-service-account";
  private static final String TOKEN_SERVER_URI_KEY = "analytics-core.auth.token-server-uri";
  private static final String PROXY_ADDRESS_KEY = "analytics-core.auth.proxy.address";
  private static final String PROXY_USERNAME_KEY = "analytics-core.auth.proxy.username";
  private static final String PROXY_PASSWORD_KEY = "analytics-core.auth.proxy.password";
  private static final String HTTP_CONNECT_TIMEOUT_KEY =
      "analytics-core.auth.http.connect-timeout-ms";
  private static final String HTTP_READ_TIMEOUT_KEY = "analytics-core.auth.http.read-timeout-ms";

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
   * @throws IllegalArgumentException If a value cannot be parsed, or if a key required by the
   *     selected {@link AuthType} is missing.
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
    getTrimmedValue(options, prefix + TOKEN_SERVER_URI_KEY)
        .map(value -> parseUri(prefix + TOKEN_SERVER_URI_KEY, value))
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

    return optionsBuilder.build(prefix);
  }

  /**
   * Parses an absolute URI.
   *
   * <p>{@link URI} accepts relative references such as {@code oauth2.googleapis.com/token}, so
   * omitting the scheme is rejected here rather than at the first token refresh, where the failure
   * no longer names the property that caused it.
   *
   * @param key The configuration key the value was read from, named in error messages.
   * @param value The URI string to parse.
   * @return The parsed URI.
   * @throws IllegalArgumentException If the value is not a syntactically valid absolute URI.
   */
  private static URI parseUri(String key, String value) {
    URI uri;
    try {
      uri = new URI(value);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(String.format("%s=%s is not a valid URI", key, value), e);
    }
    checkArgument(
        uri.isAbsolute() && uri.getHost() != null,
        "%s=%s must be an absolute URI, for example https://oauth2.googleapis.com/token",
        key,
        value);
    return uri;
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

  /**
   * Parses and validates a proxy address of the form {@code [https?://]hostname:port}.
   *
   * @param proxyAddress The proxy address string to validate, or {@code null}.
   * @return The parsed {@link URI}, or {@code null} if {@code proxyAddress} is null or empty.
   * @throws IllegalArgumentException If the proxy address does not match {@code
   *     [https?://]hostname:port}.
   */
  @Nullable
  static URI parseProxyAddress(@Nullable String proxyAddress) {
    if (isNullOrEmpty(proxyAddress)) {
      return null;
    }
    String uriString = (proxyAddress.contains("//") ? "" : "//") + proxyAddress;
    try {
      URI uri = new URI(uriString);
      String scheme = uri.getScheme();
      String host = uri.getHost();
      int port = uri.getPort();
      checkArgument(
          isNullOrEmpty(scheme) || scheme.matches("https?"),
          "HTTP proxy address '%s' has invalid scheme '%s'.",
          proxyAddress,
          scheme);
      checkArgument(!isNullOrEmpty(host), "Proxy address '%s' has no host.", proxyAddress);
      checkArgument(port != -1, "Proxy address '%s' has no port.", proxyAddress);
      checkArgument(
          uri.equals(new URI(scheme, null, host, port, null, null, null)),
          "Invalid proxy address '%s'.",
          proxyAddress);
      return uri;
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Invalid proxy address '%s'.", proxyAddress), e);
    }
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

    abstract Optional<String> getImpersonationServiceAccount();

    public GcsAuthOptions build() {
      return build("");
    }

    GcsAuthOptions build(String prefix) {
      normalizeImpersonationServiceAccount();
      GcsAuthOptions options = autoBuild();
      validateRequiredFields(options, prefix);
      validateProxyFields(options, prefix);
      validateTimeouts(options, prefix);
      return options;
    }

    abstract GcsAuthOptions autoBuild();

    private void normalizeImpersonationServiceAccount() {
      setImpersonationServiceAccount(
          getImpersonationServiceAccount()
              .map(String::trim)
              .filter(serviceAccount -> !serviceAccount.isEmpty())
              .orElse(null));
    }

    /**
     * Rejects proxy settings that cannot be honored together, so that a typo in one key is reported
     * against the configuration that caused it rather than when a transport is first created.
     */
    private static void validateProxyFields(GcsAuthOptions options, String prefix) {
      checkArgument(
          options.getProxyAddress().isPresent()
              || (options.getProxyUsername().isEmpty() && options.getProxyPassword().isEmpty()),
          "%s and %s require %s to be set",
          prefix + PROXY_USERNAME_KEY,
          prefix + PROXY_PASSWORD_KEY,
          prefix + PROXY_ADDRESS_KEY);
      checkArgument(
          options.getProxyUsername().isPresent() == options.getProxyPassword().isPresent(),
          "%s and %s must be set or unset together",
          prefix + PROXY_USERNAME_KEY,
          prefix + PROXY_PASSWORD_KEY);
      if (options.getProxyAddress().isPresent()) {
        String address = options.getProxyAddress().get();
        checkArgument(!address.trim().isEmpty(), "%s cannot be blank", prefix + PROXY_ADDRESS_KEY);
        parseProxyAddress(address);
      }
    }

    private static void validateRequiredFields(GcsAuthOptions options, String prefix) {
      switch (options.getAuthType()) {
        case SERVICE_ACCOUNT_JSON_KEYFILE:
          checkRequiredField(
              options.getServiceAccountJsonKeyfile(),
              prefix + SERVICE_ACCOUNT_JSON_KEYFILE_KEY,
              prefix + AUTH_TYPE_KEY,
              options.getAuthType());
          break;
        case WORKLOAD_IDENTITY_FEDERATION:
          checkRequiredField(
              options.getWorkloadIdentityCredentialConfigFile(),
              prefix + WORKLOAD_IDENTITY_CREDENTIAL_CONFIG_FILE_KEY,
              prefix + AUTH_TYPE_KEY,
              options.getAuthType());
          break;
        case USER_CREDENTIALS:
          checkRequiredField(
              options.getClientId(),
              prefix + CLIENT_ID_KEY,
              prefix + AUTH_TYPE_KEY,
              options.getAuthType());
          checkRequiredField(
              options.getClientSecret(),
              prefix + CLIENT_SECRET_KEY,
              prefix + AUTH_TYPE_KEY,
              options.getAuthType());
          checkRequiredField(
              options.getRefreshToken(),
              prefix + REFRESH_TOKEN_KEY,
              prefix + AUTH_TYPE_KEY,
              options.getAuthType());
          break;
        case UNAUTHENTICATED:
          checkArgument(
              options.getImpersonationServiceAccount().isEmpty(),
              "%s cannot be set when %s is %s",
              prefix + IMPERSONATION_SERVICE_ACCOUNT_KEY,
              prefix + AUTH_TYPE_KEY,
              options.getAuthType());
          break;
          // Listed rather than defaulted, so that adding an auth type without deciding which keys
          // it requires is a compile-time failure instead of silently skipped validation.
        case APPLICATION_DEFAULT:
        case COMPUTE_ENGINE:
          break;
      }
    }

    private static void validateTimeouts(GcsAuthOptions options, String prefix) {
      checkArgument(
          !options.getHttpConnectTimeout().isNegative()
              && !options.getHttpConnectTimeout().isZero(),
          "%s must be positive, got: %s",
          prefix + HTTP_CONNECT_TIMEOUT_KEY,
          options.getHttpConnectTimeout());
      checkArgument(
          !options.getHttpReadTimeout().isNegative() && !options.getHttpReadTimeout().isZero(),
          "%s must be positive, got: %s",
          prefix + HTTP_READ_TIMEOUT_KEY,
          options.getHttpReadTimeout());
    }

    private static void checkRequiredField(
        Optional<?> value, String requiredKey, String authTypeKey, AuthType authType) {
      boolean isValid =
          value.isPresent()
              && (!(value.get() instanceof String) || !((String) value.get()).trim().isEmpty());
      checkArgument(isValid, "%s is required when %s is %s", requiredKey, authTypeKey, authType);
    }
  }
}
