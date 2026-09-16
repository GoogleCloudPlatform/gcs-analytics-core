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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.gcs.analyticscore.common.RedactedString;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class GcsAuthOptionsTest {

  private static final String CLIENT_SECRET_VALUE = "super-secret-value";
  private static final String REFRESH_TOKEN_VALUE = "refresh-token-value";
  private static final String PROXY_USERNAME_VALUE = "proxy-username-value";
  private static final String PROXY_PASSWORD_VALUE = "proxy-password-value";

  @Test
  void builder_defaults_areSetCorrectly() {
    GcsAuthOptions options = GcsAuthOptions.builder().build();

    assertThat(options.getAuthType()).isEqualTo(AuthType.APPLICATION_DEFAULT);
    assertThat(options.getHttpConnectTimeout()).isEqualTo(Duration.ofSeconds(5));
    assertThat(options.getHttpReadTimeout()).isEqualTo(Duration.ofSeconds(5));
    assertThat(options.getServiceAccountJsonKeyfile()).isEmpty();
    assertThat(options.getWorkloadIdentityCredentialConfigFile()).isEmpty();
    assertThat(options.getClientId()).isEmpty();
    assertThat(options.getClientSecret()).isEmpty();
    assertThat(options.getRefreshToken()).isEmpty();
    assertThat(options.getImpersonationServiceAccount()).isEmpty();
    assertThat(options.getTokenServerUri()).isEmpty();
    assertThat(options.getProxyAddress()).isEmpty();
    assertThat(options.getProxyUsername()).isEmpty();
    assertThat(options.getProxyPassword()).isEmpty();
  }

  @Test
  void builder_customValues_areSetCorrectly() {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.SERVICE_ACCOUNT_JSON_KEYFILE)
            .setServiceAccountJsonKeyfile("/path/to/key.json")
            .setWorkloadIdentityCredentialConfigFile("/path/to/wif.json")
            .setClientId("client-123")
            .setClientSecret(RedactedString.create("secret-456"))
            .setRefreshToken(RedactedString.create("token-789"))
            .setImpersonationServiceAccount("sa@project.iam.gserviceaccount.com")
            .setTokenServerUri(URI.create("https://oauth2.googleapis.com/token"))
            .setProxyAddress("proxy.mycompany.com:8080")
            .setProxyUsername(RedactedString.create("proxyuser"))
            .setProxyPassword(RedactedString.create("proxypass"))
            .setHttpConnectTimeout(Duration.ofSeconds(12))
            .setHttpReadTimeout(Duration.ofSeconds(15))
            .build();

    assertThat(options.getAuthType()).isEqualTo(AuthType.SERVICE_ACCOUNT_JSON_KEYFILE);
    assertThat(options.getServiceAccountJsonKeyfile()).hasValue("/path/to/key.json");
    assertThat(options.getWorkloadIdentityCredentialConfigFile()).hasValue("/path/to/wif.json");
    assertThat(options.getClientId()).hasValue("client-123");
    assertThat(options.getClientSecret()).hasValue(RedactedString.create("secret-456"));
    assertThat(options.getRefreshToken()).hasValue(RedactedString.create("token-789"));
    assertThat(options.getImpersonationServiceAccount())
        .hasValue("sa@project.iam.gserviceaccount.com");
    assertThat(options.getTokenServerUri())
        .hasValue(URI.create("https://oauth2.googleapis.com/token"));
    assertThat(options.getProxyAddress()).hasValue("proxy.mycompany.com:8080");
    assertThat(options.getProxyUsername()).hasValue(RedactedString.create("proxyuser"));
    assertThat(options.getProxyPassword()).hasValue(RedactedString.create("proxypass"));
    assertThat(options.getHttpConnectTimeout()).isEqualTo(Duration.ofSeconds(12));
    assertThat(options.getHttpReadTimeout()).isEqualTo(Duration.ofSeconds(15));
  }

  @Test
  void toBuilder_modifiesFieldsCorrectly() {
    GcsAuthOptions original =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.COMPUTE_ENGINE)
            .setProxyAddress("proxy:8080")
            .build();

    GcsAuthOptions modified =
        original.toBuilder().setAuthType(AuthType.UNAUTHENTICATED).setProxyAddress(null).build();

    assertThat(modified.getAuthType()).isEqualTo(AuthType.UNAUTHENTICATED);
    assertThat(modified.getProxyAddress()).isEmpty();
    assertThat(original.getAuthType()).isEqualTo(AuthType.COMPUTE_ENGINE);
    assertThat(original.getProxyAddress()).hasValue("proxy:8080");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        CLIENT_SECRET_VALUE,
        REFRESH_TOKEN_VALUE,
        PROXY_USERNAME_VALUE,
        PROXY_PASSWORD_VALUE
      })
  void toString_secretValues_areRedacted(String secret) {
    GcsAuthOptions options = createOptionsWithSecrets();

    String description = options.toString();

    assertThat(description).doesNotContain(secret);
  }

  @Test
  void toString_nonSecretValues_areVisible() {
    GcsAuthOptions options = createOptionsWithSecrets();

    String description = options.toString();

    assertThat(description).contains("my-client-id");
  }

  @Test
  void createFromOptions_withPrefix_parsesAllFields() {
    Map<String, String> map =
        ImmutableMap.<String, String>builder()
            .put("gcs.analytics-core.auth.type", "USER_CREDENTIALS")
            .put("gcs.analytics-core.auth.service-account-json-keyfile", "/path/to/key.json")
            .put(
                "gcs.analytics-core.auth.workload-identity-federation.credential-config-file",
                "/path/to/wif.json")
            .put("gcs.analytics-core.auth.client-id", "client-id")
            .put("gcs.analytics-core.auth.client-secret", "client-secret")
            .put("gcs.analytics-core.auth.refresh-token", "refresh-token")
            .put(
                "gcs.analytics-core.auth.impersonation-service-account",
                "target@iam.gserviceaccount.com")
            .put("gcs.analytics-core.auth.token-server-uri", "https://oauth2.googleapis.com/token")
            .put("gcs.analytics-core.auth.proxy.address", "https://proxy:8443")
            .put("gcs.analytics-core.auth.proxy.username", "user")
            .put("gcs.analytics-core.auth.proxy.password", "pass")
            .put("gcs.analytics-core.auth.http.connect-timeout-ms", "8000")
            .put("gcs.analytics-core.auth.http.read-timeout-ms", "10000")
            .build();

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, "gcs.");

    assertThat(options.getAuthType()).isEqualTo(AuthType.USER_CREDENTIALS);
    assertThat(options.getServiceAccountJsonKeyfile()).hasValue("/path/to/key.json");
    assertThat(options.getWorkloadIdentityCredentialConfigFile()).hasValue("/path/to/wif.json");
    assertThat(options.getClientId()).hasValue("client-id");
    assertThat(options.getClientSecret()).hasValue(RedactedString.create("client-secret"));
    assertThat(options.getRefreshToken()).hasValue(RedactedString.create("refresh-token"));
    assertThat(options.getImpersonationServiceAccount()).hasValue("target@iam.gserviceaccount.com");
    assertThat(options.getTokenServerUri())
        .hasValue(URI.create("https://oauth2.googleapis.com/token"));
    assertThat(options.getProxyAddress()).hasValue("https://proxy:8443");
    assertThat(options.getProxyUsername()).hasValue(RedactedString.create("user"));
    assertThat(options.getProxyPassword()).hasValue(RedactedString.create("pass"));
    assertThat(options.getHttpConnectTimeout()).isEqualTo(Duration.ofSeconds(8));
    assertThat(options.getHttpReadTimeout()).isEqualTo(Duration.ofSeconds(10));
  }

  @Test
  void createFromOptions_withEmptyPrefix_parsesUnprefixedKeys() {
    Map<String, String> map = ImmutableMap.of("analytics-core.auth.type", "UNAUTHENTICATED");

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, "");

    assertThat(options.getAuthType()).isEqualTo(AuthType.UNAUTHENTICATED);
  }

  @Test
  void createFromOptions_keyWithoutPrefix_isIgnored() {
    Map<String, String> map = ImmutableMap.of("analytics-core.auth.type", "UNAUTHENTICATED");

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, "gcs.");

    assertThat(options.getAuthType()).isEqualTo(AuthType.APPLICATION_DEFAULT);
  }

  @Test
  void createFromOptions_bareAuthKeysWithoutNamespace_areIgnored() {
    Map<String, String> map =
        ImmutableMap.of("auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE", "auth.client-id", "client-id");

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, "");

    assertThat(options).isEqualTo(GcsAuthOptions.builder().build());
  }

  /**
   * The Hadoop connector hands its whole {@code fs.gs.*} configuration to Analytics Core, so its
   * auth keys must fall through to the defaults here rather than being read or rejected.
   */
  @ParameterizedTest
  @CsvSource({
    "fs.gs.auth.type, SERVICE_ACCOUNT_JSON_KEYFILE",
    "fs.gs.auth.type, ACCESS_TOKEN_PROVIDER",
    "fs.gs.auth.type, WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE",
    "fs.gs.auth.type, USER_CREDENTIALS",
    "fs.gs.auth.type, UNAUTHENTICATED",
    "fs.gs.auth.service.account.json.keyfile, /path/to/key.json",
    "fs.gs.auth.client.id, client-id",
    "fs.gs.auth.refresh.token, refresh-token",
    "fs.gs.auth.impersonation.service.account, sa@project.iam.gserviceaccount.com",
    "fs.gs.token.server.url, https://oauth2.googleapis.com/token",
    "fs.gs.proxy.address, proxy.corp:3128"
  })
  void createFromOptions_hadoopConnectorAuthKeys_areIgnored(String key, String value) {
    Map<String, String> map = ImmutableMap.of(key, value);

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, "fs.gs.");

    assertThat(options).isEqualTo(GcsAuthOptions.builder().build());
  }

  @Test
  void createFromOptions_blankValues_areIgnored() {
    Map<String, String> map =
        ImmutableMap.<String, String>builder()
            .put("analytics-core.auth.type", "   ")
            .put("analytics-core.auth.service-account-json-keyfile", "   ")
            .put("analytics-core.auth.workload-identity-federation.credential-config-file", "   ")
            .put("analytics-core.auth.client-id", "   ")
            .put("analytics-core.auth.client-secret", "   ")
            .put("analytics-core.auth.refresh-token", "   ")
            .put("analytics-core.auth.impersonation-service-account", "   ")
            .put("analytics-core.auth.token-server-uri", "   ")
            .put("analytics-core.auth.proxy.address", "   ")
            .put("analytics-core.auth.proxy.username", "   ")
            .put("analytics-core.auth.proxy.password", "   ")
            .put("analytics-core.auth.http.connect-timeout-ms", "   ")
            .put("analytics-core.auth.http.read-timeout-ms", "   ")
            .build();

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, "");

    assertThat(options).isEqualTo(GcsAuthOptions.builder().build());
  }

  @Test
  void createFromOptions_valuesWithWhitespace_areTrimmed() {
    Map<String, String> map = ImmutableMap.of("analytics-core.auth.client-id", "  client-id  ");

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, "");

    assertThat(options.getClientId()).hasValue("client-id");
  }

  @ParameterizedTest
  @ValueSource(strings = {"not-a-number", "10s", "2500ms", "0", "-5"})
  void createFromOptions_invalidConnectTimeout_throwsIllegalArgumentException(String timeout) {
    Map<String, String> map =
        ImmutableMap.of("analytics-core.auth.http.connect-timeout-ms", timeout);

    assertThrows(IllegalArgumentException.class, () -> GcsAuthOptions.createFromOptions(map, ""));
  }

  @ParameterizedTest
  @ValueSource(strings = {"not-a-number", "10s", "2500ms", "0", "-5"})
  void createFromOptions_invalidReadTimeout_throwsIllegalArgumentException(String timeout) {
    Map<String, String> map = ImmutableMap.of("analytics-core.auth.http.read-timeout-ms", timeout);

    assertThrows(IllegalArgumentException.class, () -> GcsAuthOptions.createFromOptions(map, ""));
  }

  @ParameterizedTest
  @ValueSource(strings = {"http://invalid uri", "oauth2.googleapis.com/token", "not-a-url"})
  void createFromOptions_tokenServerUriIsNotAbsolute_throwsIllegalArgumentException(String uri) {
    Map<String, String> map = ImmutableMap.of("analytics-core.auth.token-server-uri", uri);

    assertThrows(IllegalArgumentException.class, () -> GcsAuthOptions.createFromOptions(map, ""));
  }

  @ParameterizedTest
  @ValueSource(strings = {"mailto:tokens@example.com", "urn:isbn:0451450523"})
  void createFromOptions_tokenServerUriHasNoHost_throwsIllegalArgumentException(String uri) {
    Map<String, String> map = ImmutableMap.of("analytics-core.auth.token-server-uri", uri);

    assertThrows(IllegalArgumentException.class, () -> GcsAuthOptions.createFromOptions(map, ""));
  }

  @Test
  void createFromOptions_nullOptions_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> GcsAuthOptions.createFromOptions(null, "gcs."));
  }

  @Test
  void createFromOptions_nullPrefix_throwsNullPointerException() {
    Map<String, String> map = ImmutableMap.of("analytics-core.auth.type", "UNAUTHENTICATED");

    assertThrows(NullPointerException.class, () -> GcsAuthOptions.createFromOptions(map, null));
  }

  @Test
  void build_userCredentialsWithoutClientId_throwsIllegalArgumentException() {
    GcsAuthOptions.Builder optionsBuilder =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.USER_CREDENTIALS)
            .setClientSecret(RedactedString.create(CLIENT_SECRET_VALUE))
            .setRefreshToken(RedactedString.create(REFRESH_TOKEN_VALUE));

    assertThrows(IllegalArgumentException.class, optionsBuilder::build);
  }

  @Test
  void build_userCredentialsWithoutClientSecret_throwsIllegalArgumentException() {
    GcsAuthOptions.Builder optionsBuilder =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.USER_CREDENTIALS)
            .setClientId("my-client-id")
            .setRefreshToken(RedactedString.create(REFRESH_TOKEN_VALUE));

    assertThrows(IllegalArgumentException.class, optionsBuilder::build);
  }

  @Test
  void build_userCredentialsWithoutRefreshToken_throwsIllegalArgumentException() {
    GcsAuthOptions.Builder optionsBuilder =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.USER_CREDENTIALS)
            .setClientId("my-client-id")
            .setClientSecret(RedactedString.create(CLIENT_SECRET_VALUE));

    assertThrows(IllegalArgumentException.class, optionsBuilder::build);
  }

  @Test
  void build_serviceAccountKeyfileAuthWithoutKeyfile_throwsIllegalArgumentException() {
    GcsAuthOptions.Builder optionsBuilder =
        GcsAuthOptions.builder().setAuthType(AuthType.SERVICE_ACCOUNT_JSON_KEYFILE);

    assertThrows(IllegalArgumentException.class, optionsBuilder::build);
  }

  @Test
  void build_workloadIdentityFederationWithoutConfigFile_throwsIllegalArgumentException() {
    GcsAuthOptions.Builder optionsBuilder =
        GcsAuthOptions.builder().setAuthType(AuthType.WORKLOAD_IDENTITY_FEDERATION);

    assertThrows(IllegalArgumentException.class, optionsBuilder::build);
  }

  @Test
  void build_workloadIdentityFederationWithConfigFile_succeeds() {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.WORKLOAD_IDENTITY_FEDERATION)
            .setWorkloadIdentityCredentialConfigFile("/path/to/wif.json")
            .build();

    assertThat(options.getWorkloadIdentityCredentialConfigFile()).hasValue("/path/to/wif.json");
  }

  @Test
  void build_computeEngineWithoutCredentialFields_succeeds() {
    GcsAuthOptions options = GcsAuthOptions.builder().setAuthType(AuthType.COMPUTE_ENGINE).build();

    assertThat(options.getAuthType()).isEqualTo(AuthType.COMPUTE_ENGINE);
  }

  @Test
  void createFromOptions_userCredentialsMissingSecret_reportsMissingKey() {
    Map<String, String> map =
        ImmutableMap.of(
            "analytics-core.auth.type", "USER_CREDENTIALS",
            "analytics-core.auth.client-id", "client-id",
            "analytics-core.auth.refresh-token", "refresh-token");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> GcsAuthOptions.createFromOptions(map, ""));

    assertThat(exception).hasMessageThat().contains("analytics-core.auth.client-secret");
  }

  @Test
  void build_proxyUsernameWithoutProxyAddress_throwsIllegalArgumentException() {
    GcsAuthOptions.Builder builder =
        GcsAuthOptions.builder()
            .setProxyUsername(RedactedString.create("user"))
            .setProxyPassword(RedactedString.create("pass"));

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception).hasMessageThat().contains("analytics-core.auth.proxy.address");
  }

  @Test
  void build_proxyUsernameWithoutProxyPassword_throwsIllegalArgumentException() {
    GcsAuthOptions.Builder builder =
        GcsAuthOptions.builder()
            .setProxyAddress("proxy:8080")
            .setProxyUsername(RedactedString.create("user"));

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception).hasMessageThat().contains("analytics-core.auth.proxy.password");
  }

  @Test
  void build_proxyPasswordWithoutProxyUsername_throwsIllegalArgumentException() {
    GcsAuthOptions.Builder builder =
        GcsAuthOptions.builder()
            .setProxyAddress("proxy:8080")
            .setProxyPassword(RedactedString.create("pass"));

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception).hasMessageThat().contains("analytics-core.auth.proxy.username");
  }

  @Test
  void build_proxyPasswordWithoutAddressOrUsername_throwsIllegalArgumentException() {
    GcsAuthOptions.Builder builder =
        GcsAuthOptions.builder().setProxyPassword(RedactedString.create(PROXY_PASSWORD_VALUE));

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception).hasMessageThat().contains("analytics-core.auth.proxy.address");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "   "})
  void build_blankProxyAddress_throwsIllegalArgumentException(String blankAddress) {
    GcsAuthOptions.Builder builder = GcsAuthOptions.builder().setProxyAddress(blankAddress);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception).hasMessageThat().contains("analytics-core.auth.proxy.address");
  }

  @Test
  void createFromOptions_proxyCredentialsWithoutAddress_throwsIllegalArgumentException() {
    Map<String, String> map =
        ImmutableMap.of(
            "analytics-core.auth.proxy.username", "user",
            "analytics-core.auth.proxy.password", "pass");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> GcsAuthOptions.createFromOptions(map, ""));

    assertThat(exception).hasMessageThat().contains("analytics-core.auth.proxy.address");
  }

  @Test
  void createFromOptions_withPrefix_missingRequiredFieldReportsPrefixedKey() {
    Map<String, String> map =
        ImmutableMap.of(
            "gcs.analytics-core.auth.type", "USER_CREDENTIALS",
            "gcs.analytics-core.auth.client-id", "client-id",
            "gcs.analytics-core.auth.refresh-token", "refresh-token");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> GcsAuthOptions.createFromOptions(map, "gcs."));

    assertThat(exception).hasMessageThat().contains("gcs.analytics-core.auth.client-secret");
  }

  @Test
  void createFromOptions_withPrefix_proxyCredentialsWithoutAddressReportsPrefixedKey() {
    Map<String, String> map =
        ImmutableMap.of(
            "gcs.analytics-core.auth.proxy.username", "user",
            "gcs.analytics-core.auth.proxy.password", "pass");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> GcsAuthOptions.createFromOptions(map, "gcs."));

    assertThat(exception).hasMessageThat().contains("gcs.analytics-core.auth.proxy.address");
  }

  @ParameterizedTest
  @CsvSource({
    "proxy.example.com:8080, //proxy.example.com:8080",
    "http://proxy.example.com:8080, http://proxy.example.com:8080",
    "https://proxy.example.com:8443, https://proxy.example.com:8443",
    "127.0.0.1:3128, //127.0.0.1:3128"
  })
  void parseProxyAddress_validAddress_returnsExpectedUri(String input, String expected) {
    URI uri = GcsAuthOptions.parseProxyAddress(input);

    assertThat(uri).isEqualTo(URI.create(expected));
  }

  @ParameterizedTest
  @NullAndEmptySource
  void parseProxyAddress_nullOrEmptyAddress_returnsNull(String proxyAddress) {
    URI uri = GcsAuthOptions.parseProxyAddress(proxyAddress);

    assertThat(uri).isNull();
  }

  @ParameterizedTest
  @CsvSource({
    "socks5://foo-host:1234, HTTP proxy address 'socks5://foo-host:1234' has invalid scheme 'socks5'.",
    ":1234, Proxy address ':1234' has no host.",
    "foo-host, Proxy address 'foo-host' has no port.",
    "foo-host-with-illegal-char^:1234, Invalid proxy address 'foo-host-with-illegal-char^:1234'.",
    "foo-host:1234/some/path, Invalid proxy address 'foo-host:1234/some/path'."
  })
  void parseProxyAddress_invalidAddress_throwsExpectedDiagnosticMessage(
      String invalidInput, String expectedMessageSubstring) {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> GcsAuthOptions.parseProxyAddress(invalidInput));

    assertThat(exception).hasMessageThat().contains(expectedMessageSubstring);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "this is not a proxy",
        "proxy.example.com",
        "ftp://proxy.example.com:8080",
        "http://proxy.example.com:8080/path",
        ":8080",
        "http://proxy.example.com:notaport"
      })
  void build_invalidProxyAddress_throwsIllegalArgumentException(String invalidProxy) {
    GcsAuthOptions.Builder builder = GcsAuthOptions.builder().setProxyAddress(invalidProxy);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @ParameterizedTest
  @ValueSource(longs = {0L, -5L})
  void build_nonPositiveConnectTimeout_throwsIllegalArgumentException(long seconds) {
    GcsAuthOptions.Builder builder =
        GcsAuthOptions.builder().setHttpConnectTimeout(Duration.ofSeconds(seconds));

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @ParameterizedTest
  @ValueSource(longs = {0L, -5L})
  void build_nonPositiveReadTimeout_throwsIllegalArgumentException(long seconds) {
    GcsAuthOptions.Builder builder =
        GcsAuthOptions.builder().setHttpReadTimeout(Duration.ofSeconds(seconds));

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "   "})
  void build_serviceAccountKeyfileAuthWithBlankKeyfile_throwsIllegalArgumentException(
      String blankKeyfile) {
    GcsAuthOptions.Builder builder =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.SERVICE_ACCOUNT_JSON_KEYFILE)
            .setServiceAccountJsonKeyfile(blankKeyfile);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "   "})
  void build_blankImpersonationServiceAccount_isTreatedAsUnset(String blankServiceAccount) {
    GcsAuthOptions options =
        GcsAuthOptions.builder().setImpersonationServiceAccount(blankServiceAccount).build();

    assertThat(options.getImpersonationServiceAccount()).isEmpty();
  }

  @Test
  void build_impersonationServiceAccountWithWhitespace_isTrimmed() {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setImpersonationServiceAccount("  sa@project.iam.gserviceaccount.com  ")
            .build();

    assertThat(options.getImpersonationServiceAccount())
        .hasValue("sa@project.iam.gserviceaccount.com");
  }

  @Test
  void build_unauthenticatedWithImpersonation_throwsIllegalArgumentException() {
    GcsAuthOptions.Builder builder =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.UNAUTHENTICATED)
            .setImpersonationServiceAccount("sa@project.iam.gserviceaccount.com");

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception)
        .hasMessageThat()
        .contains("analytics-core.auth.impersonation-service-account");
  }

  @Test
  void build_unauthenticatedWithBlankImpersonation_succeeds() {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.UNAUTHENTICATED)
            .setImpersonationServiceAccount("   ")
            .build();

    assertThat(options.getImpersonationServiceAccount()).isEmpty();
  }

  @Test
  void createFromOptions_unauthenticatedWithImpersonation_throwsIllegalArgumentException() {
    Map<String, String> map =
        ImmutableMap.of(
            "analytics-core.auth.type",
            "UNAUTHENTICATED",
            "analytics-core.auth.impersonation-service-account",
            "sa@project.iam.gserviceaccount.com");

    assertThrows(IllegalArgumentException.class, () -> GcsAuthOptions.createFromOptions(map, ""));
  }

  private static GcsAuthOptions createOptionsWithSecrets() {
    return GcsAuthOptions.builder()
        .setAuthType(AuthType.USER_CREDENTIALS)
        .setClientId("my-client-id")
        .setClientSecret(RedactedString.create(CLIENT_SECRET_VALUE))
        .setRefreshToken(RedactedString.create(REFRESH_TOKEN_VALUE))
        .setProxyAddress("proxy.mycompany.com:8080")
        .setProxyUsername(RedactedString.create(PROXY_USERNAME_VALUE))
        .setProxyPassword(RedactedString.create(PROXY_PASSWORD_VALUE))
        .build();
  }
}
