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
            .put("gcs.auth.type", "USER_CREDENTIALS")
            .put("gcs.auth.service-account-json-keyfile", "/path/to/key.json")
            .put(
                "gcs.auth.workload-identity-federation.credential-config-file", "/path/to/wif.json")
            .put("gcs.auth.client-id", "client-id")
            .put("gcs.auth.client-secret", "client-secret")
            .put("gcs.auth.refresh-token", "refresh-token")
            .put("gcs.auth.impersonation-service-account", "target@iam.gserviceaccount.com")
            .put("gcs.auth.token-server-url", "https://oauth2.googleapis.com/token")
            .put("gcs.auth.proxy.address", "https://proxy:8443")
            .put("gcs.auth.proxy.username", "user")
            .put("gcs.auth.proxy.password", "pass")
            .put("gcs.auth.http.connect-timeout-ms", "8000")
            .put("gcs.auth.http.read-timeout-ms", "10000")
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
    Map<String, String> map = ImmutableMap.of("auth.type", "UNAUTHENTICATED");

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, "");

    assertThat(options.getAuthType()).isEqualTo(AuthType.UNAUTHENTICATED);
  }

  @Test
  void createFromOptions_keyWithoutPrefix_isIgnored() {
    Map<String, String> map = ImmutableMap.of("auth.type", "UNAUTHENTICATED");

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, "gcs.");

    assertThat(options.getAuthType()).isEqualTo(AuthType.APPLICATION_DEFAULT);
  }

  @Test
  void createFromOptions_blankValues_areIgnored() {
    Map<String, String> map =
        ImmutableMap.<String, String>builder()
            .put("auth.type", "   ")
            .put("auth.service-account-json-keyfile", "   ")
            .put("auth.workload-identity-federation.credential-config-file", "   ")
            .put("auth.client-id", "   ")
            .put("auth.client-secret", "   ")
            .put("auth.refresh-token", "   ")
            .put("auth.impersonation-service-account", "   ")
            .put("auth.token-server-url", "   ")
            .put("auth.proxy.address", "   ")
            .put("auth.proxy.username", "   ")
            .put("auth.proxy.password", "   ")
            .put("auth.http.connect-timeout-ms", "   ")
            .put("auth.http.read-timeout-ms", "   ")
            .build();

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, "");

    assertThat(options).isEqualTo(GcsAuthOptions.builder().build());
  }

  @Test
  void createFromOptions_valuesWithWhitespace_areTrimmed() {
    Map<String, String> map = ImmutableMap.of("auth.client-id", "  client-id  ");

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, "");

    assertThat(options.getClientId()).hasValue("client-id");
  }

  @ParameterizedTest
  @ValueSource(strings = {"not-a-number", "10s", "2500ms", "0", "-5"})
  void createFromOptions_invalidConnectTimeout_throwsIllegalArgumentException(String timeout) {
    Map<String, String> map = ImmutableMap.of("auth.http.connect-timeout-ms", timeout);

    assertThrows(IllegalArgumentException.class, () -> GcsAuthOptions.createFromOptions(map, ""));
  }

  @ParameterizedTest
  @ValueSource(strings = {"not-a-number", "10s", "2500ms", "0", "-5"})
  void createFromOptions_invalidReadTimeout_throwsIllegalArgumentException(String timeout) {
    Map<String, String> map = ImmutableMap.of("auth.http.read-timeout-ms", timeout);

    assertThrows(IllegalArgumentException.class, () -> GcsAuthOptions.createFromOptions(map, ""));
  }

  @Test
  void createFromOptions_invalidTokenServerUrl_throwsIllegalArgumentException() {
    Map<String, String> map = ImmutableMap.of("auth.token-server-url", "http://invalid uri");

    assertThrows(IllegalArgumentException.class, () -> GcsAuthOptions.createFromOptions(map, ""));
  }

  @Test
  void createFromOptions_nullOptions_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> GcsAuthOptions.createFromOptions(null, "gcs."));
  }

  @Test
  void createFromOptions_nullPrefix_throwsNullPointerException() {
    Map<String, String> map = ImmutableMap.of("auth.type", "UNAUTHENTICATED");

    assertThrows(NullPointerException.class, () -> GcsAuthOptions.createFromOptions(map, null));
  }

  @Test
  void build_userCredentialsWithoutClientId_throwsIllegalStateException() {
    GcsAuthOptions.Builder optionsBuilder =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.USER_CREDENTIALS)
            .setClientSecret(RedactedString.create(CLIENT_SECRET_VALUE))
            .setRefreshToken(RedactedString.create(REFRESH_TOKEN_VALUE));

    assertThrows(IllegalStateException.class, optionsBuilder::build);
  }

  @Test
  void build_userCredentialsWithoutClientSecret_throwsIllegalStateException() {
    GcsAuthOptions.Builder optionsBuilder =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.USER_CREDENTIALS)
            .setClientId("my-client-id")
            .setRefreshToken(RedactedString.create(REFRESH_TOKEN_VALUE));

    assertThrows(IllegalStateException.class, optionsBuilder::build);
  }

  @Test
  void build_userCredentialsWithoutRefreshToken_throwsIllegalStateException() {
    GcsAuthOptions.Builder optionsBuilder =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.USER_CREDENTIALS)
            .setClientId("my-client-id")
            .setClientSecret(RedactedString.create(CLIENT_SECRET_VALUE));

    assertThrows(IllegalStateException.class, optionsBuilder::build);
  }

  @Test
  void build_serviceAccountKeyfileAuthWithoutKeyfile_throwsIllegalStateException() {
    GcsAuthOptions.Builder optionsBuilder =
        GcsAuthOptions.builder().setAuthType(AuthType.SERVICE_ACCOUNT_JSON_KEYFILE);

    assertThrows(IllegalStateException.class, optionsBuilder::build);
  }

  @Test
  void build_workloadIdentityFederationWithoutConfigFile_throwsIllegalStateException() {
    GcsAuthOptions.Builder optionsBuilder =
        GcsAuthOptions.builder().setAuthType(AuthType.WORKLOAD_IDENTITY_FEDERATION);

    assertThrows(IllegalStateException.class, optionsBuilder::build);
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
            "auth.type", "USER_CREDENTIALS",
            "auth.client-id", "client-id",
            "auth.refresh-token", "refresh-token");

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> GcsAuthOptions.createFromOptions(map, ""));

    assertThat(exception).hasMessageThat().contains("auth.client-secret");
  }

  private static GcsAuthOptions createOptionsWithSecrets() {
    return GcsAuthOptions.builder()
        .setAuthType(AuthType.USER_CREDENTIALS)
        .setClientId("my-client-id")
        .setClientSecret(RedactedString.create(CLIENT_SECRET_VALUE))
        .setRefreshToken(RedactedString.create(REFRESH_TOKEN_VALUE))
        .setProxyUsername(RedactedString.create(PROXY_USERNAME_VALUE))
        .setProxyPassword(RedactedString.create(PROXY_PASSWORD_VALUE))
        .build();
  }
}
