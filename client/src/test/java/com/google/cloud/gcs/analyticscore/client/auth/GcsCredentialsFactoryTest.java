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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mockStatic;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.Credentials;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.gcs.analyticscore.common.RedactedString;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.util.Base64;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

class GcsCredentialsFactoryTest {

  private static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";

  @TempDir static Path sharedTempDir;

  private static String serviceAccountKeyFile;

  private final HttpTransportFactory tokenTransportFactory = NetHttpTransport::new;
  private final HttpTransportFactory impersonationTransportFactory = NetHttpTransport::new;

  @BeforeAll
  static void writeServiceAccountKeyFile() throws GeneralSecurityException, IOException {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048);
    PrivateKey privateKey = keyPairGenerator.generateKeyPair().getPrivate();
    String keyFileJson =
        String.format(
            "{\"type\":\"service_account\","
                + "\"project_id\":\"test-project\","
                + "\"private_key_id\":\"test-key-id\","
                + "\"private_key\":\"%s\","
                + "\"client_email\":\"test-email@gserviceaccount.com\","
                + "\"client_id\":\"test-client-id\","
                + "\"token_uri\":\"https://accounts.google.com/o/oauth2/token\"}",
            toEscapedPem(privateKey));
    Path keyFile = sharedTempDir.resolve("service-account.json");
    Files.write(keyFile, keyFileJson.getBytes(UTF_8));
    serviceAccountKeyFile = keyFile.toString();
  }

  private static String toEscapedPem(PrivateKey privateKey) {
    String base64 =
        Base64.getMimeEncoder(64, "\n".getBytes(UTF_8)).encodeToString(privateKey.getEncoded());
    return ("-----BEGIN PRIVATE KEY-----\n" + base64 + "\n-----END PRIVATE KEY-----\n")
        .replace("\n", "\\n");
  }

  @Test
  void createCredentials_unauthenticated_returnsNoCredentials() throws IOException {
    GcsAuthOptions options = GcsAuthOptions.builder().setAuthType(AuthType.UNAUTHENTICATED).build();

    Credentials credentials = GcsCredentialsFactory.createCredentials(options);

    assertThat(credentials).isSameInstanceAs(NoCredentials.getInstance());
  }

  @Test
  void createCredentials_unauthenticatedWithBlankImpersonation_returnsNoCredentials()
      throws IOException {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.UNAUTHENTICATED)
            .setImpersonationServiceAccount("   ")
            .build();

    Credentials credentials = GcsCredentialsFactory.createCredentials(options);

    assertThat(credentials).isSameInstanceAs(NoCredentials.getInstance());
  }

  @Test
  void createCredentials_computeEngine_returnsComputeEngineCredentials() throws IOException {
    GcsAuthOptions options = GcsAuthOptions.builder().setAuthType(AuthType.COMPUTE_ENGINE).build();

    Credentials credentials = createCredentials(options);

    assertThat(credentials).isInstanceOf(ComputeEngineCredentials.class);
  }

  @Test
  void createCredentials_computeEngineWithTokenServerUri_ignoresTokenServerUri()
      throws IOException {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.COMPUTE_ENGINE)
            .setTokenServerUri(URI.create("https://custom-auth.example.com/token"))
            .build();

    Credentials credentials = createCredentials(options);

    assertThat(credentials).isInstanceOf(ComputeEngineCredentials.class);
  }

  @Test
  void createCredentials_serviceAccountKeyfile_returnsServiceAccountCredentials()
      throws IOException {
    GcsAuthOptions options = serviceAccountOptions().build();

    Credentials credentials = createCredentials(options);

    assertThat(credentials).isInstanceOf(ServiceAccountCredentials.class);
    ServiceAccountCredentials saCreds = (ServiceAccountCredentials) credentials;
    assertThat(saCreds.getClientEmail()).isEqualTo("test-email@gserviceaccount.com");
    assertThat(saCreds.getPrivateKeyId()).isEqualTo("test-key-id");
  }

  @Test
  void createCredentials_serviceAccountKeyfile_appliesCloudPlatformScope() throws IOException {
    GcsAuthOptions options = serviceAccountOptions().build();

    Credentials credentials = createCredentials(options);

    assertThat(((ServiceAccountCredentials) credentials).getScopes())
        .containsExactly(CLOUD_PLATFORM_SCOPE);
  }

  @Test
  void createCredentials_serviceAccountKeyfileWithTokenServerUri_usesCustomTokenServerUri()
      throws IOException {
    URI customUri = URI.create("https://custom-auth.example.com/token");
    GcsAuthOptions options = serviceAccountOptions().setTokenServerUri(customUri).build();

    Credentials credentials = createCredentials(options);

    assertThat(((ServiceAccountCredentials) credentials).toBuilder().getTokenServerUri())
        .isEqualTo(customUri);
  }

  @Test
  void createCredentials_serviceAccountKeyfileWithTokenServerUri_keepsCloudPlatformScope()
      throws IOException {
    GcsAuthOptions options =
        serviceAccountOptions()
            .setTokenServerUri(URI.create("https://custom-auth.example.com/token"))
            .build();

    Credentials credentials = createCredentials(options);

    assertThat(((ServiceAccountCredentials) credentials).getScopes())
        .containsExactly(CLOUD_PLATFORM_SCOPE);
  }

  @Test
  void createCredentials_impersonationServiceAccountSet_returnsImpersonatedCredentials()
      throws IOException {
    String targetSa = "target-impersonated@test-project.iam.gserviceaccount.com";
    GcsAuthOptions options =
        serviceAccountOptions().setImpersonationServiceAccount(targetSa).build();

    Credentials credentials = createCredentials(options);

    assertThat(credentials).isInstanceOf(ImpersonatedCredentials.class);
    assertThat(((ImpersonatedCredentials) credentials).getAccount()).isEqualTo(targetSa);
  }

  @Test
  void createCredentials_impersonationServiceAccountSet_usesImpersonationTransport()
      throws IOException {
    GcsAuthOptions options =
        serviceAccountOptions()
            .setImpersonationServiceAccount("target@test-project.iam.gserviceaccount.com")
            .build();

    Credentials credentials = createCredentials(options);

    assertThat(((ImpersonatedCredentials) credentials).toBuilder().getHttpTransportFactory())
        .isSameInstanceAs(impersonationTransportFactory);
  }

  @Test
  void createCredentials_impersonationWithTokenServerUri_impersonatesCustomTokenSource()
      throws IOException {
    URI customUri = URI.create("https://custom-auth.example.com/token");
    GcsAuthOptions options =
        serviceAccountOptions()
            .setImpersonationServiceAccount("target@test-project.iam.gserviceaccount.com")
            .setTokenServerUri(customUri)
            .build();

    Credentials credentials = createCredentials(options);

    ServiceAccountCredentials sourceCredentials =
        (ServiceAccountCredentials) ((ImpersonatedCredentials) credentials).getSourceCredentials();
    assertThat(sourceCredentials.toBuilder().getTokenServerUri()).isEqualTo(customUri);
  }

  @Test
  void createCredentials_impersonationServiceAccountBlank_returnsSourceCredentials()
      throws IOException {
    GcsAuthOptions options = serviceAccountOptions().setImpersonationServiceAccount("   ").build();

    Credentials credentials = createCredentials(options);

    assertThat(credentials).isInstanceOf(ServiceAccountCredentials.class);
  }

  @Test
  void createCredentials_serviceAccountKeyfileMissing_throwsNoSuchFileException() {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.SERVICE_ACCOUNT_JSON_KEYFILE)
            .setServiceAccountJsonKeyfile("/non/existent/file.json")
            .build();

    assertThrows(NoSuchFileException.class, () -> createCredentials(options));
  }

  @Test
  void createCredentials_workloadIdentityFederation_returnsExternalAccountCredentials()
      throws IOException {
    GcsAuthOptions options = workloadIdentityFederationOptions().build();

    Credentials credentials = createCredentials(options);

    assertThat(credentials).isInstanceOf(ExternalAccountCredentials.class);
    ExternalAccountCredentials wifCreds = (ExternalAccountCredentials) credentials;
    assertThat(wifCreds.getAuthenticationType()).isEqualTo("OAuth2");
    assertThat(wifCreds.getAudience())
        .isEqualTo(
            "//iam.googleapis.com/projects/test/locations/global/workloadIdentityPools/test-pool/providers/tester");
  }

  @Test
  void createCredentials_workloadIdentityFederation_appliesCloudPlatformScope() throws IOException {
    GcsAuthOptions options = workloadIdentityFederationOptions().build();

    Credentials credentials = createCredentials(options);

    assertThat(((ExternalAccountCredentials) credentials).getScopes())
        .containsExactly(CLOUD_PLATFORM_SCOPE);
  }

  @Test
  void createCredentials_workloadIdentityFederationWithTokenServerUri_ignoresTokenServerUri()
      throws IOException {
    GcsAuthOptions options =
        workloadIdentityFederationOptions()
            .setTokenServerUri(URI.create("https://custom-auth.example.com/token"))
            .build();

    Credentials credentials = createCredentials(options);

    assertThat(((ExternalAccountCredentials) credentials).getTokenUrl())
        .isEqualTo("https://sts.googleapis.com/v1/token");
  }

  @Test
  void createCredentials_userCredentials_returnsUserCredentials() throws IOException {
    GcsAuthOptions options = userCredentialsOptions().build();

    Credentials credentials = createCredentials(options);

    assertThat(credentials).isInstanceOf(UserCredentials.class);
    UserCredentials userCreds = (UserCredentials) credentials;
    assertThat(userCreds.getClientId()).isEqualTo("test-client-id");
    assertThat(userCreds.getClientSecret()).isEqualTo("test-client-secret");
    assertThat(userCreds.getRefreshToken()).isEqualTo("test-refresh-token");
  }

  @Test
  void createCredentials_userCredentialsWithTokenServerUri_usesCustomTokenServerUri()
      throws IOException {
    URI customUri = URI.create("https://custom-token.example.com");
    GcsAuthOptions options = userCredentialsOptions().setTokenServerUri(customUri).build();

    Credentials credentials = createCredentials(options);

    assertThat(((UserCredentials) credentials).toBuilder().getTokenServerUri())
        .isEqualTo(customUri);
  }

  @Test
  void createCredentials_defaultTransport_returnsCredentialsWithTransports() throws IOException {
    GcsAuthOptions options =
        serviceAccountOptions()
            .setImpersonationServiceAccount("target@test-project.iam.gserviceaccount.com")
            .build();

    ImpersonatedCredentials credentials =
        (ImpersonatedCredentials) GcsCredentialsFactory.createCredentials(options);
    ServiceAccountCredentials sourceCredentials =
        (ServiceAccountCredentials) credentials.getSourceCredentials();

    assertThat(sourceCredentials.toBuilder().getHttpTransportFactory().create()).isNotNull();
    assertThat(credentials.toBuilder().getHttpTransportFactory().create()).isNotNull();
  }

  @Test
  void createCredentials_nullOptions_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> GcsCredentialsFactory.createCredentials(null));
  }

  @Test
  void createCredentials_applicationDefault_returnsScopedApplicationDefaultCredentials()
      throws IOException {
    GcsAuthOptions options =
        GcsAuthOptions.builder().setAuthType(AuthType.APPLICATION_DEFAULT).build();
    GoogleCredentials baseCredentials =
        ComputeEngineCredentials.newBuilder()
            .setHttpTransportFactory(tokenTransportFactory)
            .build();

    try (MockedStatic<GoogleCredentials> mockedGoogleCredentials =
        mockStatic(GoogleCredentials.class)) {
      mockedGoogleCredentials
          .when(() -> GoogleCredentials.getApplicationDefault(tokenTransportFactory))
          .thenReturn(baseCredentials);

      Credentials credentials = createCredentials(options);

      assertThat(credentials).isInstanceOf(ComputeEngineCredentials.class);
      assertThat(((ComputeEngineCredentials) credentials).getScopes())
          .containsExactly(CLOUD_PLATFORM_SCOPE);
    }
  }

  @Test
  void createCredentials_threeArgWithUnauthenticated_throwsAssertionError() {
    GcsAuthOptions options = GcsAuthOptions.builder().setAuthType(AuthType.UNAUTHENTICATED).build();

    assertThrows(AssertionError.class, () -> createCredentials(options));
  }

  @Test
  void createImpersonationTransport_noImpersonation_reusesTokenTransport() throws IOException {
    HttpTransport tokenTransport = new NetHttpTransport();
    GcsAuthOptions options = serviceAccountOptions().build();

    HttpTransport impersonationTransport =
        GcsCredentialsFactory.createImpersonationTransport(options, tokenTransport);

    assertThat(impersonationTransport).isSameInstanceAs(tokenTransport);
  }

  @Test
  void createImpersonationTransport_impersonationWithDefaultTokenServer_reusesTokenTransport()
      throws IOException {
    HttpTransport tokenTransport = new NetHttpTransport();
    GcsAuthOptions options =
        serviceAccountOptions()
            .setImpersonationServiceAccount("target@test-project.iam.gserviceaccount.com")
            .build();

    HttpTransport impersonationTransport =
        GcsCredentialsFactory.createImpersonationTransport(options, tokenTransport);

    assertThat(impersonationTransport).isSameInstanceAs(tokenTransport);
  }

  @Test
  void createImpersonationTransport_impersonationWithCustomTokenServer_createsSeparateTransport()
      throws IOException {
    HttpTransport tokenTransport = new NetHttpTransport();
    GcsAuthOptions options =
        serviceAccountOptions()
            .setImpersonationServiceAccount("target@test-project.iam.gserviceaccount.com")
            .setTokenServerUri(URI.create("https://custom-auth.example.com/token"))
            .build();

    HttpTransport impersonationTransport =
        GcsCredentialsFactory.createImpersonationTransport(options, tokenTransport);

    assertThat(impersonationTransport).isNotSameInstanceAs(tokenTransport);
  }

  @Test
  void tokenTrustStoreSource_noTokenServerUri_isGoogleBundled() {
    GcsAuthOptions options = GcsAuthOptions.builder().build();

    TrustStoreSource trustStoreSource = GcsCredentialsFactory.tokenTrustStoreSource(options);

    assertThat(trustStoreSource).isEqualTo(TrustStoreSource.GOOGLE_BUNDLED);
  }

  @Test
  void tokenTrustStoreSource_customTokenServerUri_isSystemDefault() {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setTokenServerUri(URI.create("https://custom-auth.example.com/token"))
            .build();

    TrustStoreSource trustStoreSource = GcsCredentialsFactory.tokenTrustStoreSource(options);

    assertThat(trustStoreSource).isEqualTo(TrustStoreSource.SYSTEM_DEFAULT);
  }

  private Credentials createCredentials(GcsAuthOptions options) throws IOException {
    return GcsCredentialsFactory.createCredentials(
        options, tokenTransportFactory, impersonationTransportFactory);
  }

  private static GcsAuthOptions.Builder serviceAccountOptions() {
    return GcsAuthOptions.builder()
        .setAuthType(AuthType.SERVICE_ACCOUNT_JSON_KEYFILE)
        .setServiceAccountJsonKeyfile(serviceAccountKeyFile);
  }

  private static GcsAuthOptions.Builder workloadIdentityFederationOptions() {
    return GcsAuthOptions.builder()
        .setAuthType(AuthType.WORKLOAD_IDENTITY_FEDERATION)
        .setWorkloadIdentityCredentialConfigFile(
            getResourcePath("test-workload-identity-config.json"));
  }

  private static GcsAuthOptions.Builder userCredentialsOptions() {
    return GcsAuthOptions.builder()
        .setAuthType(AuthType.USER_CREDENTIALS)
        .setClientId("test-client-id")
        .setClientSecret(RedactedString.create("test-client-secret"))
        .setRefreshToken(RedactedString.create("test-refresh-token"));
  }

  private static String getResourcePath(String resourceName) {
    try {
      return Paths.get(Resources.getResource(resourceName).toURI()).toString();
    } catch (URISyntaxException e) {
      throw new AssertionError("Test resource " + resourceName + " is not a valid URI", e);
    }
  }
}
