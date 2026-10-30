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
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.withSettings;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
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
import com.google.common.base.VerifyException;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.Authenticator;
import java.net.Authenticator.RequestorType;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Locale;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

class GcsCredentialsFactoryTest {

  private static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";
  private static final String TUNNELING_DISABLED_SCHEMES_PROPERTY =
      "jdk.http.auth.tunneling.disabledSchemes";
  private static final String PROXY_HOST = "proxy.example.com";
  private static final int PROXY_PORT = 8080;

  private static final String STS_TOKEN_URL = "https://sts.googleapis.com/v1/token";

  @TempDir static Path sharedTempDir;

  private static String serviceAccountKeyFile;

  private final HttpTransportFactory tokenTransportFactory = NetHttpTransport::new;
  private final HttpTransportFactory impersonationTransportFactory = NetHttpTransport::new;
  private Authenticator originalAuthenticator;
  private String originalTunnelingDisabledSchemes;

  @BeforeEach
  void captureGlobalState() {
    originalAuthenticator = Authenticator.getDefault();
    originalTunnelingDisabledSchemes = System.getProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY);
  }

  @AfterEach
  void restoreGlobalState() {
    Authenticator.setDefault(originalAuthenticator);
    if (originalTunnelingDisabledSchemes == null) {
      System.clearProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY);
    } else {
      System.setProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY, originalTunnelingDisabledSchemes);
    }
  }

  @BeforeAll
  static void writeServiceAccountKeyFile() throws GeneralSecurityException, IOException {
    serviceAccountKeyFile = TestServiceAccountKeyFile.writeTo(sharedTempDir).toString();
  }

  @Test
  void createCredentials_unauthenticated_returnsNoCredentials() throws IOException {
    GcsAuthOptions options = GcsAuthOptions.builder().setAuthType(AuthType.UNAUTHENTICATED).build();

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

    assertThat(credentials)
        .isEqualTo(
            createCredentials(
                GcsAuthOptions.builder().setAuthType(AuthType.COMPUTE_ENGINE).build()));
  }

  @Test
  void createCredentials_serviceAccountKeyfile_returnsServiceAccountCredentials()
      throws IOException {
    GcsAuthOptions options = serviceAccountOptions().build();

    Credentials credentials = createCredentials(options);

    assertThat(((ServiceAccountCredentials) credentials).getClientEmail())
        .isEqualTo(TestServiceAccountKeyFile.CLIENT_EMAIL);
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

    assertThat(((ExternalAccountCredentials) credentials).getAudience())
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

    assertThat(((ExternalAccountCredentials) credentials).getTokenUrl()).isEqualTo(STS_TOKEN_URL);
  }

  @Test
  void createCredentials_workloadIdentityFederation_sendsTokenExchangeThroughTokenTransport()
      throws IOException {
    MockHttpTransport tokenTransport = createSuccessfulTokenExchangeTransport();
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.WORKLOAD_IDENTITY_FEDERATION)
            .setWorkloadIdentityCredentialConfigFile(writeFileSourcedWorkloadIdentityConfig())
            .build();
    Credentials credentials =
        GcsCredentialsFactory.createCredentials(
            options, () -> tokenTransport, impersonationTransportFactory);

    credentials.getRequestMetadata();

    assertThat(tokenTransport.getLowLevelHttpRequest().getUrl()).isEqualTo(STS_TOKEN_URL);
  }

  @Test
  void createCredentials_userCredentials_returnsUserCredentials() throws IOException {
    GcsAuthOptions options = userCredentialsOptions().build();

    Credentials credentials = createCredentials(options);

    assertThat(credentials)
        .isEqualTo(
            UserCredentials.newBuilder()
                .setClientId("test-client-id")
                .setClientSecret("test-client-secret")
                .setRefreshToken("test-refresh-token")
                .setHttpTransportFactory(tokenTransportFactory)
                .build());
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
  void createCredentials_impersonationWithDefaultTokenServer_sharesTokenTransport()
      throws IOException {
    GcsAuthOptions options =
        serviceAccountOptions()
            .setImpersonationServiceAccount("target@test-project.iam.gserviceaccount.com")
            .build();

    ImpersonatedCredentials credentials =
        (ImpersonatedCredentials) GcsCredentialsFactory.createCredentials(options);
    ServiceAccountCredentials sourceCredentials =
        (ServiceAccountCredentials) credentials.getSourceCredentials();

    assertThat(credentials.toBuilder().getHttpTransportFactory().create())
        .isSameInstanceAs(sourceCredentials.toBuilder().getHttpTransportFactory().create());
  }

  @Test
  void createCredentials_nullOptions_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> GcsCredentialsFactory.createCredentials(null));
  }

  @Test
  void createCredentials_unauthenticatedWithProxyCredentials_enablesBasicTunneling()
      throws IOException {
    System.setProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY, "Basic");

    GcsCredentialsFactory.createCredentials(proxyCredentialsOptions(AuthType.UNAUTHENTICATED));

    assertThat(System.getProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY)).isEmpty();
  }

  @Test
  void createCredentials_unauthenticatedWithProxyCredentials_installsAuthenticator()
      throws IOException {
    Authenticator.setDefault(null);

    GcsCredentialsFactory.createCredentials(proxyCredentialsOptions(AuthType.UNAUTHENTICATED));

    assertThat(Authenticator.getDefault()).isNotNull();
  }

  @Test
  void createCredentials_withoutProxyCredentials_leavesDefaultAuthenticatorUnchanged()
      throws IOException {
    Authenticator sentinelAuthenticator = new Authenticator() {};
    Authenticator.setDefault(sentinelAuthenticator);

    GcsCredentialsFactory.createCredentials(
        GcsAuthOptions.builder().setAuthType(AuthType.UNAUTHENTICATED).build());

    assertThat(Authenticator.getDefault()).isSameInstanceAs(sentinelAuthenticator);
  }

  @Test
  void installProxyCredentialsIfConfigured_withProxyCredentials_enablesBasicTunneling() {
    System.setProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY, "Basic");

    GcsCredentialsFactory.installProxyCredentialsIfConfigured(proxyCredentialsOptions());

    assertThat(System.getProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY)).isEmpty();
  }

  @Test
  void installProxyCredentialsIfConfigured_withProxyCredentials_installsAuthenticator() {
    Authenticator.setDefault(null);

    GcsCredentialsFactory.installProxyCredentialsIfConfigured(proxyCredentialsOptions());

    assertThat(Authenticator.getDefault()).isNotNull();
  }

  @Test
  void installProxyCredentialsIfConfigured_withProxyCredentials_servesUsernameToProxy() {
    GcsCredentialsFactory.installProxyCredentialsIfConfigured(proxyCredentialsOptions());

    PasswordAuthentication credentials =
        requestProxyCredentials(PROXY_HOST, PROXY_PORT, RequestorType.PROXY);

    assertThat(credentials.getUserName()).isEqualTo("user");
  }

  @Test
  void installProxyCredentialsIfConfigured_withProxyCredentials_servesPasswordToProxy() {
    GcsCredentialsFactory.installProxyCredentialsIfConfigured(proxyCredentialsOptions());

    PasswordAuthentication credentials =
        requestProxyCredentials(PROXY_HOST, PROXY_PORT, RequestorType.PROXY);

    assertThat(credentials.getPassword()).isEqualTo("pass".toCharArray());
  }

  @Test
  void installProxyCredentialsIfConfigured_caseInsensitiveHost_servesCredentialsToProxy() {
    GcsCredentialsFactory.installProxyCredentialsIfConfigured(proxyCredentialsOptions());

    PasswordAuthentication credentials =
        requestProxyCredentials(
            PROXY_HOST.toUpperCase(Locale.ROOT), PROXY_PORT, RequestorType.PROXY);

    assertThat(credentials.getUserName()).isEqualTo("user");
  }

  @Test
  void installProxyCredentialsIfConfigured_differentHost_returnsNull() {
    GcsCredentialsFactory.installProxyCredentialsIfConfigured(proxyCredentialsOptions());

    PasswordAuthentication credentials =
        requestProxyCredentials("other.example.com", PROXY_PORT, RequestorType.PROXY);

    assertThat(credentials).isNull();
  }

  @Test
  void installProxyCredentialsIfConfigured_differentPort_returnsNull() {
    GcsCredentialsFactory.installProxyCredentialsIfConfigured(proxyCredentialsOptions());

    PasswordAuthentication credentials =
        requestProxyCredentials(PROXY_HOST, PROXY_PORT + 1, RequestorType.PROXY);

    assertThat(credentials).isNull();
  }

  @Test
  void installProxyCredentialsIfConfigured_serverRequestor_returnsNull() {
    GcsCredentialsFactory.installProxyCredentialsIfConfigured(proxyCredentialsOptions());

    PasswordAuthentication credentials =
        requestProxyCredentials(PROXY_HOST, PROXY_PORT, RequestorType.SERVER);

    assertThat(credentials).isNull();
  }

  @Test
  void installProxyCredentialsIfConfigured_nullRequestingHost_returnsNull() {
    GcsCredentialsFactory.installProxyCredentialsIfConfigured(proxyCredentialsOptions());

    PasswordAuthentication credentials =
        requestProxyCredentials(/* host= */ null, PROXY_PORT, RequestorType.PROXY);

    assertThat(credentials).isNull();
  }

  @Test
  void installProxyCredentialsIfConfigured_withoutProxyCredentials_leavesBasicTunnelingDisabled() {
    System.setProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY, "Basic");

    GcsCredentialsFactory.installProxyCredentialsIfConfigured(proxyWithoutCredentialsOptions());

    assertThat(System.getProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY)).isEqualTo("Basic");
  }

  @Test
  void installProxyCredentialsIfConfigured_withoutProxyCredentials_leavesAuthenticatorUnchanged() {
    Authenticator sentinelAuthenticator = new Authenticator() {};
    Authenticator.setDefault(sentinelAuthenticator);

    GcsCredentialsFactory.installProxyCredentialsIfConfigured(proxyWithoutCredentialsOptions());

    assertThat(Authenticator.getDefault()).isSameInstanceAs(sentinelAuthenticator);
  }

  @Test
  void installProxyCredentialsIfConfigured_nullOptions_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () -> GcsCredentialsFactory.installProxyCredentialsIfConfigured(null));
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
        mockStatic(GoogleCredentials.class, withSettings().defaultAnswer(CALLS_REAL_METHODS))) {
      mockedGoogleCredentials
          .when(() -> GoogleCredentials.getApplicationDefault(tokenTransportFactory))
          .thenReturn(baseCredentials);

      Credentials credentials = createCredentials(options);

      assertThat(((ComputeEngineCredentials) credentials).getScopes())
          .containsExactly(CLOUD_PLATFORM_SCOPE);
    }
  }

  @Test
  void createCredentials_applicationDefaultUnavailable_throwsIOException() {
    GcsAuthOptions options =
        GcsAuthOptions.builder().setAuthType(AuthType.APPLICATION_DEFAULT).build();

    try (MockedStatic<GoogleCredentials> mockedGoogleCredentials =
        mockStatic(GoogleCredentials.class, withSettings().defaultAnswer(CALLS_REAL_METHODS))) {
      mockedGoogleCredentials
          .when(() -> GoogleCredentials.getApplicationDefault(tokenTransportFactory))
          .thenThrow(new IOException("ADC not available"));

      assertThrows(IOException.class, () -> createCredentials(options));
    }
  }

  @Test
  void createCredentials_authTypeUnsetAndAdcAvailable_returnsScopedApplicationDefaultCredentials()
      throws IOException {
    GcsAuthOptions options = GcsAuthOptions.builder().build();
    GoogleCredentials baseCredentials =
        ComputeEngineCredentials.newBuilder()
            .setHttpTransportFactory(tokenTransportFactory)
            .build();

    try (MockedStatic<GoogleCredentials> mockedGoogleCredentials =
        mockStatic(GoogleCredentials.class, withSettings().defaultAnswer(CALLS_REAL_METHODS))) {
      mockedGoogleCredentials
          .when(() -> GoogleCredentials.getApplicationDefault(tokenTransportFactory))
          .thenReturn(baseCredentials);

      Credentials credentials = createCredentials(options);

      assertThat(((ComputeEngineCredentials) credentials).getScopes())
          .containsExactly(CLOUD_PLATFORM_SCOPE);
    }
  }

  @Test
  void createCredentials_authTypeUnsetAndAdcUnavailable_fallsBackToNoCredentials()
      throws IOException {
    GcsAuthOptions options = GcsAuthOptions.builder().build();

    try (MockedStatic<GoogleCredentials> mockedGoogleCredentials =
        mockStatic(GoogleCredentials.class, withSettings().defaultAnswer(CALLS_REAL_METHODS))) {
      mockedGoogleCredentials
          .when(() -> GoogleCredentials.getApplicationDefault(tokenTransportFactory))
          .thenThrow(new IOException("ADC not available"));

      Credentials credentials = createCredentials(options);

      assertThat(credentials).isSameInstanceAs(NoCredentials.getInstance());
    }
  }

  @Test
  void createCredentials_authTypeUnsetWithImpersonationAndAdcUnavailable_throwsIOException() {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setImpersonationServiceAccount("target@test-project.iam.gserviceaccount.com")
            .build();

    try (MockedStatic<GoogleCredentials> mockedGoogleCredentials =
        mockStatic(GoogleCredentials.class, withSettings().defaultAnswer(CALLS_REAL_METHODS))) {
      mockedGoogleCredentials
          .when(() -> GoogleCredentials.getApplicationDefault(tokenTransportFactory))
          .thenThrow(new IOException("ADC not available"));

      assertThrows(IOException.class, () -> createCredentials(options));
    }
  }

  @Test
  void createCredentials_authTypeUnsetWithTokenServerUriAndAdcUnavailable_throwsIOException() {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setTokenServerUri(URI.create("https://token.example.com/token"))
            .build();

    try (MockedStatic<GoogleCredentials> mockedGoogleCredentials =
        mockStatic(GoogleCredentials.class, withSettings().defaultAnswer(CALLS_REAL_METHODS))) {
      mockedGoogleCredentials
          .when(() -> GoogleCredentials.getApplicationDefault(tokenTransportFactory))
          .thenThrow(new IOException("ADC not available"));

      assertThrows(IOException.class, () -> createCredentials(options));
    }
  }

  @Test
  void createCredentials_unauthenticatedWithTransportFactories_throwsVerifyException() {
    GcsAuthOptions options = GcsAuthOptions.builder().setAuthType(AuthType.UNAUTHENTICATED).build();

    assertThrows(VerifyException.class, () -> createCredentials(options));
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
  void tokenTrustStoreSource_applicationDefaultWithCustomTokenServerUri_isSystemDefault() {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setTokenServerUri(URI.create("https://custom-auth.example.com/token"))
            .build();

    TrustStoreSource trustStoreSource = GcsCredentialsFactory.tokenTrustStoreSource(options);

    assertThat(trustStoreSource).isEqualTo(TrustStoreSource.SYSTEM_DEFAULT);
  }

  @Test
  void tokenTrustStoreSource_serviceAccountKeyfileWithCustomTokenServerUri_isSystemDefault() {
    GcsAuthOptions options =
        serviceAccountOptions()
            .setTokenServerUri(URI.create("https://custom-auth.example.com/token"))
            .build();

    TrustStoreSource trustStoreSource = GcsCredentialsFactory.tokenTrustStoreSource(options);

    assertThat(trustStoreSource).isEqualTo(TrustStoreSource.SYSTEM_DEFAULT);
  }

  @Test
  void tokenTrustStoreSource_workloadIdentityWithCustomTokenServerUri_isGoogleBundled() {
    GcsAuthOptions options =
        workloadIdentityFederationOptions()
            .setTokenServerUri(URI.create("https://custom-auth.example.com/token"))
            .build();

    TrustStoreSource trustStoreSource = GcsCredentialsFactory.tokenTrustStoreSource(options);

    assertThat(trustStoreSource).isEqualTo(TrustStoreSource.GOOGLE_BUNDLED);
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

  /**
   * Writes a Workload Identity Federation config that reads its subject token from a local file and
   * returns the config's path.
   *
   * <p>The shared test config reads its subject token from a URL, which would also go through the
   * token transport.
   */
  private static String writeFileSourcedWorkloadIdentityConfig() throws IOException {
    Path subjectTokenFile =
        Files.write(
            sharedTempDir.resolve("subject-token.txt"), "test-subject-token".getBytes(UTF_8));
    String config =
        String.format(
            "{\"type\":\"external_account\","
                + "\"audience\":\"//iam.googleapis.com/projects/test/locations/global/"
                + "workloadIdentityPools/test-pool/providers/tester\","
                + "\"subject_token_type\":\"urn:ietf:params:oauth:token-type:jwt\","
                + "\"token_url\":\"%s\","
                + "\"credential_source\":{\"file\":\"%s\"}}",
            STS_TOKEN_URL, subjectTokenFile.toString().replace("\\", "\\\\"));
    Path configFile = sharedTempDir.resolve("file-sourced-workload-identity-config.json");
    return Files.write(configFile, config.getBytes(UTF_8)).toString();
  }

  /** Returns a transport that answers every request with a successful token exchange. */
  private static MockHttpTransport createSuccessfulTokenExchangeTransport() {
    return new MockHttpTransport.Builder()
        .setLowLevelHttpResponse(
            new MockLowLevelHttpResponse()
                .setContentType("application/json")
                .setContent(
                    "{\"access_token\":\"test-access-token\","
                        + "\"issued_token_type\":\"urn:ietf:params:oauth:token-type:access_token\","
                        + "\"token_type\":\"Bearer\","
                        + "\"expires_in\":3600}"))
        .build();
  }

  private static GcsAuthOptions.Builder userCredentialsOptions() {
    return GcsAuthOptions.builder()
        .setAuthType(AuthType.USER_CREDENTIALS)
        .setClientId("test-client-id")
        .setClientSecret(RedactedString.create("test-client-secret"))
        .setRefreshToken(RedactedString.create("test-refresh-token"));
  }

  private static String getResourcePath(String resourceName) {
    Path file = sharedTempDir.resolve(resourceName);
    try {
      Files.write(file, Resources.toByteArray(Resources.getResource(resourceName)));
    } catch (IOException e) {
      throw new AssertionError("Failed to copy test resource " + resourceName, e);
    }
    return file.toString();
  }

  @Nullable
  private static PasswordAuthentication requestProxyCredentials(
      @Nullable String host, int port, RequestorType requestorType) {
    return Authenticator.requestPasswordAuthentication(
        host,
        /* addr= */ null,
        port,
        /* protocol= */ "http",
        /* prompt= */ "",
        /* scheme= */ "basic",
        /* url= */ null,
        requestorType);
  }

  private static GcsAuthOptions proxyCredentialsOptions() {
    return proxyCredentialsOptions(AuthType.APPLICATION_DEFAULT);
  }

  private static GcsAuthOptions proxyCredentialsOptions(AuthType authType) {
    return GcsAuthOptions.builder()
        .setAuthType(authType)
        .setProxyAddress(PROXY_HOST + ":" + PROXY_PORT)
        .setProxyUsername(RedactedString.create("user"))
        .setProxyPassword(RedactedString.create("pass"))
        .build();
  }

  private static GcsAuthOptions proxyWithoutCredentialsOptions() {
    return GcsAuthOptions.builder().setProxyAddress(PROXY_HOST + ":" + PROXY_PORT).build();
  }
}
