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

import static com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Operation.GCS_CLIENT_CREATE;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.gcs.analyticscore.client.GcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.CustomTelemetryOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.MetricKey;
import com.google.cloud.gcs.analyticscore.common.telemetry.Operation;
import com.google.cloud.gcs.analyticscore.common.telemetry.OperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.TelemetryOptions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Authenticator;
import java.net.URI;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Checks that the {@code analytics-core.auth.proxy.*} settings send GCS and token requests through
 * an HTTP {@code CONNECT} proxy.
 *
 * <p>The {@link ConnectProxy} never opens a tunnel, so every request fails right after the proxy
 * records it. The tests check what the proxy saw, which needs no network and no TLS. Each test
 * first checks that the proxy asked for credentials, so a request that never reached the proxy is
 * not mistaken for missing credentials.
 *
 * <p>Java reads {@code jdk.http.auth.tunneling.disabledSchemes} only once, when the JVM creates its
 * first HTTP connection object. The {@code integration-test} profile runs each test class in its
 * own JVM ({@code reuseForks=false}), so the first test that runs in this class checks that the
 * library clears the property before that first connection (see {@code #416}). The other tests
 * reuse what Java read then. Because of this:
 *
 * <ul>
 *   <li>The {@code UNAUTHENTICATED} test runs first. It builds no token transport up front, and
 *       while it builds the file system a telemetry listener makes Java read the property right
 *       before the GCS client is created. So it catches the library clearing the property too late,
 *       however the project ID is found.
 *   <li>Run this class on its own from an IDE.
 *   <li>Put any proxy test without a username in its own test class. If it ran first here, Java
 *       would read the property before the library clears it, and every test here would fail.
 * </ul>
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ProxyAuthIntegrationTest {

  private static final URI PUBLIC_OBJECT =
      URI.create("gs://cloud-samples-data/bigquery/us-states/us-states.csv");

  private static final String STORAGE_TARGET = "storage.googleapis.com:443";
  private static final String TOKEN_SERVER_TARGET =
      TestServiceAccountKeyFile.TOKEN_SERVER_HOST + ":443";

  private static final String PROXY_NOT_REACHED_MESSAGE =
      "The request never reached the proxy, so the failure is not about proxy credentials."
          + " Root cause: %s";
  private static final String FRESH_JVM_FAILURE_MESSAGE =
      "The JDK sent no Basic proxy credentials. Either building the file system did not clear"
          + " jdk.http.auth.tunneling.disabledSchemes (#416), or another class opened the first"
          + " HTTP connection in this JVM; run this class on its own from an IDE.";
  private static final String SCHEMES_NOT_READ_MESSAGE =
      "The telemetry listener did not make Java read jdk.http.auth.tunneling.disabledSchemes"
          + " while the file system was built, so this test cannot catch #416. Either"
          + " GcsFileSystemImpl no longer measures GCS_CLIENT_CREATE, or the listener failed.";

  private static final String PROXY_USERNAME = "test-proxy-user";
  private static final String PROXY_PASSWORD = "test-proxy-password";

  @TempDir Path tempDir;

  @Nullable private Authenticator originalAuthenticator;

  @BeforeEach
  void captureAuthenticator() {
    originalAuthenticator = Authenticator.getDefault();
  }

  @AfterEach
  void restoreAuthenticator() {
    Authenticator.setDefault(originalAuthenticator);
  }

  @Test
  @Order(1)
  void getFileInfoObject_proxyWithPassword_sendsAuthenticatedConnectToStorage() throws IOException {
    TunnelingSchemesReader schemesReader = new TunnelingSchemesReader();
    try (ConnectProxy proxy = ConnectProxy.requiringPassword(PROXY_USERNAME, PROXY_PASSWORD);
        GcsFileSystemImpl fileSystem =
            createFileSystem(unauthenticatedProxyConfig(proxy, PROXY_PASSWORD), schemesReader)) {
      IOException failure =
          assertThrows(IOException.class, () -> fileSystem.getFileInfoObject(PUBLIC_OBJECT));

      assertWithMessage(SCHEMES_NOT_READ_MESSAGE).that(schemesReader.hasRead()).isTrue();
      assertCredentialsSentAfterChallenge(
          proxy, proxy.authenticatedTargets(), STORAGE_TARGET, failure);
    }
  }

  @Test
  void getFileInfoObject_wrongProxyPassword_sendsCredentialsThatProxyRejects() throws IOException {
    try (ConnectProxy proxy = ConnectProxy.requiringPassword(PROXY_USERNAME, PROXY_PASSWORD);
        GcsFileSystemImpl fileSystem =
            createFileSystem(unauthenticatedProxyConfig(proxy, "wrong-password"))) {
      IOException failure =
          assertThrows(IOException.class, () -> fileSystem.getFileInfoObject(PUBLIC_OBJECT));

      assertCredentialsSentAfterChallenge(proxy, proxy.rejectedTargets(), STORAGE_TARGET, failure);
    }
  }

  @Test
  void getFileInfoObject_proxyWithServiceAccountKeyfile_sendsAuthenticatedConnectToTokenServer()
      throws GeneralSecurityException, IOException {
    Path keyFile = TestServiceAccountKeyFile.writeTo(tempDir);
    try (ConnectProxy proxy = ConnectProxy.requiringPassword(PROXY_USERNAME, PROXY_PASSWORD);
        GcsFileSystemImpl fileSystem =
            createFileSystem(
                ImmutableMap.of(
                    "analytics-core.auth.type",
                    "SERVICE_ACCOUNT_JSON_KEYFILE",
                    "analytics-core.auth.service-account-json-keyfile",
                    keyFile.toString(),
                    "analytics-core.auth.proxy.address",
                    proxy.address(),
                    "analytics-core.auth.proxy.username",
                    PROXY_USERNAME,
                    "analytics-core.auth.proxy.password",
                    PROXY_PASSWORD,
                    // Turns off Storage retries only. google-auth still retries the failed token
                    // request 3 times (about 1 + 2 + 4 seconds), so this test takes about 7
                    // seconds.
                    "analytics-core.retry.max-attempts",
                    "1"))) {
      IOException failure =
          assertThrows(IOException.class, () -> fileSystem.getFileInfoObject(PUBLIC_OBJECT));

      assertCredentialsSentAfterChallenge(
          proxy, proxy.authenticatedTargets(), TOKEN_SERVER_TARGET, failure);
    }
  }

  /**
   * Checks that the proxy asked for credentials for {@code target}, and that the client then sent
   * credentials that the proxy recorded in {@code targetsWithCredentials}.
   */
  private static void assertCredentialsSentAfterChallenge(
      ConnectProxy proxy,
      ImmutableList<String> targetsWithCredentials,
      String target,
      IOException failure) {
    assertWithMessage(PROXY_NOT_REACHED_MESSAGE, Throwables.getRootCause(failure))
        .that(proxy.challengedTargets())
        .contains(target);
    assertWithMessage(FRESH_JVM_FAILURE_MESSAGE).that(targetsWithCredentials).contains(target);
  }

  private static GcsFileSystemImpl createFileSystem(ImmutableMap<String, String> config) {
    return new GcsFileSystemImpl(GcsFileSystemOptions.createFromOptions(config, ""));
  }

  private static GcsFileSystemImpl createFileSystem(
      ImmutableMap<String, String> config, OperationListener listener) {
    TelemetryOptions telemetryOptions =
        TelemetryOptions.builder()
            .setCustomTelemetryOptions(
                CustomTelemetryOptions.builder()
                    .setOperationListeners(ImmutableList.of(listener))
                    .build())
            .build();
    return new GcsFileSystemImpl(
        GcsFileSystemOptions.createFromOptions(config, "").toBuilder()
            .setAnalyticsCoreTelemetryOptions(telemetryOptions)
            .build());
  }

  private static ImmutableMap<String, String> unauthenticatedProxyConfig(
      ConnectProxy proxy, String password) {
    return ImmutableMap.of(
        "analytics-core.auth.type",
        "UNAUTHENTICATED",
        "analytics-core.auth.proxy.address",
        proxy.address(),
        "analytics-core.auth.proxy.username",
        PROXY_USERNAME,
        "analytics-core.auth.proxy.password",
        password,
        "analytics-core.retry.max-attempts",
        "1");
  }

  /**
   * Makes Java read {@code jdk.http.auth.tunneling.disabledSchemes} when the file system starts to
   * create its GCS client, as the project ID lookup or the GCS client could while the file system
   * is being built. Creating an {@code http} connection object loads the JDK HTTP client, which
   * reads the property then. It opens no socket.
   */
  private static final class TunnelingSchemesReader implements OperationListener {

    private final AtomicBoolean read = new AtomicBoolean();

    @Override
    public void onOperationStart(Operation operation) {
      if (!operation.getName().equals(GCS_CLIENT_CREATE.name())) {
        return;
      }
      try {
        URI.create("http://localhost").toURL().openConnection();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      read.set(true);
    }

    @Override
    public void onOperationEnd(Operation operation, Map<MetricKey, Long> metrics) {}

    boolean hasRead() {
      return read.get();
    }
  }
}
