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

import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.gcs.analyticscore.client.GcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.Authenticator;
import java.net.URI;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
 * <p>Java reads {@code jdk.http.auth.tunneling.disabledSchemes} only once, when the JVM opens its
 * first HTTP connection. The {@code integration-test} profile runs each test class in its own JVM
 * ({@code reuseForks=false}), so the first test that runs in this class checks that the library
 * clears the property before that first connection (see {@code #416}). The other tests reuse what
 * Java read then. Because of this:
 *
 * <ul>
 *   <li>Run this class on its own from an IDE.
 *   <li>Put any proxy test without a username in its own test class. If it ran first here, Java
 *       would read the property before the library clears it, and every test here would fail.
 * </ul>
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
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
      "The JDK sent no Basic proxy credentials. This test class must open the first HTTP"
          + " connection in its JVM; run it on its own from an IDE (#416).";

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
  void getFileInfoObject_proxyWithPassword_sendsAuthenticatedConnectToStorage() throws IOException {
    try (ConnectProxy proxy = ConnectProxy.requiringPassword(PROXY_USERNAME, PROXY_PASSWORD);
        GcsFileSystemImpl fileSystem =
            createFileSystem(unauthenticatedProxyConfig(proxy, PROXY_PASSWORD))) {
      IOException failure =
          assertThrows(IOException.class, () -> fileSystem.getFileInfoObject(PUBLIC_OBJECT));

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
}
