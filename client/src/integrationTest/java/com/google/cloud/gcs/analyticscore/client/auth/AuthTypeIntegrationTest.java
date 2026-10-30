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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.AccessDeniedException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

/**
 * Checks that each {@code analytics-core.auth.type} gives credentials that real GCS accepts or
 * rejects as expected.
 *
 * <p>Keyfile and user credentials get their access token from a {@link FakeTokenServer}, because
 * org policy blocks service account keys in the test projects and there is no test user with a
 * refresh token. The fake server hands out a real token from Application Default Credentials, so
 * GCS still checks it. The server hands out the token only for the grant that the auth type must
 * send, so each test also checks which grant the client sent.
 *
 * <p>Workload Identity Federation has no test here. Org policy blocks workload identity pools in
 * the test projects, and a {@link FakeTokenServer} cannot stand in for the token exchange service,
 * because google-auth accepts only an {@code https} token URL for it, and the library checks that
 * URL against Google's bundled certificates. {@code GcsCredentialsFactoryTest} checks its token
 * exchange instead.
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class AuthTypeIntegrationTest {

  private static final URI PRIVATE_OBJECT =
      URI.create("gs://gcs-connector-private-test-bucket-do-not-delete/tpch_customer_1.parquet");

  /** The magic bytes every Parquet file starts with. */
  private static final String PARQUET_MAGIC = "PAR1";

  private static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";

  private static final String USER_CLIENT_ID = "test-client-id";
  private static final String USER_REFRESH_TOKEN = "test-refresh-token";

  /** The grant that service account credentials send, signed with the keyfile's private key. */
  private static final ImmutableMap<String, String> JWT_BEARER_GRANT =
      ImmutableMap.of("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer");

  /** The grant that user credentials send to swap their refresh token for an access token. */
  private static final ImmutableMap<String, String> REFRESH_TOKEN_GRANT =
      ImmutableMap.of(
          "grant_type",
          "refresh_token",
          "refresh_token",
          USER_REFRESH_TOKEN,
          "client_id",
          USER_CLIENT_ID);

  @TempDir Path tempDir;

  @Test
  void open_applicationDefaultOverGrpc_readsPrivateObject() throws IOException {
    try (GcsFileSystemImpl fileSystem =
        createFileSystem(
            ImmutableMap.of(
                "analytics-core.auth.type", "APPLICATION_DEFAULT", "client.protocol", "GRPC"))) {
      String magic = readParquetMagic(fileSystem);

      assertThat(magic).isEqualTo(PARQUET_MAGIC);
    }
  }

  // Only the Kokoro metadata server serves an account that can read the private test object.
  @Test
  @EnabledIfEnvironmentVariable(named = "KOKORO_ARTIFACTS_DIR", matches = ".+")
  void open_computeEngine_readsPrivateObject() throws IOException {
    try (GcsFileSystemImpl fileSystem =
        createFileSystem(ImmutableMap.of("analytics-core.auth.type", "COMPUTE_ENGINE"))) {
      String magic = readParquetMagic(fileSystem);

      assertThat(magic).isEqualTo(PARQUET_MAGIC);
    }
  }

  @Test
  void getFileInfoObject_unauthenticated_throwsAccessDeniedException() {
    try (GcsFileSystemImpl fileSystem =
        createFileSystem(ImmutableMap.of("analytics-core.auth.type", "UNAUTHENTICATED"))) {
      assertThrows(AccessDeniedException.class, () -> fileSystem.getFileInfoObject(PRIVATE_OBJECT));
    }
  }

  @Test
  void getFileInfoObject_serviceAccountKeyfile_authenticatesWithJwtBearerGrant()
      throws GeneralSecurityException, IOException {
    Path keyFile = TestServiceAccountKeyFile.writeTo(tempDir);
    try (FakeTokenServer tokenServer =
            FakeTokenServer.grantingAccessToken(
                fetchAccessToken().getTokenValue(), JWT_BEARER_GRANT);
        GcsFileSystemImpl fileSystem =
            createFileSystem(
                ImmutableMap.of(
                    "analytics-core.auth.type",
                    "SERVICE_ACCOUNT_JSON_KEYFILE",
                    "analytics-core.auth.service-account-json-keyfile",
                    keyFile.toString(),
                    "analytics-core.auth.token-server-uri",
                    tokenServer.tokenUri().toString()))) {
      // The lookup throws if the token server refuses the grant or GCS rejects the token.
      fileSystem.getFileInfoObject(PRIVATE_OBJECT);

      assertThat(tokenServer.grantedRequestCount()).isGreaterThan(0);
    }
  }

  @Test
  void getFileInfoObject_userCredentials_authenticatesWithRefreshTokenGrant() throws IOException {
    try (FakeTokenServer tokenServer =
            FakeTokenServer.grantingAccessToken(
                fetchAccessToken().getTokenValue(), REFRESH_TOKEN_GRANT);
        GcsFileSystemImpl fileSystem =
            createFileSystem(
                ImmutableMap.of(
                    "analytics-core.auth.type",
                    "USER_CREDENTIALS",
                    "analytics-core.auth.client-id",
                    USER_CLIENT_ID,
                    "analytics-core.auth.client-secret",
                    "test-client-secret",
                    "analytics-core.auth.refresh-token",
                    USER_REFRESH_TOKEN,
                    "analytics-core.auth.token-server-uri",
                    tokenServer.tokenUri().toString()))) {
      // The lookup throws if the token server refuses the grant or GCS rejects the token.
      fileSystem.getFileInfoObject(PRIVATE_OBJECT);

      assertThat(tokenServer.grantedRequestCount()).isGreaterThan(0);
    }
  }

  private static GcsFileSystemImpl createFileSystem(ImmutableMap<String, String> config) {
    return new GcsFileSystemImpl(GcsFileSystemOptions.createFromOptions(config, ""));
  }

  /** Returns a current access token from Application Default Credentials. */
  private static AccessToken fetchAccessToken() throws IOException {
    GoogleCredentials credentials =
        GoogleCredentials.getApplicationDefault().createScoped(CLOUD_PLATFORM_SCOPE);
    credentials.refreshIfExpired();
    return credentials.getAccessToken();
  }

  /** Reads the Parquet header bytes of {@link #PRIVATE_OBJECT} as a UTF-8 string. */
  private static String readParquetMagic(GcsFileSystem fileSystem) throws IOException {
    GcsFileInfo fileInfo = fileSystem.getFileInfoObject(PRIVATE_OBJECT);
    try (VectoredSeekableByteChannel channel =
        fileSystem.open(fileInfo, GcsReadOptions.builder().build())) {
      ByteBuffer buffer = ByteBuffer.allocate(PARQUET_MAGIC.length());
      while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
        // Keep reading until the buffer is full or the object ends.
      }
      return new String(buffer.array(), 0, buffer.position(), UTF_8);
    }
  }
}
