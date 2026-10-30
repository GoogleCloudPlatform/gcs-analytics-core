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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.util.Base64;

/**
 * Writes service account JSON keyfiles that hold a newly generated RSA key, for tests.
 *
 * <p>The key is not registered with Google, so Google's token endpoint rejects it. Tests that need
 * an access token point {@code analytics-core.auth.token-server-uri} at a local fake token server.
 */
final class TestServiceAccountKeyFile {

  static final String CLIENT_EMAIL = "test-email@gserviceaccount.com";
  static final String TOKEN_SERVER_HOST = "oauth2.googleapis.com";

  private TestServiceAccountKeyFile() {}

  /**
   * Generates an RSA key, writes it to {@code service-account.json} in {@code directory}, and
   * returns the keyfile's path.
   */
  static Path writeTo(Path directory) throws GeneralSecurityException, IOException {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048);
    PrivateKey privateKey = keyPairGenerator.generateKeyPair().getPrivate();
    String keyFileJson =
        String.format(
            "{\"type\":\"service_account\","
                + "\"project_id\":\"test-project\","
                + "\"private_key_id\":\"test-key-id\","
                + "\"private_key\":\"%s\","
                + "\"client_email\":\"%s\","
                + "\"client_id\":\"test-client-id\","
                + "\"token_uri\":\"https://%s/token\"}",
            toEscapedPem(privateKey), CLIENT_EMAIL, TOKEN_SERVER_HOST);
    return Files.write(directory.resolve("service-account.json"), keyFileJson.getBytes(UTF_8));
  }

  private static String toEscapedPem(PrivateKey privateKey) {
    String base64 =
        Base64.getMimeEncoder(64, "\n".getBytes(UTF_8)).encodeToString(privateKey.getEncoded());
    return ("-----BEGIN PRIVATE KEY-----\n" + base64 + "\n-----END PRIVATE KEY-----\n")
        .replace("\n", "\\n");
  }
}
