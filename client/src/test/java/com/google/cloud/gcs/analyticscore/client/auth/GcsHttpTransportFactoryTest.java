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

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import java.io.IOException;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class GcsHttpTransportFactoryTest {

  @ParameterizedTest
  @CsvSource({
    "proxy.example.com:8080, //proxy.example.com:8080",
    "http://proxy.example.com:8080, http://proxy.example.com:8080",
    "https://proxy.example.com:8443, https://proxy.example.com:8443",
    "127.0.0.1:3128, //127.0.0.1:3128"
  })
  void parseProxyAddress_validAddress_returnsExpectedUri(String input, String expected) {
    URI uri = GcsHttpTransportFactory.parseProxyAddress(input);

    assertThat(uri).isNotNull();
    assertThat(uri).isEqualTo(URI.create(expected));
  }

  @Test
  void parseProxyAddress_nullOrEmpty_returnsNull() {
    assertThat(GcsHttpTransportFactory.parseProxyAddress(null)).isNull();
    assertThat(GcsHttpTransportFactory.parseProxyAddress("")).isNull();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "ftp://proxy.example.com:21",
        "http://proxy.example.com",
        "proxy.example.com",
        "http://:8080",
        ":8080",
        "http://proxy.example.com:notaport",
        "   ",
        "  proxy.example.com:8080  "
      })
  void parseProxyAddress_invalidAddress_throwsIllegalArgumentException(String invalidInput) {
    assertThrows(
        IllegalArgumentException.class,
        () -> GcsHttpTransportFactory.parseProxyAddress(invalidInput));
  }

  @Test
  void createHttpTransport_noProxy_createsTransport() throws IOException {
    HttpTransport transport =
        GcsHttpTransportFactory.createHttpTransport(null, null, null, Duration.ofSeconds(5));

    assertThat(transport).isNotNull();
    assertThat(transport).isInstanceOf(NetHttpTransport.class);
  }

  @Test
  void createHttpTransport_withProxy_createsTransport() throws IOException {
    HttpTransport transport =
        GcsHttpTransportFactory.createHttpTransport(
            "proxy.example.com:8080", "user", "pass", Duration.ofSeconds(10));

    assertThat(transport).isNotNull();
    assertThat(transport).isInstanceOf(NetHttpTransport.class);
  }

  @Test
  void createHttpTransport_fromGcsAuthOptions_createsTransport() throws IOException {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setProxyAddress("proxy.example.com:8080")
            .setProxyUsername("user")
            .setProxyPassword("pass")
            .setHttpReadTimeout(Duration.ofSeconds(10))
            .build();

    HttpTransport transport = GcsHttpTransportFactory.createHttpTransport(options);

    assertThat(transport).isNotNull();
    assertThat(transport).isInstanceOf(NetHttpTransport.class);
  }

  @Test
  void createHttpTransport_proxyAuthWithoutProxyAddress_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> GcsHttpTransportFactory.createHttpTransport(null, "user", "pass", null));
  }

  @Test
  void createHttpTransport_proxyUsernameWithoutPassword_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GcsHttpTransportFactory.createHttpTransport(
                "proxy.example.com:8080", "user", null, null));
  }

  @Test
  void createHttpTransport_proxyPasswordWithoutUsername_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GcsHttpTransportFactory.createHttpTransport(
                "proxy.example.com:8080", null, "pass", null));
  }

  @Test
  void createNetHttpTransport_proxyAuthWithoutProxyUri_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GcsHttpTransportFactory.createNetHttpTransport(
                null, new PasswordAuthentication("u", "p".toCharArray()), null));
  }
}
