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

import com.google.auth.http.HttpTransportFactory;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.http.HttpTransportOptions.DefaultHttpTransportFactory;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class GcsTransportOptionsFactoryTest {

  private static final String PROXY_ADDRESS = "http://proxy.example.com:8080";

  @Test
  void createHttpTransportOptions_appliesConnectAndReadTimeouts() {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setHttpConnectTimeout(Duration.ofSeconds(12))
            .setHttpReadTimeout(Duration.ofSeconds(34))
            .build();

    HttpTransportOptions transportOptions =
        GcsTransportOptionsFactory.createHttpTransportOptions(
            options, /* storageEndpoint= */ null, /* universeDomain= */ null);

    assertThat(new int[] {transportOptions.getConnectTimeout(), transportOptions.getReadTimeout()})
        .asList()
        .containsExactly(12_000, 34_000)
        .inOrder();
  }

  @Test
  void createHttpTransportOptions_zeroTimeouts_appliesNoTimeouts() {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setHttpConnectTimeout(Duration.ZERO)
            .setHttpReadTimeout(Duration.ZERO)
            .build();

    HttpTransportOptions transportOptions =
        GcsTransportOptionsFactory.createHttpTransportOptions(
            options, /* storageEndpoint= */ null, /* universeDomain= */ null);

    assertThat(new int[] {transportOptions.getConnectTimeout(), transportOptions.getReadTimeout()})
        .asList()
        .containsExactly(0, 0);
  }

  @Test
  void createHttpTransportOptions_withoutProxy_keepsDefaultTransportFactory() {
    HttpTransportOptions transportOptions =
        GcsTransportOptionsFactory.createHttpTransportOptions(
            GcsAuthOptions.builder().build(),
            /* storageEndpoint= */ null,
            /* universeDomain= */ null);

    assertThat(transportOptions.getHttpTransportFactory())
        .isInstanceOf(DefaultHttpTransportFactory.class);
  }

  @Test
  void createHttpTransportOptions_withProxy_replacesDefaultTransportFactory() {
    GcsAuthOptions options = GcsAuthOptions.builder().setProxyAddress(PROXY_ADDRESS).build();

    HttpTransportOptions transportOptions =
        GcsTransportOptionsFactory.createHttpTransportOptions(
            options, /* storageEndpoint= */ null, /* universeDomain= */ null);

    assertThat(transportOptions.getHttpTransportFactory())
        .isNotInstanceOf(DefaultHttpTransportFactory.class);
  }

  @Test
  void createHttpTransportOptions_withProxy_createsTransportOnce() {
    GcsAuthOptions options = GcsAuthOptions.builder().setProxyAddress(PROXY_ADDRESS).build();
    HttpTransportFactory transportFactory =
        GcsTransportOptionsFactory.createHttpTransportOptions(
                options, /* storageEndpoint= */ null, /* universeDomain= */ null)
            .getHttpTransportFactory();

    assertThat(transportFactory.create()).isSameInstanceAs(transportFactory.create());
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "null, null, false",
        "null, googleapis.com, false",
        "null, ' GoogleAPIs.com ', false",
        "https://localhost:8443, null, true",
        "https://storage.googleapis.com, null, false",
        "https://storage.googleapis.com/, null, false",
        "https://restricted.googleapis.com, null, true",
        "null, apis-tpclp.goog, true",
      },
      nullValues = "null")
  void usesNonGoogleEndpoint_returnsWhetherEndpointMayNeedJvmTrustStore(
      String storageEndpoint, String universeDomain, boolean expected) {
    assertThat(GcsTransportOptionsFactory.usesNonGoogleEndpoint(storageEndpoint, universeDomain))
        .isEqualTo(expected);
  }
}
