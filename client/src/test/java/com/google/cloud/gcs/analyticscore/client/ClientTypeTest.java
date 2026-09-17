/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ClientTypeTest {

  @ParameterizedTest
  @ValueSource(strings = {"JSON", "json", "Json"})
  void fromString_jsonValues_returnsJson(String value) {
    // Act
    ClientType clientType = ClientType.fromString(value);

    // Assert
    assertThat(clientType).isEqualTo(ClientType.JSON);
  }

  @ParameterizedTest
  @ValueSource(strings = {"GRPC", "grpc", "Grpc"})
  void fromString_grpcValues_returnsGrpc(String value) {
    // Act
    ClientType clientType = ClientType.fromString(value);

    // Assert
    assertThat(clientType).isEqualTo(ClientType.GRPC);
  }

  @ParameterizedTest
  @ValueSource(strings = {"BIDI", "bidi", "Bidi"})
  void fromString_bidiValues_returnsBidi(String value) {
    // Act
    ClientType clientType = ClientType.fromString(value);

    // Assert
    assertThat(clientType).isEqualTo(ClientType.BIDI);
  }

  @Test
  void fromString_nullValue_throwsNullPointerException() {
    // Act & Assert
    assertThrows(NullPointerException.class, () -> ClientType.fromString(null));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "INVALID",
        "unknown",
        "",
        "HTTP_CLIENT",
        "http_client",
        "GRPC_CLIENT",
        "grpc_client"
      })
  void fromString_invalidValue_throwsIllegalArgumentException(String value) {
    // Act & Assert
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> ClientType.fromString(value));
    assertThat(exception).hasMessageThat().contains("Invalid client type");
  }
}
