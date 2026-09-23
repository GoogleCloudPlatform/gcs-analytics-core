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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class AuthTypeTest {

  @ParameterizedTest
  @CsvSource({
    "APPLICATION_DEFAULT, APPLICATION_DEFAULT",
    "application_default, APPLICATION_DEFAULT",
    "application-default, APPLICATION_DEFAULT",
    "COMPUTE_ENGINE, COMPUTE_ENGINE",
    "compute_engine, COMPUTE_ENGINE",
    "compute-engine, COMPUTE_ENGINE",
    "SERVICE_ACCOUNT_JSON_KEYFILE, SERVICE_ACCOUNT_JSON_KEYFILE",
    "service_account_json_keyfile, SERVICE_ACCOUNT_JSON_KEYFILE",
    "service-account-json-keyfile, SERVICE_ACCOUNT_JSON_KEYFILE",
    "WORKLOAD_IDENTITY_FEDERATION, WORKLOAD_IDENTITY_FEDERATION",
    "workload-identity-federation, WORKLOAD_IDENTITY_FEDERATION",
    "USER_CREDENTIALS, USER_CREDENTIALS",
    "user_credentials, USER_CREDENTIALS",
    "user-credentials, USER_CREDENTIALS",
    "UNAUTHENTICATED, UNAUTHENTICATED",
    "unauthenticated, UNAUTHENTICATED",
    "'  compute-engine  ', COMPUTE_ENGINE"
  })
  void fromString_validInputs_returnsExpectedAuthType(String input, AuthType expected) {
    AuthType actual = AuthType.fromString(input);

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void fromString_nullInput_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> AuthType.fromString(null));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "   "})
  void fromString_blankInput_throwsIllegalArgumentException(String input) {
    assertThrows(IllegalArgumentException.class, () -> AuthType.fromString(input));
  }

  @Test
  void fromString_typoedAuthType_reportsValueAndSupportedValues() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> AuthType.fromString("APPLICATON_DEFAULT"));

    assertThat(exception).hasMessageThat().contains("APPLICATON_DEFAULT");
    assertThat(exception).hasMessageThat().contains("APPLICATION_DEFAULT");
  }
}
