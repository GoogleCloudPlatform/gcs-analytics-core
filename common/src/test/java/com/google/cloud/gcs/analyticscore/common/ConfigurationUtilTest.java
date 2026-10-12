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

package com.google.cloud.gcs.analyticscore.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ConfigurationUtilTest {

  @Test
  void safeParseInteger_validString_returnsInteger() {
    int actual = ConfigurationUtil.safeParseInteger("key", "12345");

    assertThat(actual).isEqualTo(12345);
  }

  @Test
  void safeParseInteger_overflowValue_throwsIllegalArgumentException() {
    String overflow = String.valueOf((long) Integer.MAX_VALUE + 1);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> ConfigurationUtil.safeParseInteger("my.key", overflow));

    assertThat(exception).hasMessageThat().contains("my.key=" + overflow);
  }

  @Test
  void safeParseInteger_fromMap_returnsInteger() {
    Map<String, String> options = ImmutableMap.of("my.key", "42");

    int actual = ConfigurationUtil.safeParseInteger(options, "my.key");

    assertThat(actual).isEqualTo(42);
  }

  @Test
  void getTrimmedValue_presentNonEmptyValue_returnsTrimmedValue() {
    Map<String, String> options = ImmutableMap.of("my.key", "  value  ");

    Optional<String> actual = ConfigurationUtil.getTrimmedValue(options, "my.key");

    assertThat(actual).hasValue("value");
  }

  @Test
  void getTrimmedValue_missingKey_returnsEmptyOptional() {
    Map<String, String> options = ImmutableMap.of();

    Optional<String> actual = ConfigurationUtil.getTrimmedValue(options, "missing.key");

    assertThat(actual).isEmpty();
  }

  @Test
  void getTrimmedValue_emptyValue_returnsEmptyOptional() {
    Map<String, String> options = ImmutableMap.of("my.key", "");

    Optional<String> actual = ConfigurationUtil.getTrimmedValue(options, "my.key");

    assertThat(actual).isEmpty();
  }

  @Test
  void getTrimmedValue_whitespaceValue_returnsEmptyOptional() {
    Map<String, String> options = ImmutableMap.of("my.key", "   ");

    Optional<String> actual = ConfigurationUtil.getTrimmedValue(options, "my.key");

    assertThat(actual).isEmpty();
  }

  @Test
  void getTrimmedValue_nullOptions_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class, () -> ConfigurationUtil.getTrimmedValue(null, "my.key"));
  }

  @Test
  void getTrimmedValue_nullKey_throwsNullPointerException() {
    Map<String, String> options = ImmutableMap.of();

    assertThrows(
        NullPointerException.class, () -> ConfigurationUtil.getTrimmedValue(options, null));
  }
}
