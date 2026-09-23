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

package com.google.cloud.gcs.analyticscore.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class RedactedStringTest {

  @Test
  void toString_doesNotContainWrappedValue() {
    RedactedString redactedString = RedactedString.create("super-secret-value");

    String description = redactedString.toString();

    assertThat(description).doesNotContain("super-secret-value");
  }

  @Test
  void toString_returnsPlaceholder() {
    RedactedString redactedString = RedactedString.create("super-secret-value");

    String description = redactedString.toString();

    assertThat(description).isEqualTo("<redacted>");
  }

  @Test
  void value_returnsWrappedValue() {
    RedactedString redactedString = RedactedString.create("super-secret-value");

    String value = redactedString.value();

    assertThat(value).isEqualTo("super-secret-value");
  }

  @Test
  void equals_sameWrappedValue_areEqual() {
    RedactedString redactedString = RedactedString.create("super-secret-value");

    RedactedString otherRedactedString = RedactedString.create("super-secret-value");

    assertThat(redactedString).isEqualTo(otherRedactedString);
  }

  @Test
  void create_nullValue_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> RedactedString.create(null));
  }

  @Test
  void create_emptyValue_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> RedactedString.create(""));
  }

  @Test
  void create_blankValue_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> RedactedString.create("   "));
  }
}
