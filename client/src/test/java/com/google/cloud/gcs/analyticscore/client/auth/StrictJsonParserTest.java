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

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StrictJsonParserTest {

  @Test
  void parse_strictJson_returnsSameTreeAsGson() {
    String json =
        "{\"object\": {\"string\": \"value\", \"number\": -1.5e3, \"true\": true,"
            + " \"false\": false, \"null\": null}, \"array\": [1, \"1\", [], {}]}";

    JsonElement document = StrictJsonParser.parse(json);

    assertThat(document).isEqualTo(JsonParser.parseString(json));
  }

  @Test
  void parse_duplicateField_reportsTheObject() {
    String json = "{\"rules\": [{}, {\"condition\": \"false\", \"condition\": \"true\"}]}";

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> StrictJsonParser.parse(json));

    assertThat(e).hasMessageThat().isEqualTo("$.rules[1] has a duplicate field 'condition'");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidJson")
  void parse_invalidJson_throwsIllegalArgumentException(String description, String json) {
    assertThrows(IllegalArgumentException.class, () -> StrictJsonParser.parse(json));
  }

  @Test
  void parse_nullJson_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> StrictJsonParser.parse(null));
  }

  private static Stream<Arguments> invalidJson() {
    return Stream.of(
        Arguments.of("empty", ""),
        Arguments.of("comment", "{\"a\": /* comment */ 1}"),
        Arguments.of("single quotes", "{'a': 1}"),
        Arguments.of("unquoted name", "{a: 1}"),
        Arguments.of("content after document", "{}x"),
        Arguments.of("duplicate field after null", "{\"a\": null, \"a\": 1}"));
  }
}
