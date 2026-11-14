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
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class FakePageTokenTest {

  @TestFactory
  Stream<DynamicTest> decode_encodedName_returnsSameName() {
    return Stream.of("a", "dir/obj-0001", "dir/ spaced /name", "unicode/é/漢字")
        .map(
            name ->
                dynamicTest(
                    name,
                    () ->
                        assertThat(FakePageToken.decode(Optional.of(FakePageToken.encode(name))))
                            .hasValue(name)));
  }

  @Test
  void encode_name_returnsPrefixedUrlSafeBase64WithoutPadding() {
    String token = FakePageToken.encode("a");

    assertThat(token).isEqualTo("name:YQ");
  }

  @Test
  void decode_emptyToken_returnsEmpty() throws IOException {
    assertThat(FakePageToken.decode(Optional.empty())).isEmpty();
  }

  @TestFactory
  Stream<DynamicTest> decode_invalidToken_throwsIOException() {
    return Stream.of("not-a-cursor", "name:!!!", "name:", "name:_w")
        .map(
            token ->
                dynamicTest(
                    token,
                    () ->
                        assertThrows(
                            IOException.class, () -> FakePageToken.decode(Optional.of(token)))));
  }
}
