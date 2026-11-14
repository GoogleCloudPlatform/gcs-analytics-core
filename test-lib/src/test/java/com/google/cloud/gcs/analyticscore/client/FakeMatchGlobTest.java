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
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class FakeMatchGlobTest {

  @TestFactory
  Stream<DynamicTest> compile_validGlob_matchesExpectedNames() {
    return Stream.of(
        matchCase("*.parquet", "a.parquet", "d/a.parquet"),
        matchCase("**.parquet", "d/e/a.parquet", "d/e/a.orc"),
        matchCase("**/a.txt", "a.txt", "b.txt"),
        matchCase("d/**/a.txt", "d/x/y/a.txt", "e/a.txt"),
        matchCase("part-?.csv", "part-1.csv", "part-12.csv"),
        matchCase("part-[0-9].csv", "part-7.csv", "part-x.csv"),
        matchCase("part-[!0-9].csv", "part-x.csv", "part-7.csv"),
        matchCase("[.-0]", ".", "/"),
        matchCase("*.{parquet,orc}", "a.orc", "a.csv"),
        matchCase("file\\*.txt", "file*.txt", "fileX.txt"),
        matchCase("a+b(c).txt", "a+b(c).txt", "aab(c).txt"));
  }

  @TestFactory
  Stream<DynamicTest> compile_malformedGlob_throwsIOException() {
    return Stream.of(
            "[]",
            "[!]",
            "[unclosed",
            "[a\\",
            "[a[b]",
            "unmatched]",
            "{a,b",
            "a}",
            "{a/b,c}",
            "{a\\/b,c}",
            "{a/**/b,c}",
            "{a**b,c}",
            "[z-a]")
        .map(glob -> dynamicTest(glob, () -> assertThrows(IOException.class, () -> compile(glob))));
  }

  @Test
  void compile_trailingBackslash_matchesLiteralBackslash() throws IOException {
    Pattern pattern = compile("dir\\");

    assertThat(pattern.matcher("dir\\").matches()).isTrue();
  }

  private static DynamicTest matchCase(String glob, String matching, String nonMatching) {
    return dynamicTest(
        glob,
        () -> {
          Pattern pattern = compile(glob);

          assertThat(pattern.matcher(matching).matches()).isTrue();
          assertThat(pattern.matcher(nonMatching).matches()).isFalse();
        });
  }

  private static Pattern compile(String glob) throws IOException {
    return FakeMatchGlob.compile(glob);
  }
}
