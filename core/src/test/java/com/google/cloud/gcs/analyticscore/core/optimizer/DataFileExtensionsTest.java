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

package com.google.cloud.gcs.analyticscore.core.optimizer;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DataFileExtensionsTest {

  private static final ImmutableList<String> EXTENSIONS =
      ImmutableList.of(".parquet", ".orc", ".hudi", ".hfile");

  @Test
  void hasAnyExtension_emptyObjectName_returnsFalse() {
    // Arrange
    Optional<String> objectName = Optional.empty();

    // Act
    boolean result = DataFileExtensions.hasAnyExtension(objectName, EXTENSIONS);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  void hasAnyExtension_emptyExtensionsList_returnsFalse() {
    // Arrange
    Optional<String> objectName = Optional.of("dir/data.parquet");

    // Act
    boolean result = DataFileExtensions.hasAnyExtension(objectName, ImmutableList.of());

    // Assert
    assertThat(result).isFalse();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"part-0.parquet", "dir/sub/data.orc", "table/part.hudi", "region/01.hfile"})
  void hasAnyExtension_matchingExtensionAtAnyIndex_returnsTrue(String name) {
    // Arrange
    Optional<String> objectName = Optional.of(name);

    // Act
    boolean result = DataFileExtensions.hasAnyExtension(objectName, EXTENSIONS);

    // Assert
    assertThat(result).isTrue();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "DATA.PARQUET",
        "dir/part.Parquet",
        "dir/file.OrC",
        "dir/part-0.HUDI",
        "dir/part-0.HFILE"
      })
  void hasAnyExtension_asciiCaseInsensitiveMatch_returnsTrue(String name) {
    // Arrange
    Optional<String> objectName = Optional.of(name);

    // Act
    boolean result = DataFileExtensions.hasAnyExtension(objectName, EXTENSIONS);

    // Assert
    assertThat(result).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {".parquet", ".ORC", ".hfile"})
  void hasAnyExtension_exactExtensionLength_returnsTrue(String name) {
    // Arrange
    Optional<String> objectName = Optional.of(name);

    // Act
    boolean result = DataFileExtensions.hasAnyExtension(objectName, EXTENSIONS);

    // Assert
    assertThat(result).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "a", "orc", "parquet", "hudi"})
  void hasAnyExtension_shorterThanExtension_returnsFalse(String name) {
    // Arrange
    Optional<String> objectName = Optional.of(name);

    // Act
    boolean result = DataFileExtensions.hasAnyExtension(objectName, EXTENSIONS);

    // Assert
    assertThat(result).isFalse();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "dir/data.csv",
        "dir/my_parquet",
        "dir/file_orc",
        "dir/data.parquet.crc",
        "dir/data.orc.bak"
      })
  void hasAnyExtension_nonMatchingOrSidecarSuffix_returnsFalse(String name) {
    // Arrange
    Optional<String> objectName = Optional.of(name);

    // Act
    boolean result = DataFileExtensions.hasAnyExtension(objectName, EXTENSIONS);

    // Assert
    assertThat(result).isFalse();
  }
}
