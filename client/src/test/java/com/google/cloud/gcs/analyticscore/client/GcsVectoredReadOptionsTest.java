/*
 * Copyright 2025 Google LLC
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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class GcsVectoredReadOptionsTest {

  private static final int KB = 1024;
  private static final int MB = 1024 * KB;

  @Test
  void createFromOptions_withValidProperties_shouldCreateCorrectOptions() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "gcs.analytics-core.read.vectored.range.merge-gap.max-bytes",
            String.valueOf(8 * KB),
            "gcs.analytics-core.read.vectored.range.merged-size.max-bytes",
            String.valueOf(16 * MB));

    GcsVectoredReadOptions options = GcsVectoredReadOptions.createFromOptions(properties, "gcs.");

    assertThat(options.getMaxMergeGap()).isEqualTo(8 * KB);
    assertThat(options.getMaxMergeSize()).isEqualTo(16 * MB);
  }

  @Test
  void createFromOptions_withDefaultProperties_shouldCreateCorrectOptions() {
    ImmutableMap<String, String> properties = ImmutableMap.of();

    GcsVectoredReadOptions options = GcsVectoredReadOptions.createFromOptions(properties, "gcs.");

    assertThat(options.getMaxMergeGap()).isEqualTo(4 * KB);
    assertThat(options.getMaxMergeSize()).isEqualTo(8 * MB);
  }

  @Test
  void createFromOptions_onlyMergeGap_usesDefaultMergeSize() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "gcs.analytics-core.read.vectored.range.merge-gap.max-bytes", String.valueOf(8 * KB));

    GcsVectoredReadOptions options = GcsVectoredReadOptions.createFromOptions(properties, "gcs.");

    assertThat(options.getMaxMergeGap()).isEqualTo(8 * KB);
    assertThat(options.getMaxMergeSize()).isEqualTo(8 * MB);
  }

  @Test
  void createFromOptions_onlyMergeSize_usesDefaultMergeGap() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "gcs.analytics-core.read.vectored.range.merged-size.max-bytes",
            String.valueOf(16 * MB));

    GcsVectoredReadOptions options = GcsVectoredReadOptions.createFromOptions(properties, "gcs.");

    assertThat(options.getMaxMergeGap()).isEqualTo(4 * KB);
    assertThat(options.getMaxMergeSize()).isEqualTo(16 * MB);
  }

  @Test
  void toBuilder_overridingMaxMergeGap_preservesMaxMergeSize() {
    GcsVectoredReadOptions options =
        GcsVectoredReadOptions.builder().setMaxMergeGap(2 * KB).setMaxMergeSize(4 * MB).build();

    GcsVectoredReadOptions modified = options.toBuilder().setMaxMergeGap(4 * KB).build();

    assertThat(modified.getMaxMergeGap()).isEqualTo(4 * KB);
    assertThat(modified.getMaxMergeSize()).isEqualTo(4 * MB);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "gcs.analytics-core.read.vectored.range.merge-gap.max-bytes",
        "gcs.analytics-core.read.vectored.range.merged-size.max-bytes"
      })
  void createFromOptions_integerValuesGreaterThanIntegerMax_throwsIllegalArgumentException(
      String propertyKey) {
    String outOfBoundValue = "2147483648";
    ImmutableMap<String, String> properties = ImmutableMap.of(propertyKey, outOfBoundValue);
    String prefix = "gcs.";

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsVectoredReadOptions.createFromOptions(properties, prefix));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            String.format(
                "%s=%s cannot be greater than Integer.MAX_VALUE (%d)",
                propertyKey, outOfBoundValue, Integer.MAX_VALUE));
  }

  @Test
  void createFromOptions_withWhitespacePaddedValues_trimsAndParses() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "gcs.analytics-core.read.vectored.range.merge-gap.max-bytes",
            "  8192  ",
            "gcs.analytics-core.read.vectored.range.merged-size.max-bytes",
            "\t16777216\n");

    GcsVectoredReadOptions options = GcsVectoredReadOptions.createFromOptions(properties, "gcs.");

    assertThat(options.getMaxMergeGap()).isEqualTo(8 * KB);
    assertThat(options.getMaxMergeSize()).isEqualTo(16 * MB);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "   ", "\t\n"})
  void createFromOptions_withBlankValues_retainsDefaults(String blankValue) {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "gcs.analytics-core.read.vectored.range.merge-gap.max-bytes",
            blankValue,
            "gcs.analytics-core.read.vectored.range.merged-size.max-bytes",
            blankValue);

    GcsVectoredReadOptions options = GcsVectoredReadOptions.createFromOptions(properties, "gcs.");

    assertThat(options.getMaxMergeGap()).isEqualTo(4 * KB);
    assertThat(options.getMaxMergeSize()).isEqualTo(8 * MB);
  }

  @Test
  void createFromOptions_withNullMapValues_retainsDefaults() {
    Map<String, String> properties = new HashMap<>();
    properties.put("gcs.analytics-core.read.vectored.range.merge-gap.max-bytes", null);
    properties.put("gcs.analytics-core.read.vectored.range.merged-size.max-bytes", null);

    GcsVectoredReadOptions options = GcsVectoredReadOptions.createFromOptions(properties, "gcs.");

    assertThat(options.getMaxMergeGap()).isEqualTo(4 * KB);
    assertThat(options.getMaxMergeSize()).isEqualTo(8 * MB);
  }

  @Test
  void createFromOptions_withNullOptions_throwsNullPointerException() {
    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> GcsVectoredReadOptions.createFromOptions(null, "gcs."));

    assertThat(exception).hasMessageThat().contains("analyticsCoreOptions cannot be null");
  }

  @Test
  void createFromOptions_withNullPrefix_throwsNullPointerException() {
    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> GcsVectoredReadOptions.createFromOptions(ImmutableMap.of(), null));

    assertThat(exception).hasMessageThat().contains("prefix cannot be null");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "gcs.analytics-core.read.vectored.range.merge-gap.max-bytes",
        "gcs.analytics-core.read.vectored.range.merged-size.max-bytes"
      })
  void createFromOptions_withMalformedInteger_throws(String propertyKey) {
    ImmutableMap<String, String> properties = ImmutableMap.of(propertyKey, "not-an-int");

    assertThrows(
        NumberFormatException.class,
        () -> GcsVectoredReadOptions.createFromOptions(properties, "gcs."));
  }

  @Test
  void createFromOptions_withNegativeValues_preservesNegativeValuesToDisableMerge() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "gcs.analytics-core.read.vectored.range.merge-gap.max-bytes",
            "-1",
            "gcs.analytics-core.read.vectored.range.merged-size.max-bytes",
            "-1");

    GcsVectoredReadOptions options = GcsVectoredReadOptions.createFromOptions(properties, "gcs.");

    assertThat(options.getMaxMergeGap()).isEqualTo(-1);
    assertThat(options.getMaxMergeSize()).isEqualTo(-1);
  }
}
