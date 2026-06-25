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
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class GcsReadOptionsTest {

  private static final int KB = 1024;
  private static final int MB = 1024 * KB;

  @Test
  void createFromOptions_withAllProperties_shouldCreateCorrectOptions() {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("gcs.channel.read.chunk-size-bytes", "8192")
            .put("gcs.decryption-key", "test-key")
            .put("gcs.user-project", "test-project")
            .put("gcs.analytics-core.read.vectored.range.merge-gap.max-bytes", "1024")
            .put("gcs.analytics-core.read.vectored.range.merged-size.max-bytes", "2048")
            .put("gcs.analytics-core.footer.prefetch.enabled", "false")
            .put("gcs.analytics-core.small-file.cache.threshold-bytes", "102400")
            .put("gcs.analytics-core.large-file.footer.prefetch.size-bytes", "4194304")
            .put("gcs.analytics-core.small-file.footer.prefetch.size-bytes", "41943")
            .put("gcs.analytics-core.read.inplace-seek-limit-bytes", "16777216")
            .put("gcs.analytics-core.read.file-access-pattern", "random")
            .put("gcs.analytics-core.adaptive-read.sequential-read-threshold", "5")
            .put("gcs.analytics-core.random-read.min-request-size", "65536")
            .build();
    String prefix = "gcs.";

    GcsReadOptions readOptions = GcsReadOptions.createFromOptions(properties, prefix);
    GcsVectoredReadOptions vectoredReadOptions = readOptions.getGcsVectoredReadOptions();

    assertThat(readOptions.getChunkSize()).isEqualTo(Optional.of(8192));
    assertThat(readOptions.getDecryptionKey()).isEqualTo(Optional.of("test-key"));
    assertThat(readOptions.getUserProjectId()).isEqualTo(Optional.of("test-project"));
    assertThat(readOptions.isFooterPrefetchEnabled()).isEqualTo(false);
    assertThat(readOptions.getFooterPrefetchSizeSmallFile()).isEqualTo(41943);
    assertThat(readOptions.getFooterPrefetchSizeLargeFile()).isEqualTo(4194304);
    assertThat(readOptions.getSmallObjectCacheThresholdBytes()).isEqualTo(102400);
    assertThat(readOptions.getInplaceSeekLimit()).isEqualTo(16777216);
    assertThat(readOptions.getFileAccessPattern()).isEqualTo(FileAccessPattern.RANDOM);
    assertThat(readOptions.getAdaptiveReadSequentialReadThreshold()).isEqualTo(5);
    assertThat(readOptions.getRandomReadMinRequestSize()).isEqualTo(65536);
    properties =
        ImmutableMap.<String, String>builder()
            .put("gcs.analytics-core.read.file-access-pattern", "auto_sequential")
            .build();
    readOptions = GcsReadOptions.createFromOptions(properties, prefix);

    assertThat(readOptions.getFileAccessPattern()).isEqualTo(FileAccessPattern.AUTO_SEQUENTIAL);
    assertThat(vectoredReadOptions.getMaxMergeGap()).isEqualTo(1024);
    assertThat(vectoredReadOptions.getMaxMergeSize()).isEqualTo(2048);
  }

  @Test
  void createFromOptions_withNoProperties_shouldCreateDefaultOptions() {
    Map<String, String> properties = ImmutableMap.of();
    String prefix = "gcs.";

    GcsReadOptions readOptions = GcsReadOptions.createFromOptions(properties, prefix);
    GcsVectoredReadOptions vectoredReadOptions = readOptions.getGcsVectoredReadOptions();

    assertThat(readOptions.getChunkSize()).isEqualTo(Optional.empty());
    assertThat(readOptions.getDecryptionKey()).isEqualTo(Optional.empty());
    assertThat(readOptions.getUserProjectId()).isEqualTo(Optional.empty());
    assertThat(readOptions.isFooterPrefetchEnabled()).isEqualTo(true);
    assertThat(readOptions.getFooterPrefetchSizeSmallFile()).isEqualTo(50 * KB);
    assertThat(readOptions.getFooterPrefetchSizeLargeFile()).isEqualTo(MB);
    assertThat(readOptions.getSmallObjectCacheThresholdBytes()).isEqualTo(MB);
    assertThat(readOptions.getInplaceSeekLimit()).isEqualTo(128 * KB);
    assertThat(readOptions.getFileAccessPattern()).isEqualTo(FileAccessPattern.AUTO_SEQUENTIAL);
    assertThat(readOptions.getAdaptiveReadSequentialReadThreshold()).isEqualTo(3);
    assertThat(readOptions.getRandomReadMinRequestSize()).isEqualTo(128 * KB);
    assertThat(vectoredReadOptions.getMaxMergeGap()).isEqualTo(4 * KB);
    assertThat(vectoredReadOptions.getMaxMergeSize()).isEqualTo(8 * MB);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "gcs.analytics-core.small-file.footer.prefetch.size-bytes",
        "gcs.analytics-core.small-file.cache.threshold-bytes",
        "gcs.analytics-core.large-file.footer.prefetch.size-bytes",
        "gcs.analytics-core.read.inplace-seek-limit-bytes",
        "gcs.analytics-core.adaptive-read.sequential-read-threshold",
        "gcs.analytics-core.random-read.min-request-size",
      })
  void createFromOptions_integerValuesGreaterThanIntegerMax_throwsIllegalArgumentException(
      String propertyKey) {
    String outOfBoundValue = "2147483648";
    Map<String, String> properties = ImmutableMap.of(propertyKey, outOfBoundValue);
    String prefix = "gcs.";

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsReadOptions.createFromOptions(properties, prefix));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            String.format(
                "%s=%s cannot be greater than Integer.MAX_VALUE (%d)",
                propertyKey, outOfBoundValue, Integer.MAX_VALUE));
  }

  @Test
  void createFromOptions_withInvalidFileAccessPattern_throwsIllegalArgumentException() {
    Map<String, String> properties =
        ImmutableMap.of("gcs.analytics-core.read.file-access-pattern", "invalid");
    String prefix = "gcs.";

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsReadOptions.createFromOptions(properties, prefix));

    assertThat(exception)
        .hasMessageThat()
        .contains(
            "No enum constant com.google.cloud.gcs.analyticscore.client.FileAccessPattern.INVALID");
  }
}
