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

class GcsReadOptionsTest {

  @Test
  void createFromOptions_withAllProperties_shouldCreateCorrectOptions() {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("fs.gs.channel.read.chunk-size-bytes", "8192")
            .put("fs.gs.decryption.key", "test-key")
            .put("fs.gs.project.id", "test-project")
            .put("fs.gs.vectored.read.min.range.seek.size", "1024")
            .put("fs.gs.vectored.read.merged.range.max.size", "2048")
            .put("fs.gs.footer.prefetch.enabled", "false")
            .put("fs.gs.footer.prefetch.size.large-file", "4194304")
            .put("fs.gs.footer.prefetch.size.small-file", "41943")
            .put("fs.gs.footer.prefetch.small-object-caching.enabled", "false")
            .build();
    String prefix = "fs.gs.";

    GcsReadOptions readOptions = GcsReadOptions.createFromOptions(properties, prefix);
    GcsVectoredReadOptions vectoredReadOptions = readOptions.getGcsVectoredReadOptions();

    assertThat(readOptions.getChunkSize()).isEqualTo(Optional.of(8192));
    assertThat(readOptions.getDecryptionKey()).isEqualTo(Optional.of("test-key"));
    assertThat(readOptions.getProjectId()).isEqualTo(Optional.of("test-project"));
    assertThat(readOptions.isFooterPrefetchEnabled()).isEqualTo(false);
    assertThat(readOptions.getFooterPrefetchSizeSmallFile()).isEqualTo(41943);
    assertThat(readOptions.getFooterPrefetchSizeLargeFile()).isEqualTo(4194304);
    assertThat(readOptions.isSmallObjectCacheEnabled()).isFalse();
    assertThat(vectoredReadOptions.getMaxMergeGap()).isEqualTo(1024);
    assertThat(vectoredReadOptions.getMaxMergeSize()).isEqualTo(2048);
  }

  @Test
  void createFromOptions_withNoProperties_shouldCreateDefaultOptions() {
    Map<String, String> properties = ImmutableMap.of();
    String prefix = "fs.gs.";

    GcsReadOptions readOptions = GcsReadOptions.createFromOptions(properties, prefix);
    GcsVectoredReadOptions vectoredReadOptions = readOptions.getGcsVectoredReadOptions();

    assertThat(readOptions.getChunkSize()).isEqualTo(Optional.empty());
    assertThat(readOptions.getDecryptionKey()).isEqualTo(Optional.empty());
    assertThat(readOptions.getProjectId()).isEqualTo(Optional.empty());
    assertThat(readOptions.isFooterPrefetchEnabled()).isEqualTo(true); // Default value
    assertThat(readOptions.getFooterPrefetchSizeSmallFile()).isEqualTo(100 * 1024); // Default value
    assertThat(readOptions.getFooterPrefetchSizeLargeFile())
        .isEqualTo(1024 * 1024); // Default value
    assertThat(readOptions.isSmallObjectCacheEnabled()).isTrue(); // Default value
    assertThat(vectoredReadOptions.getMaxMergeGap()).isEqualTo(4 * 1024); // Default value
    assertThat(vectoredReadOptions.getMaxMergeSize()).isEqualTo(8 * 1024 * 1024); // Default value
  }

  @Test
  void createFromOptions_withPrefetchGreaterThanIntegerMax_shouldThrowIllegalArgumentException() {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("fs.gs.footer.prefetch.size.small-file", "2147483648")
            .build();
    String prefix = "fs.gs.";

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsReadOptions.createFromOptions(properties, prefix));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            String.format(
                "prefetchSizeForSmallFile (%s) cannot be greater than Integer.MAX_VALUE (%d)",
                "2147483648", Integer.MAX_VALUE));
  }

  @Test
  void
      createFromOptions_withPrefetchForLargeFileGreaterThanIntegerMax_shouldThrowIllegalArgumentException() {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("fs.gs.footer.prefetch.size.large-file", "2147483648")
            .build();
    String prefix = "fs.gs.";

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsReadOptions.createFromOptions(properties, prefix));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            String.format(
                "prefetchSizeForLargeFile (%s) cannot be greater than Integer.MAX_VALUE (%d)",
                "2147483648", Integer.MAX_VALUE));
  }
}
