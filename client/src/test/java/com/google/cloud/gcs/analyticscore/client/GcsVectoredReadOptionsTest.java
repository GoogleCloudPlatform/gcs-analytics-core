package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class GcsVectoredReadOptionsTest {

  @Test
  void createFromOptions_withValidProperties_shouldCreateCorrectOptions() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "fs.gs.vectored.read.min.range.seek.size", "8192",
            "fs.gs.vectored.read.merged.range.max.size", "16777216");

    GcsVectoredReadOptions options = GcsVectoredReadOptions.createFromOptions(properties, "fs.gs.");

    assertThat(options.getMaxMergeGap()).isEqualTo(8192);
    assertThat(options.getMaxMergeSize()).isEqualTo(16777216);
  }

  @Test
  void createFromOptions_withDefaultProperties_shouldCreateCorrectOptions() {
    ImmutableMap<String, String> properties = ImmutableMap.of();

    GcsVectoredReadOptions options = GcsVectoredReadOptions.createFromOptions(properties, "fs.gs.");

    assertThat(options.getMaxMergeGap()).isEqualTo(4 * 1024);
    assertThat(options.getMaxMergeSize()).isEqualTo(8 * 1024 * 1024);
  }
}
