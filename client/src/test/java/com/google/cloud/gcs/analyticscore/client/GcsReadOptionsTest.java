package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;

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
            .build();
    String prefix = "fs.gs.";

    GcsReadOptions readOptions = GcsReadOptions.createFromOptions(properties, prefix);
    GcsVectoredReadOptions vectoredReadOptions = readOptions.getGcsVectoredReadOptions();

    assertThat(readOptions.getChunkSize()).isEqualTo(Optional.of(8192));
    assertThat(readOptions.getDecryptionKey()).isEqualTo(Optional.of("test-key"));
    assertThat(readOptions.getProjectId()).isEqualTo(Optional.of("test-project"));
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
    assertThat(vectoredReadOptions.getMaxMergeGap()).isEqualTo(4 * 1024); // Default value
    assertThat(vectoredReadOptions.getMaxMergeSize()).isEqualTo(8 * 1024 * 1024); // Default value
  }
}
