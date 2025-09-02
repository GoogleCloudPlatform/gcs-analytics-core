package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class GcsFileSystemOptionsTest {

  @Test
  void createFromOptions_withValidProperties_shouldCreateCorrectOptions() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "fs.gs.project-id", "test-project",
            "fs.gs.client.type", "GRPC_CLIENT",
            "fs.gs.read-thread-count", "32");

    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    assertThat(options.getGcsClientOptions().getProjectId().get()).isEqualTo("test-project");
    assertThat(options.getClientType()).isEqualTo(GcsFileSystemOptions.ClientType.GRPC_CLIENT);
    assertThat(options.getReadThreadCount()).isEqualTo(32);
  }

  @Test
  void createFromOptions_withDefaultProperties_shouldCreateCorrectOptions() {
    ImmutableMap<String, String> properties = ImmutableMap.of();

    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    assertThat(options.getGcsClientOptions().getProjectId().isEmpty()).isTrue();
    assertThat(options.getClientType()).isEqualTo(GcsFileSystemOptions.ClientType.HTTP_CLIENT);
    assertThat(options.getReadThreadCount()).isEqualTo(16);
  }
}
