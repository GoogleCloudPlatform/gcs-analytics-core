package com.google.cloud.gcs.analyticscore.core;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.gcs.analyticscore.client.GcsClientOptions;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class GcsAnalyticsCoreOptionsTest {

  @Test
  void getFileSystemOptions_withPrefix_returnsCorrectOptions() {
    String appendPrefix = "gcs.";
    ImmutableMap<String, String> options =
        ImmutableMap.of(
            appendPrefix + "read-thread-count",
            "32",
            appendPrefix + "client.type",
            "GRPC_CLIENT",
            appendPrefix + "project-id",
            "test-project",
            appendPrefix + "client-lib-token",
            "test-token",
            appendPrefix + "service.host",
            "test-host",
            appendPrefix + "user-agent.key",
            "test-agent",
            "some-other-key",
            "some-other-value");

    GcsAnalyticsCoreOptions coreOptions = new GcsAnalyticsCoreOptions(appendPrefix, options);
    GcsFileSystemOptions fileSystemOptions = coreOptions.getFileSystemOptions();
    GcsClientOptions clientOptions = fileSystemOptions.getGcsClientOptions();

    assertThat(fileSystemOptions.getReadThreadCount()).isEqualTo(32);
    assertThat(fileSystemOptions.getClientType())
        .isEqualTo(GcsFileSystemOptions.ClientType.GRPC_CLIENT);
    assertThat(clientOptions.getProjectId().get()).isEqualTo("test-project");
    assertThat(clientOptions.getClientLibToken().get()).isEqualTo("test-token");
    assertThat(clientOptions.getServiceHost().get()).isEqualTo("test-host");
    assertThat(clientOptions.getUserAgent().get()).isEqualTo("test-agent");
  }

  @Test
  void getFileSystemOptions_emptyPrefix_returnsCorrectOptions() {
    String appendPrefix = "";
    ImmutableMap<String, String> options =
        ImmutableMap.of(
            "read-thread-count", "24",
            "client.type", "HTTP_CLIENT",
            "project-id", "test-project-no-prefix",
            "client-lib-token", "test-token-no-prefix",
            "service.host", "test-host-no-prefix",
            "user-agent.key", "test-agent-no-prefix");

    GcsAnalyticsCoreOptions coreOptions = new GcsAnalyticsCoreOptions(appendPrefix, options);
    GcsFileSystemOptions fileSystemOptions = coreOptions.getFileSystemOptions();
    GcsClientOptions clientOptions = fileSystemOptions.getGcsClientOptions();

    assertThat(fileSystemOptions.getReadThreadCount()).isEqualTo(24);
    assertThat(fileSystemOptions.getClientType())
        .isEqualTo(GcsFileSystemOptions.ClientType.HTTP_CLIENT);
    assertThat(clientOptions.getProjectId().get()).isEqualTo("test-project-no-prefix");
    assertThat(clientOptions.getClientLibToken().get()).isEqualTo("test-token-no-prefix");
    assertThat(clientOptions.getServiceHost().get()).isEqualTo("test-host-no-prefix");
    assertThat(clientOptions.getUserAgent().get()).isEqualTo("test-agent-no-prefix");
  }

  @Test
  void getFileSystemOptions_withMismatchedPrefix_returnsDefaultOptions() {
    String appendPrefix = "gcs.";
    ImmutableMap<String, String> options =
        ImmutableMap.of(
            "wrong.prefix.read-thread-count", "32",
            "wrong.prefix.client.type", "GRPC_CLIENT",
            "wrong.prefix.project-id", "test-project");
    GcsFileSystemOptions defaultOptions = GcsFileSystemOptions.builder().build();

    GcsAnalyticsCoreOptions coreOptions = new GcsAnalyticsCoreOptions(appendPrefix, options);
    GcsFileSystemOptions fileSystemOptions = coreOptions.getFileSystemOptions();
    GcsClientOptions clientOptions = fileSystemOptions.getGcsClientOptions();

    assertThat(fileSystemOptions.getReadThreadCount())
        .isEqualTo(defaultOptions.getReadThreadCount());
    assertThat(fileSystemOptions.getClientType()).isEqualTo(defaultOptions.getClientType());
    assertThat(clientOptions.getProjectId().isPresent()).isFalse();
  }
}
