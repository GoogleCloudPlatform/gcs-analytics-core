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

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class GcsClientOptionsTest {

  private static final int MB = 1024 * 1024;

  @Test
  void builder_withDefaultValues_returnsExpectedDefaults() {
    GcsClientOptions options = GcsClientOptions.builder().build();

    assertThat(options.getProjectId().isPresent()).isFalse();
    assertThat(options.getUserProject()).isEmpty();
    assertThat(options.getClientLibToken().isPresent()).isFalse();
    assertThat(options.getServiceHost().isPresent()).isFalse();
    assertThat(options.getUniverseDomain()).isEmpty();
    assertThat(options.getUserAgent().isPresent()).isFalse();
    assertThat(options.getProtocol()).isEqualTo(GcsClientOptions.Protocol.HTTP);
    assertThat(options.isBidiEnabled()).isFalse();
    assertThat(options.isGrpcEnabled()).isFalse();
    assertThat(options.getGcsReadOptions()).isNotNull();
    assertThat(options.getGcsWriteOptions()).isNotNull();

    assertThat(options.getUploadChunkSize()).isEqualTo(24 * MB);
    assertThat(options.getUploadType()).isEqualTo(GcsClientOptions.UploadType.CHUNK_UPLOAD);
    assertThat(options.getPcuBufferCount()).isEqualTo(1);
    assertThat(options.getPcuBufferCapacity()).isEqualTo(32 * MB);
    assertThat(options.getPcuPartFileCleanupType())
        .isEqualTo(GcsClientOptions.PartFileCleanupType.ALWAYS);
    assertThat(options.getPcuPartFileNamePrefix()).isEmpty();
    assertThat(options.getTemporaryPaths()).isEmpty();
    assertThat(options.getBatchThreads()).isEqualTo(15);
  }

  @Test
  void builder_withCustomValues_setsAllProperties() {
    GcsClientOptions options =
        GcsClientOptions.builder()
            .setProjectId("test-project")
            .setClientLibToken("test-token")
            .setServiceHost("test-host")
            .setUniverseDomain("test-universe.goog")
            .setUserAgent("test-agent")
            .setUploadChunkSize(1024)
            .setUploadType(GcsClientOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD)
            .setPcuBufferCount(4)
            .setPcuBufferCapacity(64 * MB)
            .setPcuPartFileCleanupType(GcsClientOptions.PartFileCleanupType.ON_SUCCESS)
            .setPcuPartFileNamePrefix("temp-prefix-")
            .setTemporaryPaths(ImmutableList.of("/tmp/path1", "/tmp/path2"))
            .setBatchThreads(8)
            .build();

    assertThat(options.getProjectId()).hasValue("test-project");
    assertThat(options.getClientLibToken()).hasValue("test-token");
    assertThat(options.getServiceHost()).hasValue("test-host");
    assertThat(options.getUniverseDomain()).hasValue("test-universe.goog");
    assertThat(options.getUserAgent()).hasValue("test-agent");
    assertThat(options.getUploadChunkSize()).isEqualTo(1024);
    assertThat(options.getUploadType())
        .isEqualTo(GcsClientOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD);
    assertThat(options.getPcuBufferCount()).isEqualTo(4);
    assertThat(options.getPcuBufferCapacity()).isEqualTo(64 * MB);
    assertThat(options.getPcuPartFileCleanupType())
        .isEqualTo(GcsClientOptions.PartFileCleanupType.ON_SUCCESS);
    assertThat(options.getPcuPartFileNamePrefix()).isEqualTo("temp-prefix-");
    assertThat(options.getTemporaryPaths()).containsExactly("/tmp/path1", "/tmp/path2").inOrder();
    assertThat(options.getBatchThreads()).isEqualTo(8);
  }

  @Test
  void createFromOptions_withValidProperties_parsesCorrectly() {
    Map<String, String> rawOptions =
        ImmutableMap.<String, String>builder()
            .put("gcs.project-id", "test-project")
            .put("gcs.client-lib-token", "test-token")
            .put("gcs.service.host", "test-host")
            .put("gcs.universe-domain", "  test-universe.goog  ")
            .put("gcs.user-agent", "test-agent")
            .put("gcs.channel.write.chunk-size-bytes", "1024")
            .put("gcs.channel.write.upload-type", "parallel_composite_upload")
            .put("gcs.channel.write.pcu.buffer.count", "4")
            .put("gcs.channel.write.pcu.buffer.capacity-bytes", "67108864")
            .put("gcs.channel.write.pcu.part-file.cleanup-type", "on_success")
            .put("gcs.channel.write.pcu.part-file.name-prefix", "temp-prefix-")
            .put("gcs.channel.write.temporary-paths", "/tmp/path1, /tmp/path2")
            .put("gcs.batch.threads", "8")
            .build();

    GcsClientOptions options = GcsClientOptions.createFromOptions(rawOptions, "gcs.");

    assertThat(options.getProjectId()).hasValue("test-project");
    assertThat(options.getClientLibToken()).hasValue("test-token");
    assertThat(options.getServiceHost()).hasValue("test-host");
    assertThat(options.getUniverseDomain()).hasValue("test-universe.goog");
    assertThat(options.getUserAgent()).hasValue("test-agent");
    assertThat(options.getUploadChunkSize()).isEqualTo(1024);
    assertThat(options.getUploadType())
        .isEqualTo(GcsClientOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD);
    assertThat(options.getPcuBufferCount()).isEqualTo(4);
    assertThat(options.getPcuBufferCapacity()).isEqualTo(64 * MB);
    assertThat(options.getPcuPartFileCleanupType())
        .isEqualTo(GcsClientOptions.PartFileCleanupType.ON_SUCCESS);
    assertThat(options.getPcuPartFileNamePrefix()).isEqualTo("temp-prefix-");
    assertThat(options.getTemporaryPaths()).containsExactly("/tmp/path1", "/tmp/path2").inOrder();
    assertThat(options.getBatchThreads()).isEqualTo(8);
  }

  @Test
  void createFromOptions_withWhitespaceAndEmptyPaths_filtersThemOut() {
    Map<String, String> rawOptions =
        ImmutableMap.of("gcs.channel.write.temporary-paths", "  , /tmp/path1 , , /tmp/path2 ");
    GcsClientOptions options = GcsClientOptions.createFromOptions(rawOptions, "gcs.");
    assertThat(options.getTemporaryPaths()).containsExactly("/tmp/path1", "/tmp/path2").inOrder();
  }

  @Test
  void createFromOptions_withHyphenatedEnums_normalizesAndParsesCorrectly() {
    Map<String, String> rawOptions =
        ImmutableMap.<String, String>builder()
            .put("gcs.channel.write.upload-type", "parallel-composite-upload")
            .put("gcs.channel.write.pcu.part-file.cleanup-type", "on-success")
            .build();

    GcsClientOptions options = GcsClientOptions.createFromOptions(rawOptions, "gcs.");

    assertThat(options.getUploadType())
        .isEqualTo(GcsClientOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD);
    assertThat(options.getPcuPartFileCleanupType())
        .isEqualTo(GcsClientOptions.PartFileCleanupType.ON_SUCCESS);
  }

  @Test
  void createFromOptions_withOverflowingUploadChunkSize_throwsIllegalArgumentException() {
    assertOverflowThrows("gcs.channel.write.chunk-size-bytes");
  }

  @Test
  void createFromOptions_withOverflowingPcuBufferCount_throwsIllegalArgumentException() {
    assertOverflowThrows("gcs.channel.write.pcu.buffer.count");
  }

  @Test
  void createFromOptions_withOverflowingPcuBufferCapacity_throwsIllegalArgumentException() {
    assertOverflowThrows("gcs.channel.write.pcu.buffer.capacity-bytes");
  }

  @Test
  void createFromOptions_withOverflowingBatchThreads_throwsIllegalArgumentException() {
    assertOverflowThrows("gcs.batch.threads");
  }

  @Test
  void createFromOptions_withZeroBatchThreads_allowsDirectExecutorMode() {
    GcsClientOptions options =
        GcsClientOptions.createFromOptions(ImmutableMap.of("gcs.batch.threads", "0"), "gcs.");

    assertThat(options.getBatchThreads()).isEqualTo(0);
  }

  @Test
  void createFromOptions_withWhitespaceBatchThreads_parsesCorrectly() {
    GcsClientOptions options =
        GcsClientOptions.createFromOptions(ImmutableMap.of("gcs.batch.threads", "  12  "), "gcs.");

    assertThat(options.getBatchThreads()).isEqualTo(12);
  }

  @Test
  void createFromOptions_withEmptyOrBlankBatchThreads_retainsDefault() {
    GcsClientOptions emptyOptions =
        GcsClientOptions.createFromOptions(ImmutableMap.of("gcs.batch.threads", ""), "gcs.");
    GcsClientOptions blankOptions =
        GcsClientOptions.createFromOptions(ImmutableMap.of("gcs.batch.threads", "   "), "gcs.");

    assertThat(emptyOptions.getBatchThreads()).isEqualTo(15);
    assertThat(blankOptions.getBatchThreads()).isEqualTo(15);
  }

  @Test
  void createFromOptions_withExcessiveBatchThreads_throwsIllegalArgumentException() {
    Map<String, String> rawOptions =
        ImmutableMap.of(
            "gcs.batch.threads", String.valueOf(GcsClientOptions.MAX_BATCH_THREADS + 1));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsClientOptions.createFromOptions(rawOptions, "gcs."));

    assertThat(exception).hasMessageThat().contains("batch.threads must be at most");
  }

  @Test
  void builder_withZeroBatchThreads_buildsSuccessfully() {
    GcsClientOptions options = GcsClientOptions.builder().setBatchThreads(0).build();

    assertThat(options.getBatchThreads()).isEqualTo(0);
  }

  @Test
  void builder_withMaxBatchThreads_buildsSuccessfully() {
    GcsClientOptions options =
        GcsClientOptions.builder().setBatchThreads(GcsClientOptions.MAX_BATCH_THREADS).build();

    assertThat(options.getBatchThreads()).isEqualTo(GcsClientOptions.MAX_BATCH_THREADS);
  }

  @Test
  void builder_withNegativeBatchThreads_throwsIllegalArgumentException() {
    GcsClientOptions.Builder builder = GcsClientOptions.builder().setBatchThreads(-1);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception).hasMessageThat().contains("batch.threads must be non-negative");
  }

  @Test
  void createFromOptions_withNegativeBatchThreads_throwsIllegalArgumentException() {
    Map<String, String> rawOptions = ImmutableMap.of("gcs.batch.threads", "-1");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsClientOptions.createFromOptions(rawOptions, "gcs."));

    assertThat(exception).hasMessageThat().contains("batch.threads must be non-negative");
  }

  @Test
  void builder_withExcessiveBatchThreads_throwsIllegalArgumentException() {
    GcsClientOptions.Builder builder =
        GcsClientOptions.builder().setBatchThreads(GcsClientOptions.MAX_BATCH_THREADS + 1);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception).hasMessageThat().contains("batch.threads must be at most");
  }

  @Test
  void createFromOptions_withNonNumericBatchThreads_throwsIllegalArgumentException() {
    Map<String, String> rawOptions = ImmutableMap.of("gcs.batch.threads", "not-a-number");

    assertThrows(
        IllegalArgumentException.class,
        () -> GcsClientOptions.createFromOptions(rawOptions, "gcs."));
  }

  private void assertOverflowThrows(String key) {
    Map<String, String> rawOptions =
        ImmutableMap.of(key, String.valueOf((long) Integer.MAX_VALUE + 1L));
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsClientOptions.createFromOptions(rawOptions, "gcs."));
    assertThat(exception.getMessage()).contains(key);
    assertThat(exception.getMessage()).contains("cannot be greater than Integer.MAX_VALUE");
  }

  @Test
  void createFromOptions_withOnlyWhitespacePaths_doesNotSetPaths() {
    Map<String, String> rawOptions = ImmutableMap.of("gcs.channel.write.temporary-paths", "   ");
    GcsClientOptions options = GcsClientOptions.createFromOptions(rawOptions, "gcs.");
    assertThat(options.getTemporaryPaths()).isEmpty();
  }

  @Test
  void createFromOptions_withProtocol_parsesAllValues() {
    assertThat(protocolFor("HTTP")).isEqualTo(GcsClientOptions.Protocol.HTTP);
    assertThat(protocolFor("GRPC")).isEqualTo(GcsClientOptions.Protocol.GRPC);
    assertThat(protocolFor("BIDI")).isEqualTo(GcsClientOptions.Protocol.BIDI);
  }

  @Test
  void createFromOptions_withMixedCaseAndWhitespaceProtocol_parsesCorrectly() {
    assertThat(protocolFor("  BiDi  ")).isEqualTo(GcsClientOptions.Protocol.BIDI);
  }

  @Test
  void createFromOptions_withGrpcProtocol_enablesGrpcButNotBidi() {
    GcsClientOptions options =
        GcsClientOptions.createFromOptions(ImmutableMap.of("gcs.client.protocol", "GRPC"), "gcs.");

    assertThat(options.isGrpcEnabled()).isTrue();
    assertThat(options.isBidiEnabled()).isFalse();
  }

  @Test
  void createFromOptions_withBidiProtocol_enablesGrpcAndBidi() {
    GcsClientOptions options =
        GcsClientOptions.createFromOptions(ImmutableMap.of("gcs.client.protocol", "BIDI"), "gcs.");

    assertThat(options.isGrpcEnabled()).isTrue();
    assertThat(options.isBidiEnabled()).isTrue();
  }

  @Test
  void createFromOptions_withUnknownProtocol_throwsIllegalArgumentException() {
    Map<String, String> rawOptions = ImmutableMap.of("gcs.client.protocol", "quic");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsClientOptions.createFromOptions(rawOptions, "gcs."));

    assertThat(exception).hasMessageThat().contains("QUIC");
  }

  @Test
  void createFromOptions_withBidiWriteWithoutBidiProtocol_throwsIllegalArgumentException() {
    Map<String, String> rawOptions = ImmutableMap.of("gcs.channel.write.bidi.enabled", "true");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsClientOptions.createFromOptions(rawOptions, "gcs."));

    assertThat(exception).hasMessageThat().contains("client.protocol");
  }

  @Test
  void builder_withBidiWriteAndBidiProtocol_buildsSuccessfully() {
    GcsClientOptions options =
        GcsClientOptions.builder()
            .setProtocol(GcsClientOptions.Protocol.BIDI)
            .setGcsWriteOptions(GcsWriteOptions.builder().setBidiWriteEnabled(true).build())
            .build();

    assertThat(options.getGcsWriteOptions().isBidiWriteEnabled()).isTrue();
  }

  @Test
  void builder_withBidiWriteAndGrpcProtocol_throwsIllegalArgumentException() {
    GcsClientOptions.Builder builder =
        GcsClientOptions.builder()
            .setProtocol(GcsClientOptions.Protocol.GRPC)
            .setGcsWriteOptions(GcsWriteOptions.builder().setBidiWriteEnabled(true).build());

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void builder_withDefaultValues_hasNoRetryOverrides() {
    GcsClientOptions options = GcsClientOptions.builder().build();

    assertThat(options.getGcsRetryOptions().hasOverrides()).isFalse();
  }

  @Test
  void createFromOptions_withRetryProperties_populatesRetryOptions() {
    Map<String, String> rawOptions =
        ImmutableMap.of(
            "gcs.analytics-core.retry.max-attempts", "11",
            "gcs.analytics-core.retry.total-timeout-ms", "60000");

    GcsClientOptions options = GcsClientOptions.createFromOptions(rawOptions, "gcs.");

    assertThat(options.getGcsRetryOptions())
        .isEqualTo(
            GcsRetryOptions.builder()
                .setMaxAttempts(11)
                .setTotalTimeout(Duration.ofMinutes(1))
                .build());
  }

  private static GcsClientOptions.Protocol protocolFor(String value) {
    return GcsClientOptions.createFromOptions(ImmutableMap.of("gcs.client.protocol", value), "gcs.")
        .getProtocol();
  }

  @Test
  void builder_withUserProject_readOptionsInheritUserProject() {
    GcsClientOptions options = GcsClientOptions.builder().setUserProject("billing-proj").build();

    assertThat(options.getGcsReadOptions().getUserProjectId()).hasValue("billing-proj");
  }

  @Test
  void builder_withUserProject_writeOptionsInheritUserProject() {
    GcsClientOptions options = GcsClientOptions.builder().setUserProject("billing-proj").build();

    assertThat(options.getGcsWriteOptions().getUserProject()).hasValue("billing-proj");
  }

  @Test
  void builder_withReadUserProjectOverride_keepsReadOverride() {
    GcsClientOptions options =
        GcsClientOptions.builder()
            .setUserProject("client-proj")
            .setGcsReadOptions(GcsReadOptions.builder().setUserProjectId("read-proj").build())
            .build();

    assertThat(options.getGcsReadOptions().getUserProjectId()).hasValue("read-proj");
  }

  @Test
  void builder_withWriteUserProjectOverride_keepsWriteOverride() {
    GcsClientOptions options =
        GcsClientOptions.builder()
            .setUserProject("client-proj")
            .setGcsWriteOptions(GcsWriteOptions.builder().setUserProject("write-proj").build())
            .build();

    assertThat(options.getGcsWriteOptions().getUserProject()).hasValue("write-proj");
  }

  @Test
  void builder_withOnlyReadUserProject_leavesClientUserProjectEmpty() {
    GcsClientOptions options =
        GcsClientOptions.builder()
            .setGcsReadOptions(GcsReadOptions.builder().setUserProjectId("read-proj").build())
            .build();

    assertThat(options.getUserProject()).isEmpty();
  }

  @Test
  void builder_withOnlyReadUserProject_leavesWriteUserProjectEmpty() {
    GcsClientOptions options =
        GcsClientOptions.builder()
            .setGcsReadOptions(GcsReadOptions.builder().setUserProjectId("read-proj").build())
            .build();

    assertThat(options.getGcsWriteOptions().getUserProject()).isEmpty();
  }

  @Test
  void builder_withOnlyWriteUserProject_leavesReadUserProjectEmpty() {
    GcsClientOptions options =
        GcsClientOptions.builder()
            .setGcsWriteOptions(GcsWriteOptions.builder().setUserProject("write-proj").build())
            .build();

    assertThat(options.getGcsReadOptions().getUserProjectId()).isEmpty();
  }

  @Test
  void builder_withPaddedUserProject_trimsUserProject() {
    GcsClientOptions options =
        GcsClientOptions.builder().setUserProject("  billing-proj  ").build();

    assertThat(options.getUserProject()).hasValue("billing-proj");
  }

  @Test
  void builder_withPaddedUserProject_equalsCreateFromOptions() {
    GcsClientOptions fromBuilder =
        GcsClientOptions.builder().setUserProject("  billing-proj  ").build();

    GcsClientOptions fromMap =
        GcsClientOptions.createFromOptions(
            ImmutableMap.of("gcs.user-project", "  billing-proj  "), "gcs.");

    assertThat(fromBuilder).isEqualTo(fromMap);
  }

  @Test
  void builder_withBlankUserProject_normalizesUserProjectToEmpty() {
    GcsClientOptions options = GcsClientOptions.builder().setUserProject("   ").build();

    assertThat(options.getUserProject()).isEmpty();
  }

  @Test
  void builder_withBlankUserProject_equalsCreateFromOptions() {
    GcsClientOptions fromBuilder = GcsClientOptions.builder().setUserProject("   ").build();

    GcsClientOptions fromMap =
        GcsClientOptions.createFromOptions(ImmutableMap.of("gcs.user-project", "   "), "gcs.");

    assertThat(fromBuilder).isEqualTo(fromMap);
  }

  @Test
  void builder_withChannelUserProjectsEqualToClient_equalsClientOnlyOptions() {
    GcsClientOptions withChannelCopies =
        GcsClientOptions.builder()
            .setUserProject("billing-proj")
            .setGcsReadOptions(GcsReadOptions.builder().setUserProjectId("billing-proj").build())
            .setGcsWriteOptions(GcsWriteOptions.builder().setUserProject("billing-proj").build())
            .build();

    GcsClientOptions clientOnly = GcsClientOptions.builder().setUserProject("billing-proj").build();

    assertThat(withChannelCopies).isEqualTo(clientOnly);
  }

  @Test
  void toBuilder_withNewUserProject_updatesInheritedReadUserProject() {
    GcsClientOptions original = GcsClientOptions.builder().setUserProject("proj-a").build();

    GcsClientOptions updated = original.toBuilder().setUserProject("proj-b").build();

    assertThat(updated.getGcsReadOptions().getUserProjectId()).hasValue("proj-b");
  }

  @Test
  void toBuilder_withNewUserProject_updatesInheritedWriteUserProject() {
    GcsClientOptions original = GcsClientOptions.builder().setUserProject("proj-a").build();

    GcsClientOptions updated = original.toBuilder().setUserProject("proj-b").build();

    assertThat(updated.getGcsWriteOptions().getUserProject()).hasValue("proj-b");
  }

  @Test
  void toBuilder_withNewUserProjectOnMapOptions_updatesInheritedReadUserProject() {
    GcsClientOptions original =
        GcsClientOptions.createFromOptions(ImmutableMap.of("gcs.user-project", "proj-a"), "gcs.");

    GcsClientOptions updated = original.toBuilder().setUserProject("proj-b").build();

    assertThat(updated.getGcsReadOptions().getUserProjectId()).hasValue("proj-b");
  }

  @Test
  void toBuilder_withNewUserProject_keepsReadOverride() {
    GcsClientOptions original =
        GcsClientOptions.builder()
            .setUserProject("proj-a")
            .setGcsReadOptions(GcsReadOptions.builder().setUserProjectId("read-proj").build())
            .build();

    GcsClientOptions updated = original.toBuilder().setUserProject("proj-b").build();

    assertThat(updated.getGcsReadOptions().getUserProjectId()).hasValue("read-proj");
  }

  @Test
  void toBuilder_withNewUserProject_keepsWriteOverride() {
    GcsClientOptions original =
        GcsClientOptions.builder()
            .setUserProject("proj-a")
            .setGcsWriteOptions(GcsWriteOptions.builder().setUserProject("write-proj").build())
            .build();

    GcsClientOptions updated = original.toBuilder().setUserProject("proj-b").build();

    assertThat(updated.getGcsWriteOptions().getUserProject()).hasValue("write-proj");
  }

  @Test
  void toBuilder_withReadUserProjectEqualToPreviousClientProject_treatsItAsInherited() {
    GcsClientOptions original = GcsClientOptions.builder().setUserProject("proj-a").build();

    GcsClientOptions updated =
        original.toBuilder()
            .setUserProject("proj-b")
            .setGcsReadOptions(GcsReadOptions.builder().setUserProjectId("proj-a").build())
            .build();

    assertThat(updated.getGcsReadOptions().getUserProjectId()).hasValue("proj-b");
  }

  @Test
  void builder_withReadOptionsCopiedFromOtherOptions_keepsCopiedProjectAsOverride() {
    GcsClientOptions other = GcsClientOptions.builder().setUserProject("proj-a").build();

    GcsClientOptions options =
        GcsClientOptions.builder()
            .setUserProject("proj-b")
            .setGcsReadOptions(other.getGcsReadOptions())
            .build();

    assertThat(options.getGcsReadOptions().getUserProjectId()).hasValue("proj-a");
  }

  @Test
  void toBuilder_withBlankUserProject_clearsInheritedReadUserProject() {
    GcsClientOptions original = GcsClientOptions.builder().setUserProject("proj-a").build();

    GcsClientOptions updated = original.toBuilder().setUserProject("   ").build();

    assertThat(updated.getGcsReadOptions().getUserProjectId()).isEmpty();
  }

  @Test
  void toBuilder_withBlankUserProjectAndReadOverride_keepsReadOverrideOnly() {
    GcsClientOptions original =
        GcsClientOptions.builder()
            .setUserProject("proj-a")
            .setGcsReadOptions(GcsReadOptions.builder().setUserProjectId("read-proj").build())
            .build();

    GcsClientOptions updated = original.toBuilder().setUserProject("   ").build();

    assertThat(updated.getUserProject()).isEmpty();
    assertThat(updated.getGcsReadOptions().getUserProjectId()).hasValue("read-proj");
    assertThat(updated.getGcsWriteOptions().getUserProject()).isEmpty();
  }

  @Test
  void toBuilder_withReadOptionsCopiedFromViewAndNewUserProject_inheritsNewUserProject() {
    GcsClientOptions original = GcsClientOptions.builder().setUserProject("proj-a").build();
    GcsReadOptions copiedReadOptions =
        original.getGcsReadOptions().toBuilder().setFooterPrefetchEnabled(false).build();

    GcsClientOptions updated =
        original.toBuilder().setGcsReadOptions(copiedReadOptions).setUserProject("proj-b").build();

    assertThat(updated.getGcsReadOptions().getUserProjectId()).hasValue("proj-b");
  }

  @Test
  void build_calledTwiceOnSameBuilder_appliesLatestUserProjectToReadOptions() {
    GcsClientOptions.Builder builder = GcsClientOptions.builder().setUserProject("proj-a");
    GcsClientOptions unused = builder.build();

    GcsClientOptions rebuilt = builder.setUserProject("proj-b").build();

    assertThat(rebuilt.getGcsReadOptions().getUserProjectId()).hasValue("proj-b");
  }

  @ParameterizedTest
  @MethodSource("userProjectConfigurations")
  void toBuilder_build_returnsEqualOptions(GcsClientOptions options) {
    assertThat(options.toBuilder().build()).isEqualTo(options);
  }

  private static Stream<GcsClientOptions> userProjectConfigurations() {
    return Stream.of(
        GcsClientOptions.builder().setUserProject("billing-proj").build(),
        GcsClientOptions.builder().setUserProject("   ").build(),
        GcsClientOptions.builder()
            .setUserProject("client-proj")
            .setGcsReadOptions(GcsReadOptions.builder().setUserProjectId("read-proj").build())
            .setGcsWriteOptions(GcsWriteOptions.builder().setUserProject("write-proj").build())
            .build(),
        GcsClientOptions.builder()
            .setGcsReadOptions(GcsReadOptions.builder().setUserProjectId("read-proj").build())
            .build(),
        GcsClientOptions.createFromOptions(
            ImmutableMap.of("gcs.user-project", "billing-proj"), "gcs."));
  }

  @Test
  void createFromOptions_withPaddedUserProject_trimsConsistentlyAcrossAllOptions() {
    GcsClientOptions options =
        GcsClientOptions.createFromOptions(
            ImmutableMap.of("gcs.user-project", "  billing-proj  "), "gcs.");

    assertThat(options.getUserProject()).hasValue("billing-proj");
    assertThat(options.getGcsReadOptions().getUserProjectId()).hasValue("billing-proj");
    assertThat(options.getGcsWriteOptions().getUserProject()).hasValue("billing-proj");
  }

  @Test
  void createFromOptions_withBlankUserProject_ignoresBlankConsistentlyAcrossAllOptions() {
    GcsClientOptions options =
        GcsClientOptions.createFromOptions(ImmutableMap.of("gcs.user-project", "   "), "gcs.");

    assertThat(options.getUserProject()).isEmpty();
    assertThat(options.getGcsReadOptions().getUserProjectId()).isEmpty();
    assertThat(options.getGcsWriteOptions().getUserProject()).isEmpty();
  }

  @Test
  void signatures_exceptPreExistingTemporaryPaths_useJdkTypesOnly() {
    // TODO: drop the temporaryPaths exclusion once getTemporaryPaths() returns a JDK Set (#456).
    Class<?>[] classes = {
      GcsClientOptions.class,
      GcsClientOptions.Builder.class,
      GcsClientOptions.builder().getClass(),
      GcsClientOptions.builder().build().getClass()
    };
    for (Class<?> clazz : classes) {
      for (Field field : clazz.getDeclaredFields()) {
        if (!isTemporaryPathsMember(field.getName())) {
          ApiSignatureTestUtils.assertNoGuavaType(clazz, field.toString(), field.getGenericType());
        }
      }
      for (Method method : clazz.getDeclaredMethods()) {
        if (!isTemporaryPathsMember(method.getName())) {
          ApiSignatureTestUtils.assertNoGuavaType(
              clazz, method.toString(), method.getGenericReturnType());
          for (Type parameterType : method.getGenericParameterTypes()) {
            ApiSignatureTestUtils.assertNoGuavaType(clazz, method.toString(), parameterType);
          }
        }
      }
    }
  }

  private static boolean isTemporaryPathsMember(String name) {
    return name.toLowerCase(Locale.ROOT).contains("temporarypaths");
  }

  @Test
  void apiSignatureTestUtils_detectsNestedGuavaAndHandlesRecursiveTypeBounds() {
    ApiSignatureTestUtils.assertNoGuavaTypesInSignatures(
        CleanRecursiveSignatureFixture.class,
        GcsReadOptions.class,
        GcsReadOptions.Builder.class,
        GcsReadOptions.builder().getClass(),
        GcsReadOptions.builder().build().getClass());

    assertThrows(
        AssertionError.class,
        () ->
            ApiSignatureTestUtils.assertNoGuavaTypesInSignatures(
                ParameterizedGuavaLeakFixture.class));
    assertThrows(
        AssertionError.class,
        () -> ApiSignatureTestUtils.assertNoGuavaTypesInSignatures(WildcardGuavaLeakFixture.class));
    assertThrows(
        AssertionError.class,
        () -> ApiSignatureTestUtils.assertNoGuavaTypesInSignatures(ArrayGuavaLeakFixture.class));
    assertThrows(
        AssertionError.class,
        () -> ApiSignatureTestUtils.assertNoGuavaTypesInSignatures(TypeVarGuavaLeakFixture.class));
  }

  private static final class CleanRecursiveSignatureFixture<T extends Comparable<T>> {
    <U extends Comparable<? super U>> Optional<U[]> echo(Optional<? extends T> input) {
      return Optional.empty();
    }
  }

  private interface ParameterizedGuavaLeakFixture {
    Optional<ImmutableSet<String>> leak();
  }

  private interface WildcardGuavaLeakFixture {
    Optional<? extends com.google.common.base.Supplier<String>> leak();
  }

  private interface ArrayGuavaLeakFixture {
    Optional<ImmutableSet<String>[]> leak();
  }

  private interface TypeVarGuavaLeakFixture {
    <T extends com.google.common.base.Predicate<String>> void leak(T value);
  }
}
