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
import java.util.Map;
import org.junit.jupiter.api.Test;

class GcsClientOptionsTest {

  private static final int MB = 1024 * 1024;

  @Test
  void builder_withDefaultValues_returnsExpectedDefaults() {
    GcsClientOptions options = GcsClientOptions.builder().build();

    assertThat(options.getProjectId().isPresent()).isFalse();
    assertThat(options.getClientLibToken().isPresent()).isFalse();
    assertThat(options.getServiceHost().isPresent()).isFalse();
    assertThat(options.getUserAgent().isPresent()).isFalse();
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
  }

  @Test
  void builder_withCustomValues_setsAllProperties() {
    GcsClientOptions options =
        GcsClientOptions.builder()
            .setProjectId("test-project")
            .setClientLibToken("test-token")
            .setServiceHost("test-host")
            .setUserAgent("test-agent")
            .setUploadChunkSize(1024)
            .setUploadType(GcsClientOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD)
            .setPcuBufferCount(4)
            .setPcuBufferCapacity(64 * MB)
            .setPcuPartFileCleanupType(GcsClientOptions.PartFileCleanupType.ON_SUCCESS)
            .setPcuPartFileNamePrefix("temp-prefix-")
            .setTemporaryPaths(ImmutableList.of("/tmp/path1", "/tmp/path2"))
            .build();

    assertThat(options.getProjectId()).hasValue("test-project");
    assertThat(options.getClientLibToken()).hasValue("test-token");
    assertThat(options.getServiceHost()).hasValue("test-host");
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
  }

  @Test
  void createFromOptions_withValidProperties_parsesCorrectly() {
    Map<String, String> rawOptions =
        ImmutableMap.<String, String>builder()
            .put("gcs.project-id", "test-project")
            .put("gcs.client-lib-token", "test-token")
            .put("gcs.service.host", "test-host")
            .put("gcs.user-agent", "test-agent")
            .put("gcs.channel.write.chunk-size-bytes", "1024")
            .put("gcs.channel.write.upload-type", "parallel_composite_upload")
            .put("gcs.channel.write.pcu.buffer.count", "4")
            .put("gcs.channel.write.pcu.buffer.capacity-bytes", "67108864")
            .put("gcs.channel.write.pcu.part-file.cleanup-type", "on_success")
            .put("gcs.channel.write.pcu.part-file.name-prefix", "temp-prefix-")
            .put("gcs.channel.write.temporary-paths", "/tmp/path1, /tmp/path2")
            .build();

    GcsClientOptions options = GcsClientOptions.createFromOptions(rawOptions, "gcs.");

    assertThat(options.getProjectId()).hasValue("test-project");
    assertThat(options.getClientLibToken()).hasValue("test-token");
    assertThat(options.getServiceHost()).hasValue("test-host");
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
}
