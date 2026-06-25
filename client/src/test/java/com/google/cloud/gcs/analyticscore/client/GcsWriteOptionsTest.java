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

class GcsWriteOptionsTest {

  private static final int MB = 1024 * 1024;

  @Test
  void builder_withDefaultValues_returnsExpectedDefaults() {
    GcsWriteOptions options = GcsWriteOptions.builder().build();

    assertThat(options.isChecksumValidationEnabled()).isFalse();
    assertThat(options.isDisableGzipContent()).isTrue();
    assertThat(options.isOverwriteExisting()).isTrue();
    assertThat(options.getUploadChunkSize()).isEqualTo(24 * MB);
    assertThat(options.getUploadType()).isEqualTo(GcsWriteOptions.UploadType.CHUNK_UPLOAD);
    assertThat(options.getPcuBufferCount()).isEqualTo(1);
    assertThat(options.getPcuBufferCapacity()).isEqualTo(32 * MB);
    assertThat(options.getPcuPartFileCleanupType())
        .isEqualTo(GcsWriteOptions.PartFileCleanupType.ALWAYS);
    assertThat(options.getPcuPartFileNamePrefix()).isEmpty();
    assertThat(options.getTemporaryPaths()).isEmpty();
    assertThat(options.getKmsKeyName().isPresent()).isFalse();
    assertThat(options.getUserProject().isPresent()).isFalse();
    assertThat(options.getEncryptionKey().isPresent()).isFalse();
  }

  @Test
  void builder_withCustomValues_setsAllProperties() {
    GcsWriteOptions options =
        GcsWriteOptions.builder()
            .setChecksumValidationEnabled(true)
            .setDisableGzipContent(false)
            .setOverwriteExisting(false)
            .setUploadChunkSize(1024)
            .setUploadType(GcsWriteOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD)
            .setPcuBufferCount(4)
            .setPcuBufferCapacity(64 * MB)
            .setPcuPartFileCleanupType(GcsWriteOptions.PartFileCleanupType.ON_SUCCESS)
            .setPcuPartFileNamePrefix("temp-prefix-")
            .setTemporaryPaths(ImmutableList.of("/tmp/path1", "/tmp/path2"))
            .setKmsKeyName("kms-key")
            .setUserProject("project-123")
            .setEncryptionKey("enc-key")
            .build();

    assertThat(options.isChecksumValidationEnabled()).isTrue();
    assertThat(options.isDisableGzipContent()).isFalse();
    assertThat(options.isOverwriteExisting()).isFalse();
    assertThat(options.getUploadChunkSize()).isEqualTo(1024);
    assertThat(options.getUploadType())
        .isEqualTo(GcsWriteOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD);
    assertThat(options.getPcuBufferCount()).isEqualTo(4);
    assertThat(options.getPcuBufferCapacity()).isEqualTo(64 * MB);
    assertThat(options.getPcuPartFileCleanupType())
        .isEqualTo(GcsWriteOptions.PartFileCleanupType.ON_SUCCESS);
    assertThat(options.getPcuPartFileNamePrefix()).isEqualTo("temp-prefix-");
    assertThat(options.getTemporaryPaths()).containsExactly("/tmp/path1", "/tmp/path2").inOrder();
    assertThat(options.getKmsKeyName()).hasValue("kms-key");
    assertThat(options.getUserProject()).hasValue("project-123");
    assertThat(options.getEncryptionKey()).hasValue("enc-key");
  }

  @Test
  void createFromOptions_withValidProperties_parsesCorrectly() {
    Map<String, String> rawOptions =
        ImmutableMap.<String, String>builder()
            .put("gcs.channel.write.checksum-validation.enabled", "true")
            .put("gcs.channel.write.disable-gzip-content", "false")
            .put("gcs.channel.write.overwrite-existing", "false")
            .put("gcs.channel.write.chunk-size-bytes", "1024")
            .put("gcs.channel.write.upload-type", "parallel_composite_upload")
            .put("gcs.channel.write.pcu.buffer.count", "4")
            .put("gcs.channel.write.pcu.buffer.capacity-bytes", "67108864")
            .put("gcs.channel.write.pcu.part-file.cleanup-type", "on_success")
            .put("gcs.channel.write.pcu.part-file.name-prefix", "temp-prefix-")
            .put("gcs.channel.write.temporary-paths", "/tmp/path1, /tmp/path2")
            .put("gcs.kms-key-name", "kms-key")
            .put("gcs.user-project", "project-123")
            .put("gcs.encryption-key", "enc-key")
            .build();

    GcsWriteOptions options = GcsWriteOptions.createFromOptions(rawOptions, "gcs.");

    assertThat(options.isChecksumValidationEnabled()).isTrue();
    assertThat(options.isDisableGzipContent()).isFalse();
    assertThat(options.isOverwriteExisting()).isFalse();
    assertThat(options.getUploadChunkSize()).isEqualTo(1024);
    assertThat(options.getUploadType())
        .isEqualTo(GcsWriteOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD);
    assertThat(options.getPcuBufferCount()).isEqualTo(4);
    assertThat(options.getPcuBufferCapacity()).isEqualTo(64 * MB);
    assertThat(options.getPcuPartFileCleanupType())
        .isEqualTo(GcsWriteOptions.PartFileCleanupType.ON_SUCCESS);
    assertThat(options.getPcuPartFileNamePrefix()).isEqualTo("temp-prefix-");
    assertThat(options.getTemporaryPaths()).containsExactly("/tmp/path1", "/tmp/path2").inOrder();
    assertThat(options.getKmsKeyName()).hasValue("kms-key");
    assertThat(options.getUserProject()).hasValue("project-123");
    assertThat(options.getEncryptionKey()).hasValue("enc-key");
  }

  @Test
  void createFromOptions_withWhitespaceAndEmptyPaths_filtersThemOut() {
    Map<String, String> rawOptions =
        ImmutableMap.of("gcs.channel.write.temporary-paths", "  , /tmp/path1 , , /tmp/path2 ");

    GcsWriteOptions options = GcsWriteOptions.createFromOptions(rawOptions, "gcs.");

    assertThat(options.getTemporaryPaths()).containsExactly("/tmp/path1", "/tmp/path2").inOrder();
  }

  @Test
  void createFromOptions_withHyphenatedEnums_normalizesAndParsesCorrectly() {
    Map<String, String> rawOptions =
        ImmutableMap.<String, String>builder()
            .put("gcs.channel.write.upload-type", "parallel-composite-upload")
            .put("gcs.channel.write.pcu.part-file.cleanup-type", "on-success")
            .build();

    GcsWriteOptions options = GcsWriteOptions.createFromOptions(rawOptions, "gcs.");

    assertThat(options.getUploadType())
        .isEqualTo(GcsWriteOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD);
    assertThat(options.getPcuPartFileCleanupType())
        .isEqualTo(GcsWriteOptions.PartFileCleanupType.ON_SUCCESS);
  }

  @Test
  void createFromOptions_withOverflowingUploadChunkSize_throwsIllegalArgumentException() {
    Map<String, String> rawOptions =
        ImmutableMap.of(
            "gcs.channel.write.chunk-size-bytes", String.valueOf((long) Integer.MAX_VALUE + 1L));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsWriteOptions.createFromOptions(rawOptions, "gcs."));

    assertThat(exception.getMessage()).contains("gcs.channel.write.chunk-size-bytes");
    assertThat(exception.getMessage()).contains("cannot be greater than Integer.MAX_VALUE");
  }

  @Test
  void createFromOptions_withOverflowingPcuBufferCount_throwsIllegalArgumentException() {
    Map<String, String> rawOptions =
        ImmutableMap.of(
            "gcs.channel.write.pcu.buffer.count", String.valueOf((long) Integer.MAX_VALUE + 1L));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsWriteOptions.createFromOptions(rawOptions, "gcs."));

    assertThat(exception.getMessage()).contains("gcs.channel.write.pcu.buffer.count");
    assertThat(exception.getMessage()).contains("cannot be greater than Integer.MAX_VALUE");
  }

  @Test
  void createFromOptions_withOverflowingPcuBufferCapacity_throwsIllegalArgumentException() {
    Map<String, String> rawOptions =
        ImmutableMap.of(
            "gcs.channel.write.pcu.buffer.capacity-bytes",
            String.valueOf((long) Integer.MAX_VALUE + 1L));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsWriteOptions.createFromOptions(rawOptions, "gcs."));

    assertThat(exception.getMessage()).contains("gcs.channel.write.pcu.buffer.capacity-bytes");
    assertThat(exception.getMessage()).contains("cannot be greater than Integer.MAX_VALUE");
  }
}
