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

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class GcsWriteOptionsTest {

  @Test
  void testDefaultValues() {
    GcsWriteOptions options = GcsWriteOptions.builder().build();

    assertThat(options.isChecksumValidationEnabled()).isFalse();
    assertThat(options.isDisableGzipContent()).isTrue();
    assertThat(options.isOverwriteExisting()).isTrue();
    assertThat(options.getUploadChunkSize()).isEqualTo(24 * 1024 * 1024);
    assertThat(options.getUploadType()).isEqualTo(GcsWriteOptions.UploadType.CHUNK_UPLOAD);
    // PCU related fields
    assertThat(options.getPcuBufferCount()).isEqualTo(1);
    assertThat(options.getPcuBufferCapacity()).isEqualTo(32 * 1024 * 1024);
    assertThat(options.getPcuPartFileCleanupType()).isEqualTo(GcsWriteOptions.PartFileCleanupType.ALWAYS);
    assertThat(options.getPcuPartFileNamePrefix()).isEmpty();
    // Other fields
    assertThat(options.getTemporaryPaths()).isEmpty();
    assertThat(options.getKmsKeyName()).isNull();
    assertThat(options.getUserProject()).isNull();
    assertThat(options.getEncryptionKey()).isNull();
  }

  @Test
  void testCustomValues() {
    GcsWriteOptions options =
        GcsWriteOptions.builder()
            .setChecksumValidationEnabled(true)
            .setDisableGzipContent(false)
            .setOverwriteExisting(false)
            .setUploadChunkSize(1024)
            .setUploadType(GcsWriteOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD)
            .setPcuBufferCount(4)
            .setPcuBufferCapacity(64 * 1024 * 1024)
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
    assertThat(options.getUploadType()).isEqualTo(GcsWriteOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD);
    // PCU related fields
    assertThat(options.getPcuBufferCount()).isEqualTo(4);
    assertThat(options.getPcuBufferCapacity()).isEqualTo(64 * 1024 * 1024);
    assertThat(options.getPcuPartFileCleanupType()).isEqualTo(GcsWriteOptions.PartFileCleanupType.ON_SUCCESS);
    assertThat(options.getPcuPartFileNamePrefix()).isEqualTo("temp-prefix-");
    // Other fields
    assertThat(options.getTemporaryPaths()).containsExactly("/tmp/path1", "/tmp/path2").inOrder();
    assertThat(options.getKmsKeyName()).isEqualTo("kms-key");
    assertThat(options.getUserProject()).isEqualTo("project-123");
    assertThat(options.getEncryptionKey()).isEqualTo("enc-key");
  }
}
