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

import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GcsWriteOptionsTest {

  @Test
  void builder_withDefaultValues_returnsExpectedDefaults() {
    GcsWriteOptions options = GcsWriteOptions.builder().build();

    assertThat(options.isChecksumValidationEnabled()).isFalse();
    assertThat(options.isDisableGzipContent()).isTrue();
    assertThat(options.isOverwriteExisting()).isTrue();
    assertThat(options.getKmsKeyName().isPresent()).isFalse();
    assertThat(options.getUserProject().isPresent()).isFalse();
    assertThat(options.getEncryptionKey().isPresent()).isFalse();
    assertThat(options.getContentType()).hasValue("application/octet-stream");
    assertThat(options.getContentEncoding().isPresent()).isFalse();
    assertThat(options.getMetadata()).isEmpty();
    assertThat(options.isBidiWriteEnabled()).isFalse();
    assertThat(options.isFinalizeOnClose()).isFalse();
  }

  @Test
  void builder_withNullMetadata_buildsSuccessfully() {
    GcsWriteOptions options = GcsWriteOptions.builder().setMetadata(null).build();

    assertThat(options.getMetadata()).isEmpty();
  }

  @Test
  void builder_withCustomValues_setsAllProperties() {
    GcsWriteOptions options =
        GcsWriteOptions.builder()
            .setChecksumValidationEnabled(true)
            .setDisableGzipContent(false)
            .setOverwriteExisting(false)
            .setKmsKeyName("kms-key")
            .setUserProject("project-123")
            .setEncryptionKey("enc-key")
            .setContentType("text/plain")
            .setContentEncoding("gzip")
            .setMetadata(ImmutableMap.of("custom-key", new byte[] {1, 2, 3}))
            .setBidiWriteEnabled(true)
            .setFinalizeOnClose(true)
            .build();

    assertThat(options.isChecksumValidationEnabled()).isTrue();
    assertThat(options.isDisableGzipContent()).isFalse();
    assertThat(options.isOverwriteExisting()).isFalse();
    assertThat(options.getKmsKeyName()).hasValue("kms-key");
    assertThat(options.getUserProject()).hasValue("project-123");
    assertThat(options.getEncryptionKey()).hasValue("enc-key");
    assertThat(options.getContentType()).hasValue("text/plain");
    assertThat(options.getContentEncoding()).hasValue("gzip");
    assertThat(options.getMetadata()).containsKey("custom-key");
    assertThat(options.getMetadata().get("custom-key")).isEqualTo(new byte[] {1, 2, 3});
    assertThat(options.isBidiWriteEnabled()).isTrue();
    assertThat(options.isFinalizeOnClose()).isTrue();
  }

  @Test
  void createFromOptions_withValidProperties_parsesCorrectly() {
    Map<String, String> rawOptions =
        ImmutableMap.<String, String>builder()
            .put("gcs." + GcsWriteOptions.CHECKSUM_VALIDATION_KEY, "true")
            .put("gcs." + GcsWriteOptions.DISABLE_GZIP_CONTENT_KEY, "false")
            .put("gcs." + GcsWriteOptions.OVERWRITE_EXISTING_KEY, "false")
            .put("gcs." + GcsWriteOptions.KMS_KEY_NAME_KEY, "kms-key")
            .put("gcs." + GcsWriteOptions.USER_PROJECT_KEY, "project-123")
            .put("gcs." + GcsWriteOptions.ENCRYPTION_KEY_KEY, "enc-key")
            .put("gcs." + GcsWriteOptions.BIDI_WRITE_ENABLED_KEY, "true")
            .put("gcs." + GcsWriteOptions.FINALIZE_ON_CLOSE_KEY, "true")
            .build();

    GcsWriteOptions options = GcsWriteOptions.createFromOptions(rawOptions, "gcs.");

    assertThat(options.isChecksumValidationEnabled()).isTrue();
    assertThat(options.isDisableGzipContent()).isFalse();
    assertThat(options.isOverwriteExisting()).isFalse();
    assertThat(options.getKmsKeyName()).hasValue("kms-key");
    assertThat(options.getUserProject()).hasValue("project-123");
    assertThat(options.getEncryptionKey()).hasValue("enc-key");
    assertThat(options.isBidiWriteEnabled()).isTrue();
    assertThat(options.isFinalizeOnClose()).isTrue();
  }

  @Test
  void builder_withContentTypeInMetadata_throwsIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                GcsWriteOptions.builder()
                    .setMetadata(ImmutableMap.of("Content-Type", new byte[] {}))
                    .build());
    assertThat(exception).hasMessageThat().contains("explicitly via the 'contentType' parameter");
  }

  @Test
  void builder_withContentEncodingInMetadata_throwsIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                GcsWriteOptions.builder()
                    .setMetadata(ImmutableMap.of("Content-Encoding", new byte[] {}))
                    .build());
    assertThat(exception)
        .hasMessageThat()
        .contains("explicitly via the 'contentEncoding' parameter");
  }

  @Test
  void generateWriteOptions_whenOverwriteExistingIsFalse_addsDoesNotExistOption() {
    GcsWriteOptions options = GcsWriteOptions.builder().setOverwriteExisting(false).build();
    GcsItemId itemId = GcsItemId.builder().setBucketName("bucket").setObjectName("object").build();

    BlobWriteOption[] sdkOptions = options.generateWriteOptions(itemId);

    List<BlobWriteOption> optionList = Arrays.asList(sdkOptions);
    assertThat(optionList).contains(BlobWriteOption.doesNotExist());
  }

  @Test
  void
      generateWriteOptions_whenOverwriteExistingIsFalseWithGeneration_addsDoesNotExistOptionAndIgnoresGeneration() {
    GcsWriteOptions options = GcsWriteOptions.builder().setOverwriteExisting(false).build();
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName("bucket")
            .setObjectName("object")
            .setContentGeneration(12345L)
            .build();

    BlobWriteOption[] sdkOptions = options.generateWriteOptions(itemId);

    List<BlobWriteOption> optionList = Arrays.asList(sdkOptions);
    assertThat(optionList).contains(BlobWriteOption.doesNotExist());
    assertThat(optionList).doesNotContain(BlobWriteOption.generationMatch(12345L));
  }

  @Test
  void generateWriteOptions_whenOverwriteExistingIsTrueWithGeneration_addsGenerationMatchOption() {
    GcsWriteOptions options = GcsWriteOptions.builder().setOverwriteExisting(true).build();
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName("bucket")
            .setObjectName("object")
            .setContentGeneration(12345L)
            .build();

    BlobWriteOption[] sdkOptions = options.generateWriteOptions(itemId);

    List<BlobWriteOption> optionList = Arrays.asList(sdkOptions);
    assertThat(optionList).contains(BlobWriteOption.generationMatch(12345L));
    assertThat(optionList).doesNotContain(BlobWriteOption.doesNotExist());
  }
}
