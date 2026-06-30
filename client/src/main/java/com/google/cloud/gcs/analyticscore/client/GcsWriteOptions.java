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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Configuration options for writing objects to Google Cloud Storage.
 *
 * <p>This class abstracts client-specific configurations into a unified, generic set of properties
 * utilized by {@code gcs-analytics-core}. By centralizing these options, it ensures that any
 * integrating analytics framework or compute engine can leverage the exact same underlying upload
 * strategies and performance optimizations.
 */
@AutoValue
public abstract class GcsWriteOptions {

  private static final int MB = 1024 * 1024;

  private static final String CHECKSUM_VALIDATION_KEY = "channel.write.checksum-validation.enabled";
  private static final String DISABLE_GZIP_CONTENT_KEY = "channel.write.disable-gzip-content";
  private static final String OVERWRITE_EXISTING_KEY = "channel.write.overwrite-existing";
  private static final String UPLOAD_CHUNK_SIZE_KEY = "channel.write.chunk-size-bytes";
  private static final String UPLOAD_TYPE_KEY = "channel.write.upload-type";
  private static final String PCU_BUFFER_COUNT_KEY = "channel.write.pcu.buffer.count";
  private static final String PCU_BUFFER_CAPACITY_KEY = "channel.write.pcu.buffer.capacity-bytes";
  private static final String PCU_PART_FILE_CLEANUP_TYPE_KEY =
      "channel.write.pcu.part-file.cleanup-type";
  private static final String PCU_PART_FILE_NAME_PREFIX_KEY =
      "channel.write.pcu.part-file.name-prefix";
  private static final String TEMPORARY_PATHS_KEY = "channel.write.temporary-paths";
  private static final String KMS_KEY_NAME_KEY = "kms-key-name";
  private static final String USER_PROJECT_KEY = "user-project";
  private static final String ENCRYPTION_KEY_KEY = "encryption-key";

  /**
   * Upload strategies matching the configurations offered by the google-cloud-storage Java client.
   */
  public enum UploadType {
    CHUNK_UPLOAD,
    WRITE_TO_DISK_THEN_UPLOAD,
    JOURNALING,
    PARALLEL_COMPOSITE_UPLOAD
  }

  /** Part file cleanup strategy for parallel composite upload. */
  public enum PartFileCleanupType {
    ALWAYS,
    NEVER,
    ON_SUCCESS
  }

  public abstract boolean isChecksumValidationEnabled();

  public abstract boolean isDisableGzipContent();

  public abstract boolean isOverwriteExisting();

  public abstract int getUploadChunkSize();

  public abstract UploadType getUploadType();

  // PCU Configurations (Active only if uploadType == PARALLEL_COMPOSITE_UPLOAD)
  public abstract int getPcuBufferCount();

  public abstract int getPcuBufferCapacity();

  public abstract PartFileCleanupType getPcuPartFileCleanupType();

  public abstract String getPcuPartFileNamePrefix();

  // Disk Buffering Configurations (Active only if uploadType == WRITE_TO_DISK_THEN_UPLOAD or
  // JOURNALING)
  public abstract ImmutableSet<String> getTemporaryPaths();

  // Metadata/Auth Configurations
  public abstract Optional<String> getKmsKeyName();

  public abstract Optional<String> getUserProject();

  public abstract Optional<String> getEncryptionKey();

  public abstract Builder toBuilder();

  public static GcsWriteOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    Builder optionsBuilder = builder();
    Optional.ofNullable(analyticsCoreOptions.get(prefix + CHECKSUM_VALIDATION_KEY))
        .map(Boolean::parseBoolean)
        .ifPresent(optionsBuilder::setChecksumValidationEnabled);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + DISABLE_GZIP_CONTENT_KEY))
        .map(Boolean::parseBoolean)
        .ifPresent(optionsBuilder::setDisableGzipContent);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + OVERWRITE_EXISTING_KEY))
        .map(Boolean::parseBoolean)
        .ifPresent(optionsBuilder::setOverwriteExisting);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + UPLOAD_CHUNK_SIZE_KEY))
        .map(val -> safeParseInteger(prefix + UPLOAD_CHUNK_SIZE_KEY, val))
        .ifPresent(optionsBuilder::setUploadChunkSize);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + UPLOAD_TYPE_KEY))
        .map(s -> UploadType.valueOf(s.replace('-', '_').toUpperCase()))
        .ifPresent(optionsBuilder::setUploadType);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + PCU_BUFFER_COUNT_KEY))
        .map(val -> safeParseInteger(prefix + PCU_BUFFER_COUNT_KEY, val))
        .ifPresent(optionsBuilder::setPcuBufferCount);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + PCU_BUFFER_CAPACITY_KEY))
        .map(val -> safeParseInteger(prefix + PCU_BUFFER_CAPACITY_KEY, val))
        .ifPresent(optionsBuilder::setPcuBufferCapacity);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + PCU_PART_FILE_CLEANUP_TYPE_KEY))
        .map(s -> PartFileCleanupType.valueOf(s.replace('-', '_').toUpperCase()))
        .ifPresent(optionsBuilder::setPcuPartFileCleanupType);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + PCU_PART_FILE_NAME_PREFIX_KEY))
        .ifPresent(optionsBuilder::setPcuPartFileNamePrefix);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + TEMPORARY_PATHS_KEY))
        .filter(pathsStr -> !pathsStr.trim().isEmpty())
        .map(
            pathsStr ->
                Arrays.stream(pathsStr.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList()))
        .ifPresent(optionsBuilder::setTemporaryPaths);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + KMS_KEY_NAME_KEY))
        .ifPresent(optionsBuilder::setKmsKeyName);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + USER_PROJECT_KEY))
        .ifPresent(optionsBuilder::setUserProject);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + ENCRYPTION_KEY_KEY))
        .ifPresent(optionsBuilder::setEncryptionKey);
    return optionsBuilder.build();
  }

  private static int safeParseInteger(String key, String valueStr) {
    long value = Long.parseLong(valueStr);
    if (value > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format(
              "%s=%d cannot be greater than Integer.MAX_VALUE (%d)",
              key, value, Integer.MAX_VALUE));
    }
    return (int) value;
  }

  public static Builder builder() {
    return new AutoValue_GcsWriteOptions.Builder()
        .setChecksumValidationEnabled(false)
        .setDisableGzipContent(true)
        .setOverwriteExisting(true)
        .setUploadChunkSize(24 * MB)
        .setUploadType(UploadType.CHUNK_UPLOAD)
        .setPcuBufferCount(1)
        .setPcuBufferCapacity(32 * MB)
        .setPcuPartFileCleanupType(PartFileCleanupType.ALWAYS)
        .setPcuPartFileNamePrefix("")
        .setTemporaryPaths(ImmutableSet.of());
  }

  /** Builder for {@link GcsWriteOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setChecksumValidationEnabled(boolean enabled);

    public abstract Builder setDisableGzipContent(boolean disable);

    public abstract Builder setOverwriteExisting(boolean overwrite);

    public abstract Builder setUploadChunkSize(int size);

    public abstract Builder setUploadType(UploadType type);

    public abstract Builder setPcuBufferCount(int count);

    public abstract Builder setPcuBufferCapacity(int capacity);

    public abstract Builder setPcuPartFileCleanupType(PartFileCleanupType cleanupType);

    public abstract Builder setPcuPartFileNamePrefix(String prefix);

    public abstract Builder setTemporaryPaths(ImmutableSet<String> paths);

    public Builder setTemporaryPaths(Collection<String> paths) {
      return setTemporaryPaths(ImmutableSet.copyOf(paths));
    }

    public abstract Builder setKmsKeyName(String key);

    public abstract Builder setUserProject(String project);

    public abstract Builder setEncryptionKey(String key);

    public abstract GcsWriteOptions build();
  }
}
