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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

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

  private static final String CHECKSUM_VALIDATION_KEY = "channel.write.checksum-validation.enabled";
  private static final String DISABLE_GZIP_CONTENT_KEY = "channel.write.disable-gzip-content";
  private static final String OVERWRITE_EXISTING_KEY = "channel.write.overwrite-existing";
  private static final String KMS_KEY_NAME_KEY = "kms-key-name";
  private static final String USER_PROJECT_KEY = "user-project";
  private static final String ENCRYPTION_KEY_KEY = "encryption-key";
  private static final String BIDI_WRITE_ENABLED_KEY = "channel.write.bidi-write.enabled";
  private static final String FINALIZE_ON_CLOSE_KEY = "channel.write.finalize-on-close";

  public abstract boolean isChecksumValidationEnabled();

  public abstract boolean isDisableGzipContent();

  public abstract boolean isOverwriteExisting();

  public abstract boolean isBidiWriteEnabled();

  public abstract boolean isFinalizeOnClose();

  // Metadata/Auth Configurations
  public abstract Optional<String> getKmsKeyName();

  public abstract Optional<String> getUserProject();

  public abstract Optional<String> getEncryptionKey();

  public abstract Optional<String> getContentType();

  public abstract Optional<String> getContentEncoding();

  @Nullable
  abstract ImmutableMap<String, byte[]> getNullableMetadata();

  public ImmutableMap<String, byte[]> getMetadata() {
    return getNullableMetadata() == null ? ImmutableMap.of() : getNullableMetadata();
  }

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
    Optional.ofNullable(analyticsCoreOptions.get(prefix + KMS_KEY_NAME_KEY))
        .ifPresent(optionsBuilder::setKmsKeyName);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + USER_PROJECT_KEY))
        .ifPresent(optionsBuilder::setUserProject);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + ENCRYPTION_KEY_KEY))
        .ifPresent(optionsBuilder::setEncryptionKey);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + BIDI_WRITE_ENABLED_KEY))
        .map(Boolean::parseBoolean)
        .ifPresent(optionsBuilder::setBidiWriteEnabled);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + FINALIZE_ON_CLOSE_KEY))
        .map(Boolean::parseBoolean)
        .ifPresent(optionsBuilder::setFinalizeOnClose);
    return optionsBuilder.build();
  }

  public static Builder builder() {
    return new AutoValue_GcsWriteOptions.Builder()
        .setChecksumValidationEnabled(false)
        .setDisableGzipContent(true)
        .setOverwriteExisting(true)
        .setContentType("application/octet-stream")
        .setMetadata(ImmutableMap.of())
        .setBidiWriteEnabled(false)
        .setFinalizeOnClose(false);
  }

  /** Builder for {@link GcsWriteOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setChecksumValidationEnabled(boolean enabled);

    public abstract Builder setDisableGzipContent(boolean disable);

    public abstract Builder setOverwriteExisting(boolean overwrite);

    public abstract Builder setKmsKeyName(String key);

    public abstract Builder setUserProject(String project);

    public abstract Builder setEncryptionKey(String key);

    public abstract Builder setBidiWriteEnabled(boolean enabled);

    public abstract Builder setFinalizeOnClose(boolean finalize);

    public abstract Builder setContentType(String contentType);

    public abstract Builder setContentEncoding(String contentEncoding);

    abstract Builder setNullableMetadata(@Nullable ImmutableMap<String, byte[]> metadata);

    public Builder setMetadata(@Nullable Map<String, byte[]> metadata) {
      return setNullableMetadata(metadata == null ? null : ImmutableMap.copyOf(metadata));
    }

    abstract GcsWriteOptions autoBuild();

    public GcsWriteOptions build() {
      GcsWriteOptions options = autoBuild();
      if (!options.getMetadata().isEmpty()) {
        boolean hasContentEncoding =
            options.getMetadata().keySet().stream()
                .anyMatch(key -> key.equalsIgnoreCase("Content-Encoding"));
        checkArgument(
            !hasContentEncoding,
            "The Content-Encoding must be provided explicitly via the 'contentEncoding' parameter");
        boolean hasContentType =
            options.getMetadata().keySet().stream()
                .anyMatch(key -> key.equalsIgnoreCase("Content-Type"));
        checkArgument(
            !hasContentType,
            "The Content-Type must be provided explicitly via the 'contentType' parameter");
      }
      return options;
    }
  }

  public BlobWriteOption[] generateWriteOptions(GcsItemId itemId) {
    List<BlobWriteOption> sdkWriteOptions = new ArrayList<>();

    // If overwrite is disabled, the target MUST NOT exist over the wire (ifGenerationMatch = 0L).
    // Otherwise, we conditionally match a targeted generation if present.
    Optional<BlobWriteOption> preconditionOption =
        !this.isOverwriteExisting()
            ? Optional.of(BlobWriteOption.doesNotExist())
            : itemId.getContentGeneration().map(BlobWriteOption::generationMatch);
    preconditionOption.ifPresent(sdkWriteOptions::add);

    if (this.isDisableGzipContent()) {
      sdkWriteOptions.add(BlobWriteOption.disableGzipContent());
    }
    if (this.isChecksumValidationEnabled()) {
      sdkWriteOptions.add(BlobWriteOption.crc32cMatch());
    }
    this.getKmsKeyName().map(BlobWriteOption::kmsKeyName).ifPresent(sdkWriteOptions::add);
    this.getEncryptionKey().map(BlobWriteOption::encryptionKey).ifPresent(sdkWriteOptions::add);
    this.getUserProject().map(BlobWriteOption::userProject).ifPresent(sdkWriteOptions::add);

    return sdkWriteOptions.toArray(new BlobWriteOption[0]);
  }
}
