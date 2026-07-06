/*
 * Copyright 2025 Google LLC
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

/** Configuration options for the GCS client. */
@AutoValue
public abstract class GcsClientOptions {

  private static final int MB = 1024 * 1024;

  private static final String CLIENT_LIB_TOKEN_KEY = "client-lib-token";
  private static final String SERVICE_HOST_KEY = "service.host";
  private static final String USER_AGENT_KEY = "user-agent";
  static final String PROJECT_ID_KEY = "project-id";

  private static final String UPLOAD_CHUNK_SIZE_KEY = "channel.write.chunk-size-bytes";
  private static final String UPLOAD_TYPE_KEY = "channel.write.upload-type";
  private static final String PCU_BUFFER_COUNT_KEY = "channel.write.pcu.buffer.count";
  private static final String PCU_BUFFER_CAPACITY_KEY = "channel.write.pcu.buffer.capacity-bytes";
  private static final String PCU_PART_FILE_CLEANUP_TYPE_KEY =
      "channel.write.pcu.part-file.cleanup-type";
  private static final String PCU_PART_FILE_NAME_PREFIX_KEY =
      "channel.write.pcu.part-file.name-prefix";
  private static final String TEMPORARY_PATHS_KEY = "channel.write.temporary-paths";

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

  public abstract Optional<String> getProjectId();

  public abstract Optional<String> getClientLibToken();

  public abstract Optional<String> getServiceHost();

  public abstract Optional<String> getUserAgent();

  public abstract GcsReadOptions getGcsReadOptions();

  public abstract GcsWriteOptions getGcsWriteOptions();

  // Upload Session configurations
  public abstract int getUploadChunkSize();

  public abstract UploadType getUploadType();

  public abstract int getPcuBufferCount();

  public abstract int getPcuBufferCapacity();

  public abstract PartFileCleanupType getPcuPartFileCleanupType();

  public abstract String getPcuPartFileNamePrefix();

  public abstract ImmutableSet<String> getTemporaryPaths();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_GcsClientOptions.Builder()
        .setGcsReadOptions(GcsReadOptions.builder().build())
        .setGcsWriteOptions(GcsWriteOptions.builder().build())
        .setUploadChunkSize(24 * MB)
        .setUploadType(UploadType.CHUNK_UPLOAD)
        .setPcuBufferCount(1)
        .setPcuBufferCapacity(32 * MB)
        .setPcuPartFileCleanupType(PartFileCleanupType.ALWAYS)
        .setPcuPartFileNamePrefix("")
        .setTemporaryPaths(ImmutableSet.of());
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

  public static GcsClientOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    GcsClientOptions.Builder optionsBuilder = builder();
    if (analyticsCoreOptions.containsKey(prefix + PROJECT_ID_KEY)) {
      optionsBuilder.setProjectId(analyticsCoreOptions.get(prefix + PROJECT_ID_KEY));
    }
    if (analyticsCoreOptions.containsKey(prefix + CLIENT_LIB_TOKEN_KEY)) {
      optionsBuilder.setClientLibToken(analyticsCoreOptions.get(prefix + CLIENT_LIB_TOKEN_KEY));
    }
    if (analyticsCoreOptions.containsKey(prefix + SERVICE_HOST_KEY)) {
      optionsBuilder.setServiceHost(analyticsCoreOptions.get(prefix + SERVICE_HOST_KEY));
    }
    if (analyticsCoreOptions.containsKey(prefix + USER_AGENT_KEY)) {
      optionsBuilder.setUserAgent(analyticsCoreOptions.get(prefix + USER_AGENT_KEY));
    }

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

    optionsBuilder.setGcsReadOptions(
        GcsReadOptions.createFromOptions(analyticsCoreOptions, prefix));
    optionsBuilder.setGcsWriteOptions(
        GcsWriteOptions.createFromOptions(analyticsCoreOptions, prefix));

    return optionsBuilder.build();
  }

  /** Builder for {@link GcsClientOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setClientLibToken(String clientLibToken);

    public abstract Builder setServiceHost(String serviceHost);

    public abstract Builder setUserAgent(String userAgent);

    public abstract Builder setGcsReadOptions(GcsReadOptions readOptions);

    public abstract Builder setGcsWriteOptions(GcsWriteOptions writeOptions);

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

    public abstract GcsClientOptions build();
  }
}
