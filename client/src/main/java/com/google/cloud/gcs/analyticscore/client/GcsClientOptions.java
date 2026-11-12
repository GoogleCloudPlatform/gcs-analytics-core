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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.cloud.gcs.analyticscore.common.ConfigurationUtil;
import com.google.cloud.storage.BlobWriteSessionConfig;
import com.google.cloud.storage.BlobWriteSessionConfigs;
import com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Configuration options for the GCS client. */
@AutoValue
public abstract class GcsClientOptions {

  private static final int MB = 1024 * 1024;

  private static final String CLIENT_LIB_TOKEN_KEY = "client-lib-token";
  private static final String SERVICE_HOST_KEY = "service.host";
  private static final String UNIVERSE_DOMAIN_KEY = "universe-domain";
  private static final String USER_AGENT_KEY = "user-agent";
  static final String PROJECT_ID_KEY = "project-id";
  static final String USER_PROJECT_KEY = "user-project";
  private static final String CLIENT_PROTOCOL_KEY = "client.protocol";

  private static final String UPLOAD_CHUNK_SIZE_KEY = "channel.write.chunk-size-bytes";
  private static final String UPLOAD_TYPE_KEY = "channel.write.upload-type";
  private static final String PCU_BUFFER_COUNT_KEY = "channel.write.pcu.buffer.count";
  private static final String PCU_BUFFER_CAPACITY_KEY = "channel.write.pcu.buffer.capacity-bytes";
  private static final String PCU_PART_FILE_CLEANUP_TYPE_KEY =
      "channel.write.pcu.part-file.cleanup-type";
  private static final String PCU_PART_FILE_NAME_PREFIX_KEY =
      "channel.write.pcu.part-file.name-prefix";
  private static final String TEMPORARY_PATHS_KEY = "channel.write.temporary-paths";
  static final String BATCH_THREADS_KEY = "batch.threads";

  // TODO: Benchmark and determine the optimal default batch executor configuration; currently
  // mirrors hadoop-connectors defaults.
  private static final int DEFAULT_BATCH_THREADS = 15;
  static final int MAX_BATCH_THREADS = 1024;

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

  /**
   * Transport protocol used by the Cloud Storage client.
   *
   * <p>This selects the transport for <em>all</em> operations, and additionally enables bidi reads
   * when set to {@link #BIDI}. It deliberately does <em>not</em> enable appendable (bidi) writes —
   * those are restricted to zonal buckets using the Rapid storage class, so they require the
   * separate {@code channel.write.bidi.enabled} option on {@link GcsWriteOptions}.
   */
  public enum Protocol {
    /** JSON over HTTP. The default. */
    HTTP,
    /** gRPC, without bidi streaming. */
    GRPC,
    /** gRPC with bidi streaming reads. Required for appendable writes. */
    BIDI
  }

  public abstract Optional<String> getProjectId();

  /**
   * Returns the Google Cloud project ID billed for Requester Pays and quota-attributed operations.
   *
   * <p>This is the client-level default. {@link #getGcsReadOptions()} and {@link
   * #getGcsWriteOptions()} inherit it unless they carry an explicit per-channel override. A user
   * project set only on the read or write options applies only to that channel.
   *
   * <p>The client sends this project as the quota project of every request ({@code
   * x-goog-user-project}) and as the {@code userProject} parameter of object, bucket, and listing
   * requests. A per-channel override is sent with that channel's requests in addition to the
   * client-level quota project, on every protocol, so set per-channel overrides only when you have
   * verified which project Cloud Storage bills for your bucket.
   */
  public abstract Optional<String> getUserProject();

  public abstract Optional<String> getClientLibToken();

  public abstract Optional<String> getServiceHost();

  public abstract Optional<String> getUniverseDomain();

  public abstract Optional<String> getUserAgent();

  public abstract Protocol getProtocol();

  /**
   * Returns whether bidi streaming reads are enabled.
   *
   * <p>Note this does <em>not</em> imply appendable writes; see {@link
   * GcsWriteOptions#isBidiWriteEnabled()}.
   */
  boolean isBidiEnabled() {
    return getProtocol() == Protocol.BIDI;
  }

  /** Returns whether the gRPC transport should be used. */
  boolean isGrpcEnabled() {
    return getProtocol() == Protocol.GRPC || getProtocol() == Protocol.BIDI;
  }

  /**
   * Returns the default read options, inheriting {@link #getUserProject()} unless they carry an
   * explicit per-channel {@link GcsReadOptions#getUserProjectId()} override.
   *
   * <p>The returned options carry the inherited project. Passing them to the builder of options
   * with a different user project makes that project an explicit read override.
   */
  public GcsReadOptions getGcsReadOptions() {
    GcsReadOptions readOptions = getRawGcsReadOptions();
    if (readOptions.getUserProjectId().isPresent() || getUserProject().isEmpty()) {
      return readOptions;
    }
    return readOptions.toBuilder().setUserProjectId(getUserProject().get()).build();
  }

  /**
   * Returns the default write options, inheriting {@link #getUserProject()} unless they carry an
   * explicit per-channel {@link GcsWriteOptions#getUserProject()} override.
   *
   * <p>The returned options carry the inherited project. Passing them to the builder of options
   * with a different user project makes that project an explicit write override.
   */
  public GcsWriteOptions getGcsWriteOptions() {
    GcsWriteOptions writeOptions = getRawGcsWriteOptions();
    if (writeOptions.getUserProject().isPresent() || getUserProject().isEmpty()) {
      return writeOptions;
    }
    return writeOptions.toBuilder().setUserProject(getUserProject().get()).build();
  }

  /**
   * Read options as stored, holding a user project only when it overrides {@link
   * #getUserProject()}.
   */
  abstract GcsReadOptions getRawGcsReadOptions();

  /**
   * Write options as stored, holding a user project only when it overrides {@link
   * #getUserProject()}.
   */
  abstract GcsWriteOptions getRawGcsWriteOptions();

  /** Returns the retry settings applied to the underlying Cloud Storage client. */
  public abstract GcsRetryOptions getGcsRetryOptions();

  // Upload Session configurations
  public abstract int getUploadChunkSize();

  public abstract UploadType getUploadType();

  public abstract int getPcuBufferCount();

  public abstract int getPcuBufferCapacity();

  public abstract PartFileCleanupType getPcuPartFileCleanupType();

  public abstract String getPcuPartFileNamePrefix();

  public abstract ImmutableSet<String> getTemporaryPaths();

  /**
   * Returns the maximum number of threads used per batch operation (default {@code 15}, valid range
   * {@code [0, 1024]}).
   *
   * <p>When {@code 0}, batch requests run synchronously on the calling thread. Configured via the
   * {@code batch.threads} option key under the embedder prefix (for example {@code
   * fs.gs.batch.threads}); values outside {@code [0, 1024]} are rejected by {@link
   * Builder#build()}.
   */
  public abstract int getBatchThreads();

  /**
   * Returns a builder initialized with these options. Read or write options set on the returned
   * builder whose user project equals this instance's {@link #getUserProject()}, including options
   * copied from {@link #getGcsReadOptions()}, are treated as inheriting, so a new {@link
   * Builder#setUserProject(String)} also applies to them.
   */
  public Builder toBuilder() {
    Builder builder = autoToBuilder();
    builder.inheritedUserProject = getUserProject();
    return builder;
  }

  abstract Builder autoToBuilder();

  // TODO: Benchmark and determine the optimal default values for write options.
  public static Builder builder() {
    return new AutoValue_GcsClientOptions.Builder()
        .setProtocol(Protocol.HTTP)
        .setGcsReadOptions(GcsReadOptions.builder().build())
        .setGcsWriteOptions(GcsWriteOptions.builder().build())
        .setGcsRetryOptions(GcsRetryOptions.builder().build())
        .setUploadChunkSize(24 * MB)
        .setUploadType(UploadType.CHUNK_UPLOAD)
        .setPcuBufferCount(1)
        .setPcuBufferCapacity(32 * MB)
        .setPcuPartFileCleanupType(PartFileCleanupType.ALWAYS)
        .setPcuPartFileNamePrefix("")
        .setTemporaryPaths(ImmutableSet.of())
        .setBatchThreads(DEFAULT_BATCH_THREADS);
  }

  public static GcsClientOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    GcsClientOptions.Builder optionsBuilder = builder();
    if (analyticsCoreOptions.containsKey(prefix + PROJECT_ID_KEY)) {
      optionsBuilder.setProjectId(analyticsCoreOptions.get(prefix + PROJECT_ID_KEY));
    }
    ConfigurationUtil.getTrimmedValue(analyticsCoreOptions, prefix + USER_PROJECT_KEY)
        .ifPresent(optionsBuilder::setUserProject);
    if (analyticsCoreOptions.containsKey(prefix + CLIENT_LIB_TOKEN_KEY)) {
      optionsBuilder.setClientLibToken(analyticsCoreOptions.get(prefix + CLIENT_LIB_TOKEN_KEY));
    }
    if (analyticsCoreOptions.containsKey(prefix + SERVICE_HOST_KEY)) {
      optionsBuilder.setServiceHost(analyticsCoreOptions.get(prefix + SERVICE_HOST_KEY));
    }
    ConfigurationUtil.getTrimmedValue(analyticsCoreOptions, prefix + UNIVERSE_DOMAIN_KEY)
        .ifPresent(optionsBuilder::setUniverseDomain);
    if (analyticsCoreOptions.containsKey(prefix + USER_AGENT_KEY)) {
      optionsBuilder.setUserAgent(analyticsCoreOptions.get(prefix + USER_AGENT_KEY));
    }

    Optional.ofNullable(analyticsCoreOptions.get(prefix + CLIENT_PROTOCOL_KEY))
        .map(s -> Protocol.valueOf(s.trim().toUpperCase(Locale.ROOT)))
        .ifPresent(optionsBuilder::setProtocol);

    Optional.ofNullable(analyticsCoreOptions.get(prefix + UPLOAD_CHUNK_SIZE_KEY))
        .map(val -> ConfigurationUtil.safeParseInteger(prefix + UPLOAD_CHUNK_SIZE_KEY, val))
        .ifPresent(optionsBuilder::setUploadChunkSize);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + UPLOAD_TYPE_KEY))
        .map(s -> UploadType.valueOf(s.replace('-', '_').toUpperCase()))
        .ifPresent(optionsBuilder::setUploadType);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + PCU_BUFFER_COUNT_KEY))
        .map(val -> ConfigurationUtil.safeParseInteger(prefix + PCU_BUFFER_COUNT_KEY, val))
        .ifPresent(optionsBuilder::setPcuBufferCount);
    Optional.ofNullable(analyticsCoreOptions.get(prefix + PCU_BUFFER_CAPACITY_KEY))
        .map(val -> ConfigurationUtil.safeParseInteger(prefix + PCU_BUFFER_CAPACITY_KEY, val))
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
    ConfigurationUtil.getTrimmedValue(analyticsCoreOptions, prefix + BATCH_THREADS_KEY)
        .map(val -> ConfigurationUtil.safeParseInteger(prefix + BATCH_THREADS_KEY, val))
        .ifPresent(optionsBuilder::setBatchThreads);

    optionsBuilder.setGcsReadOptions(
        GcsReadOptions.createFromOptions(analyticsCoreOptions, prefix));
    optionsBuilder.setGcsWriteOptions(
        GcsWriteOptions.createFromOptions(analyticsCoreOptions, prefix));
    optionsBuilder.setGcsRetryOptions(
        GcsRetryOptions.createFromOptions(analyticsCoreOptions, prefix));

    return optionsBuilder.build();
  }

  /** Builder for {@link GcsClientOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Client-level user project of the options this builder was created from, if any. */
    private Optional<String> inheritedUserProject = Optional.empty();

    public abstract Builder setProjectId(String projectId);

    /**
     * Sets the Google Cloud project ID billed for Requester Pays and quota-attributed operations.
     *
     * <p>Surrounding whitespace is trimmed and a blank value is treated as unset by {@link
     * #build()}. Read and write options without their own override inherit this value, including
     * after {@link GcsClientOptions#toBuilder()}. See {@link GcsClientOptions#getUserProject()}.
     */
    public abstract Builder setUserProject(String userProject);

    abstract Builder setUserProject(Optional<String> userProject);

    public abstract Builder setClientLibToken(String clientLibToken);

    public abstract Builder setServiceHost(String serviceHost);

    public abstract Builder setUniverseDomain(String universeDomain);

    public abstract Builder setUserAgent(String userAgent);

    public abstract Builder setProtocol(Protocol protocol);

    /**
     * Sets the default read options. A user project set on them that equals the client-level {@link
     * #setUserProject(String) user project}, or the one of the options this builder was created
     * from, is treated as inherited.
     */
    public Builder setGcsReadOptions(GcsReadOptions readOptions) {
      return setRawGcsReadOptions(readOptions);
    }

    /**
     * Sets the default write options. A user project set on them that equals the client-level
     * {@link #setUserProject(String) user project}, or the one of the options this builder was
     * created from, is treated as inherited.
     */
    public Builder setGcsWriteOptions(GcsWriteOptions writeOptions) {
      return setRawGcsWriteOptions(writeOptions);
    }

    abstract Builder setRawGcsReadOptions(GcsReadOptions readOptions);

    abstract Builder setRawGcsWriteOptions(GcsWriteOptions writeOptions);

    public abstract Builder setGcsRetryOptions(GcsRetryOptions retryOptions);

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

    /**
     * Sets the maximum number of threads used per batch operation (default {@code 15}).
     *
     * <p>Must be in {@code [0, 1024]} (validated by {@link #build()}); {@code 0} runs batch
     * requests synchronously on the calling thread.
     */
    public abstract Builder setBatchThreads(int batchThreads);

    abstract GcsClientOptions autoBuild();

    public GcsClientOptions build() {
      GcsClientOptions options = normalizeUserProject(autoBuild());
      checkArgument(
          !options.getGcsWriteOptions().isBidiWriteEnabled()
              || options.getProtocol() == Protocol.BIDI,
          "Appendable writes require %s=BIDI, but was %s",
          CLIENT_PROTOCOL_KEY,
          options.getProtocol());
      checkArgument(
          options.getBatchThreads() >= 0,
          "%s must be non-negative, but was %s",
          BATCH_THREADS_KEY,
          options.getBatchThreads());
      checkArgument(
          options.getBatchThreads() <= MAX_BATCH_THREADS,
          "%s must be at most %s, but was %s",
          BATCH_THREADS_KEY,
          MAX_BATCH_THREADS,
          options.getBatchThreads());
      return options;
    }

    /**
     * Returns options whose client-level user project is trimmed and whose read and write options
     * keep a user project only when it overrides the inherited one. Works on a copy so this builder
     * is never modified.
     */
    private GcsClientOptions normalizeUserProject(GcsClientOptions options) {
      Optional<String> userProject =
          ConfigurationUtil.trimToOptional(options.getUserProject().orElse(null));
      GcsReadOptions readOptions = options.getRawGcsReadOptions();
      GcsWriteOptions writeOptions = options.getRawGcsWriteOptions();
      Optional<String> readOverride = overrideOf(readOptions.getUserProjectId(), userProject);
      Optional<String> writeOverride = overrideOf(writeOptions.getUserProject(), userProject);
      if (userProject.equals(options.getUserProject())
          && readOverride.equals(readOptions.getUserProjectId())
          && writeOverride.equals(writeOptions.getUserProject())) {
        return options;
      }
      return options
          .autoToBuilder()
          .setUserProject(userProject)
          .setRawGcsReadOptions(readOptions.toBuilder().setUserProjectId(readOverride).build())
          .setRawGcsWriteOptions(writeOptions.toBuilder().setUserProject(writeOverride).build())
          .autoBuild();
    }

    /** Returns the channel user project unless it equals the current or inherited client one. */
    private Optional<String> overrideOf(
        Optional<String> channelUserProject, Optional<String> userProject) {
      return channelUserProject.equals(userProject)
              || channelUserProject.equals(inheritedUserProject)
          ? Optional.empty()
          : channelUserProject;
    }
  }

  public BlobWriteSessionConfig generateSessionConfig() {
    switch (this.getUploadType()) {
      case PARALLEL_COMPOSITE_UPLOAD:
        return getParallelCompositeUploadSessionConfig();
      case WRITE_TO_DISK_THEN_UPLOAD:
        return getWriteToDiskSessionConfig();
      case JOURNALING:
        return getJournalingSessionConfig();
      case CHUNK_UPLOAD:
        return BlobWriteSessionConfigs.getDefault().withChunkSize(this.getUploadChunkSize());
      default:
        return BlobWriteSessionConfigs.getDefault();
    }
  }

  private BlobWriteSessionConfig getParallelCompositeUploadSessionConfig() {
    return BlobWriteSessionConfigs.parallelCompositeUpload()
        .withBufferAllocationStrategy(
            ParallelCompositeUploadBlobWriteSessionConfig.BufferAllocationStrategy.fixedPool(
                this.getPcuBufferCount(), this.getPcuBufferCapacity()))
        .withPartCleanupStrategy(getSdkCleanupStrategy(this.getPcuPartFileCleanupType()))
        .withPartNamingStrategy(
            ParallelCompositeUploadBlobWriteSessionConfig.PartNamingStrategy.prefix(
                this.getPcuPartFileNamePrefix()));
  }

  private BlobWriteSessionConfig getWriteToDiskSessionConfig() {
    try {
      if (!this.getTemporaryPaths().isEmpty()) {
        List<Path> paths = toPaths(this.getTemporaryPaths());
        return BlobWriteSessionConfigs.bufferToDiskThenUpload(paths);
      } else {
        return BlobWriteSessionConfigs.bufferToTempDirThenUpload();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed while initializing configs for " + this.getUploadType(), e);
    }
  }

  private BlobWriteSessionConfig getJournalingSessionConfig() {
    // TODO: Add the isHttpTransport check and support for JOURNALING once gRPC support is added to
    // gcs-analytics-core.
    throw new UnsupportedOperationException(
        "JOURNALING upload type is not supported since it requires gRPC transport.");
  }

  private static List<Path> toPaths(Collection<String> pathStrings) {
    return pathStrings.stream().map(Paths::get).collect(Collectors.toList());
  }

  private static ParallelCompositeUploadBlobWriteSessionConfig.PartCleanupStrategy
      getSdkCleanupStrategy(PartFileCleanupType cleanupType) {
    switch (cleanupType) {
      case NEVER:
        return ParallelCompositeUploadBlobWriteSessionConfig.PartCleanupStrategy.never();
      case ON_SUCCESS:
        return ParallelCompositeUploadBlobWriteSessionConfig.PartCleanupStrategy.onlyOnSuccess();
      case ALWAYS:
      default:
        return ParallelCompositeUploadBlobWriteSessionConfig.PartCleanupStrategy.always();
    }
  }
}
