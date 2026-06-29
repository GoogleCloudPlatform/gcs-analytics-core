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
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.auth.Credentials;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.BlobWriteSessionConfig;
import com.google.cloud.storage.BlobWriteSessionConfigs;
import com.google.cloud.storage.HttpStorageOptions;
import com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GcsClientImpl implements GcsClient {
  private static final Logger LOG = LoggerFactory.getLogger(GcsClientImpl.class);
  private static final List<Storage.BlobField> BLOB_METADATA_FIELDS =
      ImmutableList.of(Storage.BlobField.GENERATION, Storage.BlobField.SIZE);
  private static final String USER_AGENT_PREFIX = "gcs-analytics-core/";

  @VisibleForTesting Storage storage;
  private final GcsClientOptions clientOptions;
  private Supplier<ExecutorService> executorServiceSupplier;
  private final Telemetry telemetry;

  GcsClientImpl(
      Credentials credentials,
      GcsClientOptions clientOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry)
      throws IOException {
    this(Optional.of(credentials), clientOptions, executorServiceSupplier, telemetry);
  }

  GcsClientImpl(
      GcsClientOptions clientOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry)
      throws IOException {
    this(Optional.empty(), clientOptions, executorServiceSupplier, telemetry);
  }

  private GcsClientImpl(
      Optional<Credentials> credentials,
      GcsClientOptions clientOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry)
      throws IOException {
    this.clientOptions = clientOptions;
    this.executorServiceSupplier = executorServiceSupplier;
    this.telemetry = telemetry;
    this.storage = createStorage(credentials);
  }

  @Override
  public VectoredSeekableByteChannel openReadChannel(
      GcsItemInfo gcsItemInfo, GcsReadOptions readOptions) throws IOException {
    checkNotNull(gcsItemInfo, "itemInfo should not be null");
    checkNotNull(readOptions, "readOptions should not be null");
    checkArgument(
        gcsItemInfo.getItemId().isGcsObject(),
        "Expected GCS object to be provided. But got: " + gcsItemInfo.getItemId());

    return new GcsReadChannel(
        storage, gcsItemInfo, readOptions, executorServiceSupplier, telemetry);
  }

  @Override
  public VectoredSeekableByteChannel openReadChannel(
      GcsItemId gcsItemId, GcsReadOptions readOptions) throws IOException {
    checkNotNull(gcsItemId, "gcsItemId should not be null");
    checkNotNull(readOptions, "readOptions should not be null");
    return new GcsReadChannel(storage, gcsItemId, readOptions, executorServiceSupplier, telemetry) {
      @Override
      public long size() throws IOException {
        if (itemInfo == null) {
          itemInfo = getGcsItemInfo(itemId);
          itemId = itemInfo.getItemId();
        }
        return itemInfo.getSize();
      }
    };
  }

  @Override
  public WritableByteChannel create(GcsItemId itemId, GcsWriteOptions writeOptions)
      throws IOException {
    checkNotNull(itemId, "itemId should not be null");

    BlobInfo blobInfo = createBlobInfo(itemId);

    try {
      BlobWriteOption[] sdkWriteOptions = generateWriteOptions(writeOptions, itemId);
      BlobWriteSession sdkWriteSession = storage.blobWriteSession(blobInfo, sdkWriteOptions);
      WritableByteChannel channel = sdkWriteSession.open();
      return new GcsWriteChannel(sdkWriteSession, channel, blobInfo, writeOptions);
    } catch (StorageException | IOException e) {
      throw GcsExceptionUtil.translateWriteException(
          e, "initialization", blobInfo.getBlobId(), 0L, writeOptions);
    }
  }

  private BlobWriteSessionConfig generateSessionConfig(
      GcsWriteOptions writeOptions, boolean isHttpTransport) throws IOException {
    switch (writeOptions.getUploadType()) {
      case PARALLEL_COMPOSITE_UPLOAD:
        return getParallelCompositeUploadSessionConfig(writeOptions);
      case WRITE_TO_DISK_THEN_UPLOAD:
        return getWriteToDiskSessionConfig(writeOptions);
      case JOURNALING:
        return getJournalingSessionConfig(writeOptions, isHttpTransport);
      case CHUNK_UPLOAD:
        return BlobWriteSessionConfigs.getDefault()
            .withChunkSize(writeOptions.getUploadChunkSize());
      default:
        return BlobWriteSessionConfigs.getDefault();
    }
  }

  private BlobWriteSessionConfig getParallelCompositeUploadSessionConfig(
      GcsWriteOptions writeOptions) {
    return BlobWriteSessionConfigs.parallelCompositeUpload()
        .withBufferAllocationStrategy(
            ParallelCompositeUploadBlobWriteSessionConfig.BufferAllocationStrategy.fixedPool(
                writeOptions.getPcuBufferCount(), writeOptions.getPcuBufferCapacity()))
        .withPartCleanupStrategy(getSdkCleanupStrategy(writeOptions.getPcuPartFileCleanupType()))
        .withPartNamingStrategy(
            ParallelCompositeUploadBlobWriteSessionConfig.PartNamingStrategy.prefix(
                writeOptions.getPcuPartFileNamePrefix()));
  }

  private BlobWriteSessionConfig getWriteToDiskSessionConfig(GcsWriteOptions writeOptions)
      throws IOException {
    if (!writeOptions.getTemporaryPaths().isEmpty()) {
      List<Path> paths = toPaths(writeOptions.getTemporaryPaths());
      return BlobWriteSessionConfigs.bufferToDiskThenUpload(paths);
    } else {
      return BlobWriteSessionConfigs.bufferToTempDirThenUpload();
    }
  }

  private BlobWriteSessionConfig getJournalingSessionConfig(
      GcsWriteOptions writeOptions, boolean isHttpTransport) throws IOException {
    if (isHttpTransport) {
      throw new UnsupportedOperationException(
          "JOURNALING upload type is not supported because it requires the gRPC "
              + "transport backend (HTTP transport is currently active).");
    }
    checkArgument(
        !writeOptions.getTemporaryPaths().isEmpty(),
        "Temporary paths must be configured for JOURNALING upload type");
    List<Path> paths = toPaths(writeOptions.getTemporaryPaths());
    return BlobWriteSessionConfigs.journaling(paths);
  }

  private static List<Path> toPaths(Collection<String> pathStrings) {
    return pathStrings.stream().map(Paths::get).collect(Collectors.toList());
  }

  private ParallelCompositeUploadBlobWriteSessionConfig.PartCleanupStrategy getSdkCleanupStrategy(
      GcsWriteOptions.PartFileCleanupType cleanupType) {
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

  private BlobWriteOption[] generateWriteOptions(GcsWriteOptions writeOptions, GcsItemId itemId) {
    List<BlobWriteOption> sdkWriteOptions = new ArrayList<>();

    if (writeOptions != null) {
      if (writeOptions.isDisableGzipContent()) {
        sdkWriteOptions.add(BlobWriteOption.disableGzipContent());
      }
      if (writeOptions.isChecksumValidationEnabled()) {
        sdkWriteOptions.add(BlobWriteOption.crc32cMatch());
      }
      writeOptions.getKmsKeyName().map(BlobWriteOption::kmsKeyName).ifPresent(sdkWriteOptions::add);
      writeOptions
          .getEncryptionKey()
          .map(BlobWriteOption::encryptionKey)
          .ifPresent(sdkWriteOptions::add);
      writeOptions
          .getUserProject()
          .map(BlobWriteOption::userProject)
          .ifPresent(sdkWriteOptions::add);
    }

    // Determine overwrite semantics based on exact generation ID or 'doesNotExist' flag
    if (itemId.getContentGeneration().isPresent()) {
      sdkWriteOptions.add(BlobWriteOption.generationMatch());
    } else if (writeOptions != null && !writeOptions.isOverwriteExisting()) {
      sdkWriteOptions.add(BlobWriteOption.doesNotExist());
    }

    return sdkWriteOptions.toArray(new BlobWriteOption[0]);
  }

  @Override
  public GcsItemInfo getGcsItemInfo(GcsItemId itemId) throws IOException {
    checkNotNull(itemId, "Item ID must not be null.");
    if (itemId.isGcsObject()) {
      return getGcsObjectInfo(itemId);
    }
    throw new UnsupportedOperationException(
        String.format("Expected gcs object but got %s", itemId));
  }

  @Override
  public void close() {
    try {
      storage.close();
    } catch (Exception e) {
      LOG.debug("Exception while closing storage instance", e);
    }
  }

  @VisibleForTesting
  protected Storage createStorage(Optional<Credentials> credentials) throws IOException {
    StorageOptions.Builder builder = StorageOptions.newBuilder();
    String userAgent = getUserAgent();
    builder.setHeaderProvider(FixedHeaderProvider.create(ImmutableMap.of("User-Agent", userAgent)));
    clientOptions.getProjectId().ifPresent(builder::setProjectId);
    clientOptions.getClientLibToken().ifPresent(builder::setClientLibToken);
    clientOptions.getServiceHost().ifPresent(builder::setHost);
    credentials.ifPresent(builder::setCredentials);

    boolean isHttp = builder instanceof HttpStorageOptions.Builder;
    builder.setBlobWriteSessionConfig(
        generateSessionConfig(clientOptions.getGcsWriteOptions(), isHttp));

    return builder.build().getService();
  }

  private String getVersion() {
    return VersionHelper.VERSION;
  }

  @VisibleForTesting
  String getUserAgent() {
    return USER_AGENT_PREFIX
        + getVersion()
        + clientOptions.getUserAgent().map(agent -> " " + agent).orElse("");
  }

  private GcsItemInfo getGcsObjectInfo(GcsItemId itemId) throws IOException {
    checkArgument(itemId.isGcsObject(), String.format("Expected gcs object got %s", itemId));
    Blob blob = getBlob(itemId.getBucketName(), itemId.getObjectName().get());
    if (blob == null) {
      throw new IOException("Object not found:" + itemId);
    }
    GcsItemId itemIdWithGeneration =
        GcsItemId.builder()
            .setContentGeneration(blob.getGeneration())
            .setBucketName(blob.getBucket())
            .setObjectName(blob.getName())
            .build();
    return GcsItemInfo.builder()
        .setItemId(itemIdWithGeneration)
        .setSize(blob.getSize())
        .setContentGeneration(blob.getGeneration())
        .build();
  }

  private Blob getBlob(String bucketName, String objectName) throws IOException {
    checkNotNull(bucketName);
    checkNotNull(objectName);
    BlobId blobId = BlobId.of(bucketName, objectName);
    try {
      return storage.get(
          blobId,
          Storage.BlobGetOption.fields(BLOB_METADATA_FIELDS.toArray(new Storage.BlobField[0])));
    } catch (StorageException storageException) {
      throw new IOException("Unable to access blob :" + blobId, storageException);
    }
  }

  private BlobInfo createBlobInfo(GcsItemId itemId) {
    BlobId blobId;
    if (itemId.getContentGeneration().isPresent()) {
      blobId =
          BlobId.of(
              itemId.getBucketName(),
              itemId.getObjectName().get(),
              itemId.getContentGeneration().get());
    } else {
      blobId = BlobId.of(itemId.getBucketName(), itemId.getObjectName().get());
    }
    return BlobInfo.newBuilder(blobId).build();
  }
}
