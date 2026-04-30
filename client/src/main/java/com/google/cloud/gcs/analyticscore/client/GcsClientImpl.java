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
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
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
      Telemetry telemetry) {
    this(Optional.of(credentials), clientOptions, executorServiceSupplier, telemetry);
  }

  GcsClientImpl(
      GcsClientOptions clientOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry) {
    this(Optional.empty(), clientOptions, executorServiceSupplier, telemetry);
  }

  private GcsClientImpl(
      Optional<Credentials> credentials,
      GcsClientOptions clientOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry) {
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
  public WritableByteChannel create(BlobInfo blobInfo, GcsWriteOptions writeOptions)
      throws IOException {
    try {
      BlobWriteOption[] options = generateWriteOptions(writeOptions, blobInfo);

      // 'storage' here is the internal instance, which will be the
      // StorageClientWrapper injected by Hadoop!
      BlobWriteSession writeSession = this.storage.blobWriteSession(blobInfo, options);
      return writeSession.open();

    } catch (StorageException e) {
      LOG.error(
          "Failed to initialize BlobWriteSession for object: gs://{}/{}",
          blobInfo.getBucket(),
          blobInfo.getName(),
          e);

      GcsExceptionUtil.ErrorType errorType = GcsExceptionUtil.getErrorType(e);
      if (errorType == GcsExceptionUtil.ErrorType.ALREADY_EXISTS) {
        throw (FileAlreadyExistsException)
            new FileAlreadyExistsException(
                    String.format(
                        "Object gs://%s/%s already exists.",
                        blobInfo.getBucket(), blobInfo.getName()))
                .initCause(e);
      } else if (errorType == GcsExceptionUtil.ErrorType.ACCESS_DENIED) {
        throw (AccessDeniedException)
            new AccessDeniedException(
                    String.format("gs://%s/%s", blobInfo.getBucket(), blobInfo.getName()),
                    null,
                    String.format(
                        "Access denied to object during initialization: %s", e.getMessage()))
                .initCause(e);
      }
      throw new IOException("Failed to initialize BlobWriteSession for " + blobInfo.getBlobId(), e);
    }
  }

  private BlobWriteOption[] generateWriteOptions(GcsWriteOptions writeOptions, BlobInfo blobInfo) {
    List<BlobWriteOption> options = new ArrayList<>();

    if (writeOptions != null) {
      if (writeOptions.isDisableGzipContent()) {
        options.add(BlobWriteOption.disableGzipContent());
      }
      if (writeOptions.isChecksumValidationEnabled()) {
        options.add(BlobWriteOption.crc32cMatch());
      }
      if (writeOptions.getKmsKeyName() != null) {
        options.add(BlobWriteOption.kmsKeyName(writeOptions.getKmsKeyName()));
      }
      if (writeOptions.getEncryptionKey() != null) {
        options.add(BlobWriteOption.encryptionKey(writeOptions.getEncryptionKey()));
      }
      if (writeOptions.getUserProject() != null) {
        options.add(BlobWriteOption.userProject(writeOptions.getUserProject()));
      }
    }

    // Determine overwrite semantics based on exact generation ID or 'doesNotExist' flag
    if (blobInfo.getBlobId().getGeneration() != null) {
      options.add(BlobWriteOption.generationMatch());
    } else if (writeOptions != null && !writeOptions.isOverwriteExisting()) {
      options.add(BlobWriteOption.doesNotExist());
    }

    return options.toArray(new BlobWriteOption[0]);
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
  protected Storage createStorage(Optional<Credentials> credentials) {
    StorageOptions.Builder builder = StorageOptions.newBuilder();
    String userAgent = getUserAgent();
    builder.setHeaderProvider(FixedHeaderProvider.create(ImmutableMap.of("User-Agent", userAgent)));
    clientOptions.getProjectId().ifPresent(builder::setProjectId);
    clientOptions.getClientLibToken().ifPresent(builder::setClientLibToken);
    clientOptions.getServiceHost().ifPresent(builder::setHost);
    credentials.ifPresent(builder::setCredentials);

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
}
