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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.paging.Page;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.auth.Credentials;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo.ItemType;
import com.google.cloud.gcs.analyticscore.client.GcsReadChannel.ItemInfoProvider;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.BucketInfo.HierarchicalNamespace;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.Storage.BucketField;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.Timestamp;
import com.google.storage.control.v2.Folder;
import com.google.storage.control.v2.FolderName;
import com.google.storage.control.v2.GetFolderRequest;
import com.google.storage.control.v2.StorageControlClient;
import com.google.storage.control.v2.StorageControlSettings;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GcsClientImpl implements GcsClient {
  private static final Logger LOG = LoggerFactory.getLogger(GcsClientImpl.class);
  private static final List<BlobField> BLOB_METADATA_FIELDS =
      ImmutableList.of(
          BlobField.NAME,
          BlobField.BUCKET,
          BlobField.GENERATION,
          BlobField.METAGENERATION,
          BlobField.SIZE,
          BlobField.CONTENT_TYPE,
          BlobField.CONTENT_ENCODING,
          BlobField.TIME_CREATED,
          BlobField.UPDATED,
          BlobField.MD5HASH,
          BlobField.CRC32C,
          BlobField.METADATA);
  private static final List<BucketField> BUCKET_METADATA_FIELDS =
      ImmutableList.of(
          BucketField.NAME,
          BucketField.LOCATION,
          BucketField.METAGENERATION,
          BucketField.TIME_CREATED,
          BucketField.UPDATED);
  private static final String USER_AGENT_PREFIX = "gcs-analytics-core/";

  @VisibleForTesting Storage storage;
  private final GcsClientOptions clientOptions;
  private final Optional<Credentials> credentials;
  private Supplier<ExecutorService> executorServiceSupplier;
  private final Telemetry telemetry;
  @VisibleForTesting volatile StorageControlClient storageControlClient;

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
    this.credentials = credentials;
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

    if (readOptions.isBidiReadEnabled()) {
      return new GcsBidiReadChannel(
          storage, gcsItemInfo, readOptions, executorServiceSupplier, telemetry);
    }

    return new GcsReadChannel(
        storage, gcsItemInfo, readOptions, executorServiceSupplier, telemetry);
  }

  @Override
  public VectoredSeekableByteChannel openReadChannel(
      GcsItemId gcsItemId, GcsReadOptions readOptions) throws IOException {
    checkNotNull(gcsItemId, "gcsItemId should not be null");
    checkNotNull(readOptions, "readOptions should not be null");
    ItemInfoProvider itemInfoProvider = this::getGcsItemInfo;
    if (readOptions.isBidiReadEnabled()) {
      return new GcsBidiReadChannel(
          storage, gcsItemId, readOptions, executorServiceSupplier, telemetry, itemInfoProvider);
    } else {
      return new GcsReadChannel(
          storage, gcsItemId, readOptions, executorServiceSupplier, telemetry, itemInfoProvider);
    }
  }

  @Override
  public WritableByteChannel createWriteChannel(GcsItemId itemId, GcsWriteOptions writeOptions)
      throws IOException {
    checkNotNull(itemId, "itemId should not be null");

    BlobInfo blobInfo = createBlobInfo(itemId, writeOptions);

    try {
      BlobWriteOption[] sdkWriteOptions =
          Optional.ofNullable(writeOptions)
              .orElseGet(() -> GcsWriteOptions.builder().build())
              .generateWriteOptions(itemId);
      BlobWriteSession sdkWriteSession = storage.blobWriteSession(blobInfo, sdkWriteOptions);
      WritableByteChannel channel = sdkWriteSession.open();
      return new GcsWriteChannel(sdkWriteSession, channel, blobInfo, writeOptions);
    } catch (StorageException | IOException e) {
      throw GcsExceptionUtil.translateWriteException(
          e, "initialization", blobInfo.getBlobId(), 0L, writeOptions);
    }
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
  public GcsItemInfo getBucketInfo(GcsItemId itemId) throws IOException {
    checkNotNull(itemId, "Item ID must not be null.");
    checkArgument(itemId.isBucket(), "Expected a bucket itemId");
    BucketInfo bucketInfo;
    try {
      bucketInfo =
          storage.get(
              itemId.getBucketName(),
              Storage.BucketGetOption.fields(BUCKET_METADATA_FIELDS.toArray(new BucketField[0])));
    } catch (StorageException e) {
      if (e.getCode() == 404) {
        bucketInfo = null;
      } else {
        throw new IOException("Unable to access bucket: " + itemId.getBucketName(), e);
      }
    }
    if (bucketInfo == null) {
      throw new FileNotFoundException("Bucket not found: " + itemId.getBucketName());
    }
    return fromBucketInfo(bucketInfo);
  }

  @Override
  public GcsItemInfo getFolderInfo(GcsItemId itemId) throws IOException {
    checkNotNull(itemId, "Item ID must not be null.");
    if (GcsItemId.ROOT.equals(itemId)) {
      return GcsItemInfo.ROOT_INFO;
    }
    if (itemId.isBucket()) {
      return getBucketInfo(itemId);
    }
    checkArgument(itemId.isGcsObject(), "Expected a folder itemId");
    String objectName = itemId.getObjectName().orElse("");
    String folderName = UriUtil.removeTrailingSlash(objectName);
    checkArgument(!folderName.isEmpty(), "Folder name cannot be empty");

    GetFolderRequest request =
        GetFolderRequest.newBuilder()
            .setName(FolderName.format("_", itemId.getBucketName(), folderName))
            .build();
    try {
      Folder folder = lazyGetStorageControlClient().getFolder(request);
      return fromFolder(folder, itemId);
    } catch (NotFoundException e) {
      throw new FileNotFoundException("Folder not found: " + itemId);
    } catch (Exception e) {
      throw new IOException("Failed to get folder info for: " + itemId, e);
    }
  }

  @VisibleForTesting
  StorageControlClient lazyGetStorageControlClient() throws IOException {
    if (this.storageControlClient == null) {
      synchronized (this) {
        if (this.storageControlClient == null) {
          this.storageControlClient = createStorageControlClient(this.credentials);
        }
      }
    }
    return this.storageControlClient;
  }

  @VisibleForTesting
  protected StorageControlClient createStorageControlClient(Optional<Credentials> credentials)
      throws IOException {
    StorageControlSettings.Builder builder = StorageControlSettings.newBuilder();
    credentials.ifPresent(c -> builder.setCredentialsProvider(FixedCredentialsProvider.create(c)));
    return StorageControlClient.create(builder.build());
  }

  @Override
  public List<GcsItemInfo> listFirstObjectWithPrefix(GcsItemId prefixId) throws IOException {
    checkNotNull(prefixId, "prefixId must not be null");
    String prefix = prefixId.getObjectName().orElse("");

    try {
      Page<Blob> page =
          storage.list(
              prefixId.getBucketName(),
              BlobListOption.prefix(prefix),
              BlobListOption.pageSize(1),
              BlobListOption.fields(BLOB_METADATA_FIELDS.toArray(new BlobField[0])));

      for (Blob blob : page.getValues()) {
        return ImmutableList.of(fromBlob(blob));
      }
      return ImmutableList.of();
    } catch (StorageException e) {
      if (e.getCode() == 404) {
        throw new FileNotFoundException("Bucket not found: " + prefixId.getBucketName());
      }
      throw new IOException("Failed to list the first object for prefix: " + prefixId, e);
    }
  }

  private static GcsItemInfo fromBlob(Blob blob) {
    GcsItemId.Builder idBuilder =
        GcsItemId.builder().setBucketName(blob.getBucket()).setObjectName(blob.getName());
    Optional.ofNullable(blob.getGeneration()).ifPresent(idBuilder::setContentGeneration);
    GcsItemId id = idBuilder.build();

    GcsItemInfo.Builder infoBuilder =
        GcsItemInfo.builder()
            .setItemId(id)
            .setSize(blob.getSize() == null ? 0L : blob.getSize())
            .setCreationTime(toEpochMilli(blob.getCreateTimeOffsetDateTime()))
            .setModificationTime(toEpochMilli(blob.getUpdateTimeOffsetDateTime()))
            .setVerificationAttributes(
                VerificationAttributes.create(
                    blob.getMd5() != null ? BaseEncoding.base64().decode(blob.getMd5()) : null,
                    blob.getCrc32c() != null
                        ? BaseEncoding.base64().decode(blob.getCrc32c())
                        : null));

    Optional.ofNullable(blob.getGeneration()).ifPresent(infoBuilder::setContentGeneration);
    Optional.ofNullable(blob.getContentType()).ifPresent(infoBuilder::setContentType);
    Optional.ofNullable(blob.getContentEncoding()).ifPresent(infoBuilder::setContentEncoding);
    Optional.ofNullable(blob.getMetageneration()).ifPresent(infoBuilder::setMetaGeneration);

    infoBuilder.setExtendedAttributes(GcsItemInfo.decodeMetadata(blob.getMetadata()));

    return infoBuilder.build();
  }

  private static GcsItemInfo fromBucketInfo(BucketInfo bucketInfo) {
    GcsItemId itemId = GcsItemId.builder().setBucketName(bucketInfo.getName()).build();
    GcsItemInfo.Builder builder =
        GcsItemInfo.createBucket(itemId).toBuilder()
            .setCreationTime(toEpochMilli(bucketInfo.getCreateTimeOffsetDateTime()))
            .setModificationTime(toEpochMilli(bucketInfo.getUpdateTimeOffsetDateTime()));

    Optional.ofNullable(bucketInfo.getLocation()).ifPresent(builder::setLocation);
    Optional.ofNullable(bucketInfo.getMetageneration()).ifPresent(builder::setMetaGeneration);
    return builder.build();
  }

  private static GcsItemInfo fromFolder(Folder folder, GcsItemId itemId) {
    return GcsItemInfo.builder()
        .setItemId(itemId)
        .setSize(0)
        .setItemType(ItemType.EXPLICIT_DIRECTORY)
        .setCreationTime(folder.hasCreateTime() ? toEpochMilli(folder.getCreateTime()) : 0L)
        .setModificationTime(folder.hasUpdateTime() ? toEpochMilli(folder.getUpdateTime()) : 0L)
        .setMetaGeneration(folder.getMetageneration())
        .build();
  }

  private static long toEpochMilli(OffsetDateTime dateTime) {
    return dateTime != null ? dateTime.toInstant().toEpochMilli() : 0L;
  }

  private static long toEpochMilli(Timestamp timestamp) {
    return timestamp != null
        ? Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()).toEpochMilli()
        : 0L;
  }

  BucketProperties getBucketProperties(String bucketName) throws IOException {
    checkNotNull(bucketName, "bucketName cannot be null");
    try {
      BucketInfo bucketInfo =
          storage.get(
              bucketName,
              Storage.BucketGetOption.fields(Storage.BucketField.HIERARCHICAL_NAMESPACE));
      if (bucketInfo == null) {
        LOG.warn("Bucket {} not found, HNS API will be disabled", bucketName);
        return BucketProperties.create(false);
      }
      boolean hnsEnabled =
          Optional.ofNullable(bucketInfo.getHierarchicalNamespace())
              .map(HierarchicalNamespace::getEnabled)
              .orElse(false);
      return BucketProperties.create(hnsEnabled);
    } catch (StorageException storageException) {
      if (storageException.getCode() == 403) {
        LOG.warn("Access to bucket {} is forbidden (403), HNS API will be disabled", bucketName);
        return BucketProperties.create(false);
      }
      throw new IOException("Unable to access bucket: " + bucketName, storageException);
    }
  }

  @Override
  public boolean isHnsBucket(String bucketName) throws IOException {
    return getBucketProperties(bucketName).isHnsEnabled();
  }

  @Override
  public void close() {
    try {
      storage.close();
    } catch (Exception e) {
      LOG.debug("Exception while closing storage instance", e);
    }
    if (storageControlClient != null) {
      try {
        storageControlClient.close();
      } catch (Exception e) {
        LOG.debug("Exception while closing storageControlClient", e);
      }
    }
  }

  @VisibleForTesting
  protected Storage createStorage(Optional<Credentials> credentials) {
    StorageOptions.Builder builder =
        clientOptions.getGcsReadOptions().isBidiReadEnabled()
            ? StorageOptions.grpc()
            : StorageOptions.newBuilder();
    String userAgent = getUserAgent();
    builder.setHeaderProvider(FixedHeaderProvider.create(ImmutableMap.of("User-Agent", userAgent)));
    clientOptions.getProjectId().ifPresent(builder::setProjectId);
    clientOptions.getClientLibToken().ifPresent(builder::setClientLibToken);
    clientOptions.getServiceHost().ifPresent(builder::setHost);
    credentials.ifPresent(builder::setCredentials);
    builder.setBlobWriteSessionConfig(clientOptions.generateSessionConfig());

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
      throw new FileNotFoundException("Object not found: " + itemId);
    }
    return fromBlob(blob);
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

  private static BlobInfo createBlobInfo(GcsItemId itemId, GcsWriteOptions writeOptions) {
    checkNotNull(itemId, "itemId should not be null");
    String objectName =
        itemId
            .getObjectName()
            .orElseThrow(() -> new IllegalArgumentException("Object name must be present"));
    BlobId blobId =
        itemId
            .getContentGeneration()
            .map(generation -> BlobId.of(itemId.getBucketName(), objectName, generation))
            .orElseGet(() -> BlobId.of(itemId.getBucketName(), objectName));

    BlobInfo.Builder blobInfoBuilder = BlobInfo.newBuilder(blobId);

    Optional.ofNullable(writeOptions)
        .ifPresent(
            options -> {
              options.getContentType().ifPresent(blobInfoBuilder::setContentType);
              options.getContentEncoding().ifPresent(blobInfoBuilder::setContentEncoding);
              ImmutableMap<String, byte[]> metadata = options.getMetadata();
              if (!metadata.isEmpty()) {
                blobInfoBuilder.setMetadata(GcsItemInfo.encodeMetadata(metadata));
              }
            });

    return blobInfoBuilder.build();
  }
}
