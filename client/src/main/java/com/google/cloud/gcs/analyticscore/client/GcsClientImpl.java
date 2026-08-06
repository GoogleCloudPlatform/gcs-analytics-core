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
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import com.google.storage.control.v2.Folder;
import com.google.storage.control.v2.FolderName;
import com.google.storage.control.v2.GetFolderRequest;
import com.google.storage.control.v2.StorageControlClient;
import com.google.storage.control.v2.StorageControlSettings;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GcsClientImpl implements GcsClient {
  private static final Logger LOG = LoggerFactory.getLogger(GcsClientImpl.class);
  private static final List<Storage.BlobField> BLOB_METADATA_FIELDS =
      ImmutableList.of(
          Storage.BlobField.GENERATION,
          Storage.BlobField.SIZE,
          Storage.BlobField.TIME_CREATED,
          Storage.BlobField.UPDATED,
          Storage.BlobField.METADATA);
  private static final String USER_AGENT_PREFIX = "gcs-analytics-core/";

  @VisibleForTesting Storage storage;
  private final GcsClientOptions clientOptions;
  private final Optional<Credentials> credentials;
  private Supplier<ExecutorService> executorServiceSupplier;
  private final Telemetry telemetry;
  private StorageControlClient storageControlClient;

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
    BucketInfo bucketInfo = storage.get(itemId.getBucketName());
    if (bucketInfo == null) {
      throw new FileNotFoundException("Bucket not found: " + itemId.getBucketName());
    }
    return fromBucketInfo(bucketInfo);
  }

  @Override
  public GcsItemInfo getFolderInfo(GcsItemId itemId) throws IOException {
    checkNotNull(itemId, "Item ID must not be null.");
    checkArgument(itemId.isGcsObject(), "Expected a folder itemId");
    String objectName = itemId.getObjectName().orElse("");
    String folderName = UriUtil.removeTrailingSlash(objectName);

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
      throw new IOException("Folder not found: " + itemId, e);
    }
  }

  @VisibleForTesting
  StorageControlClient lazyGetStorageControlClient() throws IOException {
    if (this.storageControlClient == null) {
      StorageControlSettings.Builder builder = StorageControlSettings.newBuilder();
      this.credentials.ifPresent(
          c -> builder.setCredentialsProvider(FixedCredentialsProvider.create(c)));
      this.storageControlClient = StorageControlClient.create(builder.build());
    }
    return this.storageControlClient;
  }

  @Override
  public java.util.List<GcsItemInfo> listObjectInfo(GcsItemId prefixId, int maxResults)
      throws IOException {
    String prefix = prefixId.getObjectName().orElse("");
    com.google.api.gax.paging.Page<Blob> page =
        storage.list(
            prefixId.getBucketName(),
            Storage.BlobListOption.prefix(prefix),
            Storage.BlobListOption.pageSize(maxResults),
            Storage.BlobListOption.fields(BLOB_METADATA_FIELDS.toArray(new Storage.BlobField[0])));

    ImmutableList.Builder<GcsItemInfo> builder = ImmutableList.builder();
    for (Blob blob : page.iterateAll()) {
      builder.add(fromBlob(blob));
      if (builder.build().size() >= maxResults && maxResults > 0) {
        break;
      }
    }
    return builder.build();
  }

  private GcsItemInfo fromBlob(Blob blob) {
    GcsItemId id =
        GcsItemId.builder()
            .setContentGeneration(blob.getGeneration())
            .setBucketName(blob.getBucket())
            .setObjectName(blob.getName())
            .build();
    GcsItemInfo.Builder infoBuilder =
        GcsItemInfo.builder()
            .setItemId(id)
            .setSize(blob.getSize())
            .setContentGeneration(blob.getGeneration())
            .setCreationTime(toEpochMilli(blob.getCreateTimeOffsetDateTime()))
            .setModificationTime(toEpochMilli(blob.getUpdateTimeOffsetDateTime()))
            .setVerificationAttributes(
                VerificationAttributes.create(
                    blob.getMd5() != null ? BaseEncoding.base64().decode(blob.getMd5()) : null,
                    blob.getCrc32c() != null
                        ? BaseEncoding.base64().decode(blob.getCrc32c())
                        : null));

    Optional.ofNullable(blob.getContentType()).ifPresent(infoBuilder::setContentType);
    Optional.ofNullable(blob.getContentEncoding()).ifPresent(infoBuilder::setContentEncoding);
    Optional.ofNullable(blob.getStorageClass())
        .ifPresent(sc -> infoBuilder.setStorageClass(sc.name()));

    if (blob.getMetadata() != null) {
      ImmutableMap.Builder<String, byte[]> xattrs = ImmutableMap.builder();
      for (java.util.Map.Entry<String, String> entry : blob.getMetadata().entrySet()) {
        if (entry.getValue() != null) {
          xattrs.put(
              entry.getKey(), entry.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
      }
      infoBuilder.setExtendedAttributes(xattrs.build());
    }

    return infoBuilder.build();
  }

  private GcsItemInfo fromBucketInfo(BucketInfo bucketInfo) {
    GcsItemId itemId = GcsItemId.builder().setBucketName(bucketInfo.getName()).build();
    GcsItemInfo.Builder builder =
        GcsItemInfo.createBucket(itemId).toBuilder()
            .setCreationTime(toEpochMilli(bucketInfo.getCreateTimeOffsetDateTime()))
            .setModificationTime(toEpochMilli(bucketInfo.getUpdateTimeOffsetDateTime()));

    Optional.ofNullable(bucketInfo.getLocation()).ifPresent(builder::setLocation);
    Optional.ofNullable(bucketInfo.getStorageClass())
        .ifPresent(sc -> builder.setStorageClass(sc.name()));
    Optional.ofNullable(bucketInfo.getMetageneration()).ifPresent(builder::setMetaGeneration);
    return builder.build();
  }

  private GcsItemInfo fromFolder(Folder folder, GcsItemId itemId) {
    return GcsItemInfo.builder()
        .setItemId(itemId)
        .setSize(0)
        .setItemType(ItemType.EXPLICIT_DIRECTORY)
        .setCreationTime(folder.hasCreateTime() ? toEpochMilli(folder.getCreateTime()) : 0L)
        .setModificationTime(folder.hasUpdateTime() ? toEpochMilli(folder.getUpdateTime()) : 0L)
        .setMetaGeneration(folder.getMetageneration())
        .build();
  }

  private static long toEpochMilli(java.time.OffsetDateTime dateTime) {
    return dateTime != null ? dateTime.toInstant().toEpochMilli() : 0L;
  }

  private static long toEpochMilli(com.google.protobuf.Timestamp timestamp) {
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

  private static ImmutableMap<String, String> encodeMetadata(
      ImmutableMap<String, byte[]> metadata) {
    ImmutableMap.Builder<String, String> encoded = ImmutableMap.builder();
    metadata.forEach((k, v) -> encoded.put(k, BaseEncoding.base64().encode(v)));
    return encoded.build();
  }

  private BlobInfo createBlobInfo(GcsItemId itemId, GcsWriteOptions writeOptions) {
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
                blobInfoBuilder.setMetadata(encodeMetadata(metadata));
              }
            });

    return blobInfoBuilder.build();
  }
}
