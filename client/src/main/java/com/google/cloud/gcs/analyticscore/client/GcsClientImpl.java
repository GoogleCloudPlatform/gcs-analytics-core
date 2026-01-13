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
import com.google.cloud.storage.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GcsClientImpl implements GcsClient {
  private static final Logger LOG = LoggerFactory.getLogger(GcsClientImpl.class);
  private static final List<Storage.BlobField> BLOB_METADATA_FIELDS =
      ImmutableList.of(Storage.BlobField.GENERATION, Storage.BlobField.SIZE);

  @VisibleForTesting Storage storage;
  private final GcsClientOptions clientOptions;
  private Supplier<ExecutorService> executorServiceSupplier;

  GcsClientImpl(
      Credentials credentials,
      GcsClientOptions clientOptions,
      Supplier<ExecutorService> executorServiceSupplier) {
    this.clientOptions = clientOptions;
    this.storage = createStorage(Optional.of(credentials));
    this.executorServiceSupplier = executorServiceSupplier;
  }

  GcsClientImpl(GcsClientOptions clientOptions, Supplier<ExecutorService> executorServiceSupplier) {
    this.clientOptions = clientOptions;
    this.storage = createStorage(Optional.empty());
    this.executorServiceSupplier = executorServiceSupplier;
  }

  @Override
  public VectoredSeekableByteChannel openReadChannel(
      GcsItemInfo gcsItemInfo, GcsReadOptions readOptions) throws IOException {
    checkNotNull(gcsItemInfo, "itemInfo should not be null");
    checkNotNull(readOptions, "readOptions should not be null");
    checkArgument(
        gcsItemInfo.getItemId().isGcsObject(),
        "Expected GCS object to be provided. But got: " + gcsItemInfo.getItemId());

    if (readOptions.getAdaptiveRangeReadEnabled()) {
      return new GcsAdaptiveReadChannel(storage, gcsItemInfo, readOptions, executorServiceSupplier);
    }
    return new GcsSimpleReadChannel(storage, gcsItemInfo, readOptions, executorServiceSupplier);
  }

  @Override
  public VectoredSeekableByteChannel openReadChannel(
      GcsItemId gcsItemId, GcsReadOptions readOptions) throws IOException {
    checkNotNull(gcsItemId, "gcsItemId should not be null");
    checkNotNull(readOptions, "readOptions should not be null");
    return new GcsSimpleReadChannel(storage, gcsItemId, readOptions, executorServiceSupplier) {
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
    clientOptions
        .getUserAgent()
        .ifPresent(
            userAgent ->
                builder.setHeaderProvider(
                    FixedHeaderProvider.create(ImmutableMap.of("User-agent", userAgent))));
    clientOptions.getProjectId().ifPresent(builder::setProjectId);
    clientOptions.getClientLibToken().ifPresent(builder::setClientLibToken);
    clientOptions.getServiceHost().ifPresent(builder::setHost);
    credentials.ifPresent(builder::setCredentials);

    return builder.build().getService();
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
