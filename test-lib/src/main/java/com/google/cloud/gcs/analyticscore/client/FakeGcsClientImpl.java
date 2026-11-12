/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.auth.Credentials;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCache;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.base.Supplier;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public class FakeGcsClientImpl extends GcsClientImpl {
  public static Storage storage = LocalStorageHelper.getOptions().getService();

  private static int openReadChannelCount = 0;
  private static int closeCount = 0;

  FakeGcsClientImpl(
      Credentials credentials,
      GcsClientOptions clientOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry,
      AnalyticsCache<String, BucketProperties> bucketPropertiesCache) {
    super(credentials, clientOptions, executorServiceSupplier, telemetry, bucketPropertiesCache);
  }

  FakeGcsClientImpl(
      GcsClientOptions clientOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry,
      AnalyticsCache<String, BucketProperties> bucketPropertiesCache) {
    super(clientOptions, executorServiceSupplier, telemetry, bucketPropertiesCache);
  }

  @Override
  protected Storage createStorage(Optional<Credentials> credentials) {
    return storage;
  }

  @Override
  protected Storage createGrpcStorage(Optional<Credentials> credentials) {
    return storage;
  }

  @Override
  public BucketProperties getBucketProperties(String bucketName) {
    // LocalStorageHelper does not support bucket operations
    return BucketProperties.create(false, false);
  }

  /**
   * Deletes the object, enforcing {@link GcsItemId#getContentGeneration()} as a generation-match
   * precondition, which {@link LocalStorageHelper} ignores.
   */
  @Override
  public void deleteObject(GcsItemId itemId) throws IOException {
    if (itemId != null && itemId.isGcsObject()) {
      Optional<Long> generation = itemId.getContentGeneration().filter(gen -> gen > 0L);
      if (generation.isPresent()) {
        Blob blob = storage.get(BlobId.of(itemId.getBucketName(), itemId.getObjectName().get()));
        if (blob != null && !generation.get().equals(blob.getGeneration())) {
          throw GcsExceptionUtil.translateDeleteObjectException(
              new StorageException(412, "Precondition Failed"), itemId);
        }
      }
    }
    super.deleteObject(itemId);
  }

  /**
   * Deletes an empty bucket.
   *
   * <p>{@link LocalStorageHelper} does not model buckets, so every bucket is treated as existing:
   * deletion succeeds when the bucket holds no objects and fails with {@link
   * java.nio.file.DirectoryNotEmptyException} otherwise. Deleting a bucket never removes it from
   * later lookups.
   */
  @Override
  public void deleteBucket(GcsItemId itemId) throws IOException {
    checkNotNull(itemId, "itemId must not be null");
    checkArgument(itemId.isBucket(), "Expected a bucket itemId, but got: %s", itemId);
    String bucketName = itemId.getBucketName();
    if (storage.list(bucketName).getValues().iterator().hasNext()) {
      throw GcsExceptionUtil.translateDeleteBucketException(
          new StorageException(409, "The bucket you tried to delete is not empty"), bucketName);
    }
  }

  @Override
  public VectoredSeekableByteChannel openReadChannel(
      GcsItemInfo itemInfo, GcsReadOptions readOptions) throws IOException {
    openReadChannelCount++;
    return super.openReadChannel(itemInfo, readOptions);
  }

  @Override
  public void close() {
    closeCount++;
    super.close();
  }

  public static int getOpenReadChannelCount() {
    return openReadChannelCount;
  }

  public static int getCloseCount() {
    return closeCount;
  }

  public static void resetCounts() {
    openReadChannelCount = 0;
    closeCount = 0;
  }
}
