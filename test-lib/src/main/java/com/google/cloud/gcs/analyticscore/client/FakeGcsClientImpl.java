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
   * Moves the object as a copy followed by a delete of the source, enforcing the generation
   * preconditions given by {@link GcsItemId#getContentGeneration()}.
   *
   * <p>{@link Storage#moveBlob} is not used: in {@link LocalStorageHelper} it ignores the
   * preconditions, keeps the source name in the moved object's metadata, and fails for an object
   * without a generation.
   */
  @Override
  public void moveObject(GcsItemId srcId, GcsItemId dstId) throws IOException {
    GcsClientImpl.checkMoveObjectArguments(srcId, dstId);
    BlobId srcBlobId = BlobId.of(srcId.getBucketName(), srcId.getObjectName().get());
    BlobId dstBlobId = BlobId.of(dstId.getBucketName(), dstId.getObjectName().get());
    Blob src = storage.get(srcBlobId);
    // GCS reports a missing source before it checks any precondition.
    if (src == null) {
      throw GcsExceptionUtil.translateMoveObjectException(
          new StorageException(404, "Not Found"), srcId, dstId);
    }
    Blob dst = storage.get(dstBlobId);
    boolean sourceChanged =
        srcId.getContentGeneration().filter(gen -> !gen.equals(src.getGeneration())).isPresent();
    boolean destinationChanged =
        dstId
            .getContentGeneration()
            .filter(
                gen -> gen == 0L ? dst != null : dst == null || !gen.equals(dst.getGeneration()))
            .isPresent();
    if (sourceChanged || destinationChanged) {
      throw GcsExceptionUtil.translateMoveObjectException(
          new StorageException(412, "Precondition Failed"), srcId, dstId);
    }
    storage
        .copy(
            Storage.CopyRequest.newBuilder()
                .setSource(srcBlobId)
                .setTarget(src.toBuilder().setBlobId(dstBlobId).build())
                .build())
        .getResult();
    storage.delete(srcBlobId);
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
