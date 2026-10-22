/*
 * Copyright 2026 Google LLC
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
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Builds a {@link GcsFileSystem} whose {@link GcsClientImpl} is backed by {@link InMemoryStorage},
 * so tests and benchmarks drive the real HTTP read path without credentials or network access.
 */
public final class HermeticGcsFileSystem {
  // GcsClientImpl calls createStorage() from its constructor, before subclass fields exist.
  private static final ThreadLocal<Storage> PENDING_STORAGE = new ThreadLocal<>();

  private HermeticGcsFileSystem() {}

  public static GcsFileSystem create(Map<GcsItemId, byte[]> objects, GcsFileSystemOptions options) {
    return create(InMemoryStorage.create(objects), options);
  }

  public static GcsFileSystem create(InMemoryStorage storage, GcsFileSystemOptions options) {
    checkArgument(!options.getGcsClientOptions().isBidiEnabled(), "bidi reads are not hermetic");
    ExecutorService executor =
        Executors.newFixedThreadPool(
            options.getReadThreadCount(),
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("hermetic-range-pool-%d")
                .build());
    PENDING_STORAGE.set(storage.storage());
    try {
      return new GcsFileSystemImpl(new InMemoryGcsClient(storage, options, executor), options);
    } catch (RuntimeException | Error e) {
      executor.shutdownNow();
      throw e;
    } finally {
      PENDING_STORAGE.remove();
    }
  }

  /** Public bridge to the package-private {@link VectoredIoUtil#mergeGcsObjectRanges}. */
  public static ImmutableList<GcsObjectCombinedRange> mergeGcsObjectRanges(
      ImmutableList<GcsObjectRange> ranges, int maxMergeGap, int maxMergeSize) {
    return VectoredIoUtil.mergeGcsObjectRanges(ranges, maxMergeGap, maxMergeSize);
  }

  private static final class InMemoryGcsClient extends GcsClientImpl {
    private final InMemoryStorage inMemoryStorage;
    private final ExecutorService executor;

    private InMemoryGcsClient(
        InMemoryStorage inMemoryStorage, GcsFileSystemOptions options, ExecutorService executor) {
      super(options.getGcsClientOptions(), () -> executor, new Telemetry(ImmutableList.of()));
      this.inMemoryStorage = inMemoryStorage;
      this.executor = executor;
    }

    @Override
    protected Storage createStorage(Optional<Credentials> credentials) {
      return PENDING_STORAGE.get();
    }

    @Override
    BucketProperties getBucketProperties(String bucketName) {
      return BucketProperties.create(false);
    }

    @Override
    public GcsItemInfo getGcsItemInfo(GcsItemId itemId) throws IOException {
      checkNotNull(itemId, "Item ID must not be null.");
      if (!itemId.isGcsObject()) {
        throw new UnsupportedOperationException("Expected gcs object but got " + itemId);
      }
      byte[] data =
          inMemoryStorage
              .find(itemId)
              .orElseThrow(() -> GcsExceptionUtil.createFileNotFoundException(itemId));
      return GcsItemInfo.builder().setItemId(itemId).setSize(data.length).build();
    }

    @Override
    public Optional<GcsItemInfo> listFirstObjectWithPrefix(GcsItemId prefixId) {
      checkNotNull(prefixId, "prefixId must not be null");
      return Optional.empty();
    }

    @Override
    public void close() {
      executor.shutdownNow();
      super.close();
    }
  }
}
