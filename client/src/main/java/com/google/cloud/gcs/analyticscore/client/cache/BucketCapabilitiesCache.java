/*
 * Copyright 2026 Google LLC
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
package com.google.cloud.gcs.analyticscore.client.cache;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.cloud.gcs.analyticscore.common.BucketCapabilities;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;

/**
 * A highly concurrent, thread-safe cache wrapper dedicated to BucketCapabilities. Implements a
 * strict time-based TTL to mitigate state drift in mixed-client workloads.
 */
public final class BucketCapabilitiesCache {

  private final Cache<String, BucketCapabilities> cache;

  public BucketCapabilitiesCache(long maxSize, long timeout, TimeUnit unit) {
    checkArgument(maxSize > 0, "maxSize must be strictly positive: %s", maxSize);
    checkArgument(timeout >= 0, "timeout must be non-negative: %s", timeout);
    checkNotNull(unit, "TimeUnit must not be null");

    this.cache = Caffeine.newBuilder().maximumSize(maxSize).expireAfterWrite(timeout, unit).build();
  }

  /**
   * Performs an atomic, thread-safe lookup. Propagates checked IOExceptions from the low-level
   * GcsClient without swallowing or wrapping them.
   */
  public BucketCapabilities get(String bucketName, BucketProber loader) throws IOException {
    checkNotNull(bucketName, "bucketName must not be null");
    checkNotNull(loader, "loader must not be null");

    try {
      return cache.get(
          bucketName,
          key -> {
            try {
              BucketCapabilities capabilities = loader.probe(key);
              return checkNotNull(
                  capabilities,
                  "BucketProber is obligated to return a non-null BucketCapabilities instance");
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  @FunctionalInterface
  public interface BucketProber {
    BucketCapabilities probe(String bucketName) throws IOException;
  }
}
