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

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkNotNull;

import com.github.benmanes.caffeine.cache.Weigher;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCache;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCacheCaffeineImpl;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCacheNoOpImpl;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Manages the caching layer for GCS objects. This class is thread-safe and acts as a registry for
 * various specialized caches (e.g., Parquet footer cache).
 */
public class AnalyticsCacheManager {

  /**
   * TTL for bucket properties cache in minutes. A 10-minute TTL is used because a bucket's
   * Hierarchical Namespace and Rapid (Zonal) storage class configurations are immutable unless the
   * bucket is deleted and re-created.
   */
  private static final long BUCKET_PROPERTIES_CACHE_TTL_MINUTES = 10;

  private final AnalyticsCache<GcsItemId, ByteBuffer> footerCache;
  private final AnalyticsCache<GcsItemId, ByteBuffer> smallObjectCache;
  private final AnalyticsCache<String, BucketProperties> bucketPropertiesCache;
  private final Optional<PrefetchBufferCache> prefetchBufferCache;
  private final Optional<SchemaAccessHistory> schemaAccessHistory;

  /**
   * Creates a new {@link AnalyticsCacheManager} with prefetching left at its default configuration.
   *
   * @param options The configuration options for the caching layer.
   */
  public AnalyticsCacheManager(GcsCacheOptions options) {
    this(options, GcsPrefetchOptions.builder().build(), new Telemetry(ImmutableList.of()));
  }

  /**
   * Creates a new {@link AnalyticsCacheManager} with the specified options.
   *
   * <p>When predictive prefetching is enabled, the footer cache is enabled as well, because
   * prefetching learns the Parquet layout from the cached footer.
   *
   * @param options The configuration options for the caching layer.
   * @param prefetchOptions The configuration options for predictive prefetching.
   * @param telemetry Telemetry instance used by the prefetch buffer cache.
   */
  public AnalyticsCacheManager(
      GcsCacheOptions options, GcsPrefetchOptions prefetchOptions, Telemetry telemetry) {
    checkNotNull(options, "options cannot be null");
    checkNotNull(prefetchOptions, "prefetchOptions cannot be null");
    checkNotNull(telemetry, "telemetry cannot be null");
    Weigher<GcsItemId, ByteBuffer> weigher = (key, value) -> value.remaining();
    this.footerCache =
        options.isFooterCacheEnabled() || prefetchOptions.isEnabled()
            ? AnalyticsCacheCaffeineImpl.create(options.getFooterCacheMaxSizeBytes(), weigher)
            : AnalyticsCacheNoOpImpl.getInstance();
    this.smallObjectCache =
        options.isSmallObjectCacheEnabled()
            ? AnalyticsCacheCaffeineImpl.create(options.getSmallObjectCacheMaxSizeBytes(), weigher)
            : AnalyticsCacheNoOpImpl.getInstance();
    this.bucketPropertiesCache =
        AnalyticsCacheCaffeineImpl.createWithTtlOnly(
            BUCKET_PROPERTIES_CACHE_TTL_MINUTES, TimeUnit.MINUTES);
    this.prefetchBufferCache =
        prefetchOptions.isEnabled()
            ? Optional.of(
                new PrefetchBufferCache(
                    prefetchOptions.getBufferCacheMaxSizeBytes(),
                    prefetchOptions.getBufferCacheTtlSeconds(),
                    telemetry))
            : Optional.empty();
    this.schemaAccessHistory =
        prefetchOptions.isEnabled() ? Optional.of(new SchemaAccessHistory()) : Optional.empty();
  }

  /**
   * Returns the cache holding speculatively prefetched byte blocks, or {@code Optional.empty()}
   * when predictive prefetching is disabled.
   */
  public Optional<PrefetchBufferCache> getPrefetchBufferCache() {
    return prefetchBufferCache;
  }

  /**
   * Returns the column access history shared across objects opened with this cache manager, or
   * {@code Optional.empty()} when predictive prefetching is disabled.
   */
  public Optional<SchemaAccessHistory> getSchemaAccessHistory() {
    return schemaAccessHistory;
  }

  /**
   * Returns the cached footer for the given {@code itemId}, or {@code Optional.empty()} if it is
   * not present in the cache.
   */
  public Optional<ByteBuffer> getFooter(GcsItemId itemId) {
    checkNotNull(itemId, "itemId cannot be null");
    return footerCache.get(itemId).map(ByteBuffer::asReadOnlyBuffer);
  }

  /**
   * Returns the cached footer for the given {@code itemId}, obtaining it from the {@code
   * footerLoader} if necessary. This method is atomic; the {@code footerLoader} will be applied at
   * most once per itemId during concurrent access.
   *
   * <p>If the {@code footerLoader} throws an exception, it will be propagated to the caller and the
   * result will not be cached.
   *
   * @throws IOException if the loader throws an {@link IOException}.
   */
  public ByteBuffer getFooter(GcsItemId itemId, FooterLoader footerLoader) throws IOException {
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(footerLoader, "footerLoader cannot be null");

    return footerCache
        .get(itemId, cachedItemId -> footerLoader.load(cachedItemId))
        .asReadOnlyBuffer();
  }

  /**
   * Returns the cached small object for the given {@code itemId}, obtaining it from the {@code
   * smallObjectLoader} if necessary. This method is atomic.
   *
   * @throws IOException if the loader throws an {@link IOException}.
   */
  public ByteBuffer getSmallObject(GcsItemId itemId, SmallObjectLoader smallObjectLoader)
      throws IOException {
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(smallObjectLoader, "smallObjectLoader cannot be null");

    return smallObjectCache
        .get(itemId, cachedItemId -> smallObjectLoader.load(cachedItemId))
        .asReadOnlyBuffer();
  }

  /** Invalidates the cached footer for the given {@code itemId}. */
  public void invalidateFooter(GcsItemId itemId) {
    checkNotNull(itemId, "itemId cannot be null");
    footerCache.invalidate(itemId);
  }

  /** Invalidates the cached small object for the given {@code itemId}. */
  public void invalidateSmallObject(GcsItemId itemId) {
    checkNotNull(itemId, "itemId cannot be null");
    smallObjectCache.invalidate(itemId);
  }

  /**
   * Returns the cached properties for the given {@code bucketName}, obtaining it from the {@code
   * bucketPropertiesLoader} if necessary. This method is atomic.
   *
   * @throws IOException if the loader throws an {@link IOException}.
   */
  public BucketProperties getBucketProperties(
      String bucketName, BucketPropertiesLoader bucketPropertiesLoader) throws IOException {
    checkNotNull(bucketName, "bucketName cannot be null");
    checkNotNull(bucketPropertiesLoader, "bucketPropertiesLoader cannot be null");

    return bucketPropertiesCache.get(bucketName, bucketPropertiesLoader::load);
  }

  /** Invalidates the cached properties for the given {@code bucketName}. */
  public void invalidateBucketProperties(String bucketName) {
    checkNotNull(bucketName, "bucketName cannot be null");
    bucketPropertiesCache.invalidate(bucketName);
  }

  /** Returns the cache used for bucket properties. */
  AnalyticsCache<String, BucketProperties> getBucketPropertiesCache() {
    return bucketPropertiesCache;
  }

  /** Invalidates all cached entries. */
  public void invalidateAll() {
    footerCache.invalidateAll();
    smallObjectCache.invalidateAll();
    bucketPropertiesCache.invalidateAll();
    prefetchBufferCache.ifPresent(PrefetchBufferCache::invalidateAll);
    schemaAccessHistory.ifPresent(SchemaAccessHistory::invalidateAll);
  }

  /** A loader for GCS object footers. */
  @FunctionalInterface
  public interface FooterLoader {
    /** Loads the footer for the given {@code itemId}. */
    ByteBuffer load(GcsItemId itemId) throws IOException;
  }

  /** A loader for small GCS objects. */
  @FunctionalInterface
  public interface SmallObjectLoader {
    /** Loads the small object for the given {@code itemId}. */
    ByteBuffer load(GcsItemId itemId) throws IOException;
  }

  /** A loader for GCS bucket properties. */
  @FunctionalInterface
  public interface BucketPropertiesLoader {
    /** Loads the properties for the given {@code bucketName}. */
    BucketProperties load(String bucketName) throws IOException;
  }
}
