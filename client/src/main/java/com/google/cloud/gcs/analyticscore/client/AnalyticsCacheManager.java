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
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Manages the caching layer for GCS objects. This class is thread-safe and acts as a registry for
 * various specialized caches (e.g., Parquet footer cache).
 */
public class AnalyticsCacheManager {

  private final AnalyticsCache<GcsItemId, ByteBuffer> footerCache;

  /**
   * Creates a new {@link AnalyticsCacheManager} with the specified options.
   *
   * @param options The configuration options for the caching layer.
   */
  public AnalyticsCacheManager(GcsCacheOptions options) {
    checkNotNull(options, "options cannot be null");
    Weigher<GcsItemId, ByteBuffer> weigher = (key, value) -> value.remaining();
    this.footerCache =
        options.isFooterCacheEnabled()
            ? AnalyticsCacheCaffeineImpl.create(options.getFooterCacheMaxSizeBytes(), weigher)
            : AnalyticsCacheNoOpImpl.getInstance();
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

  /** Invalidates the cached footer for the given {@code itemId}. */
  public void invalidateFooter(GcsItemId itemId) {
    checkNotNull(itemId, "itemId cannot be null");
    footerCache.invalidate(itemId);
  }

  /** Invalidates all cached entries. */
  public void invalidateAll() {
    footerCache.invalidateAll();
  }

  /** A loader for GCS object footers. */
  @FunctionalInterface
  public interface FooterLoader {
    /** Loads the footer for the given {@code itemId}. */
    ByteBuffer load(GcsItemId itemId) throws IOException;
  }
}
