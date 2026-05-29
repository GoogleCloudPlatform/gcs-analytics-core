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

package com.google.cloud.gcs.analyticscore.common.cache;

import java.util.Optional;

/**
 * A simple, generic interface for an in-memory cache. All implementations of this interface must be
 * thread-safe.
 *
 * @param <K> The type of keys maintained by this cache.
 * @param <V> The type of mapped values.
 */
public interface AnalyticsCache<K, V> {

  /**
   * Returns the value associated with the {@code key} in this cache, or {@code Optional.empty()} if
   * there is no cached value for {@code key}.
   */
  Optional<V> get(K key);

  /**
   * Returns the value associated with the {@code key} in this cache, obtaining it from the {@code
   * mappingFunction} if necessary. This method is atomic; the {@code mappingFunction} will be
   * applied at most once per key during concurrent access.
   *
   * <p>This method supports mapping functions that throw checked exceptions. If the {@code
   * mappingFunction} throws an exception, it will be propagated to the caller and the result will
   * not be cached.
   *
   * @param <E> The type of the exception that may be thrown by the mapping function.
   * @throws E if the {@code mappingFunction} throws an exception.
   * @throws NullPointerException if the {@code mappingFunction} returns {@code null} or if any
   *     argument is {@code null}.
   */
  <E extends Exception> V get(K key, ThrowingFunction<? super K, ? extends V, E> mappingFunction)
      throws E;

  /**
   * Associates the {@code value} with the {@code key} in this cache. If the cache previously
   * contained a value associated with the {@code key}, the old value is replaced by the new {@code
   * value}.
   */
  void put(K key, V value);

  /** Discards any cached value for the {@code key}. */
  void invalidate(K key);

  /** Discards all entries in the cache. */
  void invalidateAll();

  /** Returns the approximate number of entries in this cache. */
  long size();

  /** Performs any pending maintenance operations needed by the cache. */
  default void cleanUp() {
    // No-op by default
  }
}
