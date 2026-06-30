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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import java.util.Optional;

/**
 * An {@link AnalyticsCache} implementation backed by a Caffeine {@link Cache}. This implementation
 * is thread-safe.
 *
 * @param <K> The type of keys maintained by this cache.
 * @param <V> The type of mapped values.
 */
public class AnalyticsCacheCaffeineImpl<K, V> implements AnalyticsCache<K, V> {

  private final Cache<K, V> cache;

  private AnalyticsCacheCaffeineImpl(long maxWeight, Weigher<K, V> weigher) {
    checkArgument(maxWeight > 0, "maxWeight must be positive");
    checkNotNull(weigher, "weigher cannot be null");
    this.cache = Caffeine.newBuilder().maximumWeight(maxWeight).weigher(weigher).build();
  }

  /**
   * Creates a new {@link AnalyticsCacheCaffeineImpl} with the specified maximum weight and weigher.
   */
  public static <K, V> AnalyticsCacheCaffeineImpl<K, V> create(
      long maxWeight, Weigher<K, V> weigher) {
    return new AnalyticsCacheCaffeineImpl<>(maxWeight, weigher);
  }

  /** {@inheritDoc} */
  @Override
  public Optional<V> get(K key) {
    checkNotNull(key, "key cannot be null");
    return Optional.ofNullable(cache.getIfPresent(key));
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public <E extends Exception> V get(
      K key, ThrowingFunction<? super K, ? extends V, E> mappingFunction) throws E {
    checkNotNull(key, "key cannot be null");
    checkNotNull(mappingFunction, "mappingFunction cannot be null");
    try {
      return cache.get(
          key,
          keyToLoad -> {
            try {
              V computed = mappingFunction.apply(keyToLoad);
              if (computed == null) {
                throw new NullPointerException(
                    "mappingFunction returned null for key: " + keyToLoad);
              }
              return computed;
            } catch (Exception exception) {
              throw new ExecutionException(exception);
            }
          });
    } catch (ExecutionException executionException) {
      Throwable cause = executionException.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw (E) cause;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void put(K key, V value) {
    checkNotNull(key, "key cannot be null");
    checkNotNull(value, "value cannot be null");
    cache.put(key, value);
  }

  /** {@inheritDoc} */
  @Override
  public void invalidate(K key) {
    checkNotNull(key, "key cannot be null");
    cache.invalidate(key);
  }

  /** {@inheritDoc} */
  @Override
  public void invalidateAll() {
    cache.invalidateAll();
  }

  /** {@inheritDoc} */
  @Override
  public long size() {
    return cache.estimatedSize();
  }

  /** {@inheritDoc} */
  @Override
  public void cleanUp() {
    cache.cleanUp();
  }

  /** A private runtime exception used to wrap checked exceptions during cache loading. */
  private static class ExecutionException extends RuntimeException {
    ExecutionException(Throwable cause) {
      super(cause);
    }
  }
}
