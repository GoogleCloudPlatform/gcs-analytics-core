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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Optional;

/**
 * An {@link AnalyticsCache} implementation that does nothing. All lookups result in a miss, and all
 * puts are ignored.
 *
 * @param <K> The type of keys maintained by this cache.
 * @param <V> The type of mapped values.
 */
public class AnalyticsCacheNoOpImpl<K, V> implements AnalyticsCache<K, V> {

  private static final AnalyticsCacheNoOpImpl<Object, Object> INSTANCE =
      new AnalyticsCacheNoOpImpl<>();

  private AnalyticsCacheNoOpImpl() {}

  /** Returns the singleton instance of {@link AnalyticsCacheNoOpImpl}. */
  @SuppressWarnings("unchecked")
  public static <K, V> AnalyticsCacheNoOpImpl<K, V> getInstance() {
    return (AnalyticsCacheNoOpImpl<K, V>) INSTANCE;
  }

  /** {@inheritDoc} */
  @Override
  public Optional<V> get(K key) {
    checkNotNull(key, "key cannot be null");
    return Optional.empty();
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Exception> V get(
      K key, ThrowingFunction<? super K, ? extends V, E> mappingFunction) throws E {
    checkNotNull(key, "key cannot be null");
    checkNotNull(mappingFunction, "mappingFunction cannot be null");
    V value = mappingFunction.apply(key);
    if (value == null) {
      throw new NullPointerException("mappingFunction returned null for key: " + key);
    }
    return value;
  }

  /** {@inheritDoc} */
  @Override
  public void put(K key, V value) {
    checkNotNull(key, "key cannot be null");
    checkNotNull(value, "value cannot be null");
    // Do nothing
  }

  /** {@inheritDoc} */
  @Override
  public void invalidate(K key) {
    checkNotNull(key, "key cannot be null");
    // Do nothing
  }

  /** {@inheritDoc} */
  @Override
  public void invalidateAll() {
    // Do nothing
  }

  /** {@inheritDoc} */
  @Override
  public long size() {
    return 0;
  }
}
