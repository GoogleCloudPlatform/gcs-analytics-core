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
package com.google.cloud.gcs.analyticscore.common.telemetry;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

/**
 * Compact metric map for {@link Telemetry#measure} with a three-phase lifecycle: synchronized
 * recording during the operation, a {@link #freeze} transition on completion, and lock-free
 * unmodifiable reads thereafter. Stores up to two entries inline to avoid per-operation {@code
 * ConcurrentHashMap} and {@code Long} boxing allocations on the telemetry hot path.
 */
final class CompactMetricsMap extends AbstractMap<MetricKey, Long> {

  private MetricKey firstKey;
  private long firstValue;
  private MetricKey secondKey;
  private long secondValue;
  private Map<MetricKey, Long> overflow;
  private boolean frozen;
  private volatile int size;

  synchronized void record(MetricKey key, long value) {
    if (frozen) {
      return;
    }
    update(key, value, /* accumulate= */ true);
  }

  synchronized void freeze(Optional<Metric> durationMetric, long durationNs) {
    if (durationMetric.isPresent()) {
      update(
          MetricKey.builder().setMetric(durationMetric.get()).build(),
          durationNs,
          /* accumulate= */ false);
    }
    frozen = true;
    final int computedSize;
    if (overflow != null) {
      overflow = Collections.unmodifiableMap(overflow);
      computedSize = overflow.size();
    } else if (secondKey != null) {
      computedSize = 2;
    } else if (firstKey != null) {
      computedSize = 1;
    } else {
      computedSize = 0;
    }
    this.size = computedSize;
  }

  private void update(MetricKey key, long value, boolean accumulate) {
    if (overflow != null) {
      if (accumulate) {
        overflow.merge(key, value, Long::sum);
      } else {
        overflow.put(key, value);
      }
      return;
    }
    if (firstKey == null) {
      firstKey = key;
      firstValue = value;
      return;
    }
    if (firstKey.equals(key)) {
      firstValue = accumulate ? firstValue + value : value;
      return;
    }
    if (secondKey == null) {
      secondKey = key;
      secondValue = value;
      return;
    }
    if (secondKey.equals(key)) {
      secondValue = accumulate ? secondValue + value : value;
      return;
    }
    overflow = new HashMap<>(8);
    overflow.put(firstKey, firstValue);
    overflow.put(secondKey, secondValue);
    overflow.put(key, value);
    firstKey = null;
    secondKey = null;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public Long get(Object key) {
    int currentSize = size;
    if (currentSize == 0) {
      return null;
    }
    if (overflow != null) {
      return overflow.get(key);
    }
    if (firstKey.equals(key)) {
      return firstValue;
    }
    if (currentSize == 2 && secondKey.equals(key)) {
      return secondValue;
    }
    return null;
  }

  @Override
  public boolean containsKey(Object key) {
    return get(key) != null;
  }

  @Override
  public Set<Entry<MetricKey, Long>> entrySet() {
    int currentSize = size;
    if (currentSize == 0) {
      return Collections.emptySet();
    }
    if (overflow != null) {
      return overflow.entrySet();
    }
    return new AbstractSet<>() {
      @Override
      public int size() {
        return CompactMetricsMap.this.size;
      }

      @Override
      public Iterator<Entry<MetricKey, Long>> iterator() {
        return new Iterator<>() {
          private int index;

          @Override
          public boolean hasNext() {
            return index < CompactMetricsMap.this.size;
          }

          @Override
          public Entry<MetricKey, Long> next() {
            int entryCount = CompactMetricsMap.this.size;
            if (index >= entryCount) {
              throw new NoSuchElementException();
            }
            if (index++ == 0) {
              return new SimpleImmutableEntry<>(firstKey, firstValue);
            }
            return new SimpleImmutableEntry<>(secondKey, secondValue);
          }
        };
      }
    };
  }
}
