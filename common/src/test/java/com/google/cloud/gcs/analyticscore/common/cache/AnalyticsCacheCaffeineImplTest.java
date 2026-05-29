/*
 * Copyright 2025 Google LLC
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnalyticsCacheCaffeineImplTest {

  private AnalyticsCacheCaffeineImpl<String, String> cache;

  @BeforeEach
  void setUp() {
    cache = AnalyticsCacheCaffeineImpl.create(2);
  }

  @Test
  void putAndGet_succeeds() {
    String key = "key1";
    String value = "value1";

    cache.put(key, value);

    assertThat(cache.get(key)).hasValue(value);
  }

  @Test
  void get_nonExistentKey_returnsEmpty() {
    assertThat(cache.get("non-existent")).isEmpty();
  }

  @Test
  void getWithMappingFunction_computesAndCachesValue() {
    String key = "key1";
    AtomicInteger callCount = new AtomicInteger(0);

    String value =
        cache.get(
            key,
            k -> {
              callCount.incrementAndGet();
              return "computed-" + k;
            });

    assertThat(value).isEqualTo("computed-key1");
    assertThat(callCount.get()).isEqualTo(1);
    assertThat(cache.get(key)).hasValue("computed-key1");

    // Second call should return cached value without calling the function
    String secondValue =
        cache.get(
            key,
            k -> {
              callCount.incrementAndGet();
              return "should-not-happen";
            });
    assertThat(secondValue).isEqualTo("computed-key1");
    assertThat(callCount.get()).isEqualTo(1);
  }

  @Test
  void getWithMappingFunction_returnsNull_throwsException() {
    assertThrows(NullPointerException.class, () -> cache.get("key1", k -> null));
  }

  @Test
  void invalidate_removesEntry() {
    cache.put("key1", "value1");

    cache.invalidate("key1");

    assertThat(cache.get("key1")).isEmpty();
  }

  @Test
  void invalidateAll_clearsCache() {
    cache.put("key1", "value1");
    cache.put("key2", "value2");

    cache.invalidateAll();

    assertThat(cache.size()).isEqualTo(0);
    assertThat(cache.get("key1")).isEmpty();
    assertThat(cache.get("key2")).isEmpty();
  }

  @Test
  void lruEviction_succeeds() {
    cache.put("key1", "value1");
    cache.put("key2", "value2");

    cache.put("key3", "value3");
    cache.cleanUp();

    assertThat(cache.size()).isEqualTo(2);
    // One of them must be empty.
    boolean anyEmpty =
        cache.get("key1").isEmpty() || cache.get("key2").isEmpty() || cache.get("key3").isEmpty();
    assertThat(anyEmpty).isTrue();
  }

  @Test
  void create_withInvalidMaxEntries_throwsException() {
    assertThrows(IllegalArgumentException.class, () -> AnalyticsCacheCaffeineImpl.create(0));
    assertThrows(IllegalArgumentException.class, () -> AnalyticsCacheCaffeineImpl.create(-1));
  }
}
