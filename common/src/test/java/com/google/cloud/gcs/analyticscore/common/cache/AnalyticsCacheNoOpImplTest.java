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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnalyticsCacheNoOpImplTest {

  private AnalyticsCacheNoOpImpl<String, String> cache;

  @BeforeEach
  void setUp() {
    cache = AnalyticsCacheNoOpImpl.getInstance();
  }

  @Test
  void get_anyKey_alwaysReturnsEmpty() {
    cache.put("key1", "value1");

    assertThat(cache.get("key1")).isEmpty();
  }

  @Test
  void get_withMappingFunction_anyKey_alwaysComputesButDoesNotCache() throws Exception {
    AtomicInteger callCount = new AtomicInteger(0);

    String value1 =
        cache.get(
            "key1",
            k -> {
              callCount.incrementAndGet();
              return "val1";
            });
    String value2 =
        cache.get(
            "key1",
            k -> {
              callCount.incrementAndGet();
              return "val1";
            });

    assertThat(value1).isEqualTo("val1");
    assertThat(value2).isEqualTo("val1");
    assertThat(callCount.get()).isEqualTo(2);
    assertThat(cache.get("key1")).isEmpty();
  }

  @Test
  void get_withMappingFunction_returnsNull_throwsException() {
    assertThrows(NullPointerException.class, () -> cache.get("key1", k -> null));
  }

  @Test
  void get_withMappingFunction_throwsCheckedException_rethrowsException() {
    assertThrows(
        IOException.class,
        () ->
            cache.get(
                "key1",
                k -> {
                  throw new IOException("test-exception");
                }));
  }

  @Test
  void size_anyEntries_alwaysReturnsZero() {
    cache.put("key1", "value1");

    assertThat(cache.size()).isEqualTo(0);
  }

  @Test
  void invalidate_anyKey_doesNothing() {
    cache.put("key1", "value1");

    cache.invalidate("key1");
    cache.invalidateAll();

    assertThat(cache.size()).isEqualTo(0);
  }
}
