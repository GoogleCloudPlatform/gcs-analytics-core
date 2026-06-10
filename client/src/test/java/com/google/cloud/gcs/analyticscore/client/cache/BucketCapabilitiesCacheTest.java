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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.cloud.gcs.analyticscore.common.BucketCapabilities;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;

public class BucketCapabilitiesCacheTest {

  private BucketCapabilitiesCache cache;

  @Before
  public void setUp() {
    cache = new BucketCapabilitiesCache(10, 10, TimeUnit.MINUTES);
  }

  @Test
  public void testCacheReturnsValueAndCachesIt() throws IOException {
    AtomicInteger calls = new AtomicInteger(0);
    BucketCapabilities capabilities = new BucketCapabilities(true);

    BucketCapabilitiesCache.BucketProber loader =
        bucketName -> {
          calls.incrementAndGet();
          return capabilities;
        };

    BucketCapabilities result1 = cache.get("test-bucket", loader);
    BucketCapabilities result2 = cache.get("test-bucket", loader);

    assertEquals(capabilities, result1);
    assertEquals(capabilities, result2);
    assertEquals(1, calls.get()); // The prober should only be called once
  }

  @Test
  public void testCachePropagatesIOException() {
    BucketCapabilitiesCache.BucketProber loader =
        bucketName -> {
          throw new IOException("Probing failed");
        };

    IOException exception = assertThrows(IOException.class, () -> cache.get("test-bucket", loader));
    assertEquals("Probing failed", exception.getMessage());
  }

  @Test
  public void testInvalidArguments() {
    assertThrows(
        IllegalArgumentException.class, () -> new BucketCapabilitiesCache(0, 10, TimeUnit.MINUTES));
    assertThrows(
        IllegalArgumentException.class,
        () -> new BucketCapabilitiesCache(10, -1, TimeUnit.MINUTES));
    assertThrows(NullPointerException.class, () -> new BucketCapabilitiesCache(10, 10, null));
  }

  @Test
  public void testGetNullArguments() {
    assertThrows(NullPointerException.class, () -> cache.get(null, b -> null));
    assertThrows(NullPointerException.class, () -> cache.get("test", null));
  }

  @Test
  public void testProberReturnsNull() {
    BucketCapabilitiesCache.BucketProber loader = bucketName -> null;
    assertThrows(NullPointerException.class, () -> cache.get("test-bucket", loader));
  }
}
