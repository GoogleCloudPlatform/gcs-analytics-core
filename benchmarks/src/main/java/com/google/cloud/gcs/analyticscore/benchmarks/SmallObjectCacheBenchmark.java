/*
 * Copyright 2026 Google LLC
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

package com.google.cloud.gcs.analyticscore.benchmarks;

import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import java.io.IOException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;

/** Hermetic benchmark comparing cold vs warm reads through {@code SmallObjectOptimizer}. */
public class SmallObjectCacheBenchmark extends HermeticBenchmarkBase {
  private final byte[] buffer = new byte[64 * 1024];
  private GcsFileInfo fileInfo;

  @Override
  @Setup(Level.Trial)
  public void setUp() throws IOException {
    initFileSystem("small.parquet", 256 * 1024, "small-file.cache.enabled", "true");
    fileInfo = fileSystem.getFileInfo(itemId);
    warmRead(new IoCounters());
  }

  /** Invalidates the cached entry before reading so every invocation misses the cache. */
  @Benchmark
  public long coldRead(IoCounters counters) throws IOException {
    fileSystem.getCacheManager().invalidateSmallObject(fileInfo.getItemInfo().getItemId());
    return warmRead(counters);
  }

  /** Reads against a trial-scoped file system whose small-object cache was warmed in setup. */
  @Benchmark
  public long warmRead(IoCounters counters) throws IOException {
    try (var s = GoogleCloudStorageInputStream.create(fileSystem, fileInfo)) {
      long total = 0;
      for (int n; (n = s.read(buffer, 0, buffer.length)) != -1; ) {
        total += n;
      }
      counters.record(storage);
      return total;
    }
  }
}
