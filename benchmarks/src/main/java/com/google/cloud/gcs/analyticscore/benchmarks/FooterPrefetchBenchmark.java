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

import java.io.IOException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

/** Hermetic benchmark for {@code GcsFooterOptimizer} tail and unknown-size footer reads. */
public class FooterPrefetchBenchmark extends HermeticBenchmarkBase {
  @Param({"true", "false"})
  public boolean prefetchEnabled = true;

  private final byte[] buffer = new byte[64 * 1024];

  @Override
  @Setup(Level.Trial)
  public void setUp() {
    initFileSystem("f.parquet", 1024 * 1024, "footer.prefetch.enabled", "" + prefetchEnabled);
  }

  /** 64 KiB {@code readTail} on a 1 MiB {@code .parquet} object. */
  @Benchmark
  public int readTail(IoCounters counters) throws IOException {
    try (var s = open()) {
      int n = s.readTail(buffer, 0, buffer.length);
      counters.record(storage);
      return n;
    }
  }

  /** Two-step Parquet trailer + footer read on an unknown-size stream ({@code GcsItemId} open). */
  @Benchmark
  public int readFooterUnknownSize(IoCounters counters) throws IOException {
    try (var s = open()) {
      s.seek(1024 * 1024 - 8);
      int n = s.read(buffer, 0, 8);
      s.seek(1024 * 1024 - buffer.length);
      n += s.read(buffer, 0, buffer.length);
      counters.record(storage);
      return n;
    }
  }
}
