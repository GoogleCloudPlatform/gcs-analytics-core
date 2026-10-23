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

import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.HermeticGcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.InMemoryStorage;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/** Hermetic benchmark comparing skip-in-place vs channel reopen across GCS read strategies. */
public class ReadStrategyBenchmark extends HermeticBenchmarkBase {
  @Param({"SEQUENTIAL", "RANDOM", "ADAPTIVE"})
  public String strategy = "ADAPTIVE";

  @Param({"0", "4096", "65536", "1048576"})
  public int forwardGapBytes = 65536;

  private final byte[] buffer = new byte[4096];

  @Override
  @Setup(Level.Trial)
  public void setUp() {
    String pattern = strategy.equals("ADAPTIVE") ? "AUTO_SEQUENTIAL" : strategy;
    initFileSystem("strategy.bin", 2 * 1024 * 1024, "read.file-access-pattern", pattern);
  }

  /** Reads 4 KiB at offset 0, seeks forward by {@code forwardGapBytes}, and reads 4 KiB. */
  @Benchmark
  public int seekAndRead(IoCounters counters) throws IOException {
    try (var s = open()) {
      int n = s.read(buffer, 0, buffer.length);
      s.seek((long) n + forwardGapBytes);
      n = s.read(buffer, 0, buffer.length);
      counters.record(storage);
      return n;
    }
  }
}

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Threads(1)
@State(Scope.Thread)
abstract class HermeticBenchmarkBase implements AutoCloseable {
  InMemoryStorage storage;
  GcsFileSystem fileSystem;
  GcsItemId itemId;

  abstract void setUp() throws IOException;

  void initFileSystem(String name, int size, String... kv) {
    close();
    var props =
        ImmutableMap.<String, String>builder()
            .put("analytics-core.read.thread.count", "1")
            .put("analytics-core.small-file.footer.prefetch.size-bytes", "131072")
            .put("analytics-core.small-file.cache.max-size-bytes", "10485760");
    for (int i = 0; i < kv.length; i += 2) {
      props.put("analytics-core." + kv[i], kv[i + 1]);
    }
    itemId = GcsItemId.builder().setBucketName("bench").setObjectName(name).build();
    storage = InMemoryStorage.create(ImmutableMap.of(itemId, new byte[size]));
    var options = GcsFileSystemOptions.createFromOptions(props.buildKeepingLast(), "");
    fileSystem = HermeticGcsFileSystem.create(storage, options);
  }

  GoogleCloudStorageInputStream open() throws IOException {
    return GoogleCloudStorageInputStream.create(fileSystem, itemId);
  }

  @Override
  @TearDown(Level.Trial)
  public void close() {
    if (fileSystem != null) {
      fileSystem.close();
    }
  }
}
