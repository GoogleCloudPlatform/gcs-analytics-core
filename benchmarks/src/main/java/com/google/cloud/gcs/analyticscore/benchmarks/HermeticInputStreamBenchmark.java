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

import com.google.cloud.gcs.analyticscore.client.GcsClientOptions;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import com.google.cloud.gcs.analyticscore.client.HermeticGcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.InMemoryStorage;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * Hermetic micro-benchmarks for {@link GoogleCloudStorageInputStream}: the whole read stack runs
 * against {@link InMemoryStorage} (no credentials, bucket or network), so scores reflect library
 * CPU and allocation overhead; each invocation opens a fresh stream. Run with:
 *
 * <pre>
 * ./mvnw -B -q -pl benchmarks -am test-compile dependency:build-classpath -DskipTests \
 *   -Dmdep.includeScope=test -Dmdep.outputFile=/tmp/cp.txt
 * $JAVA_HOME/bin/java -cp benchmarks/target/classes:$(cat /tmp/cp.txt) \
 *   org.openjdk.jmh.Main HermeticInputStreamBenchmark -rf json -rff /tmp/jmh.json -prof gc
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Threads(1)
@State(Scope.Thread)
public class HermeticInputStreamBenchmark {
  private static final int READ_BUFFER_SIZE = 64 * 1024;
  private static final int POSITIONED_READ_COUNT = 64;
  private static final int POSITIONED_READ_SIZE = 4096;
  private static final int VECTORED_RANGE_COUNT = 16;

  private static final int FOOTER_PREFETCH_SIZE = 128 * 1024;

  @Param({"1048576", "16777216"})
  public int objectSize;

  InMemoryStorage storage;
  private GcsFileSystem fileSystem;
  private GcsItemId itemId;
  private long[] positions;
  private byte[] buffer;

  @Setup(Level.Trial)
  public void setUp() {
    Random random = new Random(42);
    byte[] data = new byte[objectSize];
    random.nextBytes(data);
    positions = new long[POSITIONED_READ_COUNT];
    for (int i = 0; i < positions.length; i++) {
      positions[i] = random.nextInt(objectSize - POSITIONED_READ_SIZE + 1);
    }
    buffer = new byte[READ_BUFFER_SIZE];
    String name = "o-" + objectSize + ".parquet"; // the suffix engages GcsFooterOptimizer
    itemId = GcsItemId.builder().setBucketName("bench").setObjectName(name).build();
    GcsReadOptions readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchEnabled(true)
            .setFooterPrefetchSizeSmallFile(FOOTER_PREFETCH_SIZE)
            .build();
    storage = InMemoryStorage.create(ImmutableMap.of(itemId, data));
    fileSystem =
        HermeticGcsFileSystem.create(
            storage,
            GcsFileSystemOptions.builder()
                .setGcsClientOptions(
                    GcsClientOptions.builder().setGcsReadOptions(readOptions).build())
                .build());
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    fileSystem.close();
  }

  /** Reads the whole object with 64 KiB {@code read(byte[], int, int)} calls. */
  @Benchmark
  public long sequentialRead() throws IOException {
    long total = 0;
    try (GoogleCloudStorageInputStream stream =
        GoogleCloudStorageInputStream.create(fileSystem, itemId)) {
      int read;
      while ((read = stream.read(buffer, 0, buffer.length)) != -1) {
        total += read;
      }
    }
    return total;
  }

  /** 64 × 4 KiB {@code readFully} at a fixed pseudo-random position sequence. */
  @Benchmark
  public long positionedReads(IoCounters counters) throws IOException {
    try (GoogleCloudStorageInputStream stream =
        GoogleCloudStorageInputStream.create(fileSystem, itemId)) {
      for (long position : positions) {
        stream.readFully(position, buffer, 0, POSITIONED_READ_SIZE);
      }
    }
    counters.record(storage);
    return (long) POSITIONED_READ_COUNT * POSITIONED_READ_SIZE;
  }

  /** 16 non-overlapping 64 KiB ranges spread evenly across the object, all futures awaited. */
  @Benchmark
  public void readVectored(IoCounters counters, Blackhole bh) throws Exception {
    long stride = (long) objectSize / VECTORED_RANGE_COUNT;
    ImmutableList.Builder<GcsObjectRange> ranges = ImmutableList.builder();
    for (int i = 0; i < VECTORED_RANGE_COUNT; i++) {
      ranges.add(
          GcsObjectRange.builder()
              .setOffset(i * stride)
              .setLength(READ_BUFFER_SIZE)
              .setByteBufferFuture(new CompletableFuture<>())
              .build());
    }
    ImmutableList<GcsObjectRange> fileRanges = ranges.build();
    try (GoogleCloudStorageInputStream stream =
        GoogleCloudStorageInputStream.create(fileSystem, itemId)) {
      stream.readVectored(fileRanges, ByteBuffer::allocate);
      for (GcsObjectRange range : fileRanges) {
        bh.consume(range.getByteBufferFuture().get());
      }
    }
    counters.record(storage);
  }

  /** 64 KiB {@code readTail} served from {@code GcsFooterOptimizer}'s 128 KiB prefetch window. */
  @Benchmark
  public int readTailWithFooterPrefetch() throws IOException {
    try (GoogleCloudStorageInputStream stream =
        GoogleCloudStorageInputStream.create(fileSystem, itemId)) {
      return stream.readTail(buffer, 0, buffer.length);
    }
  }
}
