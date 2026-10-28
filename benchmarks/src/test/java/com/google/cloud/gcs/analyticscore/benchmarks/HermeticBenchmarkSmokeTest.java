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

import static com.google.common.truth.Truth.assertThat;
import static java.util.stream.Collectors.toList;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

class HermeticBenchmarkSmokeTest {
  private final HermeticInputStreamBenchmark benchmark = new HermeticInputStreamBenchmark();

  @BeforeEach
  void setUp() {
    benchmark.objectSize = 1_048_576;
    benchmark.setUp();
  }

  @AfterEach
  void tearDown() {
    benchmark.tearDown();
  }

  @Test
  void everyBenchmarkMethod_runsOnceWithoutError() throws Exception {
    runSuite(benchmark);
    for (HermeticBenchmarkBase s :
        List.of(
            new ReadStrategyBenchmark(),
            new FooterPrefetchBenchmark(),
            new SmallObjectCacheBenchmark(),
            new VectoredRangeCoalescingBenchmark(),
            new GcsReadChannelOpenBenchmark(),
            new TelemetryOverheadBenchmark())) {
      try (s) {
        s.setUp();
        runSuite(s);
      }
    }
  }

  private static void runSuite(Object benchmark) throws Exception {
    List<Method> benchmarks =
        Arrays.stream(benchmark.getClass().getMethods())
            .filter(m -> m.isAnnotationPresent(Benchmark.class))
            .collect(toList());
    assertThat(benchmarks).isNotEmpty();
    for (Method method : benchmarks) {
      Object[] args =
          Arrays.stream(method.getParameterTypes())
              .map(HermeticBenchmarkSmokeTest::argumentFor)
              .toArray();
      try {
        method.invoke(benchmark, args);
      } finally {
        for (Object arg : args) {
          if (arg instanceof AutoCloseable) {
            ((AutoCloseable) arg).close();
          }
        }
      }
    }
  }

  @Test
  void readTailWithFooterPrefetch_isServedByOneBoundedPrefetchRead() throws Exception {
    IoCounters counters = new IoCounters();
    // First call primes IoCounters; subsequent calls accumulate per-operation deltas.
    benchmark.readTailWithFooterPrefetch(counters);
    long opensBefore = benchmark.storage.readerOpens();
    long bytesBefore = benchmark.storage.bytesServed();

    benchmark.readTailWithFooterPrefetch(counters);

    // Unread sequential readers do not open streams; footer prefetch issues one 128 KiB GET.
    assertThat(benchmark.storage.readerOpens() - opensBefore).isEqualTo(1);
    assertThat(benchmark.storage.bytesServed() - bytesBefore).isEqualTo(128 * 1024);
    assertThat(counters.readerOpens).isEqualTo(1);
    assertThat(counters.bytesServed).isEqualTo(128 * 1024);
  }

  private static Object argumentFor(Class<?> type) {
    if (type == Blackhole.class) {
      return new Blackhole(
          "Today's password is swordfish. I understand instantiating Blackholes directly is"
              + " dangerous.");
    }
    if (type == IoCounters.class) {
      return new IoCounters();
    }
    if (type == TelemetryOverheadBenchmark.TelemetryState.class) {
      var state = new TelemetryOverheadBenchmark.TelemetryState();
      state.setUp();
      return state;
    }
    throw new IllegalArgumentException("Unsupported benchmark parameter type: " + type);
  }
}
