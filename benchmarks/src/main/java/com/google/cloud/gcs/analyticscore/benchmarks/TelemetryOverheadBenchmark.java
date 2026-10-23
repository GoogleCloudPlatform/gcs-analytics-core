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

import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCache;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCacheCaffeineImpl;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCacheNoOpImpl;
import com.google.cloud.gcs.analyticscore.common.telemetry.MetricKey;
import com.google.cloud.gcs.analyticscore.common.telemetry.Operation;
import com.google.cloud.gcs.analyticscore.common.telemetry.OperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
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

/**
 * Hermetic benchmark for {@link Telemetry} operation/metric recording and {@link AnalyticsCache}
 * Caffeine vs no-op lookup overhead.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Threads(1)
@State(Scope.Benchmark)
public class TelemetryOverheadBenchmark extends HermeticBenchmarkBase {
  private static final ImmutableMap<String, String> ATTRIBUTES =
      ImmutableMap.of(
          GcsAnalyticsCoreTelemetryConstants.Attribute.CLASS_NAME.name(),
          TelemetryOverheadBenchmark.class.getName());
  private static final Long CACHE_VALUE = 4096L;

  private AnalyticsCache<GcsItemId, Long> caffeineCache;
  private AnalyticsCache<GcsItemId, Long> noOpCache;
  private GcsItemId lookupKey;

  /** Benchmark state parameterized by {@link OperationListener} count for telemetry benchmarks. */
  @State(Scope.Benchmark)
  public static class TelemetryState implements AutoCloseable {
    @Param({"0", "1", "8"})
    public int listenerCount = 1;

    private Telemetry telemetry;
    private volatile long listenerSink;

    @Setup(Level.Trial)
    public void setUp() {
      close();
      var listeners = ImmutableList.<OperationListener>builderWithExpectedSize(listenerCount);
      for (int i = 0; i < listenerCount; i++) {
        listeners.add(
            new OperationListener() {
              @Override
              public void onOperationStart(Operation operation) {
                listenerSink = operation.getName().hashCode();
              }

              @Override
              public void onOperationEnd(Operation operation, Map<MetricKey, Long> metrics) {
                listenerSink = operation.getName().hashCode() ^ metrics.size();
              }
            });
      }
      telemetry = new Telemetry(listeners.build());
    }

    @Override
    @TearDown(Level.Trial)
    public void close() {
      if (telemetry != null) {
        telemetry.close();
        telemetry = null;
      }
    }
  }

  private static GcsItemId newCacheKey() {
    return GcsItemId.builder()
        .setBucketName("bench")
        .setObjectName("obj.parquet")
        .setContentGeneration(1L)
        .build();
  }

  @Override
  @Setup(Level.Trial)
  public void setUp() {
    GcsItemId putKey = newCacheKey();
    lookupKey = newCacheKey();
    checkState(putKey != lookupKey && putKey.equals(lookupKey));
    caffeineCache = AnalyticsCacheCaffeineImpl.create(64, (k, v) -> 1);
    caffeineCache.put(putKey, CACHE_VALUE);
    noOpCache = AnalyticsCacheNoOpImpl.getInstance();
  }

  /**
   * Measures a scoped operation that records one counter metric (mirrors data-plane {@code read}).
   */
  @Benchmark
  public int measureOperation(TelemetryState state) {
    return state.telemetry.measure(
        GcsAnalyticsCoreTelemetryConstants.Operation.READ.name(),
        GcsAnalyticsCoreTelemetryConstants.Metric.READ_DURATION,
        ATTRIBUTES,
        recorder -> {
          recorder.record(
              GcsAnalyticsCoreTelemetryConstants.Metric.READ_BYTES, 4096L, Collections.emptyMap());
          return 4096;
        });
  }

  /** Records a standalone counter metric outside an operation scope (mirrors cache-hit path). */
  @Benchmark
  public long recordMetric(TelemetryState state) {
    state.telemetry.recordMetric(
        GcsAnalyticsCoreTelemetryConstants.Metric.SMALL_OBJECT_CACHE_HIT, 1L, ATTRIBUTES);
    return state.listenerSink;
  }

  /** Looks up a warm entry in a small-capacity {@link AnalyticsCacheCaffeineImpl}. */
  @Benchmark
  public Long caffeineCacheHit() {
    return caffeineCache.get(lookupKey, k -> CACHE_VALUE);
  }

  /**
   * Invokes {@link AnalyticsCacheNoOpImpl#get(Object,
   * com.google.cloud.gcs.analyticscore.common.cache.ThrowingFunction)}.
   */
  @Benchmark
  public Long noOpCacheLookup() {
    return noOpCache.get(lookupKey, k -> CACHE_VALUE);
  }
}
