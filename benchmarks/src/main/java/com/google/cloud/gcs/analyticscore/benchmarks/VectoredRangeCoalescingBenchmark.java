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

import com.google.cloud.gcs.analyticscore.client.GcsObjectCombinedRange;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.HermeticGcsFileSystem;
import com.google.common.collect.ImmutableList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

/** Pure-CPU benchmark for {@code VectoredIoUtil#mergeGcsObjectRanges}. */
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class VectoredRangeCoalescingBenchmark extends HermeticBenchmarkBase {
  @Param({"16", "256", "4096"})
  public int ranges = 16;

  @Param({"0", "1024", "1048576"})
  public int gapBytes = 1024;

  private ImmutableList<GcsObjectRange> rangeList;

  @Override
  @Setup(Level.Trial)
  public void setUp() {
    var builder = ImmutableList.<GcsObjectRange>builderWithExpectedSize(ranges);
    for (int i = 0; i < ranges; i++) {
      builder.add(
          GcsObjectRange.builder()
              .setOffset(i * (4096L + gapBytes))
              .setLength(4096)
              .setByteBufferFuture(new CompletableFuture<>())
              .build());
    }
    rangeList = builder.build();
  }

  /** Merges sorted ranges using production-default {@code maxMergeGap} and {@code maxMergeSize}. */
  @Benchmark
  public ImmutableList<GcsObjectCombinedRange> mergeRanges() {
    return HermeticGcsFileSystem.mergeGcsObjectRanges(rangeList, 4 * 1024, 8 * 1024 * 1024);
  }
}
