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

import com.google.cloud.gcs.analyticscore.client.InMemoryStorage;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

/**
 * JMH aux columns for {@link InMemoryStorage} counters; informational, and exact only for
 * {@code @Threads(1)} because the storage is {@code Scope.Benchmark}-shared.
 */
@State(Scope.Thread)
@AuxCounters(AuxCounters.Type.EVENTS)
public class IoCounters {
  public long readerOpens;
  public long bytesServed;
  private long lastOpens;
  private long lastBytes;
  private boolean primed;

  /** Adds growth since the last call; never resets the storage. The first call only primes. */
  public void record(InMemoryStorage storage) {
    long opens = storage.readerOpens();
    long bytes = storage.bytesServed();
    if (primed) {
      readerOpens += opens - lastOpens;
      bytesServed += bytes - lastBytes;
    }
    primed = true;
    lastOpens = opens;
    lastBytes = bytes;
  }
}
