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

/**
 * Hermetic benchmark for per-open channel/strategy construction and {@code
 * GcsReadChannelMetadataExtractor}'s reflective first-read probe when object size is unknown.
 */
public class GcsReadChannelOpenBenchmark extends HermeticBenchmarkBase {
  private GcsFileInfo fileInfo;

  @Override
  @Setup(Level.Trial)
  public void setUp() throws IOException {
    initFileSystem("open.bin", 64 * 1024, "small-file.cache.enabled", "false");
    fileInfo = fileSystem.getFileInfo(itemId);
  }

  /** Opens with a resolved {@link GcsFileInfo}, reads 1 byte, and closes the stream. */
  @Benchmark
  public int openAndReadOneByte_withFileInfo(IoCounters counters) throws IOException {
    try (var stream = GoogleCloudStorageInputStream.create(fileSystem, fileInfo)) {
      int b = stream.read();
      counters.record(storage);
      return b;
    }
  }

  /**
   * Opens with a bare {@code GcsItemId} (size unknown at open time, triggering {@code
   * GcsReadChannelMetadataExtractor} on the first read), reads 1 byte, and closes the stream.
   */
  @Benchmark
  public int openAndReadOneByte_withItemId(IoCounters counters) throws IOException {
    try (var stream = GoogleCloudStorageInputStream.create(fileSystem, itemId)) {
      int b = stream.read();
      counters.record(storage);
      return b;
    }
  }
}
