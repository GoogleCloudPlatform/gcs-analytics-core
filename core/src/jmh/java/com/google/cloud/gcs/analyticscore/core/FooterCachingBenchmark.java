/*
 * Copyright 2025 Google LLC
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

package com.google.cloud.gcs.analyticscore.core;

import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 2, time = 1)
@Fork(value = 2, warmups = 1)
public class FooterCachingBenchmark {

  private GcsFileSystem gcsFileSystem;
  private URI uri;

  @Param({"true", "false"})
  public boolean footerCachingEnabled;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    IntegrationTestHelper.uploadSampleParquetFilesIfNotExists();
    GcsFileSystemOptions gcsFileSystemOptions =
        GcsFileSystemOptions.createFromOptions(
            Map.of(
                "gcs.analytics-core.footer.prefetch.enabled",
                String.valueOf(footerCachingEnabled),
                "gcs.analytics-core.small-file.footer.prefetch.size-bytes",
                "102400"),
            "gcs.");
    gcsFileSystem = new GcsFileSystemImpl(gcsFileSystemOptions);
    uri = IntegrationTestHelper.getGcsObjectUriForFile(IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    gcsFileSystem.close();
  }

  @Benchmark
  public void readMetadata() throws IOException {
    ParquetHelper.readParquetMetadataWithFileSystem(uri, gcsFileSystem);
  }
}
