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

package com.google.cloud.gcs.analyticscore.core;

import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;

@State(Scope.Benchmark)
public class PredictivePrefetchBenchmark {

  private static final String PREFETCH_ENABLED_KEY = "gcs.analytics-core.prefetch.enabled";

  @Setup(Level.Trial)
  public void uploadSampleFiles() throws IOException {
    IntegrationTestHelper.uploadSampleParquetFilesIfNotExists();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 1, time = 1)
  @Measurement(iterations = 2, time = 1)
  @Fork(value = 2, warmups = 1)
  public void projectedColumnsLargeFile(PredictivePrefetchState state) throws IOException {
    GcsFileSystemOptions gcsFileSystemOptions =
        GcsFileSystemOptions.createFromOptions(
            Map.of(
                PREFETCH_ENABLED_KEY,
                state.prefetchEnabled,
                "gcs.analytics-core.footer.cache.enabled",
                "true",
                "gcs.analytics-core.small-file.footer.prefetch.size-bytes",
                "102400",
                "gcs.analytics-core.large-file.footer.prefetch.size-bytes",
                "1048576"),
            "gcs.");
    String requestedSchema =
        "message requested_schema {\n"
            + "required binary c_customer_id (STRING);\n"
            + "optional binary c_first_name (STRING);\n"
            + "optional binary c_email_address (STRING);\n"
            + "}";
    URI uri =
        IntegrationTestHelper.getGcsObjectUriForFile(
            IntegrationTestHelper.TPCDS_CUSTOMER_LARGE_FILE);
    ParquetHelper.readParquetObjectRecords(
        uri, requestedSchema, state.enableVectoredRead, gcsFileSystemOptions);
  }
}
