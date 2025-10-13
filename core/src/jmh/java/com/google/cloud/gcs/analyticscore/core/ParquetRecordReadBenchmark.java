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

import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class ParquetRecordReadBenchmark {
    private static final String TPCDS_CUSTOMER_XLARGE_FILE = "tpcds_customer_xlarge.parquet";

    @Setup(Level.Trial)
    public void uploadSampleFiles() throws IOException {
        IntegrationTestHelper.uploadSampleParquetFilesIfNotExists();
        if (!IntegrationTestHelper.objectPresentInBucket(TPCDS_CUSTOMER_XLARGE_FILE)) {
            TpcdsCustomerParquetWriter writer = new TpcdsCustomerParquetWriter();
            File file = writer.createSampleParquetFile(100000000, TPCDS_CUSTOMER_XLARGE_FILE);
            try {
                IntegrationTestHelper.uploadFileToGcs(file);
            } finally {
                file.delete();
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void smallFile(ParquetRecordReadState state) throws IOException {
      GcsFileSystemOptions gcsFileSystemOptions = GcsFileSystemOptions.createFromOptions(
              Map.of("gcs.analytics-core.small-file.footer.prefetch.size-bytes", state.footerPrefetchSize,
                      "gcs.analytics-core.large-file.footer.prefetch.size-bytes", state.footerPrefetchSize,
                      "gcs.analytics-core.small-file.cache.threshold-bytes", "1048576"), "gcs.");
        String requestedSchema = "message requested_schema {\n"
                + "required binary c_customer_id (STRING);\n"
                + "optional binary c_first_name (STRING);\n"
                + "optional binary c_email_address (STRING);\n"
                + "}";
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile(IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE);
        ParquetHelper.readParquetObjectRecords(uri, requestedSchema, state.enableVectoredRead, gcsFileSystemOptions);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void mediumFile(ParquetRecordReadState state) throws IOException {
      GcsFileSystemOptions gcsFileSystemOptions = GcsFileSystemOptions.createFromOptions(
              Map.of("gcs.analytics-core.small-file.footer.prefetch.size-bytes", state.footerPrefetchSize,
                      "gcs.analytics-core.large-file.footer.prefetch.size-bytes", state.footerPrefetchSize,
                      "gcs.analytics-core.small-file.cache.threshold-bytes", "1048576"), "gcs.");
        String requestedSchema = "message requested_schema {\n"
                + "required binary c_customer_id (STRING);\n"
                + "optional binary c_first_name (STRING);\n"
                + "optional binary c_email_address (STRING);\n"
                + "}";
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile(IntegrationTestHelper.TPCDS_CUSTOMER_MEDIUM_FILE);
        ParquetHelper.readParquetObjectRecords(uri, requestedSchema, state.enableVectoredRead, gcsFileSystemOptions);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void largeFile(ParquetRecordReadState state) throws IOException {
      GcsFileSystemOptions gcsFileSystemOptions = GcsFileSystemOptions.createFromOptions(
              Map.of("gcs.analytics-core.small-file.footer.prefetch.size-bytes", state.footerPrefetchSize,
                      "gcs.analytics-core.large-file.footer.prefetch.size-bytes", state.footerPrefetchSize,
                      "gcs.analytics-core.small-file.cache.threshold-bytes", "1048576"), "gcs.");
        String requestedSchema = "message requested_schema {\n"
                + "required binary c_customer_id (STRING);\n"
                + "optional binary c_first_name (STRING);\n"
                + "optional binary c_email_address (STRING);\n"
                + "}";
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile(IntegrationTestHelper.TPCDS_CUSTOMER_LARGE_FILE);
        ParquetHelper.readParquetObjectRecords(uri, requestedSchema, state.enableVectoredRead, gcsFileSystemOptions);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void xLargeFile(ParquetRecordReadState state) throws IOException {
      GcsFileSystemOptions gcsFileSystemOptions = GcsFileSystemOptions.createFromOptions(
              Map.of("gcs.analytics-core.small-file.footer.prefetch.size-bytes", state.footerPrefetchSize,
                      "gcs.analytics-core.large-file.footer.prefetch.size-bytes", state.footerPrefetchSize,
                      "gcs.analytics-core.small-file.cache.threshold-bytes", "1048576"), "gcs.");
        String requestedSchema = "message requested_schema {\n"
                + "required binary c_customer_id (STRING);\n"
                + "optional binary c_first_name (STRING);\n"
                + "optional binary c_email_address (STRING);\n"
                + "}";
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile(TPCDS_CUSTOMER_XLARGE_FILE);
        ParquetHelper.readParquetObjectRecords(uri, requestedSchema, state.enableVectoredRead, gcsFileSystemOptions);
    }
}
