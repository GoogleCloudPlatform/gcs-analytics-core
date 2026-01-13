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

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 2, time = 1)
@Fork(value = 2, warmups = 1)
public class AdaptiveRangeReadBenchmark {

    private static final String SEQUENTIAL_SCHEMA = "message requested_schema {\n"
            + "required int64 c_customer_sk;\n"
            + "required binary c_customer_id (STRING);\n"
            + "optional int64 c_current_cdemo_sk;\n"
            + "optional int64 c_current_hdemo_sk;\n"
            + "optional int64 c_current_addr_sk;\n"
            + "optional int64 c_first_shipto_date_sk;\n"
            + "optional int64 c_first_sales_date_sk;\n"
            + "optional binary c_salutation (STRING);\n"
            + "optional binary c_first_name (STRING);\n"
            + "optional binary c_last_name (STRING);\n"
            + "optional binary c_preferred_cust_flag (STRING);\n"
            + "optional int32 c_birth_day;\n"
            + "optional int32 c_birth_month;\n"
            + "optional int32 c_birth_year;\n"
            + "optional binary c_birth_country (STRING);\n"
            + "optional binary c_login (STRING);\n"
            + "optional binary c_email_address (STRING);\n"
            + "optional int64 c_last_review_date_sk;\n"
            + "}";

    private static final String RANDOM_SCHEMA = "message requested_schema {\n"
            + "required binary c_customer_id (STRING);\n"
            + "optional binary c_first_name (STRING);\n"
            + "optional binary c_email_address (STRING);\n"
            + "}";

    private URI smallFileUri;
    private URI mediumFileUri;
    private URI largeFileUri;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        IntegrationTestHelper.uploadSampleParquetFilesIfNotExists();
        smallFileUri = IntegrationTestHelper.getGcsObjectUriForFile(IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE);
        mediumFileUri = IntegrationTestHelper.getGcsObjectUriForFile(IntegrationTestHelper.TPCDS_CUSTOMER_MEDIUM_FILE);
        largeFileUri = IntegrationTestHelper.getGcsObjectUriForFile(IntegrationTestHelper.TPCDS_CUSTOMER_LARGE_FILE);
    }

    @Benchmark
    public void sequentialReadSmall(AdaptiveRangeReadState state) throws IOException {
        runBenchmark(state, SEQUENTIAL_SCHEMA, smallFileUri);
    }

    @Benchmark
    public void sequentialReadMedium(AdaptiveRangeReadState state) throws IOException {
        runBenchmark(state, SEQUENTIAL_SCHEMA, mediumFileUri);
    }

    @Benchmark
    public void sequentialReadLarge(AdaptiveRangeReadState state) throws IOException {
        runBenchmark(state, SEQUENTIAL_SCHEMA, largeFileUri);
    }

    @Benchmark
    public void randomReadSmall(AdaptiveRangeReadState state) throws IOException {
        runBenchmark(state, RANDOM_SCHEMA, smallFileUri);
    }

    @Benchmark
    public void randomReadMedium(AdaptiveRangeReadState state) throws IOException {
        runBenchmark(state, RANDOM_SCHEMA, mediumFileUri);
    }

    @Benchmark
    public void randomReadLarge(AdaptiveRangeReadState state) throws IOException {
        runBenchmark(state, RANDOM_SCHEMA, largeFileUri);
    }

    private void runBenchmark(AdaptiveRangeReadState state, String requestedSchema, URI fileUri) throws IOException {
       GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(
                Map.of(
                        "gcs.analytics-core.read.adaptive-range.file-access-pattern", state.fileAccessPattern,
                        "gcs.analytics-core.read.adaptive-range.min-range-request-size-bytes", state.minRangeRequestSize,
                        "gcs.analytics-core.read.adaptive-range.inplace-seek-limit-bytes", state.inplaceSeekLimit,
                        "gcs.analytics-core.read.adaptive-range.adaptive-range-read-enabled", String.valueOf(state.adaptiveRangeReadEnabled),
                        "gcs.channel.vectored.enabled", String.valueOf(state.enableVectoredRead)
                ),
                "gcs."
        );

        ParquetHelper.readParquetObjectRecords(fileUri, requestedSchema, state.enableVectoredRead, options);
    }
}
