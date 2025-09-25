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

import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

public class ParquetRecordReadBenchmark {

    @Setup(Level.Invocation)
    public void uploadSampleFiles() throws IOException {
        // TODO - Generate and upload files used for benchmarking
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void smallFile(ParquetReadState state) throws IOException {
        String requestedSchema = "message requested_schema {\n"
                + "optional binary c_customer_id (STRING);\n"
                + "optional binary c_first_name (STRING);\n"
                + "optional binary c_email_address (STRING);\n"
                + "}";
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile("tpcds_customer_small.parquet");
        ParquetHelper.readParquetObjectRecords(uri, requestedSchema, state.enableVectoredRead, state.enableFooterPrefetch);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void mediumFile(ParquetReadState state) throws IOException {
        String requestedSchema = "message requested_schema {\n"
                + "optional binary c_customer_id (STRING);\n"
                + "optional binary c_first_name (STRING);\n"
                + "optional binary c_email_address (STRING);\n"
                + "}";
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile("tpcds_customer_medium.parquet");
        ParquetHelper.readParquetObjectRecords(uri, requestedSchema, state.enableVectoredRead, state.enableFooterPrefetch);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void largeFile(ParquetReadState state) throws IOException {
        String requestedSchema = "message requested_schema {\n"
                + "optional binary c_customer_id (STRING);\n"
                + "optional binary c_first_name (STRING);\n"
                + "optional binary c_email_address (STRING);\n"
                + "}";
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile("tpcds_customer_large.parquet");
        ParquetHelper.readParquetObjectRecords(uri, requestedSchema, state.enableVectoredRead, state.enableFooterPrefetch);
    }
}
