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

import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 2, time = 1)
@Fork(value = 2, warmups = 1)
public class SmallObjectCachingBenchmark {

    private static final String TPCDS_CUSTOMER_MEDIUM_FILE = "tpcds_customer_medium.parquet";
    private static final String TPCDS_CUSTOMER_SMALL_FILE = "tpcds_customer_small.parquet";

    @Setup(Level.Trial)
    public void uploadSampleFiles() throws IOException {
        TpcdsCustomerParquetWriter writer = new TpcdsCustomerParquetWriter();
        if (!IntegrationTestHelper.objectPresentInBucket(TPCDS_CUSTOMER_MEDIUM_FILE)) {
            IntegrationTestHelper.uploadParquetFileIfNotExists(writer, TPCDS_CUSTOMER_SMALL_FILE, 100000, "small");
        }
        if (!IntegrationTestHelper.objectPresentInBucket(TPCDS_CUSTOMER_SMALL_FILE)) {
            IntegrationTestHelper.uploadParquetFileIfNotExists(writer, TPCDS_CUSTOMER_MEDIUM_FILE, 1000000, "medium");
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void smallFile(ParquetFooterPrefetchState state) throws IOException {
        String requestedSchema = "message requested_schema {\n"
                + "required binary c_customer_id (STRING);\n"
                + "optional binary c_first_name (STRING);\n"
                + "optional binary c_email_address (STRING);\n"
                + "}";
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile(IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE);
        ParquetHelper.readParquetMetadata(uri, 2097152, state.smallObjectCache);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void mediumFile(ParquetFooterPrefetchState state) throws IOException {
        String requestedSchema = "message requested_schema {\n"
                + "required binary c_customer_id (STRING);\n"
                + "optional binary c_first_name (STRING);\n"
                + "optional binary c_email_address (STRING);\n"
                + "}";
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile(IntegrationTestHelper.TPCDS_CUSTOMER_MEDIUM_FILE);
        ParquetHelper.readParquetMetadata(uri, 20971520, state.smallObjectCache);
    }
}