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
import java.io.File;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 2, time = 1)
@Fork(value = 2, warmups = 1)
public class SmallObjectCachingBenchmark {
    private static final String TPCDS_CUSTOMER_SMALL_FILE = "tpcds_customer_small.parquet";

    @Setup(Level.Trial)
    public void uploadSampleFiles() throws IOException {
        IntegrationTestHelper.uploadSampleParquetFilesIfNotExists();
        if (!IntegrationTestHelper.objectPresentInBucket(TPCDS_CUSTOMER_SMALL_FILE)) {
            TpcdsCustomerParquetWriter writer = new TpcdsCustomerParquetWriter();
            File file = writer.createSampleParquetFile(100000, TPCDS_CUSTOMER_SMALL_FILE);
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
    public void smallFile(SmallObjectCacheState state) throws IOException {
        String requestedSchema = "message requested_schema {\n"
                + "required binary c_customer_id (STRING);\n"
                + "optional binary c_first_name (STRING);\n"
                + "optional binary c_email_address (STRING);\n"
                + "}";
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile(IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE);
        ParquetHelper.readParquetMetadata(uri, state.footerPrefetchSize, state.smallObjectCache);
    }
}