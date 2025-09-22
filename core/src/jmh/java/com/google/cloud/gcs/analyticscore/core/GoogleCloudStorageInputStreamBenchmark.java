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

@State(Scope.Benchmark)
public class GoogleCloudStorageInputStreamBenchmark {
//    private static final File TPCDS_CUSTOMER_SF1 = IntegrationTestHelper.getFileFromResources(
//            "/sampleParquetFiles/tpcds_customer_sf1.parquet");
//    private static final File TPCDS_CUSTOMER_SF10 = IntegrationTestHelper.getFileFromResources(
//            "/sampleParquetFiles/tpcds_customer_sf10.parquet");
//    private static final File TPCDS_CUSTOMER_SF100 = IntegrationTestHelper.getFileFromResources(
//            "/sampleParquetFiles/tpcds_customer_sf100.parquet");

    @Setup(org.openjdk.jmh.annotations.Level.Invocation)
    public void uploadSampleFiles() throws IOException {
        IntegrationTestHelper.uploadFileToGcs(getClass().getResourceAsStream("/sampleParquetFiles/tpcds_customer_sf1.parquet"), "tpcds_customer_sf1.parquet");
//        IntegrationTestHelper.uploadFileToGcs(TPCDS_CUSTOMER_SF10);
//        IntegrationTestHelper.uploadFileToGcs(TPCDS_CUSTOMER_SF100);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void parquetFooterParsing_3mbFile_withFooterPrefetchingEnabled() throws IOException {
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile("tpcds_customer_sf1.parquet");
        System.out.println(uri);
        ParquetHelper.readParquetMetadata(uri, true);
    }
}
