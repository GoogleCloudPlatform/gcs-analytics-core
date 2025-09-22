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

    @Setup(Level.Invocation)
    public void uploadSampleFiles() throws IOException {
        uploadBundledResourceToGcs("tpcds_customer_sf1.parquet");
        uploadBundledResourceToGcs("tpcds_customer_sf10.parquet");
        uploadBundledResourceToGcs("tpcds_customer_sf100.parquet");
    }

    @TearDown(Level.Invocation)
    public void deleteSampleFiles() throws IOException {
        IntegrationTestHelper.deleteUploadedFilesFromGcs();
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void parquetFooterParsing_3mbFile_withFooterPrefetchingEnabled() throws IOException {
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile("tpcds_customer_sf1.parquet");
        ParquetHelper.readParquetMetadata(uri, true);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void parquetFooterParsing_3mbFile_withFooterPrefetchingDisabled() throws IOException {
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile("tpcds_customer_sf1.parquet");
        ParquetHelper.readParquetMetadata(uri, false);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void parquetFooterParsing_18mbFile_withFooterPrefetchingEnabled() throws IOException {
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile("tpcds_customer_sf10.parquet");
        ParquetHelper.readParquetMetadata(uri, true);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void parquetFooterParsing_18mbFile_withFooterPrefetchingDisabled() throws IOException {
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile("tpcds_customer_sf10.parquet");
        ParquetHelper.readParquetMetadata(uri, false);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void parquetFooterParsing_50mbFile_withFooterPrefetchingEnabled() throws IOException {
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile("tpcds_customer_sf100.parquet");
        ParquetHelper.readParquetMetadata(uri, true);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void parquetFooterParsing_50mbFile_withFooterPrefetchingDisabled() throws IOException {
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile("tpcds_customer_sf100.parquet");
        ParquetHelper.readParquetMetadata(uri, false);
    }

    private void uploadBundledResourceToGcs(String fileName) {
        IntegrationTestHelper.uploadFileToGcs(
                getClass().getResourceAsStream("/sampleParquetFiles/" + fileName),
                fileName);
    }
}
