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

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;


@State(Scope.Benchmark)
public class JsonVsGrpcBenchmark {
  GcsFileSystem grpcFileSystem;
  GcsFileSystem jsonFileSystem;

  @Setup(Level.Trial)
  public void uploadSampleFiles() throws IOException {
    IntegrationTestHelper.uploadSampleParquetFilesIfNotExists();

    grpcFileSystem = new GcsFileSystemImpl(GcsFileSystemOptions.createFromOptions(
            Map.of("gcs.client.type", "GRPC_CLIENT"), "gcs."));
    jsonFileSystem = new GcsFileSystemImpl(GcsFileSystemOptions.builder().build());

    // warm up the underlying storage object
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE);
    ParquetHelper.readParquetObjectRecords(uri, /* readVectoredEnabled= */ true, grpcFileSystem);
    ParquetHelper.readParquetObjectRecords(uri, /* readVectoredEnabled= */ true, jsonFileSystem);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 1, time = 1)
  @Measurement(iterations = 2, time = 1)
  @Fork(value = 2, warmups = 1)
  public void readParquetRecords(JsonVsGrpcState state, Blackhole blackhole) throws IOException {
    GcsFileSystemOptions gcsFileSystemOptions = GcsFileSystemOptions.createFromOptions(
            Map.of("gcs.client.type", state.clientType), "gcs.");
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(state.fileSize);

    long recordCount = ParquetHelper.readParquetObjectRecords(uri, /* readVectoredEnabled= */ true, gcsFileSystemOptions);

    blackhole.consume(recordCount);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 1, time = 1)
  @Measurement(iterations = 2, time = 1)
  @Fork(value = 2, warmups = 1)
  public void readParquetRecordsReusingFileSystem(JsonVsGrpcState state, Blackhole blackhole) throws IOException {
    GcsFileSystem fileSystem = state.clientType.equals("GRPC_CLIENT") ? grpcFileSystem : jsonFileSystem;
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(state.fileSize);

    long recordCount = ParquetHelper.readParquetObjectRecords(uri, /* readVectoredEnabled= */ true, fileSystem);

    blackhole.consume(recordCount);
  }
}
