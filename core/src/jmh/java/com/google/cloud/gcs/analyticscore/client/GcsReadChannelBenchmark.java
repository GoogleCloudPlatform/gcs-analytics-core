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

package com.google.cloud.gcs.analyticscore.client;

import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import com.google.cloud.gcs.analyticscore.core.IntegrationTestHelper;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class GcsReadChannelBenchmark {

    private GcsItemId itemId;
    private byte[] buffer;
    private GoogleCloudStorageInputStream stream;
    private GcsFileSystem gcsFileSystem;
    private GcsReadOptions options;

    @Setup(Level.Trial)
    public void setup(GcsReadChannelBenchmarkState state) throws IOException {
        IntegrationTestHelper.uploadSampleParquetFilesIfNotExists();
        itemId = GcsItemId.builder()
                .setBucketName(IntegrationTestHelper.BUCKET_NAME)
                .setObjectName(IntegrationTestHelper.getFolderName() + IntegrationTestHelper.TPCDS_CUSTOMER_MEDIUM_FILE)
                .build();
        buffer = new byte[1024];

        options = GcsReadOptions.builder()
                .setInplaceSeekLimit(state.scenario.getInplaceSeekLimit())
                .build();
        GcsFileSystemOptions fileSystemOptions = GcsFileSystemOptions.builder()
                .setGcsClientOptions(GcsClientOptions.builder()
                        .setGcsReadOptions(options)
                        .build())
                .build();
        gcsFileSystem = new GcsFileSystemImpl(fileSystemOptions);
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws IOException {
        stream = GoogleCloudStorageInputStream.create(gcsFileSystem, itemId);
        stream.read(buffer);
    }

    @TearDown(Level.Invocation)
    public void tearDownInvocation() throws IOException {
        if (stream != null) {
            stream.close();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(value = 1, warmups = 0)
    public void seek(GcsReadChannelBenchmarkState state) throws IOException {
        stream.seek(state.scenario.getSeekDistance());
        stream.read(buffer);
    }
}
