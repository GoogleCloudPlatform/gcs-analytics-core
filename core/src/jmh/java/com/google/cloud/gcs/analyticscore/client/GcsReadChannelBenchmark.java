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
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class GcsReadChannelBenchmark {

    private GcsItemId itemId;
    private byte[] buffer;
    private byte[] largeBuffer;
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
        largeBuffer = new byte[100 * 1024];

        options = GcsReadOptions.builder()
                .setFileAccessPattern(state.accessPattern)
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
    }

    @TearDown(Level.Invocation)
    public void tearDownInvocation() throws IOException {
        if (stream != null) {
            stream.close();
        }
    }

    private void readWithSeek(int seekDistance) throws IOException {
        stream.read(buffer);
        stream.seek(seekDistance);
        stream.read(buffer);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 10, time = 1)
    @Fork(value = 2, warmups = 0)
    public void readWith128KSeek(GcsReadChannelBenchmarkState state) throws IOException {
        readWithSeek(128 * 1024);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 10, time = 1)
    @Fork(value = 2, warmups = 0)
    public void readWith512KSeek(GcsReadChannelBenchmarkState state) throws IOException {
        readWithSeek(512 * 1024);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 10, time = 1)
    @Fork(value = 2, warmups = 0)
    public void readWith1MSeek(GcsReadChannelBenchmarkState state) throws IOException {
        readWithSeek(1024 * 1024);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 10, time = 1)
    @Fork(value = 2, warmups = 0)
    public void sequentialRead(GcsReadChannelBenchmarkState state) throws IOException {
        ByteBuffer localBuffer = ByteBuffer.allocate(64 * 1024);
        int bytesToRead = 2 * 1024 * 1024;
        int bytesRead = 0;
        while (bytesRead < bytesToRead) {
            localBuffer.clear();
            int read = stream.read(localBuffer);
            if (read == -1) break;
            bytesRead += read;
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 10, time = 1)
    @Fork(value = 2, warmups = 0)
    public void readWithBackwardSeek(GcsReadChannelBenchmarkState state) throws IOException {
        stream.read(buffer);
        stream.seek(512 * 1024);
        stream.read(buffer);
        stream.seek(256 * 1024);
        stream.read(buffer);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 10, time = 1)
    @Fork(value = 2, warmups = 0)
    public void readWithBackwardSeekLargeRead(GcsReadChannelBenchmarkState state) throws IOException {
        stream.read(buffer);
        stream.seek(512 * 1024);
        stream.read(buffer);
        stream.seek(256 * 1024);
        stream.read(largeBuffer);
    }
}
