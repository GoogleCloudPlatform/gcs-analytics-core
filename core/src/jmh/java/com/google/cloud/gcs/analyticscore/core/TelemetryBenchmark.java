/*
 * Copyright 2026 Google LLC
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
import com.google.cloud.gcs.analyticscore.common.telemetry.CustomTelemetryOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.LoggingTelemetryOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.MetricKey;
import com.google.cloud.gcs.analyticscore.common.telemetry.OpenTelemetryOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.Operation;
import com.google.cloud.gcs.analyticscore.common.telemetry.OperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.TelemetryOptions;
import com.google.common.collect.ImmutableList;
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
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class TelemetryBenchmark {

    @Setup(Level.Trial)
    public void uploadSampleFiles() throws IOException {
        IntegrationTestHelper.uploadSampleParquetFilesIfNotExists();
    }

    private static final String REQUESTED_SCHEMA = "message requested_schema {\n"
            + "required binary c_customer_id (STRING);\n"
            + "optional binary c_first_name (STRING);\n"
            + "optional binary c_email_address (STRING);\n"
            + "}";

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 10, time = 1)
    @Fork(value = 2, warmups = 1)
    public long openTelemetryWithPeriodicLoggingProvider() throws IOException {
        TelemetryOptions telemetryOptions = TelemetryOptions.builder()
                .setOpenTelemetryOptions(
                        OpenTelemetryOptions.builder()
                                .setEnabled(true)
                                .setProviderType(OpenTelemetryOptions.ProviderType.LOGGING)
                                .build())
                .build();
        return readParquetObjectRecordsWithTelemetry(telemetryOptions);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 10, time = 1)
    @Fork(value = 2, warmups = 1)
    public long loggingTelemetry() throws IOException {
        TelemetryOptions telemetryOptions = TelemetryOptions.builder()
                .setLoggingTelemetryOptions(
                        LoggingTelemetryOptions.builder()
                                .setEnabled(true)
                                .setLogLevel(LoggingTelemetryOptions.LogLevel.INFO)
                                .build())
                .build();
        return readParquetObjectRecordsWithTelemetry(telemetryOptions);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 10, time = 1)
    @Fork(value = 2, warmups = 1)
    public long customTelemetry() throws IOException {
        TelemetryOptions telemetryOptions = TelemetryOptions.builder()
                .setCustomTelemetryOptions(
                        CustomTelemetryOptions.builder()
                                .setOperationListeners(
                                        ImmutableList.of(
                                                new OperationListener() {
                                                    @Override
                                                    public void onOperationStart(Operation operation) {}

                                                    @Override
                                                    public void onOperationEnd(
                                                            Operation operation, Map<MetricKey, Long> metrics) {}
                                                }))
                                .build())
                .build();
        return readParquetObjectRecordsWithTelemetry(telemetryOptions);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 10, time = 1)
    @Fork(value = 2, warmups = 1)
    public long noTelemetry() throws IOException {
        return readParquetObjectRecordsWithTelemetry(TelemetryOptions.builder().build());
    }

    private long readParquetObjectRecordsWithTelemetry(TelemetryOptions telemetryOptions) throws IOException {
        GcsFileSystemOptions gcsFileSystemOptions = GcsFileSystemOptions.builder()
                .setAnalyticsCoreTelemetryOptions(telemetryOptions)
                .build();
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile(IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE);
        return ParquetHelper.readParquetObjectRecords(uri, REQUESTED_SCHEMA, true, gcsFileSystemOptions);
    }

}
