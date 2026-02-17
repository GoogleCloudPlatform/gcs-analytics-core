/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.gcs.analyticscore.common.telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.exporter.logging.LoggingMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import java.time.Duration;

/**
 * A provider that programmatically builds an OpenTelemetry SDK explicitly configured with a
 * LoggingMetricExporter.
 */
public class LoggingOpenTelemetryProvider implements OpenTelemetryProvider {

  private volatile OpenTelemetrySdk sdk;
  private final Duration exportInterval;

  public LoggingOpenTelemetryProvider(OpenTelemetryOptions openTelemetryOptions) {
    this.exportInterval = Duration.ofSeconds(openTelemetryOptions.getExportIntervalSeconds());
  }

  @Override
  public OpenTelemetry getOpenTelemetry() {
    if (sdk == null) {
      synchronized (this) {
        if (sdk == null) {
          sdk = OpenTelemetrySdk.builder().setMeterProvider(getLoggingExporter()).build();
        }
      }
    }
    return sdk;
  }

  @Override
  public void close() {
    if (sdk != null) {
      synchronized (this) {
        if (sdk != null) {
          sdk.close();
          sdk = null;
        }
      }
    }
  }

  private SdkMeterProvider getLoggingExporter() {
    SdkMeterProviderBuilder meterProviderBuilder = SdkMeterProvider.builder();
    LoggingMetricExporter loggingExporter = LoggingMetricExporter.create();
    PeriodicMetricReader loggingReader =
        PeriodicMetricReader.builder(loggingExporter).setInterval(exportInterval).build();
    return meterProviderBuilder.registerMetricReader(loggingReader).build();
  }
}
