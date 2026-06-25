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
package com.google.cloud.gcs.analyticscore.common.telemetry;

import com.google.cloud.opentelemetry.metric.GoogleCloudMetricExporter;
import com.google.cloud.opentelemetry.metric.MetricConfiguration;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.contrib.gcp.resource.GCPResourceProvider;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A provider that programmatically builds an OpenTelemetry SDK explicitly configured with a
 * GoogleCloudMetricExporter.
 */
public class CloudMonitoringOpenTelemetryProvider implements OpenTelemetryProvider {
  private static final Logger LOG =
      LoggerFactory.getLogger(CloudMonitoringOpenTelemetryProvider.class);

  private static final String JVM_INSTANCE_ID = UUID.randomUUID().toString();
  private static final Object LOCK = new Object();

  private static int activeCount = 0;
  private static volatile OpenTelemetrySdk openTelemetrySdk = null;

  private final Duration exportInterval;
  private final Optional<String> projectId;
  private volatile boolean closed = false;

  public CloudMonitoringOpenTelemetryProvider(OpenTelemetryOptions openTelemetryOptions) {
    this.exportInterval = Duration.ofSeconds(openTelemetryOptions.getExportIntervalSeconds());
    this.projectId = openTelemetryOptions.getProjectId();
    synchronized (LOCK) {
      activeCount++;
    }
  }

  @Override
  public OpenTelemetry getOpenTelemetry() {
    if (openTelemetrySdk == null) {
      synchronized (LOCK) {
        if (openTelemetrySdk == null) {
          openTelemetrySdk =
              OpenTelemetrySdk.builder()
                  .setMeterProvider(
                      getMeterProviderWithCloudMonitoringExporter(exportInterval, projectId))
                  .build();
        }
      }
    }
    return openTelemetrySdk;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    synchronized (LOCK) {
      if (closed) {
        return;
      }
      closed = true;
      activeCount--;

      if (openTelemetrySdk != null) {
        flushSdk(openTelemetrySdk, activeCount == 0);
        if (activeCount == 0) {
          openTelemetrySdk.close();
          openTelemetrySdk = null;
        }
      }
    }
  }

  private static void flushSdk(OpenTelemetrySdk sdk, boolean waitForCompletion) {
    try {
      CompletableResultCode flushResult = sdk.getSdkMeterProvider().forceFlush();
      if (!waitForCompletion) {
        return;
      }
      if (!flushResult.join(10, TimeUnit.SECONDS).isSuccess()) {
        LOG.warn("OpenTelemetry SDK forceFlush timed out or failed before completion");
      }
    } catch (Exception e) {
      LOG.warn("Exception encountered during OpenTelemetry SDK forceFlush", e);
    }
  }

  private SdkMeterProvider getMeterProviderWithCloudMonitoringExporter(
      Duration exportInterval, Optional<String> projectId) {
    Resource resource = Resource.getDefault().merge(getGcpResource()).merge(getInstanceResource());
    SdkMeterProviderBuilder meterProviderBuilder = SdkMeterProvider.builder().setResource(resource);
    MetricExporter cloudExporter =
        GoogleCloudMetricExporter.createWithConfiguration(getMetricConfiguration(projectId));
    PeriodicMetricReader metricReader =
        PeriodicMetricReader.builder(cloudExporter).setInterval(exportInterval).build();

    return meterProviderBuilder.registerMetricReader(metricReader).build();
  }

  private Resource getInstanceResource() {
    return Resource.builder()
        .put(AttributeKey.stringKey("service.instance.id"), JVM_INSTANCE_ID)
        .build();
  }

  private Resource getGcpResource() {
    Resource gcpResource = Resource.empty();
    try {
      gcpResource = new GCPResourceProvider().createResource(null);
    } catch (Exception e) {
      LOG.warn("Failed to detect GCP platform attributes via GCPResourceProvider", e);
    }
    return gcpResource;
  }

  private MetricConfiguration getMetricConfiguration(Optional<String> projectId) {
    MetricConfiguration.Builder configBuilder = MetricConfiguration.builder();
    projectId.ifPresent(configBuilder::setProjectId);
    return configBuilder.build();
  }
}
