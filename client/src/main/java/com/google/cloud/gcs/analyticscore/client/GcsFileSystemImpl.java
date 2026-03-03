/*
 * Copyright 2025 Google LLC
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
package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auth.Credentials;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants;
import com.google.cloud.gcs.analyticscore.common.telemetry.LoggingTelemetryOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.LoggingTelemetryReporter;
import com.google.cloud.gcs.analyticscore.common.telemetry.OpenTelemetryOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.OpenTelemetryReporter;
import com.google.cloud.gcs.analyticscore.common.telemetry.OperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.gcs.analyticscore.common.telemetry.TelemetryOptions;
import com.google.cloud.storage.BlobId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class GcsFileSystemImpl implements GcsFileSystem {

  private final GcsClient gcsClient;
  private final GcsFileSystemOptions fileSystemOptions;
  private final Supplier<ExecutorService> executorServiceSupplier;

  private final Telemetry telemetry;

  public GcsFileSystemImpl(GcsFileSystemOptions fileSystemOptions) {
    this.fileSystemOptions = fileSystemOptions;
    this.executorServiceSupplier = initializeExecutionServiceSupplier();
    this.telemetry = createTelemetry(fileSystemOptions.getAnalyticsCoreTelemetryOptions());
    this.gcsClient =
        telemetry.measure(
            GcsAnalyticsCoreTelemetryConstants.Operation.GCS_CLIENT_CREATE.name(),
            GcsAnalyticsCoreTelemetryConstants.Metric.GCS_CLIENT_CREATE_DURATION.name(),
            Collections.emptyMap(),
            recorder ->
                new GcsClientImpl(
                    fileSystemOptions.getGcsClientOptions(), executorServiceSupplier, telemetry));
  }

  public GcsFileSystemImpl(Credentials credentials, GcsFileSystemOptions fileSystemOptions) {
    this.fileSystemOptions = fileSystemOptions;
    this.executorServiceSupplier = initializeExecutionServiceSupplier();
    this.telemetry = createTelemetry(fileSystemOptions.getAnalyticsCoreTelemetryOptions());
    this.gcsClient =
        telemetry.measure(
            GcsAnalyticsCoreTelemetryConstants.Operation.GCS_CLIENT_CREATE.name(),
            GcsAnalyticsCoreTelemetryConstants.Metric.GCS_CLIENT_CREATE_DURATION.name(),
            Collections.emptyMap(),
            recorder ->
                new GcsClientImpl(
                    credentials,
                    fileSystemOptions.getGcsClientOptions(),
                    executorServiceSupplier,
                    telemetry));
  }

  @VisibleForTesting
  GcsFileSystemImpl(GcsClient gcsClient, GcsFileSystemOptions fileSystemOptions) {
    this(
        gcsClient,
        fileSystemOptions,
        createTelemetry(fileSystemOptions.getAnalyticsCoreTelemetryOptions()));
  }

  @VisibleForTesting
  GcsFileSystemImpl(
      GcsClient gcsClient, GcsFileSystemOptions fileSystemOptions, Telemetry telemetry) {
    this.gcsClient = gcsClient;
    this.fileSystemOptions = fileSystemOptions;
    this.executorServiceSupplier = initializeExecutionServiceSupplier();
    this.telemetry = telemetry;
  }

  @Override
  public VectoredSeekableByteChannel open(GcsFileInfo gcsFileInfo, GcsReadOptions readOptions)
      throws IOException {
    checkNotNull(gcsFileInfo, "fileInfo should not be null");
    GcsItemId itemId = UriUtil.getItemIdFromString(gcsFileInfo.getUri().toString());
    checkArgument(itemId.isGcsObject(), "Expected GCS object to be provided. But got: " + itemId);
    return gcsClient.openReadChannel(gcsFileInfo.getItemInfo(), readOptions);
  }

  @Override
  public VectoredSeekableByteChannel open(GcsItemId gcsItemId, GcsReadOptions readOptions)
      throws IOException {
    checkNotNull(gcsItemId, "gcsItemId should not be null");
    checkArgument(
        gcsItemId.isGcsObject(), "Expected GCS object to be provided. But got: " + gcsItemId);
    return gcsClient.openReadChannel(gcsItemId, readOptions);
  }

  @Override
  public GcsFileInfo getFileInfo(URI path) throws IOException {
    checkNotNull(path, "path should not be null");
    GcsItemId itemId = UriUtil.getItemIdFromString(path.toString());
    return getFileInfo(itemId);
  }

  @Override
  public GcsFileInfo getFileInfo(GcsItemId itemId) throws IOException {
    GcsItemInfo gcsItemInfo = gcsClient.getGcsItemInfo(itemId);
    return GcsFileInfo.builder()
        .setItemInfo(gcsItemInfo)
        .setUri(
            URI.create(
                BlobId.of(itemId.getBucketName(), itemId.getObjectName().get()).toGsUtilUri()))
        .setAttributes(Collections.emptyMap())
        .build();
  }

  @Override
  public GcsFileSystemOptions getFileSystemOptions() {
    return this.fileSystemOptions;
  }

  @Override
  public GcsClient getGcsClient() {
    return this.gcsClient;
  }

  @Override
  public Telemetry getTelemetry() {
    return telemetry;
  }

  @Override
  public void close() {
    ExecutorService executorService = executorServiceSupplier.get();
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
    gcsClient.close();
    telemetry.close();
  }

  @VisibleForTesting
  static Telemetry createTelemetry(TelemetryOptions telemetryOptions) {
    ImmutableList.Builder<OperationListener> listeners = ImmutableList.builder();
    telemetryOptions
        .getLoggingTelemetryOptions()
        .filter(LoggingTelemetryOptions::isEnabled)
        .ifPresent(options -> listeners.add(new LoggingTelemetryReporter(options)));
    telemetryOptions
        .getOpenTelemetryOptions()
        .filter(OpenTelemetryOptions::isEnabled)
        .ifPresent(options -> listeners.add(new OpenTelemetryReporter(options)));
    telemetryOptions
        .getCustomTelemetryOptions()
        .ifPresent(options -> listeners.addAll(options.getOperationListeners()));
    return new Telemetry(listeners.build());
  }

  @VisibleForTesting
  Supplier<ExecutorService> initializeExecutionServiceSupplier() {
    return Suppliers.memoize(
        () ->
            new ThreadPoolExecutor(
                fileSystemOptions.getReadThreadCount(),
                fileSystemOptions.getReadThreadCount(),
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryBuilder()
                    .setNameFormat("gcs-filesystem-range-pool-%d")
                    .setDaemon(true)
                    .build()));
  }
}
