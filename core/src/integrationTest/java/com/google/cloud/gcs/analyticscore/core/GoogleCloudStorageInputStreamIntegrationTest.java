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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.common.telemetry.MetricKey;
import com.google.cloud.gcs.analyticscore.common.telemetry.CustomTelemetryOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.Operation;
import com.google.cloud.gcs.analyticscore.common.telemetry.OperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.gcs.analyticscore.common.telemetry.TelemetryOptions;
import com.google.cloud.storage.BlobId;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@EnabledIfSystemProperty(named = "gcs.integration.test.bucket", matches = ".+")
@EnabledIfSystemProperty(named = "gcs.integration.test.project-id", matches = ".+")
class GoogleCloudStorageInputStreamIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(GoogleCloudStorageInputStreamIntegrationTest.class);

  @BeforeAll
  public static void uploadSampleParquetFilesToGcs() throws IOException {
    IntegrationTestHelper.uploadSampleParquetFilesIfNotExists();
  }

  @ParameterizedTest
  @ValueSource(
          strings = {IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE,
                  IntegrationTestHelper.TPCDS_CUSTOMER_MEDIUM_FILE,
                  IntegrationTestHelper.TPCDS_CUSTOMER_LARGE_FILE})
  void forSampleParquetFiles_vectoredIOEnabled_footerPrefetchingDisabled_readsFileSuccessfully(String fileName) {
    GcsFileSystemOptions gcsFileSystemOptions = GcsFileSystemOptions.createFromOptions(
            Map.of("gcs.analytics-core.footer.prefetch.enabled", "false",
                    "gcs.analytics-core.small-file.cache.threshold-bytes", "1048576"), "gcs.");
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    ParquetHelper.readParquetObjectRecords(uri, /* readVectoredEnabled= */ true, gcsFileSystemOptions);
  }

  @ParameterizedTest
  @ValueSource(
          strings = {IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE,
                  IntegrationTestHelper.TPCDS_CUSTOMER_MEDIUM_FILE,
                  IntegrationTestHelper.TPCDS_CUSTOMER_LARGE_FILE})
  void forSampleParquetFiles_vectoredIOEnabled_footerPrefetchingEnabled_readsFileSuccessfully(String fileName) {
    GcsFileSystemOptions gcsFileSystemOptions = GcsFileSystemOptions.createFromOptions(
            Map.of("gcs.analytics-core.small-file.footer.prefetch.size-bytes", "102400",
                    "gcs.analytics-core.large-file.footer.prefetch.size-bytes", "1048576",
                    "gcs.analytics-core.small-file.cache.threshold-bytes", "1048576"), "gcs.");
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    ParquetHelper.readParquetObjectRecords(uri, /* readVectoredEnabled= */ true, gcsFileSystemOptions);
  }

  @ParameterizedTest
  @ValueSource(
          strings = {IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE,
                  IntegrationTestHelper.TPCDS_CUSTOMER_MEDIUM_FILE,
                  IntegrationTestHelper.TPCDS_CUSTOMER_LARGE_FILE})
  void forSampleParquetFiles_vectoredIODisabled_readsFileSuccessfully(String fileName) {
    GcsFileSystemOptions gcsFileSystemOptions = GcsFileSystemOptions.createFromOptions(
            Map.of("gcs.analytics-core.small-file.footer.prefetch.size-bytes", "102400",
                    "gcs.analytics-core.large-file.footer.prefetch.size-bytes", "1048576",
                    "gcs.analytics-core.small-file.cache.threshold-bytes", "1048576"), "gcs.");
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    ParquetHelper.readParquetObjectRecords(uri, /* readVectoredEnabled= */ false, gcsFileSystemOptions);
  }

  @ParameterizedTest
  @ValueSource(
          strings = {IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE,
                  IntegrationTestHelper.TPCDS_CUSTOMER_MEDIUM_FILE,
                  IntegrationTestHelper.TPCDS_CUSTOMER_LARGE_FILE})
  void tpcdsCustomerTableData_footerPrefetchingEnabled_parsesParquetSchemaCorrectly(String fileName) throws IOException {
    GcsFileSystemOptions gcsFileSystemOptions = GcsFileSystemOptions.createFromOptions(
            Map.of("gcs.analytics-core.small-file.footer.prefetch.size-bytes", "102400",
                    "gcs.analytics-core.large-file.footer.prefetch.size-bytes", "1048576",
                    "gcs.analytics-core.small-file.cache.threshold-bytes", "1048576"), "gcs.");
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);

    ParquetMetadata metadata = ParquetHelper.readParquetMetadata(uri, gcsFileSystemOptions);

    List<ColumnDescriptor> columnDescriptorsList = metadata.getFileMetaData().getSchema().getColumns();
    assertThat(columnDescriptorsList)
        .containsExactlyElementsIn(ParquetHelper.TPCDS_CUSTOMER_TABLE_COLUMNS);
  }

  @ParameterizedTest
  @ValueSource(
          strings = {IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE,
                  IntegrationTestHelper.TPCDS_CUSTOMER_MEDIUM_FILE,
                  IntegrationTestHelper.TPCDS_CUSTOMER_LARGE_FILE})
  void tpcdsCustomerTableData_footerPrefetchingDisabled_parsesParquetSchemaCorrectly(String fileName) throws IOException {
    GcsFileSystemOptions gcsFileSystemOptions = GcsFileSystemOptions.createFromOptions(
            Map.of("gcs.analytics-core.footer.prefetch.enabled", "false",
                    "gcs.analytics-core.small-file.cache.threshold-bytes", "0"), "gcs.");
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);

    ParquetMetadata metadata = ParquetHelper.readParquetMetadata(uri, gcsFileSystemOptions);

    List<ColumnDescriptor> columnDescriptorsList = metadata.getFileMetaData().getSchema().getColumns();
    assertThat(columnDescriptorsList)
        .containsExactlyElementsIn(ParquetHelper.TPCDS_CUSTOMER_TABLE_COLUMNS);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE,
        IntegrationTestHelper.TPCDS_CUSTOMER_MEDIUM_FILE,
        IntegrationTestHelper.TPCDS_CUSTOMER_LARGE_FILE
      })
  void initializeWithGcsItemId_readsFileSuccessfully(String fileName) throws IOException {
    String bucket = System.getProperty("gcs.integration.test.bucket");
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    GcsItemId gcsItemId = GcsItemId.builder().setBucketName(blobId.getBucket()).setObjectName(blobId.getName()).build();
    GcsFileSystemOptions gcsFileSystemOptions =
        GcsFileSystemOptions.createFromOptions(
            Map.of(
                "gcs.analytics-core.small-file.footer.prefetch.size-bytes",
                "102400",
                "gcs.analytics-core.large-file.footer.prefetch.size-bytes",
                "1048576",
                "gcs.analytics-core.small-file.cache.threshold-bytes",
                "1048576"),
            "gcs.");
    try (GcsFileSystem gcsFileSystem = new GcsFileSystemImpl(gcsFileSystemOptions);
         GoogleCloudStorageInputStream googleCloudStorageInputStream =
             GoogleCloudStorageInputStream.create(gcsFileSystem, gcsItemId)) {
      byte[] buffer = new byte[1024];
      int bytesRead = googleCloudStorageInputStream.read(buffer);
      assertTrue(bytesRead > 0);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE,
      })
  void read_capturesTelemetryAttributes_withCorrectReadLength(String fileName) throws IOException {
    AtomicReference<Map<MetricKey, Long>> capturedReadMetrics = new AtomicReference<>();
    AtomicReference<Operation> capturedReadOperation = new AtomicReference<>();
    OperationListener listener =
        new OperationListener() {
          @Override
          public void onOperationStart(Operation operation) {}

          @Override
          public void onOperationEnd(Operation operation, Map<MetricKey, Long> metrics) {
            if (operation.getName().equals("READ")) {
              capturedReadOperation.set(operation);
              capturedReadMetrics.set(metrics);
            }
          }
        };
    List<OperationListener> listeners = new ArrayList<>();
    listeners.add(listener);
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    GcsItemId gcsItemId =
        GcsItemId.builder()
            .setBucketName(blobId.getBucket())
            .setObjectName(blobId.getName())
            .build();
    GcsFileSystemOptions gcsFileSystemOptions =
        GcsFileSystemOptions.createFromOptions(Map.of(), "gcs.");
    gcsFileSystemOptions =
        gcsFileSystemOptions.toBuilder()
            .setAnalyticsCoreTelemetryOptions(
                TelemetryOptions.builder()
                    .setCustomTelemetryOptions(
                        CustomTelemetryOptions.builder()
                            .setOperationListeners(listeners)
                            .build())
                    .build())
            .build();
    try (GcsFileSystem gcsFileSystem = new GcsFileSystemImpl(gcsFileSystemOptions);
        GoogleCloudStorageInputStream googleCloudStorageInputStream =
            GoogleCloudStorageInputStream.create(gcsFileSystem, gcsItemId)) {
      ByteBuffer buffer = ByteBuffer.allocate(10);
      buffer.limit(5);

      googleCloudStorageInputStream.read(buffer);
    }
    
    MetricKey bytesReadKey =
        capturedReadMetrics.get().keySet().stream()
            .filter(k -> k.getMetric().getName().equals("READ_BYTES"))
            .findFirst()
            .get();
    assertThat(capturedReadMetrics.get().get(bytesReadKey)).isEqualTo(5L);
    assertThat(capturedReadOperation.get().getAttributes().get("READ_LENGTH")).isEqualTo("5");
  }
}
