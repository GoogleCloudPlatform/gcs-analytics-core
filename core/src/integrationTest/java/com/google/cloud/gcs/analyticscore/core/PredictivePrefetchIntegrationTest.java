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

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@EnabledIfSystemProperty(named = "gcs.integration.test.bucket", matches = ".+")
@EnabledIfSystemProperty(named = "gcs.integration.test.project-id", matches = ".+")
class PredictivePrefetchIntegrationTest {
  private static final String PREFETCH_ENABLED_KEY = "gcs.analytics-core.prefetch.enabled";
  private static final String PREFETCH_DISABLED = "false";
  private static final String PREFETCH_ENABLED = "true";

  private static final String NON_PARQUET_OBJECT_NAME = "predictive_prefetch_payload.txt";
  private static final String NON_PARQUET_CONTENT =
      "predictive prefetch integration test payload\n".repeat(512);

  private static final String PROJECTED_SCHEMA =
      "message requested_schema {\n"
          + "required binary c_customer_id (STRING);\n"
          + "optional binary c_first_name (STRING);\n"
          + "optional binary c_email_address (STRING);\n"
          + "}";

  private static final int READ_BUFFER_BYTES = 65536;
  private static final int SAMPLED_RANGE_BYTES = 65536;
  private static final int SAMPLED_RANGE_COUNT = 8;

  @BeforeAll
  public static void uploadTestObjectsToGcs() throws IOException {
    IntegrationTestHelper.uploadSampleParquetFilesIfNotExists();
    IntegrationTestHelper.uploadFileToGcs(
        new ByteArrayInputStream(NON_PARQUET_CONTENT.getBytes(UTF_8)), NON_PARQUET_OBJECT_NAME);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE,
        IntegrationTestHelper.TPCDS_CUSTOMER_MEDIUM_FILE
      })
  void readWholeObject_prefetchEnabled_returnsSameBytesAsPrefetchDisabled(String fileName)
      throws IOException {
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    byte[] expectedBytes = readWholeObject(uri, createOptions(PREFETCH_DISABLED));

    byte[] actualBytes = readWholeObject(uri, createOptions(PREFETCH_ENABLED));

    assertThat(actualBytes).isEqualTo(expectedBytes);
  }

  @Test
  void readSampledRanges_prefetchEnabled_returnsSameBytesAsPrefetchDisabled() throws IOException {
    URI uri =
        IntegrationTestHelper.getGcsObjectUriForFile(
            IntegrationTestHelper.TPCDS_CUSTOMER_LARGE_FILE);
    byte[] expectedBytes = readSampledRanges(uri, createOptions(PREFETCH_DISABLED));

    byte[] actualBytes = readSampledRanges(uri, createOptions(PREFETCH_ENABLED));

    assertThat(actualBytes).isEqualTo(expectedBytes);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void readProjectedRecords_prefetchEnabled_returnsSameRecordCountAsPrefetchDisabled(
      boolean readVectoredEnabled) {
    URI uri =
        IntegrationTestHelper.getGcsObjectUriForFile(
            IntegrationTestHelper.TPCDS_CUSTOMER_MEDIUM_FILE);
    long expectedRecordCount =
        ParquetHelper.readParquetObjectRecords(
            uri, PROJECTED_SCHEMA, readVectoredEnabled, createOptions(PREFETCH_DISABLED));

    long actualRecordCount =
        ParquetHelper.readParquetObjectRecords(
            uri, PROJECTED_SCHEMA, readVectoredEnabled, createOptions(PREFETCH_ENABLED));

    assertThat(actualRecordCount).isEqualTo(expectedRecordCount);
  }

  @Test
  void readNonParquetObject_prefetchEnabled_returnsUploadedContent() throws IOException {
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(NON_PARQUET_OBJECT_NAME);

    byte[] actualBytes = readWholeObject(uri, createOptions(PREFETCH_ENABLED));

    assertThat(new String(actualBytes, UTF_8)).isEqualTo(NON_PARQUET_CONTENT);
  }

  private static GcsFileSystemOptions createOptions(String prefetchEnabled) {
    return GcsFileSystemOptions.createFromOptions(
        Map.of(PREFETCH_ENABLED_KEY, prefetchEnabled), "gcs.");
  }

  private static byte[] readWholeObject(URI uri, GcsFileSystemOptions gcsFileSystemOptions)
      throws IOException {
    try (GcsFileSystem gcsFileSystem = new GcsFileSystemImpl(gcsFileSystemOptions);
        GoogleCloudStorageInputStream inputStream =
            GoogleCloudStorageInputStream.create(gcsFileSystem, uri)) {
      ByteArrayOutputStream collectedBytes = new ByteArrayOutputStream();
      byte[] buffer = new byte[READ_BUFFER_BYTES];
      int bytesRead = inputStream.read(buffer, 0, buffer.length);
      while (bytesRead != -1) {
        collectedBytes.write(buffer, 0, bytesRead);
        bytesRead = inputStream.read(buffer, 0, buffer.length);
      }
      return collectedBytes.toByteArray();
    }
  }

  private static byte[] readSampledRanges(URI uri, GcsFileSystemOptions gcsFileSystemOptions)
      throws IOException {
    try (GcsFileSystem gcsFileSystem = new GcsFileSystemImpl(gcsFileSystemOptions);
        GoogleCloudStorageInputStream inputStream =
            GoogleCloudStorageInputStream.create(gcsFileSystem, uri)) {
      long fileSize = gcsFileSystem.getFileInfo(uri).getItemInfo().getSize();
      long stride = fileSize / SAMPLED_RANGE_COUNT;
      ByteArrayOutputStream collectedBytes = new ByteArrayOutputStream();
      for (int sample = 0; sample < SAMPLED_RANGE_COUNT; sample++) {
        long offset = Math.min(sample * stride, fileSize - SAMPLED_RANGE_BYTES);
        collectedBytes.write(readRange(inputStream, offset, SAMPLED_RANGE_BYTES));
      }
      return collectedBytes.toByteArray();
    }
  }

  private static byte[] readRange(
      GoogleCloudStorageInputStream inputStream, long offset, int length) throws IOException {
    inputStream.seek(offset);
    byte[] range = new byte[length];
    int totalBytesRead = 0;
    int bytesRead = 0;
    while (totalBytesRead < length && bytesRead != -1) {
      bytesRead = inputStream.read(range, totalBytesRead, length - totalBytesRead);
      totalBytesRead += Math.max(bytesRead, 0);
    }
    return range;
  }
}
