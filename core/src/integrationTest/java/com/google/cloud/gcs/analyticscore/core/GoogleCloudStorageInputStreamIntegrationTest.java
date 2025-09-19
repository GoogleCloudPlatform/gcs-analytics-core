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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@EnabledIfSystemProperty(named = "gcs.integration.test.bucket", matches = ".+")
@EnabledIfSystemProperty(named = "gcs.integration.test.project-id", matches = ".+")
class GoogleCloudStorageInputStreamIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(GoogleCloudStorageInputStreamIntegrationTest.class);
  private static final File TPCDS_CUSTOMER_SF1 = IntegrationTestHelper.getFileFromResources(
          "/sampleParquetFiles/tpcds_customer_sf1.parquet");
  private static final File TPCDS_CUSTOMER_SF10 = IntegrationTestHelper.getFileFromResources(
          "/sampleParquetFiles/tpcds_customer_sf10.parquet");
  private static final File TPCDS_CUSTOMER_SF100 = IntegrationTestHelper.getFileFromResources(
          "/sampleParquetFiles/tpcds_customer_sf100.parquet");
  private static final File TPCH_CUSTOMER_SF10 = IntegrationTestHelper.getFileFromResources(
          "/sampleParquetFiles/tpch_customer_sf10.parquet");

  @BeforeAll
  public static void uploadSampleParquetFilesToGcs() throws IOException {
    IntegrationTestHelper.uploadFileToGcs(TPCDS_CUSTOMER_SF1);
    IntegrationTestHelper.uploadFileToGcs(TPCDS_CUSTOMER_SF10);
    IntegrationTestHelper.uploadFileToGcs(TPCDS_CUSTOMER_SF100);
    IntegrationTestHelper.uploadFileToGcs(TPCH_CUSTOMER_SF10);
  }

  @AfterAll
  public static void deleteUploadedFileFromGcs() throws IOException {
    IntegrationTestHelper.deleteUploadedFilesFromGcs();
  }

  @Test
  void forSampleParquetFiles_vectoredIOEnabled_readsFileSuccessfully() throws IOException {
    URI uri1 = IntegrationTestHelper.getGcsObjectUriForFile(TPCDS_CUSTOMER_SF1.getName());
    URI uri2 = IntegrationTestHelper.getGcsObjectUriForFile(TPCDS_CUSTOMER_SF10.getName());
    URI uri3 = IntegrationTestHelper.getGcsObjectUriForFile(TPCDS_CUSTOMER_SF100.getName());
    URI uri5 = IntegrationTestHelper.getGcsObjectUriForFile(TPCH_CUSTOMER_SF10.getName());

    ParquetHelper.readParquetObjectRecords(true, uri1);
    ParquetHelper.readParquetObjectRecords(true, uri2);
    ParquetHelper.readParquetObjectRecords(true, uri3);
    ParquetHelper.readParquetObjectRecords(true, uri5);
  }

  @Test
  void forSampleParquetFiles_vectoredIODisabled_readsFileSuccessfully() throws IOException {
    URI uri1 = IntegrationTestHelper.getGcsObjectUriForFile(TPCDS_CUSTOMER_SF1.getName());
    URI uri2 = IntegrationTestHelper.getGcsObjectUriForFile(TPCDS_CUSTOMER_SF10.getName());
    URI uri3 = IntegrationTestHelper.getGcsObjectUriForFile(TPCDS_CUSTOMER_SF100.getName());
    URI uri5 = IntegrationTestHelper.getGcsObjectUriForFile(TPCH_CUSTOMER_SF10.getName());

    ParquetHelper.readParquetObjectRecords(false, uri1);
    ParquetHelper.readParquetObjectRecords(false, uri2);
    ParquetHelper.readParquetObjectRecords(false, uri3);
    ParquetHelper.readParquetObjectRecords(false, uri5);
  }

  @ParameterizedTest
  @ValueSource(
          strings = {"tpcds_customer_sf1.parquet",
                  "tpcds_customer_sf10.parquet",
                  "tpcds_customer_sf100.parquet"})
  void tpcdsCustomerTableData_footerPrefetchingEnabled_parsesParquetSchemaCorrectly(String fileName) throws IOException {
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);

    ParquetMetadata metadata = ParquetHelper.readParquetMetadata(uri, true);

    List<ColumnDescriptor> columnDescriptorsList = metadata.getFileMetaData().getSchema().getColumns();
    for(ColumnDescriptor descriptor : ParquetHelper.TPCDS_CUSTOMER_TABLE_COLUMNS) {
      assertTrue(columnDescriptorsList.contains(descriptor));
    }
    assertTrue(columnDescriptorsList.size() == ParquetHelper.TPCDS_CUSTOMER_TABLE_COLUMNS.size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"tpch_customer_sf10.parquet"})
  void tpchCustomerTableData_footerPrefetchingEnabled_parsesParquetSchemaCorrectly(String fileName) throws IOException {
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);

    ParquetMetadata metadata = ParquetHelper.readParquetMetadata(uri, true);

    List<ColumnDescriptor> columnDescriptorsList = metadata.getFileMetaData().getSchema().getColumns();
    for(ColumnDescriptor descriptor : ParquetHelper.TPCH_CUSTOMER_TABLE_COLUMNS) {
      assertTrue(columnDescriptorsList.contains(descriptor));
    }
    assertTrue(columnDescriptorsList.size() == ParquetHelper.TPCH_CUSTOMER_TABLE_COLUMNS.size());
  }

  @ParameterizedTest
  @ValueSource(
          strings = {"tpcds_customer_sf1.parquet",
                  "tpcds_customer_sf10.parquet",
                  "tpcds_customer_sf100.parquet"})
  void parseParquetSchema_performsBetterWithFooterPrefetchingEnabled(String fileName) throws IOException {
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);

    long executionTimeWithoutPrefetching = IntegrationTestHelper.measureExecutionTime(() -> {
      try {
        ParquetHelper.readParquetMetadata(uri, false);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    long executionTimeWithPrefetching = IntegrationTestHelper.measureExecutionTime(() -> {
      try {
        ParquetHelper.readParquetMetadata(uri, true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    logger.warn("Execution times (with/without prefetching): {}ms / {}ms",
            executionTimeWithPrefetching, executionTimeWithoutPrefetching);
  }
}
