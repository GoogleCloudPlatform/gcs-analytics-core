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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@EnabledIfSystemProperty(named = "gcs.integration.test.bucket", matches = ".+")
@EnabledIfSystemProperty(named = "gcs.integration.test.project-id", matches = ".+")
// TODO - Add generator function on place of bundling sample parquet files in resources.
class GoogleCloudStorageInputStreamIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(GoogleCloudStorageInputStreamIntegrationTest.class);
  private static final File TPCDS_CUSTOMER_SMALL = IntegrationTestHelper.getFileFromResources(
          "/sampleParquetFiles/tpcds_customer_small.parquet");
  private static final File TPCDS_CUSTOMER_MEDIUM = IntegrationTestHelper.getFileFromResources(
          "/sampleParquetFiles/tpcds_customer_medium.parquet");
  private static final File TPCDS_CUSTOMER_LARGE = IntegrationTestHelper.getFileFromResources(
          "/sampleParquetFiles/tpcds_customer_large.parquet");
  private static final File TPCH_CUSTOMER_MEDIUM = IntegrationTestHelper.getFileFromResources(
          "/sampleParquetFiles/tpch_customer_medium.parquet");

  @BeforeAll
  public static void uploadSampleParquetFilesToGcs() throws IOException {
    // TODO - Skip uploading if file already present.
    IntegrationTestHelper.uploadFileToGcs(TPCDS_CUSTOMER_SMALL);
    IntegrationTestHelper.uploadFileToGcs(TPCDS_CUSTOMER_MEDIUM);
    IntegrationTestHelper.uploadFileToGcs(TPCDS_CUSTOMER_LARGE);
    IntegrationTestHelper.uploadFileToGcs(TPCH_CUSTOMER_MEDIUM);
  }

  @ParameterizedTest
  @ValueSource(
          strings = {"tpcds_customer_small.parquet",
                  "tpcds_customer_medium.parquet",
                  "tpcds_customer_large.parquet",
                  "tpch_customer_medium.parquet"})
  void forSampleParquetFiles_vectoredIOEnabled_readsFileSuccessfully(String fileName) throws IOException {
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    ParquetHelper.readParquetObjectRecords(uri, true, true);
  }

  @ParameterizedTest
  @ValueSource(
          strings = {"tpcds_customer_small.parquet",
                  "tpcds_customer_medium.parquet",
                  "tpcds_customer_large.parquet",
                  "tpch_customer_medium.parquet"})
  void forSampleParquetFiles_vectoredIODisabled_readsFileSuccessfully(String fileName) throws IOException {
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    ParquetHelper.readParquetObjectRecords(uri, false, false);
  }

  @ParameterizedTest
  @ValueSource(
          strings = {"tpcds_customer_small.parquet",
                  "tpcds_customer_medium.parquet",
                  "tpcds_customer_large.parquet"})
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
  @ValueSource(strings = {"tpch_customer_medium.parquet"})
  void tpchCustomerTableData_footerPrefetchingEnabled_parsesParquetSchemaCorrectly(String fileName) throws IOException {
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);

    ParquetMetadata metadata = ParquetHelper.readParquetMetadata(uri, true);

    List<ColumnDescriptor> columnDescriptorsList = metadata.getFileMetaData().getSchema().getColumns();
    for(ColumnDescriptor descriptor : ParquetHelper.TPCH_CUSTOMER_TABLE_COLUMNS) {
      assertTrue(columnDescriptorsList.contains(descriptor));
    }
    assertTrue(columnDescriptorsList.size() == ParquetHelper.TPCH_CUSTOMER_TABLE_COLUMNS.size());
  }
}
