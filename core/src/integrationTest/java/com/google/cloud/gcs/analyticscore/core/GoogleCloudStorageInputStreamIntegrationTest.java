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
  void forSampleParquetFiles_vectoredIOEnabled_readsFileSuccessfully(String fileName) {
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    ParquetHelper.readParquetObjectRecords(uri, /* readVectoredEnabled= */ true, /* footerPrefetchSize= */ 0);
  }

  @ParameterizedTest
  @ValueSource(
          strings = {IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE,
                  IntegrationTestHelper.TPCDS_CUSTOMER_MEDIUM_FILE,
                  IntegrationTestHelper.TPCDS_CUSTOMER_LARGE_FILE})
  void forSampleParquetFiles_vectoredIOEnabled_footerPrefetchingEnabled_readsFileSuccessfully(String fileName) {
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    ParquetHelper.readParquetObjectRecords(uri, /* readVectoredEnabled= */ true, /* footerPrefetchSize= */ 102400);
  }

  @ParameterizedTest
  @ValueSource(
          strings = {IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE,
                  IntegrationTestHelper.TPCDS_CUSTOMER_MEDIUM_FILE,
                  IntegrationTestHelper.TPCDS_CUSTOMER_LARGE_FILE})
  void forSampleParquetFiles_vectoredIODisabled_readsFileSuccessfully(String fileName) {
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    ParquetHelper.readParquetObjectRecords(uri, /* readVectoredEnabled= */ false, /* footerPrefetchSize= */ 0);
  }

  @ParameterizedTest
  @ValueSource(
          strings = {IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE,
                  IntegrationTestHelper.TPCDS_CUSTOMER_MEDIUM_FILE,
                  IntegrationTestHelper.TPCDS_CUSTOMER_LARGE_FILE})
  void tpcdsCustomerTableData_footerPrefetchingEnabled_parsesParquetSchemaCorrectly(String fileName) throws IOException {
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);

    ParquetMetadata metadata = ParquetHelper.readParquetMetadata(uri, /* footerPrefetchSize= */ 102400);

    List<ColumnDescriptor> columnDescriptorsList = metadata.getFileMetaData().getSchema().getColumns();
    for(ColumnDescriptor descriptor : ParquetHelper.TPCDS_CUSTOMER_TABLE_COLUMNS) {
      assertTrue(columnDescriptorsList.contains(descriptor));
    }
    assertTrue(columnDescriptorsList.size() == ParquetHelper.TPCDS_CUSTOMER_TABLE_COLUMNS.size());
  }

  @ParameterizedTest
  @ValueSource(
          strings = {IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE,
                  IntegrationTestHelper.TPCDS_CUSTOMER_MEDIUM_FILE,
                  IntegrationTestHelper.TPCDS_CUSTOMER_LARGE_FILE})
  void tpcdsCustomerTableData_footerPrefetchingDisabled_parsesParquetSchemaCorrectly(String fileName) throws IOException {
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);

    ParquetMetadata metadata = ParquetHelper.readParquetMetadata(uri, /* footerPrefetchSize= */ 0);

    List<ColumnDescriptor> columnDescriptorsList = metadata.getFileMetaData().getSchema().getColumns();
    for(ColumnDescriptor descriptor : ParquetHelper.TPCDS_CUSTOMER_TABLE_COLUMNS) {
      assertTrue(columnDescriptorsList.contains(descriptor));
    }
    assertTrue(columnDescriptorsList.size() == ParquetHelper.TPCDS_CUSTOMER_TABLE_COLUMNS.size());
  }
}
