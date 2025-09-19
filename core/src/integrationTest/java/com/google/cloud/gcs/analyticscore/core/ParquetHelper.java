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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public class ParquetHelper {

    private static final Logger logger = LoggerFactory.getLogger(ParquetHelper.class);

    public static final ImmutableList<ColumnDescriptor> TPCDS_CUSTOMER_TABLE_COLUMNS = ImmutableList.of(
            new ColumnDescriptor(new String[] {"c_customer_sk"}, new PrimitiveType(Type.Repetition.OPTIONAL, INT64, "c_customer_sk"), 0, 1),
            new ColumnDescriptor(new String[] {"c_customer_id"}, new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "c_customer_id"), 0, 1),
            new ColumnDescriptor(new String[] {"c_current_cdemo_sk"}, new PrimitiveType(Type.Repetition.OPTIONAL, INT64, "c_current_cdemo_sk"), 0, 1),
            new ColumnDescriptor(new String[] {"c_current_hdemo_sk"}, new PrimitiveType(Type.Repetition.OPTIONAL, INT64, "c_current_hdemo_sk"), 0, 1),
            new ColumnDescriptor(new String[] {"c_current_addr_sk"}, new PrimitiveType(Type.Repetition.OPTIONAL, INT64, "c_current_addr_sk"), 0, 1),
            new ColumnDescriptor(new String[] {"c_first_shipto_date_sk"}, new PrimitiveType(Type.Repetition.OPTIONAL, INT64, "c_first_shipto_date_sk"), 0, 1),
            new ColumnDescriptor(new String[] {"c_first_sales_date_sk"}, new PrimitiveType(Type.Repetition.OPTIONAL, INT64, "c_first_sales_date_sk"), 0, 1),
            new ColumnDescriptor(new String[] {"c_salutation"}, new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "c_salutation"), 0, 1),
            new ColumnDescriptor(new String[] {"c_first_name"}, new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "c_first_name"), 0, 1),
            new ColumnDescriptor(new String[] {"c_last_name"}, new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "c_last_name"), 0, 1),
            new ColumnDescriptor(new String[] {"c_preferred_cust_flag"}, new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "c_preferred_cust_flag"), 0, 1),
            new ColumnDescriptor(new String[] {"c_birth_day"}, new PrimitiveType(Type.Repetition.OPTIONAL, INT32, "c_birth_day"), 0, 1),
            new ColumnDescriptor(new String[] {"c_birth_month"}, new PrimitiveType(Type.Repetition.OPTIONAL, INT32, "c_birth_month"), 0, 1),
            new ColumnDescriptor(new String[] {"c_birth_year"}, new PrimitiveType(Type.Repetition.OPTIONAL, INT32, "c_birth_year"), 0, 1),
            new ColumnDescriptor(new String[] {"c_birth_country"}, new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "c_birth_country"), 0, 1),
            new ColumnDescriptor(new String[] {"c_login"}, new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "c_login"), 0, 1),
            new ColumnDescriptor(new String[] {"c_email_address"}, new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "c_email_address"), 0, 1),
            new ColumnDescriptor(new String[] {"c_last_review_date_sk"}, new PrimitiveType(Type.Repetition.OPTIONAL, INT64, "c_last_review_date_sk"), 0, 1)
    );

    public static final ImmutableList<ColumnDescriptor> TPCH_CUSTOMER_TABLE_COLUMNS = ImmutableList.of(
            new ColumnDescriptor(new String[] {"custkey"}, new PrimitiveType(Type.Repetition.OPTIONAL, INT64, "custkey"), 0, 1),
            new ColumnDescriptor(new String[] {"name"}, new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "name"), 0, 1),
            new ColumnDescriptor(new String[] {"address"}, new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "address"), 0, 1),
            new ColumnDescriptor(new String[] {"nationkey"}, new PrimitiveType(Type.Repetition.OPTIONAL, INT64, "nationkey"), 0, 1),
            new ColumnDescriptor(new String[] {"phone"}, new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "phone"), 0, 1),
            new ColumnDescriptor(new String[] {"acctbal"}, new PrimitiveType(Type.Repetition.OPTIONAL, DOUBLE, "acctbal"), 0, 1),
            new ColumnDescriptor(new String[] {"mktsegment"}, new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "mktsegment"), 0, 1),
            new ColumnDescriptor(new String[] {"comment"}, new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "comment"), 0, 1)
    );

    /**
     * Reads the metadata from a Parquet file.
     *
     * @param fileUri The URI of the Parquet file.
     * @return The ParquetMetadata object.
     * @throws IOException if an I/O error occurs while reading the file.
     */
    public static ParquetMetadata readParquetMetadata(URI fileUri, boolean enableFooterPrefetch) throws IOException {
        logger.info("Reading parquet file metadata: {} with enableFooterPrefetch: {}", fileUri, enableFooterPrefetch);
        InputFile inputFile = new TestInputStreamInputFile(fileUri, false, enableFooterPrefetch);
        // Configuration can be customized if needed
        Configuration conf = new Configuration();
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            return reader.getFooter();
        }
    }


    /**
     * Reads the records from a Parquet file and returns the total record count.
     *
     * @param readVectoredEnabled Whether to use vectored read or not.
     * @param fileUri The URI of the Parquet file.
     * @return The total number of records in the file.
     */
    public static long readParquetObjectRecords(boolean readVectoredEnabled, URI fileUri)  {
        logger.info("Reading parquet file:{} with vectoredIOEnabled={}", fileUri, readVectoredEnabled);
        try {
            InputFile inputFile = new TestInputStreamInputFile(fileUri, readVectoredEnabled);
            long recordCount = 0;
            try (ParquetReader<Group> reader = new GroupParquetReaderBuilder(inputFile)
                    .withConf(new Configuration()) // Use default Hadoop config
                    .build()) {
                Group group;
                while ((group = reader.read()) != null) {
                    recordCount += 1;
                }
            }
            return recordCount;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
