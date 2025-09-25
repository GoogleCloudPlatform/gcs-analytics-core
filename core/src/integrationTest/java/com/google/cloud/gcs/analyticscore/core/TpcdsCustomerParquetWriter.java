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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;


/**
 * Creates a sample Parquet file for the TPC-DS Customer table using a
 * single-threaded generator.
 *
 * The Avro schema is hardcoded in this class.
 */
public class TpcdsCustomerParquetWriter {

    private static final Logger logger = LoggerFactory.getLogger(TpcdsCustomerParquetWriter.class);

    // Hardcoded Avro schema string
    private static final String AVRO_SCHEMA_STRING = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"Customer\",\n" +
            "  \"namespace\": \"com.example.tpcds\",\n" +
            "  \"doc\": \"TPC-DS Customer Table Schema\",\n" +
            "  \"fields\": [\n" +
            "    { \"name\": \"c_customer_sk\", \"type\": \"long\" },\n" +
            "    { \"name\": \"c_customer_id\", \"type\": \"string\" },\n" +
            "    { \"name\": \"c_current_cdemo_sk\", \"type\": [\"null\", \"long\"], \"default\": null },\n" +
            "    { \"name\": \"c_current_hdemo_sk\", \"type\": [\"null\", \"long\"], \"default\": null },\n" +
            "    { \"name\": \"c_current_addr_sk\", \"type\": [\"null\", \"long\"], \"default\": null },\n" +
            "    { \"name\": \"c_first_shipto_date_sk\", \"type\": [\"null\", \"long\"], \"default\": null },\n" +
            "    { \"name\": \"c_first_sales_date_sk\", \"type\": [\"null\", \"long\"], \"default\": null },\n" +
            "    { \"name\": \"c_salutation\", \"type\": [\"null\", \"string\"], \"default\": null },\n" +
            "    { \"name\": \"c_first_name\", \"type\": [\"null\", \"string\"], \"default\": null },\n" +
            "    { \"name\": \"c_last_name\", \"type\": [\"null\", \"string\"], \"default\": null },\n" +
            "    { \"name\": \"c_preferred_cust_flag\", \"type\": [\"null\", \"string\"], \"default\": null },\n" +
            "    { \"name\": \"c_birth_day\", \"type\": [\"null\", \"int\"], \"default\": null },\n" +
            "    { \"name\": \"c_birth_month\", \"type\": [\"null\", \"int\"], \"default\": null },\n" +
            "    { \"name\": \"c_birth_year\", \"type\": [\"null\", \"int\"], \"default\": null },\n" +
            "    { \"name\": \"c_birth_country\", \"type\": [\"null\", \"string\"], \"default\": null },\n" +
            "    { \"name\": \"c_login\", \"type\": [\"null\", \"string\"], \"default\": null },\n" +
            "    { \"name\": \"c_email_address\", \"type\": [\"null\", \"string\"], \"default\": null },\n" +
            "    { \"name\": \"c_last_review_date_sk\", \"type\": [\"null\", \"long\"], \"default\": null }\n" +
            "  ]\n" +
            "}";

    // Static data for generation
    private static final List<String> SALUTATIONS = Arrays.asList("Mr.", "Mrs.", "Ms.", "Dr.", "Sir");
    private static final List<String> COUNTRIES = Arrays.asList("USA", "Canada", "Mexico", "Germany",
            "France", "UK", "Japan", "China", "India", "Brazil");
    private static final List<String> PREFERRED_FLAGS = Arrays.asList("Y", "N");

    /**
     * Public method to create the sample Parquet file.
     *
     * @param numRows  The total number of customer rows to generate.
     * @param fileName The name of the output Parquet file (e.g., "customer.parquet").
     * This file will be created in the system's temporary directory.
     * @return The generated File object.
     * @throws IOException If an I/O error occurs during writing.
     */
    public File createSampleParquetFile(long numRows, String fileName) throws IOException {
        // Get system temp directory
        String tempDir = System.getProperty("java.io.tmpdir");
        // Create file object in temp directory
        File file = new File(tempDir, fileName);

        Path path = new Path(file.toURI());
        logger.info("Starting Parquet file generation for {} rows at {}", numRows, file.getAbsolutePath());

        Schema localSchema = new Schema.Parser().parse(AVRO_SCHEMA_STRING);
        Random rand = new Random();
        long rowsWritten = 0;

        Configuration conf = new Configuration();

        // Set Parquet properties
        AvroParquetWriter.Builder<GenericRecord> builder = AvroParquetWriter.<GenericRecord>builder(path)
                .withSchema(localSchema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withDictionaryEncoding(true);

        try (ParquetWriter<GenericRecord> writer = builder.build()) {
            for (long i = 0; i < numRows; i++) {
                GenericRecord record = generateCustomerRecord(i, localSchema, rand);
                writer.write(record);
                rowsWritten++;
                if (rowsWritten % 100_000 == 0) {
                    logger.info("... rows written: {} / {}", rowsWritten, numRows);
                }
            }
        } catch (IOException e) {
            logger.error("Parquet writer I/O error", e);
            throw e;
        }
        logger.info("Writer finished. Total rows written: {}", rowsWritten);
        return file;
    }

    /**
     * Generates a single TPC-DS Customer record.
     * This is a simplified data generator.
     *
     * @param customerSk The primary key for this customer.
     * @param schema     The Avro schema.
     * @param rand       A Random instance.
     * @return A GenericRecord matching the customer schema.
     */
    private static GenericRecord generateCustomerRecord(long customerSk, Schema schema, Random rand) {
        GenericRecord record = new GenericData.Record(schema);

        // Required fields
        record.put("c_customer_sk", customerSk);
        record.put("c_customer_id", "CUSTOMER_" + customerSk);

        // Optional fields - we'll randomly make some null
        record.put("c_current_cdemo_sk", shouldSetNonNullValue(rand) ? customerSk % 1000 : null);
        record.put("c_current_hdemo_sk", shouldSetNonNullValue(rand) ? customerSk % 2000 : null);
        record.put("c_current_addr_sk", shouldSetNonNullValue(rand) ? customerSk % 5000 : null);
        record.put("c_first_shipto_date_sk", shouldSetNonNullValue(rand) ? 2450000L + (customerSk % 10000) : null);
        record.put("c_first_sales_date_sk", shouldSetNonNullValue(rand) ? 2450000L + (customerSk % 10000) : null);

        record.put("c_salutation", shouldSetNonNullValue(rand) ? SALUTATIONS.get(rand.nextInt(SALUTATIONS.size())) : null);
        record.put("c_first_name", shouldSetNonNullValue(rand) ? "FirstName_" + (customerSk % 500) : null);
        record.put("c_last_name", shouldSetNonNullValue(rand) ? "LastName_" + (customerSk % 1000) : null);
        record.put("c_preferred_cust_flag", shouldSetNonNullValue(rand) ? PREFERRED_FLAGS.get(rand.nextInt(PREFERRED_FLAGS.size())) : null);

        record.put("c_birth_day", shouldSetNonNullValue(rand) ? 1 + rand.nextInt(28) : null);
        record.put("c_birth_month", shouldSetNonNullValue(rand) ? 1 + rand.nextInt(12) : null);
        record.put("c_birth_year", shouldSetNonNullValue(rand) ? 1920 + rand.nextInt(80) : null);
        record.put("c_birth_country", shouldSetNonNullValue(rand) ? COUNTRIES.get(rand.nextInt(COUNTRIES.size())) : null);

        record.put("c_login", shouldSetNonNullValue(rand) ? "user_" + customerSk : null);
        record.put("c_email_address", shouldSetNonNullValue(rand) ? "customer_" + customerSk + "@example.com" : null);
        record.put("c_last_review_date_sk", shouldSetNonNullValue(rand) ? 2452000L + (customerSk % 5000) : null);

        return record;
    }

    private static boolean shouldSetNonNullValue(Random rand) {
        return rand.nextInt(10) != 0;
    }
}
