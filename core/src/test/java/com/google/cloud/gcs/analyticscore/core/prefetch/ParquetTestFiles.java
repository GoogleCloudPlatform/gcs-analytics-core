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

package com.google.cloud.gcs.analyticscore.core.prefetch;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

/** Writes real Parquet files so parsing can be verified against the reference implementation. */
final class ParquetTestFiles {

  static final String ID_COLUMN = "id";
  static final String CATEGORY_COLUMN = "category";
  static final String VALUE_COLUMN = "value";
  static final long SINGLE_ROW_GROUP_SIZE_BYTES = 128L * 1024L;
  static final long TINY_ROW_GROUP_SIZE_BYTES = 1024L;

  private static final int DISTINCT_CATEGORY_COUNT = 4;
  private static final Schema RECORD_SCHEMA =
      new Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":["
                  + "{\"name\":\"id\",\"type\":\"long\"},"
                  + "{\"name\":\"category\",\"type\":\"string\"},"
                  + "{\"name\":\"value\",\"type\":\"double\"}]}");
  private static final Schema ALTERNATE_SCHEMA =
      new Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"OtherRecord\",\"fields\":["
                  + "{\"name\":\"key\",\"type\":\"string\"}]}");

  private ParquetTestFiles() {}

  static File createFile(File directory, String fileName, int recordCount, long rowGroupSizeBytes)
      throws IOException {
    File target = new File(directory, fileName);
    try (ParquetWriter<GenericRecord> writer =
        createWriter(target, RECORD_SCHEMA, rowGroupSizeBytes)) {
      for (int index = 0; index < recordCount; index++) {
        writer.write(createRecord(index));
      }
    }
    return target;
  }

  static File createFileWithAlternateSchema(File directory, String fileName, int recordCount)
      throws IOException {
    File target = new File(directory, fileName);
    try (ParquetWriter<GenericRecord> writer =
        createWriter(target, ALTERNATE_SCHEMA, SINGLE_ROW_GROUP_SIZE_BYTES)) {
      for (int index = 0; index < recordCount; index++) {
        GenericRecord record = new GenericData.Record(ALTERNATE_SCHEMA);
        record.put("key", "key-" + index);
        writer.write(record);
      }
    }
    return target;
  }

  static File createFileWithDictionaryFallback(File directory, String fileName, int recordCount)
      throws IOException {
    File target = new File(directory, fileName);
    try (ParquetWriter<GenericRecord> writer =
        AvroParquetWriter.<GenericRecord>builder(
                HadoopOutputFile.fromPath(toHadoopPath(target), new Configuration()))
            .withSchema(RECORD_SCHEMA)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withRowGroupSize(SINGLE_ROW_GROUP_SIZE_BYTES)
            .withPageSize(64)
            .withDictionaryPageSize(64)
            .withDictionaryEncoding(true)
            .build()) {
      for (int index = 0; index < recordCount; index++) {
        writer.write(createRecord(index));
      }
    }
    return target;
  }

  static byte[] readAllBytes(File file) throws IOException {
    return Files.readAllBytes(file.toPath());
  }

  static Path toHadoopPath(File file) {
    return new Path(file.getAbsolutePath());
  }

  private static ParquetWriter<GenericRecord> createWriter(
      File target, Schema schema, long rowGroupSizeBytes) throws IOException {
    return AvroParquetWriter.<GenericRecord>builder(
            HadoopOutputFile.fromPath(toHadoopPath(target), new Configuration()))
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withRowGroupSize(rowGroupSizeBytes)
        .withDictionaryEncoding(true)
        .build();
  }

  private static GenericRecord createRecord(int index) {
    GenericRecord record = new GenericData.Record(RECORD_SCHEMA);
    record.put(ID_COLUMN, (long) index);
    record.put(CATEGORY_COLUMN, "category-" + (index % DISTINCT_CATEGORY_COUNT));
    record.put(VALUE_COLUMN, (index % DISTINCT_CATEGORY_COUNT) * 1.5d);
    return record;
  }
}
