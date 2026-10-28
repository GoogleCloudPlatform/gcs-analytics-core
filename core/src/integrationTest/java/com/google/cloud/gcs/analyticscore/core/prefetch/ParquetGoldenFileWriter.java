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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;

/**
 * Writes the golden Parquet files that the prefetch unit tests parse, and prints the layout that
 * parquet-mr reports for each of them.
 *
 * <p>{@code ParquetGoldenFiles} in the unit test sources hard-codes that layout, so the unit tests
 * need neither Hadoop nor Avro. To regenerate the files, for example after upgrading parquet-mr, run from the repository
 * root:
 *
 * <pre>{@code
 * mvn -q -Pintegration-test -pl core -am test-compile dependency:build-classpath \
 *     -Dmdep.includeScope=test -Dmdep.outputFile=target/test.classpath
 * java -cp "core/target/test-classes:core/target/classes:$(cat core/target/test.classpath)" \
 *     com.google.cloud.gcs.analyticscore.core.prefetch.ParquetGoldenFileWriter \
 *     core/src/test/resources/parquet
 * }</pre>
 *
 * <p>Then update the constants in {@code ParquetGoldenFiles} from the printed layout. This class
 * cannot refer to them directly: the benchmarks module compiles these sources without the unit test
 * sources.
 */
public final class ParquetGoldenFileWriter {

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

  private static final String SINGLE_ROW_GROUP_FILE = "single_row_group.parquet";
  private static final String MULTI_ROW_GROUP_FILE = "multi_row_group.parquet";
  private static final String DICTIONARY_FALLBACK_FILE = "dictionary_fallback.parquet";
  private static final String ALTERNATE_SCHEMA_FILE = "alternate_schema.parquet";
  private static final String ID_COLUMN = "id";
  private static final String CATEGORY_COLUMN = "category";
  private static final String VALUE_COLUMN = "value";

  private static final int DISTINCT_CATEGORY_COUNT = 4;
  private static final long SINGLE_ROW_GROUP_SIZE_BYTES = 128L * 1024L;
  // Small enough that parquet-mr closes a row group at its first size check, every 100 records.
  private static final long TINY_ROW_GROUP_SIZE_BYTES = 1024L;
  private static final int SINGLE_ROW_GROUP_RECORDS = 100;
  private static final int MULTI_ROW_GROUP_RECORDS = 400;
  private static final int ALTERNATE_SCHEMA_RECORDS = 10;
  // The first page holds repeating categories, so parquet-mr keeps the dictionary for it. The
  // unique categories after it overflow the small dictionary, so later pages fall back to plain.
  private static final int FALLBACK_RECORDS = 200;
  private static final int FALLBACK_REPEATING_RECORDS = 100;
  private static final int FALLBACK_PAGE_SIZE_BYTES = 64;
  private static final int FALLBACK_DICTIONARY_PAGE_SIZE_BYTES = 256;

  private ParquetGoldenFileWriter() {}

  /**
   * Writes every golden file into the directory {@code args[0]}, then prints the layout of each.
   */
  public static void main(String[] args) throws IOException {
    File directory = new File(args[0]);

    File singleRowGroup = new File(directory, SINGLE_ROW_GROUP_FILE);
    try (ParquetWriter<GenericRecord> writer =
        createWriter(singleRowGroup, RECORD_SCHEMA, SINGLE_ROW_GROUP_SIZE_BYTES).build()) {
      writeRecords(writer, SINGLE_ROW_GROUP_RECORDS, SINGLE_ROW_GROUP_RECORDS);
    }

    File multiRowGroup = new File(directory, MULTI_ROW_GROUP_FILE);
    try (ParquetWriter<GenericRecord> writer =
        createWriter(multiRowGroup, RECORD_SCHEMA, TINY_ROW_GROUP_SIZE_BYTES).build()) {
      writeRecords(writer, MULTI_ROW_GROUP_RECORDS, MULTI_ROW_GROUP_RECORDS);
    }

    File dictionaryFallback = new File(directory, DICTIONARY_FALLBACK_FILE);
    try (ParquetWriter<GenericRecord> writer =
        createWriter(dictionaryFallback, RECORD_SCHEMA, SINGLE_ROW_GROUP_SIZE_BYTES)
            .withPageSize(FALLBACK_PAGE_SIZE_BYTES)
            .withDictionaryPageSize(FALLBACK_DICTIONARY_PAGE_SIZE_BYTES)
            .build()) {
      writeRecords(writer, FALLBACK_RECORDS, FALLBACK_REPEATING_RECORDS);
    }

    File alternateSchema = new File(directory, ALTERNATE_SCHEMA_FILE);
    try (ParquetWriter<GenericRecord> writer =
        createWriter(alternateSchema, ALTERNATE_SCHEMA, SINGLE_ROW_GROUP_SIZE_BYTES).build()) {
      for (int index = 0; index < ALTERNATE_SCHEMA_RECORDS; index++) {
        GenericRecord record = new GenericData.Record(ALTERNATE_SCHEMA);
        record.put("key", "key-" + index);
        writer.write(record);
      }
    }

    for (File file : new File[] {singleRowGroup, multiRowGroup, dictionaryFallback, alternateSchema}) {
      printLayout(file);
    }
  }

  private static AvroParquetWriter.Builder<GenericRecord> createWriter(
      File target, Schema schema, long rowGroupSizeBytes) throws IOException {
    if (target.exists() && !target.delete()) {
      throw new IOException("Cannot replace " + target);
    }
    // A local output file, unlike a Hadoop one, writes no .crc checksum file next to the target.
    return AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(target.toPath()))
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withRowGroupSize(rowGroupSizeBytes)
        .withDictionaryEncoding(true);
  }

  /**
   * Writes {@code recordCount} records whose categories repeat for the first {@code
   * repeatingRecordCount} records and are unique after that.
   */
  private static void writeRecords(
      ParquetWriter<GenericRecord> writer, int recordCount, int repeatingRecordCount)
      throws IOException {
    for (int index = 0; index < recordCount; index++) {
      GenericRecord record = new GenericData.Record(RECORD_SCHEMA);
      record.put(ID_COLUMN, (long) index);
      record.put(
          CATEGORY_COLUMN,
          index < repeatingRecordCount
              ? "category-" + (index % DISTINCT_CATEGORY_COUNT)
              : "unique-category-" + index);
      record.put(VALUE_COLUMN, (index % DISTINCT_CATEGORY_COUNT) * 1.5d);
      writer.write(record);
    }
  }

  private static void printLayout(File file) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file.toPath()))) {
      System.out.printf("%s (%d bytes)%n", file.getName(), file.length());
      int rowGroupIndex = 0;
      for (BlockMetaData rowGroup : reader.getFooter().getBlocks()) {
        for (ColumnChunkMetaData chunk : rowGroup.getColumns()) {
          System.out.printf(
              "  row group %d, %s: start %d, size %d, dictionary page %d, first data page %d,"
                  + " data page encodings %s%n",
              rowGroupIndex,
              chunk.getPath().toDotString(),
              chunk.getStartingPos(),
              chunk.getTotalSize(),
              chunk.getDictionaryPageOffset(),
              chunk.getFirstDataPageOffset(),
              chunk.getEncodingStats().getDataEncodings());
        }
        rowGroupIndex++;
      }
    }
  }
}
