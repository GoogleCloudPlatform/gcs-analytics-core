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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.InputStream;

/**
 * Small Parquet files written by parquet-mr and checked in under {@code
 * src/test/resources/parquet}, together with the layout that parquet-mr reports for them.
 *
 * <p>Checking the files in keeps Hadoop and Avro out of the unit tests. {@code
 * ParquetGoldenFileWriter}, in the integration test sources, regenerates them and prints the layout
 * recorded here.
 */
final class ParquetGoldenFiles {

  static final String ID_COLUMN = "id";
  static final String CATEGORY_COLUMN = "category";
  static final String VALUE_COLUMN = "value";

  /**
   * One row group of 100 records. The {@code id} chunk is plain encoded, while the {@code category}
   * and {@code value} chunks are dictionary encoded.
   */
  static final String SINGLE_ROW_GROUP = "single_row_group.parquet";

  /** The start offsets of the column chunks of {@link #SINGLE_ROW_GROUP}, in column order. */
  static final ImmutableList<Long> SINGLE_ROW_GROUP_START_OFFSETS =
      ImmutableList.of(4L, 830L, 957L);

  /** The sizes of the column chunks of {@link #SINGLE_ROW_GROUP}, in column order. */
  static final ImmutableList<Long> SINGLE_ROW_GROUP_COMPRESSED_SIZES =
      ImmutableList.of(826L, 127L, 103L);

  /** The dictionary page of the {@code category} chunk of {@link #SINGLE_ROW_GROUP}. */
  static final Range<Long> SINGLE_ROW_GROUP_CATEGORY_DICTIONARY_PAGE = Range.closedOpen(830L, 905L);

  /** The schema of {@link #SINGLE_ROW_GROUP}, with 400 records split into four row groups. */
  static final String MULTI_ROW_GROUP = "multi_row_group.parquet";

  /** The start offsets of the row groups of {@link #MULTI_ROW_GROUP}, in row group order. */
  static final ImmutableList<Long> MULTI_ROW_GROUP_START_OFFSETS =
      ImmutableList.of(4L, 1060L, 2116L, 3172L);

  /**
   * The schema of {@link #SINGLE_ROW_GROUP} in one row group of 200 records. The {@code id} chunk
   * has no dictionary page, the {@code category} chunk fell back to plain data pages after its
   * first dictionary encoded page, and the {@code value} chunk is dictionary encoded throughout.
   */
  static final String DICTIONARY_FALLBACK = "dictionary_fallback.parquet";

  /** A single {@code key} column, for comparison with a different schema. */
  static final String ALTERNATE_SCHEMA = "alternate_schema.parquet";

  private ParquetGoldenFiles() {}

  /** Returns the bytes of the golden file named {@code fileName}. */
  static byte[] read(String fileName) throws IOException {
    try (InputStream file = ParquetGoldenFiles.class.getResourceAsStream("/parquet/" + fileName)) {
      checkState(file != null, "Missing golden file %s", fileName);
      return ByteStreams.toByteArray(file);
    }
  }
}
