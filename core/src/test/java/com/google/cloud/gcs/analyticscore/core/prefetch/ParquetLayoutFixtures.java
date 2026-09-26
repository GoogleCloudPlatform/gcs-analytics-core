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

import com.google.common.collect.ImmutableList;

/** Builders for Parquet layout values used by the layout model tests. */
final class ParquetLayoutFixtures {

  private ParquetLayoutFixtures() {}

  /** Returns a chunk whose data pages start at the first byte of the chunk. */
  static ParquetColumnChunk columnChunk(String columnPath, long startOffset, long compressedSize) {
    return ParquetColumnChunk.builder()
        .setColumnPath(columnPath)
        .setStartOffset(startOffset)
        .setCompressedSize(compressedSize)
        .setDataPageOffset(startOffset)
        .build();
  }

  /** Returns a chunk with a dictionary page occupying the bytes before {@code dataPageOffset}. */
  static ParquetColumnChunk dictionaryEncodedColumnChunk(
      String columnPath, long startOffset, long compressedSize, long dataPageOffset) {
    return ParquetColumnChunk.builder()
        .setColumnPath(columnPath)
        .setStartOffset(startOffset)
        .setCompressedSize(compressedSize)
        .setDataPageOffset(dataPageOffset)
        .setDictionaryPageOffset(startOffset)
        .build();
  }

  static ParquetRowGroup rowGroup(int index, ParquetColumnChunk... columnChunks) {
    ParquetRowGroup.Builder builder = ParquetRowGroup.builder().setIndex(index);
    for (ParquetColumnChunk columnChunk : columnChunks) {
      builder.addColumnChunk(columnChunk);
    }
    return builder.build();
  }

  static ParquetFileLayout layout(ParquetRowGroup... rowGroups) {
    return ParquetFileLayout.create(/* schemaFingerprint= */ 7, ImmutableList.copyOf(rowGroups));
  }
}
