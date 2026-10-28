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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The byte layout of a single Parquet row group: at least one column chunk, kept in byte order,
 * where no two chunks share a byte.
 */
@AutoValue
abstract class ParquetRowGroup {

  /** Returns the zero-based index of this row group within the file. */
  abstract int getIndex();

  /** Returns the column chunks of this row group in byte order, keyed by dotted column path. */
  abstract ImmutableMap<String, ParquetColumnChunk> getColumnChunks();

  /** Returns the offset of the first byte of this row group. */
  abstract long getStartOffset();

  /** Returns the offset one past the last byte of this row group. */
  abstract long getEndOffset();

  /**
   * Creates a row group from its column chunks, given in any order.
   *
   * @throws IllegalArgumentException if the index is negative, there are no column chunks, two
   *     chunks share a column path, or two chunks share a byte
   */
  static ParquetRowGroup create(int index, Collection<ParquetColumnChunk> columnChunks) {
    checkArgument(index >= 0, "Row group index must not be negative: %s", index);
    checkArgument(!columnChunks.isEmpty(), "Row group %s has no column chunks", index);
    List<ParquetColumnChunk> sortedChunks = new ArrayList<>(columnChunks);
    sortedChunks.sort(Comparator.comparingLong(ParquetColumnChunk::getStartOffset));
    ImmutableMap.Builder<String, ParquetColumnChunk> chunksByPath =
        ImmutableMap.builderWithExpectedSize(sortedChunks.size());
    long endOffset = 0;
    for (ParquetColumnChunk chunk : sortedChunks) {
      checkArgument(
          chunk.getStartOffset() >= endOffset,
          "Column chunk %s starts before the previous chunk ends at %s",
          chunk.getColumnPath(),
          endOffset);
      chunksByPath.put(chunk.getColumnPath(), chunk);
      endOffset = chunk.getEndOffset();
    }
    return new AutoValue_ParquetRowGroup(
        index, chunksByPath.buildOrThrow(), sortedChunks.get(0).getStartOffset(), endOffset);
  }

  /** Returns the chunk for the given dotted column path, if this row group contains it. */
  final Optional<ParquetColumnChunk> getColumnChunk(String columnPath) {
    return Optional.ofNullable(getColumnChunks().get(columnPath));
  }

  /** Returns whether {@code position} falls inside this row group. */
  final boolean containsPosition(long position) {
    return position >= getStartOffset() && position < getEndOffset();
  }

  /**
   * Returns the paths of every column chunk in this row group whose data pages overlap {@code
   * [startOffset, endOffset)}.
   */
  final ImmutableSet<String> findDataColumnsInRange(long startOffset, long endOffset) {
    ImmutableSet.Builder<String> touchedColumns = ImmutableSet.builder();
    for (ParquetColumnChunk chunk : getColumnChunks().values()) {
      if (chunk.getStartOffset() >= endOffset) {
        break; // Chunks are in byte order, so no later chunk overlaps the range.
      }
      if (chunk.touchesDataPages(startOffset, endOffset)) {
        touchedColumns.add(chunk.getColumnPath());
      }
    }
    return touchedColumns.build();
  }

  /**
   * Returns the paths of every column chunk in this row group whose dictionary page overlaps {@code
   * [startOffset, endOffset)}.
   */
  final ImmutableSet<String> findDictionaryColumnsInRange(long startOffset, long endOffset) {
    ImmutableSet.Builder<String> touchedColumns = ImmutableSet.builder();
    for (ParquetColumnChunk chunk : getColumnChunks().values()) {
      if (chunk.getStartOffset() >= endOffset) {
        break; // Chunks are in byte order, so no later chunk overlaps the range.
      }
      if (chunk.touchesDictionaryPage(startOffset, endOffset)) {
        touchedColumns.add(chunk.getColumnPath());
      }
    }
    return touchedColumns.build();
  }

  /** Returns the data-page byte range of the column chunk containing {@code position}, if any. */
  final Optional<Range<Long>> findDataPageRangeAt(long position) {
    for (ParquetColumnChunk chunk : getColumnChunks().values()) {
      if (chunk.getStartOffset() > position) {
        break; // Chunks are in byte order, so no later chunk contains the position.
      }
      if (chunk.containsDataPagePosition(position)) {
        return Optional.of(chunk.getDataPageRange());
      }
    }
    return Optional.empty();
  }

  /**
   * Returns the byte ranges of the whole column chunks of {@code columns}, dictionary pages
   * included, in byte order. Columns that this row group does not contain are skipped.
   */
  final ImmutableList<Range<Long>> getColumnChunkRanges(Set<String> columns) {
    ImmutableList.Builder<Range<Long>> ranges = ImmutableList.builder();
    for (ParquetColumnChunk chunk : getColumnChunks().values()) {
      if (columns.contains(chunk.getColumnPath())) {
        ranges.add(chunk.getByteRange());
      }
    }
    return ranges.build();
  }

  /**
   * Returns the byte ranges of the dictionary pages of {@code columns}, in byte order. Columns
   * without a dictionary page that can filter them are skipped.
   */
  final ImmutableList<Range<Long>> getDictionaryPageRanges(Set<String> columns) {
    ImmutableList.Builder<Range<Long>> ranges = ImmutableList.builder();
    for (ParquetColumnChunk chunk : getColumnChunks().values()) {
      if (columns.contains(chunk.getColumnPath())) {
        chunk.getDictionaryPageRange().ifPresent(ranges::add);
      }
    }
    return ranges.build();
  }
}
