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
import com.google.common.collect.Range;
import java.util.Optional;

/**
 * The byte layout of a single Parquet column chunk within one row group.
 *
 * <p>All offsets are absolute positions within the Parquet object. A chunk is one contiguous range
 * that {@link #getDataPageOffset()} splits in two: the bytes before it are a dictionary page that
 * can filter the chunk on its own, and the bytes from it to the end of the chunk are the data
 * pages, which always hold at least one byte.
 */
@AutoValue
abstract class ParquetColumnChunk {

  /** Returns the dotted column path, for example {@code address.city}. */
  abstract String getColumnPath();

  /** Returns the offset of the first byte of this chunk, dictionary page included. */
  abstract long getStartOffset();

  /** Returns the total compressed size of this chunk in bytes. */
  abstract long getCompressedSize();

  /**
   * Returns the offset of the first data page of this chunk.
   *
   * <p>This equals {@link #getStartOffset()} when the chunk has no dictionary page that can filter
   * it on its own.
   */
  abstract long getDataPageOffset();

  /**
   * Creates a chunk from the offsets recorded in the file footer.
   *
   * <p>A dictionary page can filter the chunk on its own only when every data page is dictionary
   * encoded. Otherwise the dictionary page is read together with the data pages, so the chunk
   * counts it as part of them.
   *
   * @param columnPath the dotted column path
   * @param startOffset the offset of the first byte of the chunk, dictionary page included
   * @param compressedSize the total compressed size of the chunk in bytes
   * @param dataPageOffset the offset of the first data page, as recorded in the footer
   * @param everyDataPageDictionaryEncoded whether every data page of the chunk is dictionary
   *     encoded
   * @throws IllegalArgumentException if the offsets do not describe one contiguous chunk whose data
   *     pages start inside it
   */
  static ParquetColumnChunk create(
      String columnPath,
      long startOffset,
      long compressedSize,
      long dataPageOffset,
      boolean everyDataPageDictionaryEncoded) {
    // Checks the recorded data page offset even when the dictionary page is counted as data, so a
    // corrupt offset is never hidden.
    checkArgument(
        startOffset >= 0
            && dataPageOffset >= startOffset
            && dataPageOffset - startOffset < compressedSize
            && compressedSize <= Long.MAX_VALUE - startOffset,
        "Column chunk %s has inconsistent offsets: start %s, size %s, first data page %s",
        columnPath,
        startOffset,
        compressedSize,
        dataPageOffset);
    return new AutoValue_ParquetColumnChunk(
        columnPath,
        startOffset,
        compressedSize,
        everyDataPageDictionaryEncoded ? dataPageOffset : startOffset);
  }

  /** Returns the offset one past the last byte of this chunk. */
  final long getEndOffset() {
    return getStartOffset() + getCompressedSize();
  }

  /** Returns the byte range covering the entire column chunk, dictionary page included. */
  final Range<Long> getByteRange() {
    return Range.closedOpen(getStartOffset(), getEndOffset());
  }

  /** Returns the byte range covering the data pages of this chunk. */
  final Range<Long> getDataPageRange() {
    return Range.closedOpen(getDataPageOffset(), getEndOffset());
  }

  /** Returns whether {@code position} falls inside the data pages of this chunk. */
  final boolean containsDataPagePosition(long position) {
    return position >= getDataPageOffset() && position < getEndOffset();
  }

  /** Returns whether {@code [startOffset, endOffset)} overlaps the data pages of this chunk. */
  final boolean touchesDataPages(long startOffset, long endOffset) {
    return overlaps(getDataPageOffset(), getEndOffset(), startOffset, endOffset);
  }

  /**
   * Returns whether {@code [startOffset, endOffset)} overlaps the dictionary page of this chunk.
   */
  final boolean touchesDictionaryPage(long startOffset, long endOffset) {
    return overlaps(getStartOffset(), getDataPageOffset(), startOffset, endOffset);
  }

  /** Returns whether this chunk has a dictionary page that can filter it on its own. */
  final boolean hasDictionaryPage() {
    return getStartOffset() < getDataPageOffset();
  }

  /** Returns the byte range covering only the dictionary page, if this chunk has one. */
  final Optional<Range<Long>> getDictionaryPageRange() {
    if (!hasDictionaryPage()) {
      return Optional.empty();
    }
    return Optional.of(Range.closedOpen(getStartOffset(), getDataPageOffset()));
  }

  /** Returns whether two half-open ranges share a byte; an empty range shares none. */
  private static boolean overlaps(long start, long end, long otherStart, long otherEnd) {
    return start < end && otherStart < otherEnd && start < otherEnd && otherStart < end;
  }
}
