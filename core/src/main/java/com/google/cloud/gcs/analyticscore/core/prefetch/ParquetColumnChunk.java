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

import com.google.auto.value.AutoValue;
import com.google.common.collect.Range;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * The byte layout of a single Parquet column chunk within one row group.
 *
 * <p>All offsets are absolute positions within the Parquet object.
 */
@AutoValue
abstract class ParquetColumnChunk {

  /** Returns the dotted column path, for example {@code address.city}. */
  abstract String getColumnPath();

  /** Returns the offset of the first byte of this chunk, dictionary page included. */
  abstract long getStartOffset();

  /** Returns the total compressed size of this chunk in bytes. */
  abstract long getCompressedSize();

  /** Returns the offset of the first data page of this chunk. */
  abstract long getDataPageOffset();

  /** Returns the offset of the dictionary page, if this chunk has one. */
  abstract OptionalLong getDictionaryPageOffset();

  static Builder builder() {
    return new AutoValue_ParquetColumnChunk.Builder();
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
    return getDictionaryPageRange()
        .filter(
            range -> overlaps(range.lowerEndpoint(), range.upperEndpoint(), startOffset, endOffset))
        .isPresent();
  }

  /** Returns whether this chunk is dictionary encoded. */
  final boolean hasDictionaryPage() {
    return getDictionaryPageOffset().isPresent();
  }

  /**
   * Returns the byte range covering only the dictionary page, which is the region between the
   * dictionary page offset and the first data page.
   */
  final Optional<Range<Long>> getDictionaryPageRange() {
    if (!hasDictionaryPage()) {
      return Optional.empty();
    }
    long dictionaryStart = getDictionaryPageOffset().getAsLong();
    if (getDataPageOffset() <= dictionaryStart) {
      return Optional.empty();
    }
    return Optional.of(Range.closedOpen(dictionaryStart, getDataPageOffset()));
  }

  private static boolean overlaps(long start, long end, long otherStart, long otherEnd) {
    return start < otherEnd && end > otherStart;
  }

  /** Builder for {@link ParquetColumnChunk}. */
  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setColumnPath(String columnPath);

    abstract Builder setStartOffset(long startOffset);

    abstract Builder setCompressedSize(long compressedSize);

    abstract Builder setDataPageOffset(long dataPageOffset);

    abstract Builder setDictionaryPageOffset(long dictionaryPageOffset);

    abstract ParquetColumnChunk build();
  }
}
