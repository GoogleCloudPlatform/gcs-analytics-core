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

import java.util.List;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageEncodingStats;
import org.apache.parquet.format.PageType;

/**
 * Decides whether the dictionary page of a Parquet column chunk can filter the chunk on its own.
 *
 * <p>That holds only when every data page is dictionary encoded. A writer whose dictionary grows
 * too large falls back to plain data pages mid-chunk, and the values on those pages are missing
 * from the dictionary.
 *
 * <p>The generated Thrift code treats an encoding added to Parquet after {@code
 * parquet-format-structures} was built in one of two ways. In page encoding stats, the encoding is
 * a required field, so the footer fails to decode. In the encodings list, the value is dropped, so
 * a chunk without page encoding stats that lists {@code PLAIN_DICTIONARY} and an unknown fallback
 * encoding looks fully dictionary encoded. parquet-mr's {@code DictionaryFilter} has the same gap.
 * The answer is therefore a hint for laying out reads, not a guarantee that filtering can rely on.
 */
final class ParquetDictionaryEncoding {

  private ParquetDictionaryEncoding() {}

  /**
   * Returns whether every data page of the chunk described by {@code metaData} uses the dictionary.
   *
   * <p>This does not check that the chunk has a dictionary page. {@link ParquetColumnChunk} finds
   * that from the chunk's offsets.
   */
  static boolean encodesEveryDataPage(ColumnMetaData metaData) {
    return metaData.isSetEncoding_stats()
        ? statsShowOnlyDictionaryDataPages(metaData.getEncoding_stats())
        : encodingsShowOnlyDictionaryDataPages(metaData.getEncodings());
  }

  /**
   * Requires at least one dictionary-encoded data page and no other data page entries.
   *
   * <p>Like parquet-mr's {@code EncodingStats}, an entry for a non-dictionary data page counts
   * whatever its page count says. A negative count marks the stats as corrupt.
   */
  private static boolean statsShowOnlyDictionaryDataPages(List<PageEncodingStats> encodingStats) {
    boolean hasDictionaryDataPages = false;
    for (PageEncodingStats stats : encodingStats) {
      if (!isDataPage(stats.getPage_type())) {
        continue;
      }
      if (stats.getCount() < 0 || !isDictionaryEncoding(stats.getEncoding())) {
        return false;
      }
      hasDictionaryDataPages |= stats.getCount() > 0;
    }
    return hasDictionaryDataPages;
  }

  /**
   * Infers the answer from the chunk's encodings, as parquet-mr's {@code DictionaryFilter} does for
   * files written without page statistics.
   *
   * <p>Only the Parquet 1.0 encoding {@code PLAIN_DICTIONARY} proves dictionary-encoded data pages,
   * and {@code RLE} and {@code BIT_PACKED} encode only repetition and definition levels there. A
   * Parquet 2.0 chunk lists {@code PLAIN} for its dictionary page next to {@code RLE_DICTIONARY},
   * which makes a fallback to plain data pages impossible to rule out.
   */
  private static boolean encodingsShowOnlyDictionaryDataPages(List<Encoding> encodings) {
    boolean hasDictionaryDataPages = false;
    for (Encoding encoding : encodings) {
      if (encoding == Encoding.PLAIN_DICTIONARY) {
        hasDictionaryDataPages = true;
      } else if (encoding != Encoding.RLE && encoding != Encoding.BIT_PACKED) {
        return false;
      }
    }
    return hasDictionaryDataPages;
  }

  private static boolean isDataPage(PageType pageType) {
    return pageType == PageType.DATA_PAGE || pageType == PageType.DATA_PAGE_V2;
  }

  private static boolean isDictionaryEncoding(Encoding encoding) {
    return encoding == Encoding.PLAIN_DICTIONARY || encoding == Encoding.RLE_DICTIONARY;
  }
}
