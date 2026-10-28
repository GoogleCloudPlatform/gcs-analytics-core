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
 */
final class ParquetDictionaryEncoding {

  private ParquetDictionaryEncoding() {}

  /**
   * Returns whether every data page of the chunk described by {@code metaData} uses the dictionary.
   */
  static boolean encodesEveryDataPage(ColumnMetaData metaData) {
    return metaData.isSetEncoding_stats()
        ? statsShowOnlyDictionaryDataPages(metaData.getEncoding_stats())
        : encodingsShowOnlyDictionaryDataPages(metaData.getEncodings());
  }

  private static boolean statsShowOnlyDictionaryDataPages(List<PageEncodingStats> encodingStats) {
    for (PageEncodingStats stats : encodingStats) {
      PageType pageType = stats.getPage_type();
      if ((pageType == PageType.DATA_PAGE || pageType == PageType.DATA_PAGE_V2)
          && stats.getCount() > 0
          && !isDictionaryEncoding(stats.getEncoding())) {
        return false;
      }
    }
    return true;
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

  private static boolean isDictionaryEncoding(Encoding encoding) {
    return encoding == Encoding.PLAIN_DICTIONARY || encoding == Encoding.RLE_DICTIONARY;
  }
}
