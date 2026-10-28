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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageEncodingStats;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;

class ParquetDictionaryEncodingTest {

  @Test
  void encodesEveryDataPage_statsWithPlainDictionaryDataPages_returnsTrue() {
    ColumnMetaData metaData =
        metaDataWithStats(
            dictionaryPageStats(),
            new PageEncodingStats(PageType.DATA_PAGE, Encoding.PLAIN_DICTIONARY, 3));

    assertThat(ParquetDictionaryEncoding.encodesEveryDataPage(metaData)).isTrue();
  }

  @Test
  void encodesEveryDataPage_statsWithRleDictionaryDataPages_returnsTrue() {
    ColumnMetaData metaData =
        metaDataWithStats(
            dictionaryPageStats(),
            new PageEncodingStats(PageType.DATA_PAGE_V2, Encoding.RLE_DICTIONARY, 3));

    assertThat(ParquetDictionaryEncoding.encodesEveryDataPage(metaData)).isTrue();
  }

  @Test
  void encodesEveryDataPage_statsWithPlainDataPage_returnsFalse() {
    ColumnMetaData metaData =
        metaDataWithStats(
            dictionaryPageStats(),
            new PageEncodingStats(PageType.DATA_PAGE, Encoding.PLAIN_DICTIONARY, 2),
            new PageEncodingStats(PageType.DATA_PAGE, Encoding.PLAIN, 1));

    assertThat(ParquetDictionaryEncoding.encodesEveryDataPage(metaData)).isFalse();
  }

  @Test
  void encodesEveryDataPage_statsWithPlainDataPageV2_returnsFalse() {
    ColumnMetaData metaData =
        metaDataWithStats(
            dictionaryPageStats(), new PageEncodingStats(PageType.DATA_PAGE_V2, Encoding.PLAIN, 1));

    assertThat(ParquetDictionaryEncoding.encodesEveryDataPage(metaData)).isFalse();
  }

  @Test
  void encodesEveryDataPage_statsWithZeroPlainDataPages_returnsTrue() {
    ColumnMetaData metaData =
        metaDataWithStats(
            dictionaryPageStats(),
            new PageEncodingStats(PageType.DATA_PAGE, Encoding.PLAIN_DICTIONARY, 3),
            new PageEncodingStats(PageType.DATA_PAGE, Encoding.PLAIN, 0));

    assertThat(ParquetDictionaryEncoding.encodesEveryDataPage(metaData)).isTrue();
  }

  @Test
  void encodesEveryDataPage_statsWithUnknownDataPageEncoding_returnsFalse() {
    ColumnMetaData metaData =
        metaDataWithStats(
            dictionaryPageStats(),
            new PageEncodingStats(PageType.DATA_PAGE, /* encoding= */ null, 1));

    assertThat(ParquetDictionaryEncoding.encodesEveryDataPage(metaData)).isFalse();
  }

  @Test
  void encodesEveryDataPage_plainDictionaryAndLevelEncodingsWithoutStats_returnsTrue() {
    ColumnMetaData metaData =
        metaDataWithEncodings(
            ImmutableList.of(Encoding.PLAIN_DICTIONARY, Encoding.RLE, Encoding.BIT_PACKED));

    assertThat(ParquetDictionaryEncoding.encodesEveryDataPage(metaData)).isTrue();
  }

  @Test
  void encodesEveryDataPage_plainEncodingWithoutStats_returnsFalse() {
    ColumnMetaData metaData =
        metaDataWithEncodings(
            ImmutableList.of(Encoding.PLAIN_DICTIONARY, Encoding.PLAIN, Encoding.RLE));

    assertThat(ParquetDictionaryEncoding.encodesEveryDataPage(metaData)).isFalse();
  }

  @Test
  void encodesEveryDataPage_rleDictionaryEncodingWithoutStats_returnsFalse() {
    ColumnMetaData metaData =
        metaDataWithEncodings(
            ImmutableList.of(Encoding.PLAIN, Encoding.RLE_DICTIONARY, Encoding.RLE));

    assertThat(ParquetDictionaryEncoding.encodesEveryDataPage(metaData)).isFalse();
  }

  @Test
  void encodesEveryDataPage_onlyLevelEncodingsWithoutStats_returnsFalse() {
    ColumnMetaData metaData =
        metaDataWithEncodings(ImmutableList.of(Encoding.RLE, Encoding.BIT_PACKED));

    assertThat(ParquetDictionaryEncoding.encodesEveryDataPage(metaData)).isFalse();
  }

  @Test
  void encodesEveryDataPage_unknownEncodingWithoutStats_returnsFalse() {
    ColumnMetaData metaData =
        metaDataWithEncodings(Arrays.asList(Encoding.PLAIN_DICTIONARY, /* unknown= */ null));

    assertThat(ParquetDictionaryEncoding.encodesEveryDataPage(metaData)).isFalse();
  }

  private static ColumnMetaData metaDataWithEncodings(List<Encoding> encodings) {
    return new ColumnMetaData(
        Type.BYTE_ARRAY,
        encodings,
        ImmutableList.of("category"),
        CompressionCodec.UNCOMPRESSED,
        /* num_values= */ 10,
        /* total_uncompressed_size= */ 100,
        /* total_compressed_size= */ 100,
        /* data_page_offset= */ 24);
  }

  /** Returns metadata whose encodings would say otherwise, to show that the stats take priority. */
  private static ColumnMetaData metaDataWithStats(PageEncodingStats... encodingStats) {
    return metaDataWithEncodings(ImmutableList.of(Encoding.PLAIN))
        .setEncoding_stats(ImmutableList.copyOf(encodingStats));
  }

  private static PageEncodingStats dictionaryPageStats() {
    return new PageEncodingStats(PageType.DICTIONARY_PAGE, Encoding.PLAIN, 1);
  }
}
