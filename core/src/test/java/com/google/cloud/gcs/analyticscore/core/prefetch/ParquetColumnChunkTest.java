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

import static com.google.cloud.gcs.analyticscore.core.prefetch.ParquetLayoutFixtures.columnChunk;
import static com.google.cloud.gcs.analyticscore.core.prefetch.ParquetLayoutFixtures.dictionaryEncodedColumnChunk;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

class ParquetColumnChunkTest {

  @Test
  void create_notEveryDataPageDictionaryEncoded_countsDictionaryPageAsDataPages() {
    ParquetColumnChunk chunk =
        ParquetColumnChunk.create(
            "price", 100, 50, 120, /* everyDataPageDictionaryEncoded= */ false);

    assertThat(chunk.getDataPageOffset()).isEqualTo(100);
  }

  @Test
  void create_dictionaryCountedAsDataWithDataPageOffsetPastEnd_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ParquetColumnChunk.create(
                "price", 100, 50, 999, /* everyDataPageDictionaryEncoded= */ false));
  }

  @Test
  void create_negativeStartOffset_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> createChunk(-1, 50, -1));
  }

  @Test
  void create_negativeCompressedSize_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> createChunk(100, -1, 100));
  }

  @Test
  void create_zeroCompressedSize_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> createChunk(100, 0, 100));
  }

  @Test
  void create_endOffsetBeyondLongRange_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> createChunk(Long.MAX_VALUE - 10, 50, Long.MAX_VALUE - 10));
  }

  @Test
  void create_dataPageOffsetBeforeStartOffset_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> createChunk(100, 50, 99));
  }

  @Test
  void create_dataPageOffsetAtEndOffset_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> createChunk(100, 50, 150));
  }

  @Test
  void getEndOffset_returnsStartOffsetPlusCompressedSize() {
    ParquetColumnChunk chunk = columnChunk("price", 100, 50);

    long endOffset = chunk.getEndOffset();

    assertThat(endOffset).isEqualTo(150);
  }

  @Test
  void hasDictionaryPage_chunkWithoutDictionary_returnsFalse() {
    ParquetColumnChunk chunk = columnChunk("price", 100, 50);

    boolean hasDictionaryPage = chunk.hasDictionaryPage();

    assertThat(hasDictionaryPage).isFalse();
  }

  @Test
  void hasDictionaryPage_chunkWithDictionary_returnsTrue() {
    ParquetColumnChunk chunk = dictionaryEncodedColumnChunk("price", 100, 50, 120);

    boolean hasDictionaryPage = chunk.hasDictionaryPage();

    assertThat(hasDictionaryPage).isTrue();
  }

  @Test
  void getDictionaryPageRange_chunkWithoutDictionary_returnsEmpty() {
    ParquetColumnChunk chunk = columnChunk("price", 100, 50);

    assertThat(chunk.getDictionaryPageRange()).isEmpty();
  }

  @Test
  void getDictionaryPageRange_chunkWithDictionary_spansStartOffsetToDataPageOffset() {
    ParquetColumnChunk chunk = dictionaryEncodedColumnChunk("price", 100, 50, 120);

    assertThat(chunk.getDictionaryPageRange()).hasValue(Range.closedOpen(100L, 120L));
  }

  @Test
  void getByteRange_spansStartOffsetToEndOffset() {
    ParquetColumnChunk chunk = dictionaryEncodedColumnChunk("price", 100, 50, 120);

    assertThat(chunk.getByteRange()).isEqualTo(Range.closedOpen(100L, 150L));
  }

  @Test
  void getDataPageRange_spansDataPageOffsetToEndOffset() {
    ParquetColumnChunk chunk = dictionaryEncodedColumnChunk("price", 100, 50, 120);

    assertThat(chunk.getDataPageRange()).isEqualTo(Range.closedOpen(120L, 150L));
  }

  @Test
  void containsDataPagePosition_positionInDataPages_returnsTrue() {
    ParquetColumnChunk chunk = dictionaryEncodedColumnChunk("price", 100, 50, 120);

    assertThat(chunk.containsDataPagePosition(120)).isTrue();
  }

  @Test
  void containsDataPagePosition_positionInDictionaryPage_returnsFalse() {
    ParquetColumnChunk chunk = dictionaryEncodedColumnChunk("price", 100, 50, 120);

    assertThat(chunk.containsDataPagePosition(110)).isFalse();
  }

  @Test
  void containsDataPagePosition_positionAtEndOffset_returnsFalse() {
    ParquetColumnChunk chunk = dictionaryEncodedColumnChunk("price", 100, 50, 120);

    assertThat(chunk.containsDataPagePosition(150)).isFalse();
  }

  @Test
  void touchesDataPages_overlappingRange_returnsTrue() {
    ParquetColumnChunk chunk = dictionaryEncodedColumnChunk("price", 100, 50, 120);

    assertThat(chunk.touchesDataPages(140, 160)).isTrue();
  }

  @Test
  void touchesDataPages_dictionaryOnlyRange_returnsFalse() {
    ParquetColumnChunk chunk = dictionaryEncodedColumnChunk("price", 100, 50, 120);

    assertThat(chunk.touchesDataPages(100, 120)).isFalse();
  }

  @Test
  void touchesDataPages_emptyRangeInsideDataPages_returnsFalse() {
    ParquetColumnChunk chunk = dictionaryEncodedColumnChunk("price", 100, 50, 120);

    assertThat(chunk.touchesDataPages(130, 130)).isFalse();
  }

  @Test
  void touchesDictionaryPage_overlappingRange_returnsTrue() {
    ParquetColumnChunk chunk = dictionaryEncodedColumnChunk("price", 100, 50, 120);

    assertThat(chunk.touchesDictionaryPage(105, 115)).isTrue();
  }

  @Test
  void touchesDictionaryPage_emptyRangeInsideDictionaryPage_returnsFalse() {
    ParquetColumnChunk chunk = dictionaryEncodedColumnChunk("price", 100, 50, 120);

    assertThat(chunk.touchesDictionaryPage(110, 110)).isFalse();
  }

  @Test
  void touchesDictionaryPage_chunkWithoutDictionary_returnsFalse() {
    ParquetColumnChunk chunk = columnChunk("price", 100, 50);

    assertThat(chunk.touchesDictionaryPage(100, 150)).isFalse();
  }

  @Test
  void touchesDictionaryPage_dataPageOnlyRange_returnsFalse() {
    ParquetColumnChunk chunk = dictionaryEncodedColumnChunk("price", 100, 50, 120);

    assertThat(chunk.touchesDictionaryPage(120, 140)).isFalse();
  }

  private static ParquetColumnChunk createChunk(
      long startOffset, long compressedSize, long dataPageOffset) {
    return ParquetColumnChunk.create(
        "price",
        startOffset,
        compressedSize,
        dataPageOffset,
        /* everyDataPageDictionaryEncoded= */ true);
  }
}
