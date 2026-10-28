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
import static com.google.cloud.gcs.analyticscore.core.prefetch.ParquetLayoutFixtures.rowGroup;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

class ParquetRowGroupTest {

  @Test
  void create_keysEachChunkByItsColumnPath() {
    ParquetRowGroup group = rowGroup(0, columnChunk("address.city", 100, 50));

    assertThat(group.getColumnChunks().keySet()).containsExactly("address.city");
  }

  @Test
  void create_chunksOutOfByteOrder_keepsChunksInByteOrder() {
    ParquetRowGroup group =
        rowGroup(0, columnChunk("price", 300, 50), columnChunk("name", 100, 50));

    assertThat(group.getColumnChunks().keySet()).containsExactly("name", "price").inOrder();
  }

  @Test
  void create_negativeIndex_throwsIllegalArgumentException() {
    ImmutableList<ParquetColumnChunk> chunks = ImmutableList.of(columnChunk("price", 100, 50));

    assertThrows(IllegalArgumentException.class, () -> ParquetRowGroup.create(-1, chunks));
  }

  @Test
  void create_noColumnChunks_throwsIllegalArgumentException() {
    ImmutableList<ParquetColumnChunk> chunks = ImmutableList.of();

    assertThrows(IllegalArgumentException.class, () -> ParquetRowGroup.create(0, chunks));
  }

  @Test
  void create_overlappingColumnChunks_throwsIllegalArgumentException() {
    ImmutableList<ParquetColumnChunk> chunks =
        ImmutableList.of(columnChunk("id", 100, 50), columnChunk("name", 149, 50));

    assertThrows(IllegalArgumentException.class, () -> ParquetRowGroup.create(0, chunks));
  }

  @Test
  void create_duplicateColumnPath_throwsIllegalArgumentException() {
    ImmutableList<ParquetColumnChunk> chunks =
        ImmutableList.of(columnChunk("id", 100, 50), columnChunk("id", 150, 50));

    assertThrows(IllegalArgumentException.class, () -> ParquetRowGroup.create(0, chunks));
  }

  @Test
  void getColumnChunk_knownColumnPath_returnsThatChunk() {
    ParquetColumnChunk price = columnChunk("price", 100, 50);
    ParquetRowGroup group = rowGroup(0, price, columnChunk("name", 150, 30));

    assertThat(group.getColumnChunk("price")).hasValue(price);
  }

  @Test
  void getColumnChunk_unknownColumnPath_returnsEmpty() {
    ParquetRowGroup group = rowGroup(0, columnChunk("price", 100, 50));

    assertThat(group.getColumnChunk("absent")).isEmpty();
  }

  @Test
  void getStartOffset_returnsLowestChunkStartOffset() {
    ParquetRowGroup group =
        rowGroup(0, columnChunk("price", 300, 50), columnChunk("name", 100, 50));

    long startOffset = group.getStartOffset();

    assertThat(startOffset).isEqualTo(100);
  }

  @Test
  void getEndOffset_returnsHighestChunkEndOffset() {
    ParquetRowGroup group =
        rowGroup(0, columnChunk("price", 300, 50), columnChunk("name", 100, 50));

    long endOffset = group.getEndOffset();

    assertThat(endOffset).isEqualTo(350);
  }

  @Test
  void containsPosition_positionWithinBounds_returnsTrue() {
    ParquetRowGroup group = rowGroup(0, columnChunk("price", 100, 50));

    assertThat(group.containsPosition(125)).isTrue();
  }

  @Test
  void containsPosition_positionOutsideBounds_returnsFalse() {
    ParquetRowGroup group = rowGroup(0, columnChunk("price", 100, 50));

    assertThat(group.containsPosition(150)).isFalse();
  }

  @Test
  void containsPosition_positionBeforeStartOffset_returnsFalse() {
    ParquetRowGroup group = rowGroup(0, columnChunk("price", 100, 50));

    assertThat(group.containsPosition(99)).isFalse();
  }

  @Test
  void findDataColumnsInRange_returnsOnlyColumnsWhoseDataPagesOverlap() {
    ParquetRowGroup group =
        rowGroup(
            0,
            columnChunk("id", 100, 50),
            dictionaryEncodedColumnChunk("category", 150, 50, 170),
            columnChunk("value", 200, 50));

    assertThat(group.findDataColumnsInRange(120, 160)).containsExactly("id");
  }

  @Test
  void findDictionaryColumnsInRange_returnsOnlyColumnsWhoseDictionaryPagesOverlap() {
    ParquetRowGroup group =
        rowGroup(
            0,
            columnChunk("id", 100, 50),
            dictionaryEncodedColumnChunk("category", 150, 50, 170),
            dictionaryEncodedColumnChunk("status", 200, 50, 220));

    assertThat(group.findDictionaryColumnsInRange(155, 175)).containsExactly("category");
  }

  @Test
  void findDictionaryColumnsInRange_rangeInDataPagesOnly_returnsEmpty() {
    ParquetRowGroup group = rowGroup(0, dictionaryEncodedColumnChunk("category", 150, 50, 170));

    assertThat(group.findDictionaryColumnsInRange(170, 190)).isEmpty();
  }

  @Test
  void findDataPageRangeAt_positionInDataPage_returnsChunkDataPageRange() {
    ParquetRowGroup group = rowGroup(0, dictionaryEncodedColumnChunk("category", 150, 50, 170));

    assertThat(group.findDataPageRangeAt(180)).hasValue(Range.closedOpen(170L, 200L));
  }

  @Test
  void findDataPageRangeAt_positionInDictionaryPage_returnsEmpty() {
    ParquetRowGroup group = rowGroup(0, dictionaryEncodedColumnChunk("category", 150, 50, 170));

    assertThat(group.findDataPageRangeAt(160)).isEmpty();
  }

  @Test
  void getColumnChunkRanges_returnsRangesOfKnownColumnsInByteOrder() {
    ParquetRowGroup group =
        rowGroup(
            0,
            columnChunk("id", 100, 50),
            columnChunk("category", 150, 50),
            columnChunk("value", 250, 50));

    assertThat(group.getColumnChunkRanges(ImmutableSet.of("value", "id", "category", "absent")))
        .containsExactly(
            Range.closedOpen(100L, 150L),
            Range.closedOpen(150L, 200L),
            Range.closedOpen(250L, 300L))
        .inOrder();
  }

  @Test
  void getColumnChunkRanges_dictionaryEncodedColumn_includesDictionaryPage() {
    ParquetRowGroup group = rowGroup(0, dictionaryEncodedColumnChunk("category", 150, 50, 170));

    assertThat(group.getColumnChunkRanges(ImmutableSet.of("category")))
        .containsExactly(Range.closedOpen(150L, 200L));
  }

  @Test
  void getColumnChunkRanges_noMatchingColumns_returnsEmptyList() {
    ParquetRowGroup group = rowGroup(0, columnChunk("id", 100, 50));

    assertThat(group.getColumnChunkRanges(ImmutableSet.of("absent"))).isEmpty();
  }

  @Test
  void getDictionaryPageRanges_returnsDictionaryPagesOfDictionaryEncodedColumnsOnly() {
    ParquetRowGroup group =
        rowGroup(
            0,
            columnChunk("id", 100, 50),
            dictionaryEncodedColumnChunk("status", 250, 50, 270),
            dictionaryEncodedColumnChunk("category", 150, 50, 170));

    assertThat(group.getDictionaryPageRanges(ImmutableSet.of("id", "status", "category")))
        .containsExactly(Range.closedOpen(150L, 170L), Range.closedOpen(250L, 270L))
        .inOrder();
  }
}
