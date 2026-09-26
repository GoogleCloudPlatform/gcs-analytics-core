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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

public class ParquetRowGroupTest {

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
  void getStartOffset_rowGroupWithoutChunks_returnsZero() {
    ParquetRowGroup group = rowGroup(0);

    long startOffset = group.getStartOffset();

    assertThat(startOffset).isEqualTo(0);
  }

  @Test
  void getEndOffset_rowGroupWithoutChunks_returnsZero() {
    ParquetRowGroup group = rowGroup(0);

    long endOffset = group.getEndOffset();

    assertThat(endOffset).isEqualTo(0);
  }

  @Test
  void addColumnChunk_keysTheChunkByItsColumnPath() {
    ParquetRowGroup group = rowGroup(0, columnChunk("address.city", 100, 50));

    assertThat(group.getColumnChunks().keySet()).containsExactly("address.city");
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
  void touchesDictionaryPage_rangeOverlapsDictionaryPage_returnsTrue() {
    ParquetRowGroup group = rowGroup(0, dictionaryEncodedColumnChunk("category", 150, 50, 170));

    assertThat(group.touchesDictionaryPage(155, 165)).isTrue();
  }

  @Test
  void touchesDictionaryPage_rangeOutsideDictionaryPages_returnsFalse() {
    ParquetRowGroup group = rowGroup(0, dictionaryEncodedColumnChunk("category", 150, 50, 170));

    assertThat(group.touchesDictionaryPage(170, 190)).isFalse();
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
  void getCoalescedColumnRanges_mergesTouchingColumnsAndSeparatesDisjointColumns() {
    ParquetRowGroup group =
        rowGroup(
            0,
            columnChunk("id", 100, 50),
            columnChunk("category", 150, 50),
            columnChunk("value", 250, 50));

    assertThat(group.getCoalescedColumnRanges(ImmutableSet.of("value", "id", "category", "absent")))
        .containsExactly(Range.closedOpen(100L, 200L), Range.closedOpen(250L, 300L))
        .inOrder();
  }

  @Test
  void getCoalescedColumnRanges_noMatchingColumns_returnsEmptyList() {
    ParquetRowGroup group = rowGroup(0, columnChunk("id", 100, 50));

    assertThat(group.getCoalescedColumnRanges(ImmutableSet.of("absent"))).isEmpty();
  }
}
