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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

class ByteRangeMergerTest {

  private static final long MAX_RANGE_BYTES = 100;

  @Test
  void merge_touchingRanges_mergesThemAndKeepsSeparatedRangesApart() {
    ImmutableList<Range<Long>> ranges =
        ImmutableList.of(
            Range.closedOpen(100L, 150L),
            Range.closedOpen(150L, 200L),
            Range.closedOpen(250L, 300L));

    assertThat(ByteRangeMerger.merge(ranges, /* maxRangeBytes= */ 1_000))
        .containsExactly(Range.closedOpen(100L, 200L), Range.closedOpen(250L, 300L))
        .inOrder();
  }

  @Test
  void merge_overlappingRanges_mergesThemIntoTheirSpan() {
    ImmutableList<Range<Long>> ranges =
        ImmutableList.of(Range.closedOpen(100L, 160L), Range.closedOpen(150L, 200L));

    assertThat(ByteRangeMerger.merge(ranges, MAX_RANGE_BYTES))
        .containsExactly(Range.closedOpen(100L, 200L));
  }

  @Test
  void merge_noRanges_returnsEmptyList() {
    assertThat(ByteRangeMerger.merge(ImmutableList.of(), MAX_RANGE_BYTES)).isEmpty();
  }

  @Test
  void merge_rangeLongerThanMax_splitsItIntoConsecutiveRanges() {
    ImmutableList<Range<Long>> ranges = ImmutableList.of(Range.closedOpen(100L, 350L));

    assertThat(ByteRangeMerger.merge(ranges, MAX_RANGE_BYTES))
        .containsExactly(
            Range.closedOpen(100L, 200L),
            Range.closedOpen(200L, 300L),
            Range.closedOpen(300L, 350L))
        .inOrder();
  }

  @Test
  void merge_touchingRangesLongerThanMaxTogether_mergesThemBeforeSplitting() {
    ImmutableList<Range<Long>> ranges =
        ImmutableList.of(Range.closedOpen(0L, 30L), Range.closedOpen(30L, 250L));

    assertThat(ByteRangeMerger.merge(ranges, MAX_RANGE_BYTES))
        .containsExactly(
            Range.closedOpen(0L, 100L), Range.closedOpen(100L, 200L), Range.closedOpen(200L, 250L))
        .inOrder();
  }

  @Test
  void merge_touchingRangesExactlyMaxTogether_mergesThemIntoOneRange() {
    ImmutableList<Range<Long>> ranges =
        ImmutableList.of(Range.closedOpen(0L, 50L), Range.closedOpen(50L, 100L));

    assertThat(ByteRangeMerger.merge(ranges, MAX_RANGE_BYTES))
        .containsExactly(Range.closedOpen(0L, 100L));
  }

  @Test
  void merge_nonPositiveMaxRangeBytes_throwsIllegalArgumentException() {
    ImmutableList<Range<Long>> ranges = ImmutableList.of(Range.closedOpen(0L, 50L));

    assertThrows(
        IllegalArgumentException.class,
        () -> ByteRangeMerger.merge(ranges, /* maxRangeBytes= */ 0));
  }
}
