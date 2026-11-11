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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Merges the byte ranges that prefetching fetches, so that touching ranges are fetched as one range
 * and no range is longer than a limit.
 */
final class ByteRangeMerger {

  private ByteRangeMerger() {}

  /**
   * Merges ranges that touch or overlap, then cuts every merged range longer than {@code
   * maxRangeBytes} into consecutive ranges of at most that length.
   *
   * <p>For example, with a limit of 100 bytes, {@code [0, 30)} and {@code [30, 250)} merge into
   * {@code [0, 250)}, which comes back as {@code [0, 100)}, {@code [100, 200)}, and {@code [200,
   * 250)}.
   *
   * @param sortedRanges closed-open ranges sorted by their start
   * @param maxRangeBytes the length of the longest range to return
   * @return the merged ranges, sorted by their start
   */
  static ImmutableList<Range<Long>> merge(
      Collection<Range<Long>> sortedRanges, long maxRangeBytes) {
    checkArgument(maxRangeBytes > 0, "maxRangeBytes must be positive: %s", maxRangeBytes);
    return splitLongRanges(mergeTouchingRanges(sortedRanges), maxRangeBytes);
  }

  private static List<Range<Long>> mergeTouchingRanges(Collection<Range<Long>> sortedRanges) {
    List<Range<Long>> mergedRanges = new ArrayList<>();
    for (Range<Long> range : sortedRanges) {
      int lastIndex = mergedRanges.size() - 1;
      if (lastIndex >= 0 && mergedRanges.get(lastIndex).isConnected(range)) {
        mergedRanges.set(lastIndex, mergedRanges.get(lastIndex).span(range));
      } else {
        mergedRanges.add(range);
      }
    }
    return mergedRanges;
  }

  private static ImmutableList<Range<Long>> splitLongRanges(
      List<Range<Long>> ranges, long maxRangeBytes) {
    ImmutableList.Builder<Range<Long>> splitRanges = ImmutableList.builder();
    for (Range<Long> range : ranges) {
      long start = range.lowerEndpoint();
      long end = range.upperEndpoint();
      while (end - start > maxRangeBytes) {
        splitRanges.add(Range.closedOpen(start, start + maxRangeBytes));
        start += maxRangeBytes;
      }
      splitRanges.add(Range.closedOpen(start, end));
    }
    return splitRanges.build();
  }
}
