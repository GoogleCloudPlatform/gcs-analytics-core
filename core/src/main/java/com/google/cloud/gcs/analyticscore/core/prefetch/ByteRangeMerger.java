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
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.Collection;

/**
 * Turns the byte ranges that prefetching asks for into the ranges it fetches: only bytes inside the
 * file, with touching ranges fetched as one range and no range longer than a limit.
 */
final class ByteRangeMerger {

  private ByteRangeMerger() {}

  /**
   * Keeps the bytes of {@code ranges} that lie inside the file, merges ranges that touch or
   * overlap, then cuts every merged range longer than {@code maxRangeBytes} into consecutive ranges
   * of at most that length.
   *
   * <p>For example, with a limit of 100 bytes, {@code [0, 30)} and {@code [30, 250)} merge into
   * {@code [0, 250)}, which comes back as {@code [0, 100)}, {@code [100, 200)}, and {@code [200,
   * 250)}.
   *
   * @param ranges closed-open ranges, in any order
   * @param fileSize the size of the file; bytes outside {@code [0, fileSize)} are dropped
   * @param maxRangeBytes the length of the longest range to return
   * @return the merged ranges, sorted by their start
   */
  static ImmutableList<Range<Long>> merge(
      Collection<Range<Long>> ranges, long fileSize, long maxRangeBytes) {
    checkArgument(fileSize >= 0, "fileSize must not be negative: %s", fileSize);
    checkArgument(maxRangeBytes > 0, "maxRangeBytes must be positive: %s", maxRangeBytes);
    RangeSet<Long> mergedRanges = TreeRangeSet.create();
    for (Range<Long> range : ranges) {
      checkArgument(isClosedOpen(range), "range must be closed-open: %s", range);
      mergedRanges.add(range);
    }
    RangeSet<Long> rangesInFile = mergedRanges.subRangeSet(Range.closedOpen(0L, fileSize));
    return splitLongRanges(rangesInFile.asRanges(), maxRangeBytes);
  }

  private static boolean isClosedOpen(Range<Long> range) {
    return range.hasLowerBound()
        && range.hasUpperBound()
        && range.equals(Range.closedOpen(range.lowerEndpoint(), range.upperEndpoint()));
  }

  private static ImmutableList<Range<Long>> splitLongRanges(
      Iterable<Range<Long>> ranges, long maxRangeBytes) {
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
