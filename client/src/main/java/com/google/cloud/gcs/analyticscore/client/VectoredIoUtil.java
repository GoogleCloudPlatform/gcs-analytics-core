/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.gcs.analyticscore.client;

import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.Comparator;
import javax.annotation.Nullable;
import org.checkerframework.checker.nullness.qual.NonNull;

class VectoredIoUtil {
  private static final Comparator<GcsObjectRange> OFFSET_COMPARATOR =
      Comparator.comparingLong(GcsObjectRange::getOffset);

  public static @NonNull ImmutableList<GcsObjectRange> sortGcsObjectRanges(
      @NonNull ImmutableList<GcsObjectRange> ranges) {
    return ImmutableList.sortedCopyOf(OFFSET_COMPARATOR, ranges);
  }

  public static ImmutableList<GcsObjectCombinedRange> mergeGcsObjectRanges(
      ImmutableList<GcsObjectRange> ranges, int maxMergeGap, int maxMergeSize) {
    if (ranges == null || ranges.isEmpty()) {
      return ImmutableList.of();
    }

    ImmutableList<GcsObjectRange> sortedRanges = sortGcsObjectRanges(ranges);
    ImmutableList.Builder<GcsObjectCombinedRange> combinedRanges = ImmutableList.builder();

    GcsObjectRange currentFirstRange = sortedRanges.get(0);
    long currentOffset = currentFirstRange.getOffset();
    long currentEnd = currentOffset + currentFirstRange.getLength();
    ImmutableList.Builder<GcsObjectRange> currentUnderlyingRanges = null;

    for (int i = 1; i < sortedRanges.size(); i++) {
      GcsObjectRange nextRange = sortedRanges.get(i);
      long nextEnd = nextRange.getOffset() + nextRange.getLength();
      long gap = nextRange.getOffset() - currentEnd;
      long mergedEnd = Math.max(currentEnd, nextEnd);
      long potentialMergedSize = mergedEnd - currentOffset;

      if (canMerge(gap, potentialMergedSize, maxMergeGap, maxMergeSize)) {
        if (currentUnderlyingRanges == null) {
          currentUnderlyingRanges = ImmutableList.builder();
          currentUnderlyingRanges.add(currentFirstRange);
        }
        currentUnderlyingRanges.add(nextRange);
        currentEnd = mergedEnd;
      } else {
        combinedRanges.add(
            buildCombinedRange(
                currentFirstRange, currentOffset, currentEnd, currentUnderlyingRanges));
        currentFirstRange = nextRange;
        currentOffset = nextRange.getOffset();
        currentEnd = nextEnd;
        currentUnderlyingRanges = null;
      }
    }

    combinedRanges.add(
        buildCombinedRange(currentFirstRange, currentOffset, currentEnd, currentUnderlyingRanges));

    return combinedRanges.build();
  }

  public static ByteBuffer fetchUnderlyingRangeData(
      ByteBuffer dataBuffer, GcsObjectCombinedRange combinedRange, GcsObjectRange underlyingRange) {
    int requestOffset = (int) (underlyingRange.getOffset() - combinedRange.getOffset());
    int requestLength = underlyingRange.getLength();
    ByteBuffer result = dataBuffer.slice();
    result.position(requestOffset);
    result.limit(requestOffset + requestLength);
    return result;
  }

  private static @NonNull GcsObjectCombinedRange buildCombinedRange(
      @NonNull GcsObjectRange firstRange,
      long offset,
      long end,
      @Nullable ImmutableList.Builder<GcsObjectRange> underlyingRanges) {
    return GcsObjectCombinedRange.builder()
        .setOffset(offset)
        .setLength((int) (end - offset))
        .setUnderlyingRanges(
            underlyingRanges != null ? underlyingRanges.build() : ImmutableList.of(firstRange))
        .build();
  }

  private static boolean canMerge(
      long gap, long potentialMergedSize, long maxMergeGap, long maxMergeSize) {
    return gap < maxMergeGap && potentialMergedSize <= maxMergeSize;
  }
}
