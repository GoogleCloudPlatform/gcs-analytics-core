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
import java.util.function.Consumer;

/** Utilities shared by vectored read implementations. */
public final class VectoredIoUtil {
  private VectoredIoUtil() {}

  /**
   * Releases an unpublished buffer, suppressing cleanup failures onto the original failure.
   *
   * @param buffer the allocated buffer still owned by the read implementation
   * @param release the caller's release callback
   * @param failure the original read failure
   */
  public static void releaseOnFailure(
      ByteBuffer buffer, Consumer<ByteBuffer> release, Throwable failure) {
    try {
      release.accept(buffer);
    } catch (Throwable releaseException) {
      if (releaseException != failure) {
        failure.addSuppressed(releaseException);
      }
    }
  }

  static ImmutableList<GcsObjectRange> sortGcsObjectRanges(
      ImmutableList<GcsObjectRange> ranges) {
    return ranges.stream()
        .sorted(Comparator.comparingLong(GcsObjectRange::getOffset))
        .collect(ImmutableList.toImmutableList());
  }

  static ImmutableList<GcsObjectCombinedRange> mergeGcsObjectRanges(
      ImmutableList<GcsObjectRange> ranges, int maxMergeGap, int maxMergeSize) {
    if (ranges == null || ranges.isEmpty()) {
      return ImmutableList.of();
    }

    ImmutableList<GcsObjectRange> sortedRanges = sortGcsObjectRanges(ranges);
    ImmutableList.Builder<GcsObjectCombinedRange> combinedRanges = ImmutableList.builder();
    GcsObjectCombinedRange currentCombinedRange = null;

    for (GcsObjectRange nextRange : sortedRanges) {
      if (currentCombinedRange == null) {
        currentCombinedRange = createCombinedRangeFromSingleRange(nextRange);
        continue;
      }

      long currentEnd = currentCombinedRange.getOffset() + currentCombinedRange.getLength();
      long nextEnd = nextRange.getOffset() + nextRange.getLength();
      long gap = nextRange.getOffset() - currentEnd;
      long potentialMergedSize = Math.max(currentEnd, nextEnd) - currentCombinedRange.getOffset();

      if (canMerge(gap, potentialMergedSize, maxMergeGap, maxMergeSize)) {
        currentCombinedRange = currentCombinedRange.union(nextRange);
      } else {
        combinedRanges.add(currentCombinedRange);
        currentCombinedRange = createCombinedRangeFromSingleRange(nextRange);
      }
    }

    if (currentCombinedRange != null) {
      combinedRanges.add(currentCombinedRange);
    }

    return combinedRanges.build();
  }

  static ByteBuffer fetchUnderlyingRangeData(
      ByteBuffer dataBuffer, GcsObjectCombinedRange combinedRange, GcsObjectRange underlyingRange) {
    int requestOffset = (int) (underlyingRange.getOffset() - combinedRange.getOffset());
    int requestLength = underlyingRange.getLength();
    ByteBuffer result = dataBuffer.slice();
    result.position(requestOffset);
    result.limit(requestOffset + requestLength);
    return result;
  }

  private static GcsObjectCombinedRange createCombinedRangeFromSingleRange(GcsObjectRange range) {
    return GcsObjectCombinedRange.builder()
        .setOffset(range.getOffset())
        .setLength(range.getLength())
        .setUnderlyingRanges(ImmutableList.of(range))
        .build();
  }

  private static boolean canMerge(
      long gap, long potentialMergedSize, long maxMergeGap, long maxMergeSize) {
    return gap < maxMergeGap && potentialMergedSize <= maxMergeSize;
  }
}
