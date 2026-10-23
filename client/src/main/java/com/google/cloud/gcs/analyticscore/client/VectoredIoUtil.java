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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nonnull;

class VectoredIoUtil {
  public static ImmutableList<GcsObjectRange> sortGcsObjectRanges(
      ImmutableList<GcsObjectRange> ranges) {
    return ranges.stream()
        .sorted(Comparator.comparingLong(GcsObjectRange::getOffset))
        .collect(ImmutableList.toImmutableList());
  }

  /**
   * Settles ranges that must never be sent to GCS and returns, in their original order, the ranges
   * that still need to be read.
   *
   * <ul>
   *   <li>A zero-length range completes immediately with an empty buffer. Issuing it would cost a
   *       request on the HTTP path and, on the bidi path, {@code RangeSpec.of(offset, 0)} means
   *       "read until EOF", so the caller would receive the object tail instead of nothing.
   *   <li>A negative offset completes with {@link EOFException}, mirroring {@code position(long)}.
   *   <li>A negative length completes with {@link IllegalArgumentException}.
   * </ul>
   *
   * <p>Empty results are built with {@link ByteBuffer#allocate(int)} rather than the caller's
   * allocator so pooled allocators are never asked for, or expected to track, empty buffers.
   */
  @Nonnull
  static ImmutableList<GcsObjectRange> completeDegenerateRanges(
      @Nonnull List<GcsObjectRange> ranges, @Nonnull GcsItemId itemId) {
    checkNotNull(ranges, "ranges cannot be null");
    checkNotNull(itemId, "itemId cannot be null");
    ImmutableList.Builder<GcsObjectRange> pendingRanges =
        ImmutableList.builderWithExpectedSize(ranges.size());
    for (GcsObjectRange range : ranges) {
      checkNotNull(range, "range cannot be null");
      if (range.getOffset() < 0) {
        range
            .getByteBufferFuture()
            .completeExceptionally(
                new EOFException(
                    String.format(
                        "Invalid seek offset: position value (%d) must be >= 0 for '%s'",
                        range.getOffset(), itemId)));
      } else if (range.getLength() < 0) {
        range
            .getByteBufferFuture()
            .completeExceptionally(
                new IllegalArgumentException(
                    String.format(
                        "Invalid range length: length value (%d) must be >= 0 for '%s'",
                        range.getLength(), itemId)));
      } else if (range.getLength() == 0) {
        range.getByteBufferFuture().complete(ByteBuffer.allocate(0));
      } else {
        pendingRanges.add(range);
      }
    }
    return pendingRanges.build();
  }

  public static ImmutableList<GcsObjectCombinedRange> mergeGcsObjectRanges(
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

  public static ByteBuffer fetchUnderlyingRangeData(
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
