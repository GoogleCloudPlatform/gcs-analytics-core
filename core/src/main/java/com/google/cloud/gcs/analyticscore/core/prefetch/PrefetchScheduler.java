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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.PrefetchBufferCache;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Schedules and cancels speculative range requests for a single open stream against {@link
 * PrefetchBufferCache}.
 *
 * <p>A range is skipped when one range in {@link PrefetchBufferCache}, cached or in flight, covers
 * all of it. A range that several cached ranges cover only together is fetched again.
 *
 * <p>This class is thread-safe.
 */
final class PrefetchScheduler implements AutoCloseable {

  /**
   * The longest range that one request fetches into one buffer. Some JVMs reserve header words in
   * every array, so this is the largest array length that all of them can allocate.
   */
  private static final long MAX_RANGE_BYTES = Integer.MAX_VALUE - 8;

  private final GcsItemId itemId;
  private final PrefetchBufferCache bufferCache;
  private final PrefetchBufferCache.RegisteredStream registeredStream;
  // The fetches that no reader waits on yet. Futures compare by identity, so two fetches of the
  // same bytes are two entries.
  private final Set<CompletableFuture<ByteBuffer>> cancellableFetches =
      ConcurrentHashMap.newKeySet();

  PrefetchScheduler(GcsItemId itemId, PrefetchBufferCache bufferCache) {
    this.itemId = checkNotNull(itemId, "itemId cannot be null");
    this.bufferCache = checkNotNull(bufferCache, "bufferCache cannot be null");
    this.registeredStream = bufferCache.registerStream(itemId);
  }

  /**
   * Schedules speculative reads for the bytes of the given ranges that lie inside the file,
   * skipping any range that the cache already covers.
   *
   * <p>Touching ranges are fetched as one range, and a range longer than {@code MAX_RANGE_BYTES} is
   * fetched as consecutive ranges that each fit in one buffer.
   *
   * @param byteRanges closed-open byte ranges {@code [startOffset, endOffset)}, in any order
   * @param fileSize the size of the object; bytes outside {@code [0, fileSize)} are not fetched
   * @return {@code true} if every range inside the file is cached or in flight when this method
   *     returns, or {@code false} if a fetch failed or the cache did not accept a range
   * @throws IllegalArgumentException if {@code fileSize} is negative or a range is not closed-open
   */
  boolean schedule(
      VectoredSeekableByteChannel source, Collection<Range<Long>> byteRanges, long fileSize) {
    checkNotNull(source, "source cannot be null");
    ImmutableList<Range<Long>> rangesInFile =
        ByteRangeMerger.merge(byteRanges, fileSize, MAX_RANGE_BYTES);
    List<GcsObjectRange> rangesToFetch = new ArrayList<>();
    // A cache reader waits on a registered range without a timeout, so every range registered here
    // must complete, even when scheduling fails part way.
    try {
      for (Range<Long> range : rangesInFile) {
        tryRegisterRange(range.lowerEndpoint(), lengthOf(range)).ifPresent(rangesToFetch::add);
      }
      if (!rangesToFetch.isEmpty()) {
        source.prefetchVectored(rangesToFetch, ByteBuffer::allocate);
      }
    } catch (IOException | RuntimeException e) {
      failIncompleteRanges(rangesToFetch, e);
      return false;
    } catch (Error e) {
      failIncompleteRanges(rangesToFetch, e);
      throw e;
    }
    return rangesInFile.stream().allMatch(this::isCovered);
  }

  private Optional<GcsObjectRange> tryRegisterRange(long startOffset, int length) {
    CompletableFuture<ByteBuffer> fetchFuture = new CompletableFuture<>();
    Runnable stopTracking = () -> cancellableFetches.remove(fetchFuture);
    // The fetch is tracked before the cache can hand the range to a reader. A reader that starts
    // waiting on the fetch stops the tracking, so cancelAll() leaves that fetch running.
    cancellableFetches.add(fetchFuture);
    if (!registeredStream.registerRange(startOffset, length, fetchFuture, stopTracking)) {
      stopTracking.run();
      return Optional.empty();
    }
    CompletableFuture<ByteBuffer> unused =
        fetchFuture.whenComplete((data, error) -> stopTracking.run());
    return Optional.of(
        GcsObjectRange.builder()
            .setOffset(startOffset)
            .setLength(length)
            .setByteBufferFuture(fetchFuture)
            .build());
  }

  private static void failIncompleteRanges(List<GcsObjectRange> ranges, Throwable failure) {
    for (GcsObjectRange range : ranges) {
      range.getByteBufferFuture().completeExceptionally(failure);
    }
  }

  private boolean isCovered(Range<Long> range) {
    return bufferCache.isCovered(itemId, range.lowerEndpoint(), lengthOf(range));
  }

  private static int lengthOf(Range<Long> range) {
    return Math.toIntExact(range.upperEndpoint() - range.lowerEndpoint());
  }

  /** Cancels every outstanding speculative request that no reader waits on. */
  void cancelAll() {
    for (CompletableFuture<ByteBuffer> fetchFuture : cancellableFetches) {
      // A fetch that a reader started waiting on during this loop is no longer in the set.
      if (cancellableFetches.remove(fetchFuture)) {
        fetchFuture.cancel(/* mayInterruptIfRunning= */ false);
      }
    }
  }

  @Override
  public void close() {
    // The stream closes first, so a schedule() call that runs at the same time cannot register a
    // range after the cancel.
    registeredStream.close();
    cancelAll();
  }
}
