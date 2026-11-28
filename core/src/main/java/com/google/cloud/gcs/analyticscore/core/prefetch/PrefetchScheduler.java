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
import com.google.common.collect.Range;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Schedules and cancels speculative range requests for a single open stream against {@link
 * PrefetchBufferCache}.
 *
 * <p>Ranges already present or in-flight in {@link PrefetchBufferCache} are skipped automatically.
 *
 * <p>This class is thread-safe.
 */
final class PrefetchScheduler implements AutoCloseable {

  /**
   * The longest range that one request fetches into one buffer. Some JVMs reserve header words in
   * every array, so this is the largest array length that all of them can allocate.
   */
  private static final long MAX_RANGE_BYTES = Integer.MAX_VALUE - 8;

  private static final Comparator<Range<Long>> BY_START_THEN_END =
      Comparator.<Range<Long>>comparingLong(Range::lowerEndpoint)
          .thenComparingLong(Range::upperEndpoint);

  private final GcsItemId itemId;
  private final PrefetchBufferCache.RegisteredStream registeredStream;
  private final ConcurrentSkipListMap<Range<Long>, GcsObjectRange> inFlightRequests =
      new ConcurrentSkipListMap<>(BY_START_THEN_END);

  PrefetchScheduler(GcsItemId itemId, PrefetchBufferCache bufferCache) {
    this.itemId = checkNotNull(itemId, "itemId cannot be null");
    this.registeredStream =
        checkNotNull(bufferCache, "bufferCache cannot be null").registerStream(this.itemId);
  }

  /**
   * Schedules speculative reads for the bytes of the given ranges that lie inside the file,
   * ignoring any that are already covered in the cache.
   *
   * <p>Touching ranges are fetched as one range, and a range longer than {@code MAX_RANGE_BYTES} is
   * fetched as consecutive ranges that each fit in one buffer.
   *
   * @param byteRanges closed-open byte ranges {@code [startOffset, endOffset)}, in any order
   * @param fileSize the size of the object; bytes outside {@code [0, fileSize)} are not fetched
   * @return {@code true} if every range is now cached or in flight
   * @throws IllegalArgumentException if {@code fileSize} is negative or a range is not closed-open
   */
  boolean schedule(
      VectoredSeekableByteChannel source, Collection<Range<Long>> byteRanges, long fileSize) {
    List<GcsObjectRange> rangesToFetch = new ArrayList<>();
    for (Range<Long> byteRange : ByteRangeMerger.merge(byteRanges, fileSize, MAX_RANGE_BYTES)) {
      long startOffset = byteRange.lowerEndpoint();
      int length = Math.toIntExact(byteRange.upperEndpoint() - startOffset);
      tryRegisterRange(startOffset, length).ifPresent(rangesToFetch::add);
    }

    if (rangesToFetch.isEmpty()) {
      return true;
    }
    return dispatchVectoredRead(source, rangesToFetch);
  }

  private Optional<GcsObjectRange> tryRegisterRange(long startOffset, int length) {
    CompletableFuture<ByteBuffer> fetchFuture = new CompletableFuture<>();
    GcsObjectRange objectRange =
        GcsObjectRange.builder()
            .setOffset(startOffset)
            .setLength(length)
            .setByteBufferFuture(fetchFuture)
            .build();
    Range<Long> key = Range.closedOpen(startOffset, startOffset + length);
    if (!registeredStream.registerRange(
        startOffset, length, fetchFuture, () -> inFlightRequests.remove(key, objectRange))) {
      return Optional.empty();
    }
    inFlightRequests.put(key, objectRange);
    CompletableFuture<ByteBuffer> unused =
        fetchFuture.whenComplete((data, error) -> inFlightRequests.remove(key, objectRange));
    return Optional.of(objectRange);
  }

  private static boolean dispatchVectoredRead(
      VectoredSeekableByteChannel source, List<GcsObjectRange> rangesToFetch) {
    try {
      source.prefetchVectored(rangesToFetch, ByteBuffer::allocate);
    } catch (IOException | RuntimeException e) {
      for (GcsObjectRange range : rangesToFetch) {
        range.getByteBufferFuture().completeExceptionally(e);
      }
      return false;
    }
    return rangesToFetch.stream()
        .noneMatch(range -> range.getByteBufferFuture().isCompletedExceptionally());
  }

  /** Cancels every outstanding speculative request. */
  void cancelAll() {
    for (GcsObjectRange objectRange : inFlightRequests.values()) {
      objectRange.getByteBufferFuture().cancel(/* mayInterruptIfRunning= */ false);
    }
    inFlightRequests.clear();
  }

  @Override
  public void close() {
    cancelAll();
    registeredStream.close();
  }
}
