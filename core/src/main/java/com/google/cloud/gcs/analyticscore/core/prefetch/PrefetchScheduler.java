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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Schedules and cancels speculative range requests for a single open stream against {@link
 * PrefetchBufferCache}.
 *
 * <p>Ranges already present or in-flight in {@link PrefetchBufferCache} are skipped automatically.
 *
 * <p>This class is thread-safe.
 */
final class PrefetchScheduler implements AutoCloseable {

  private final GcsItemId itemId;
  private final PrefetchBufferCache bufferCache;
  private final PrefetchBufferCache.RegisteredStream registeredStream;
  private final int maxConcurrentRanges;
  private final ConcurrentHashMap<Long, GcsObjectRange> inFlightRequests =
      new ConcurrentHashMap<>();

  PrefetchScheduler(GcsItemId itemId, PrefetchBufferCache bufferCache, int maxConcurrentRanges) {
    this.itemId = checkNotNull(itemId, "itemId cannot be null");
    this.bufferCache = checkNotNull(bufferCache, "bufferCache cannot be null");
    this.registeredStream = this.bufferCache.registerStream(this.itemId);
    this.maxConcurrentRanges = maxConcurrentRanges;
  }

  /**
   * Schedules speculative reads for the given byte ranges, ignoring any that are already covered in
   * the cache or beyond the concurrency budget.
   *
   * @param byteRanges exact closed-open byte ranges {@code [startOffset, endOffset)} in ascending
   *     order
   * @param fileSize the size of the object, used to truncate ranges extending past EOF
   * @return {@code true} if every range is now cached or in flight
   */
  boolean schedule(
      VectoredSeekableByteChannel source, Collection<Range<Long>> byteRanges, long fileSize) {
    List<GcsObjectRange> rangesToFetch = new ArrayList<>();
    boolean allCovered = true;
    for (Range<Long> byteRange : byteRanges) {
      if (inFlightRequests.size() >= maxConcurrentRanges) {
        allCovered = false;
        break;
      }
      long startOffset = byteRange.lowerEndpoint();
      long endOffset = Math.min(byteRange.upperEndpoint(), fileSize);
      int length = (int) (endOffset - startOffset);
      if (length > 0) {
        tryRegisterRange(startOffset, length).ifPresent(rangesToFetch::add);
      }
    }

    if (rangesToFetch.isEmpty()) {
      return allCovered;
    }
    return dispatchVectoredRead(source, rangesToFetch) && allCovered;
  }

  private Optional<GcsObjectRange> tryRegisterRange(long startOffset, int length) {
    CompletableFuture<ByteBuffer> fetchFuture = new CompletableFuture<>();
    if (!bufferCache.registerRange(itemId, startOffset, length, fetchFuture)) {
      return Optional.empty();
    }
    GcsObjectRange objectRange =
        GcsObjectRange.builder()
            .setOffset(startOffset)
            .setLength(length)
            .setByteBufferFuture(fetchFuture)
            .build();
    inFlightRequests.put(startOffset, objectRange);
    CompletableFuture<ByteBuffer> unused =
        fetchFuture.whenComplete((data, error) -> inFlightRequests.remove(startOffset));
    return Optional.of(objectRange);
  }

  private static boolean dispatchVectoredRead(
      VectoredSeekableByteChannel source, List<GcsObjectRange> rangesToFetch) {
    try {
      source.readVectored(rangesToFetch, ByteBuffer::allocate);
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
