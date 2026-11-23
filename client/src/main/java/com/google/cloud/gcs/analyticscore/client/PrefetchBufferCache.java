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

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.math.LongMath;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;

/**
 * Holds speculatively fetched bytes as exact byte ranges addressed by absolute file offset, and
 * serves single-buffer and vectored reads from those ranges.
 *
 * <p>Ranges are offered through a {@link RegisteredStream}, so every stored range belongs to an
 * object with at least one open stream. The total length of stored ranges, including ranges that
 * are still downloading, never exceeds the configured byte limit. A range that does not fit is not
 * stored, and its reader reads those bytes from the object instead. A range that is evicted while
 * it is still downloading stops counting toward the limit, and its download is not cancelled.
 *
 * <p>A small range may be cached alongside a larger range that contains it, for example a
 * dictionary page next to the whole column chunk. A read is served by a covering range that is
 * already downloaded if there is one, and otherwise by the smallest range covering it, so it never
 * waits on a download it does not need. Bytes that a read copies are marked as consumed on every
 * range that holds them.
 *
 * <p>A stored range leaves the cache when every one of its bytes has been consumed, when its
 * download fails or returns the wrong number of bytes, or when the last stream on its object
 * closes. A range that has been idle for the configured time is evicted when the cache needs room
 * for a new range.
 *
 * <p>This class is thread-safe.
 */
public final class PrefetchBufferCache {

  private static final Comparator<RangeKey> BY_START_THEN_END =
      Comparator.comparingLong(RangeKey::getStartOffset).thenComparingLong(RangeKey::getEndOffset);

  private final long maxSizeBytes;
  private final long ttlNanos;
  private final Telemetry telemetry;
  private final LongSupplier nanoClock;
  private final AtomicLong reservedBytes = new AtomicLong();

  // Every change to an item's ranges runs inside itemStates.compute for that item. Code that can
  // call back into this class (futures, telemetry) never runs inside that function.
  private final ConcurrentHashMap<GcsItemId, ItemState> itemStates = new ConcurrentHashMap<>();

  /**
   * Creates a cache storing exact byte ranges.
   *
   * @param maxSizeBytes the maximum total length of stored ranges, including in-flight ranges
   * @param ttlSeconds how long after a range was registered, downloaded, or last read it may be
   *     evicted to make room for a new range
   * @param telemetry telemetry recorder for prefetch cache metrics
   */
  public PrefetchBufferCache(long maxSizeBytes, long ttlSeconds, Telemetry telemetry) {
    this(maxSizeBytes, ttlSeconds, telemetry, System::nanoTime);
  }

  @VisibleForTesting
  PrefetchBufferCache(
      long maxSizeBytes, long ttlSeconds, Telemetry telemetry, LongSupplier nanoClock) {
    checkArgument(maxSizeBytes > 0, "maxSizeBytes must be positive");
    checkArgument(ttlSeconds > 0, "ttlSeconds must be positive");
    this.maxSizeBytes = maxSizeBytes;
    this.ttlNanos = TimeUnit.SECONDS.toNanos(ttlSeconds);
    this.telemetry = checkNotNull(telemetry, "telemetry cannot be null");
    this.nanoClock = checkNotNull(nanoClock, "nanoClock cannot be null");
  }

  /**
   * Registers an open stream reading {@code itemId} and returns a handle through which the stream
   * offers ranges. The item's cached ranges are evicted when its last open stream closes.
   */
  public RegisteredStream registerStream(GcsItemId itemId) {
    checkNotNull(itemId, "itemId cannot be null");
    itemStates.compute(
        itemId,
        (id, existingState) -> {
          ItemState state = existingState != null ? existingState : new ItemState();
          state.openStreams++;
          return state;
        });
    return new RegisteredStream(itemId);
  }

  /**
   * Copies cached bytes for {@code itemId} starting at {@code position} into {@code dst} and
   * returns the number of bytes served. If the covering range is still downloading, the reader
   * waits for it. A read that spans adjacent ranges is stitched from them.
   *
   * <p>Returns {@code 0} if no cached range holds the byte at {@code position}, if every range
   * holding it fails to download, or if the thread is interrupted while waiting.
   *
   * <p>Records a cache hit when it serves any bytes. A caller that expected the bytes to be cached
   * reports a miss with {@link #recordCacheMiss()}.
   */
  public int serveFromCache(GcsItemId itemId, long position, ByteBuffer dst) {
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(dst, "dst cannot be null");
    checkArgument(position >= 0, "position %s must be non-negative", position);
    ItemState state = itemStates.get(itemId);
    if (state == null) {
      return 0;
    }
    int servedBytes = copyFromRanges(state, position, dst);
    if (servedBytes > 0) {
      telemetry.recordMetric(
          GcsAnalyticsCoreTelemetryConstants.Metric.PREFETCH_CACHE_HIT, 1L, Collections.emptyMap());
    }
    return servedBytes;
  }

  /** Records that a read expected to find its bytes in this cache and found none of them. */
  public void recordCacheMiss() {
    telemetry.recordMetric(
        GcsAnalyticsCoreTelemetryConstants.Metric.PREFETCH_CACHE_MISS, 1L, Collections.emptyMap());
  }

  /**
   * Serves the ranges of a vectored read whose bytes are all cached for {@code itemId}, and returns
   * the ranges it does not serve so the caller reads them from the object.
   *
   * <p>A served range's future completes with a buffer from {@code allocate} once the downloads
   * holding its bytes finish, and fails if {@code allocate} fails. A range that spans adjacent
   * cached ranges is stitched from them. If a download fails and no other cached range holds the
   * bytes, the range is passed to {@code onDownloadFailed} so the caller can read it from the
   * object. If {@code onDownloadFailed} throws, the range's future fails with that exception.
   *
   * <p>{@code allocate} and {@code onDownloadFailed} run on the calling thread or on a thread that
   * completes a download, so they must be thread-safe and must not block.
   *
   * <p>Records a cache hit for each range completed from the cache. Never records a miss. A caller
   * that expected the bytes to be cached reports misses with {@link #recordCacheMiss()}.
   */
  public List<GcsObjectRange> serveVectoredFromCache(
      GcsItemId itemId,
      List<GcsObjectRange> ranges,
      IntFunction<ByteBuffer> allocate,
      Consumer<GcsObjectRange> onDownloadFailed) {
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(ranges, "ranges cannot be null");
    checkNotNull(allocate, "allocate cannot be null");
    checkNotNull(onDownloadFailed, "onDownloadFailed cannot be null");
    ItemState state = itemStates.get(itemId);
    List<GcsObjectRange> unservedRanges = new ArrayList<>();
    for (GcsObjectRange range : ranges) {
      List<Segment> segments = state == null ? null : findSegments(state, range);
      if (segments == null) {
        unservedRanges.add(range);
      } else {
        completeFromSegments(state, range, segments, allocate, onDownloadFailed);
      }
    }
    return Collections.unmodifiableList(unservedRanges);
  }

  /**
   * Returns whether a single cached range (in-flight or resident) completely covers {@code [offset,
   * offset + length)}.
   */
  public boolean isCovered(GcsItemId itemId, long offset, int length) {
    checkNotNull(itemId, "itemId cannot be null");
    ItemState state = itemStates.get(itemId);
    return state != null && length >= 0 && findCovering(state, offset, length) != null;
  }

  /** Discards all cached ranges. Open streams stay registered. */
  public void invalidateAll() {
    for (GcsItemId itemId : itemStates.keySet()) {
      itemStates.computeIfPresent(
          itemId,
          (id, state) -> {
            evictAll(state);
            return state;
          });
    }
  }

  @VisibleForTesting
  long getReservedBytes() {
    return reservedBytes.get();
  }

  private boolean offer(Entry entry, AtomicBoolean streamClosed) {
    if (streamClosed.get()
        || entry.range.getLength() > maxSizeBytes
        || downloadFailed(entry.range)) {
      return false;
    }
    makeRoomFor(entry);
    AtomicBoolean admitted = new AtomicBoolean();
    itemStates.computeIfPresent(
        entry.itemId,
        (id, state) -> {
          // Read again inside the compute, so a stream that closes during this call never stores
          // a range.
          admitted.set(!streamClosed.get() && admit(state, entry));
          return state;
        });
    if (admitted.get()) {
      watchDownload(entry);
    }
    return admitted.get();
  }

  private static boolean downloadFailed(CachedRange range) {
    CompletableFuture<ByteBuffer> future = range.getFuture();
    return future.isDone()
        && (future.isCompletedExceptionally() || !holdsWholeRange(future.getNow(null), range));
  }

  private void makeRoomFor(Entry entry) {
    CachedRange range = entry.range;
    if (!hasRoomFor(range.getLength())
        && !isCovered(entry.itemId, range.getStartOffset(), range.getLength())) {
      evictIdleRanges();
    }
  }

  private boolean admit(ItemState state, Entry entry) {
    CachedRange range = entry.range;
    boolean covered = findCovering(state, range.getStartOffset(), range.getLength()) != null;
    if (covered || !tryReserve(range.getLength())) {
      return false;
    }
    state.maxRangeLength = Math.max(state.maxRangeLength, range.getLength());
    state.index.put(entry.key, entry);
    return true;
  }

  private boolean hasRoomFor(int length) {
    return length <= maxSizeBytes - reservedBytes.get();
  }

  private boolean tryReserve(int length) {
    long reserved;
    do {
      reserved = reservedBytes.get();
      if (length > maxSizeBytes - reserved) {
        return false;
      }
    } while (!reservedBytes.compareAndSet(reserved, reserved + length));
    return true;
  }

  private void evictIdleRanges() {
    long now = nanoClock.getAsLong();
    for (ItemState state : itemStates.values()) {
      for (Entry entry : state.index.values()) {
        if (now - entry.lastUsedNanos >= ttlNanos) {
          evict(entry);
        }
      }
    }
  }

  private void watchDownload(Entry entry) {
    CompletableFuture<ByteBuffer> unused =
        entry
            .range
            .getFuture()
            .whenComplete(
                (buffer, error) -> {
                  if (error == null && holdsWholeRange(buffer, entry.range)) {
                    entry.lastUsedNanos = nanoClock.getAsLong();
                  } else {
                    evict(entry);
                  }
                });
  }

  private static boolean holdsWholeRange(@Nullable ByteBuffer buffer, CachedRange range) {
    return buffer != null && buffer.remaining() == range.getLength();
  }

  private int copyFromRanges(ItemState state, long position, ByteBuffer dst) {
    int totalCopied = 0;
    while (dst.hasRemaining()) {
      long currentPosition = position + totalCopied;
      Entry entry = selectEntry(state, currentPosition, dst.remaining());
      if (entry == null) {
        break;
      }
      int copied = entry.range.copyInto(currentPosition, dst);
      if (copied > 0) {
        entry.lastUsedNanos = nanoClock.getAsLong();
        recordConsumption(state, entry, currentPosition, copied);
        totalCopied += copied;
      } else if (Thread.currentThread().isInterrupted()) {
        break;
      } else {
        evict(entry);
      }
    }
    return totalCopied;
  }

  private void recordConsumption(ItemState state, Entry served, long offset, int length) {
    int newlyConsumed = served.range.recordBytesConsumed(offset, length);
    if (newlyConsumed > 0) {
      // Best effort: threads that read the same bytes through different ranges at the same time
      // can each count them.
      telemetry.recordMetric(
          GcsAnalyticsCoreTelemetryConstants.Metric.PREFETCH_BYTES_CONSUMED,
          newlyConsumed,
          Collections.emptyMap());
    }
    evictIfFullyConsumed(served);
    for (Entry entry : entriesStartingAtOrBefore(state, offset + length - 1)) {
      if (entry.key.getStartOffset() < earliestStartReaching(state, offset)) {
        break;
      }
      if (entry == served || entry.key.getEndOffset() <= offset) {
        continue;
      }
      int unused = entry.range.recordBytesConsumed(offset, length);
      evictIfFullyConsumed(entry);
    }
  }

  private void evictIfFullyConsumed(Entry entry) {
    if (entry.range.isFullyConsumed()) {
      evict(entry);
    }
  }

  /**
   * Returns, in order, the parts of cached ranges that together hold every byte of {@code range},
   * or {@code null} if some byte of it is not cached.
   */
  @Nullable
  private static List<Segment> findSegments(ItemState state, GcsObjectRange range) {
    if (range.getLength() <= 0 || range.getOffset() > Long.MAX_VALUE - range.getLength()) {
      return null;
    }
    long endOffset = range.getOffset() + range.getLength();
    List<Segment> segments = new ArrayList<>();
    long cursor = range.getOffset();
    while (cursor < endOffset) {
      Entry entry = selectEntry(state, cursor, (int) (endOffset - cursor));
      if (entry == null) {
        return null;
      }
      long segmentEnd = Math.min(entry.key.getEndOffset(), endOffset);
      segments.add(new Segment(entry, cursor, (int) (segmentEnd - cursor)));
      cursor = segmentEnd;
    }
    return segments;
  }

  private void completeFromSegments(
      ItemState state,
      GcsObjectRange range,
      List<Segment> segments,
      IntFunction<ByteBuffer> allocate,
      Consumer<GcsObjectRange> onDownloadFailed) {
    List<CompletableFuture<ByteBuffer>> slices = new ArrayList<>(segments.size());
    for (Segment segment : segments) {
      slices.add(segment.entry.range.sliceAsync(segment.offset, segment.length));
    }
    CompletableFuture<?> unused =
        whenAllDownloaded(slices)
            .whenComplete(
                (ignored, downloadError) -> {
                  try {
                    if (range.getByteBufferFuture().isDone()) {
                      return;
                    }
                    if (downloadError == null) {
                      completeFromSlices(state, range, segments, slices, allocate);
                    } else {
                      evictFailedDownloads(segments, slices);
                      retryFromCache(state, range, allocate, onDownloadFailed);
                    }
                  } catch (Throwable t) {
                    range.getByteBufferFuture().completeExceptionally(t);
                  }
                });
  }

  private static CompletableFuture<?> whenAllDownloaded(
      List<CompletableFuture<ByteBuffer>> slices) {
    return slices.size() == 1
        ? slices.get(0)
        : CompletableFuture.allOf(slices.toArray(new CompletableFuture<?>[0]));
  }

  private void completeFromSlices(
      ItemState state,
      GcsObjectRange range,
      List<Segment> segments,
      List<CompletableFuture<ByteBuffer>> slices,
      IntFunction<ByteBuffer> allocate) {
    ByteBuffer buffer = copyToAllocated(slices, range.getLength(), allocate);
    if (range.getByteBufferFuture().complete(buffer)) {
      recordVectoredHit(state, segments);
    }
  }

  private void evictFailedDownloads(
      List<Segment> segments, List<CompletableFuture<ByteBuffer>> slices) {
    for (int i = 0; i < segments.size(); i++) {
      if (slices.get(i).isCompletedExceptionally()) {
        evict(segments.get(i).entry);
      }
    }
  }

  private void retryFromCache(
      ItemState state,
      GcsObjectRange range,
      IntFunction<ByteBuffer> allocate,
      Consumer<GcsObjectRange> onDownloadFailed) {
    List<Segment> segments = findSegments(state, range);
    if (segments == null) {
      onDownloadFailed.accept(range);
    } else {
      completeFromSegments(state, range, segments, allocate, onDownloadFailed);
    }
  }

  private static ByteBuffer copyToAllocated(
      List<CompletableFuture<ByteBuffer>> slices, int length, IntFunction<ByteBuffer> allocate) {
    ByteBuffer target = allocate.apply(length);
    checkArgument(target != null, "Buffer allocation returned null for %s bytes", length);
    for (CompletableFuture<ByteBuffer> slice : slices) {
      target.put(slice.join());
    }
    target.flip();
    return target;
  }

  private void recordVectoredHit(ItemState state, List<Segment> segments) {
    for (Segment segment : segments) {
      segment.entry.lastUsedNanos = nanoClock.getAsLong();
      recordConsumption(state, segment.entry, segment.offset, segment.length);
    }
    telemetry.recordMetric(
        GcsAnalyticsCoreTelemetryConstants.Metric.PREFETCH_CACHE_HIT, 1L, Collections.emptyMap());
  }

  @Nullable
  private static Entry findCovering(ItemState state, long offset, int length) {
    Entry selected = selectEntry(state, offset, length);
    boolean covers =
        selected != null && selected.key.getEndOffset() >= LongMath.saturatedAdd(offset, length);
    return covers ? selected : null;
  }

  /**
   * Returns the preferred range covering {@code [offset, offset + length)}, or else the range
   * holding the byte at {@code offset} that reaches furthest, so a stitched read uses as few
   * segments as possible. Returns {@code null} if no range holds the byte at {@code offset}.
   */
  @Nullable
  private static Entry selectEntry(ItemState state, long offset, int length) {
    long endOffset = LongMath.saturatedAdd(offset, length);
    Entry covering = null;
    Entry furthestReaching = null;
    for (Entry entry : entriesStartingAtOrBefore(state, offset)) {
      RangeKey key = entry.key;
      if (key.getStartOffset() < earliestStartReaching(state, offset)) {
        break;
      }
      if (key.getEndOffset() <= offset) {
        continue;
      }
      if (key.getEndOffset() < endOffset) {
        furthestReaching = furtherReaching(furthestReaching, entry);
      } else {
        covering = preferredCovering(covering, entry);
      }
    }
    return covering != null ? covering : furthestReaching;
  }

  /**
   * Prefers a range whose download has finished, so a read does not wait while its bytes are
   * already in memory, and then the shorter range. A finished download that failed is evicted by
   * the read that picks it, and that read then selects again.
   */
  private static Entry preferredCovering(@Nullable Entry current, Entry candidate) {
    if (current == null) {
      return candidate;
    }
    boolean candidateDownloaded = candidate.range.getFuture().isDone();
    if (candidateDownloaded != current.range.getFuture().isDone()) {
      return candidateDownloaded ? candidate : current;
    }
    return candidate.key.getLength() < current.key.getLength() ? candidate : current;
  }

  private static Entry furtherReaching(@Nullable Entry current, Entry candidate) {
    boolean further = current == null || candidate.key.getEndOffset() > current.key.getEndOffset();
    return further ? candidate : current;
  }

  /** Returns the item's ranges that start at or before {@code offset}, nearest start first. */
  private static Collection<Entry> entriesStartingAtOrBefore(ItemState state, long offset) {
    RangeKey lastKeyAtOffset = RangeKey.create(offset, Long.MAX_VALUE);
    return state.index.headMap(lastKeyAtOffset, /* inclusive= */ true).descendingMap().values();
  }

  /**
   * Returns the smallest start offset a range of the item can have and still hold the byte at
   * {@code offset}. No range of the item is longer than its {@code maxRangeLength}, which bounds
   * every scan. A scan reads this bound for each range it visits, because a longer range can be
   * registered while the scan runs.
   */
  private static long earliestStartReaching(ItemState state, long offset) {
    return offset - (state.maxRangeLength - 1);
  }

  private void evict(Entry entry) {
    itemStates.computeIfPresent(
        entry.itemId,
        (id, state) -> {
          if (state.index.remove(entry.key, entry)) {
            reservedBytes.addAndGet(-entry.range.getLength());
          }
          return state;
        });
  }

  /** Runs inside the item's compute. */
  private void evictAll(ItemState state) {
    long evictedBytes = 0;
    for (Entry entry : state.index.values()) {
      evictedBytes += entry.range.getLength();
    }
    state.index.clear();
    state.maxRangeLength = 0;
    reservedBytes.addAndGet(-evictedBytes);
  }

  private void closeStream(GcsItemId itemId) {
    itemStates.computeIfPresent(
        itemId,
        (id, state) -> {
          state.openStreams--;
          if (state.openStreams > 0) {
            return state;
          }
          evictAll(state);
          return null;
        });
  }

  /**
   * A handle for an open stream reading an item. The stream offers ranges of the item through this
   * handle, and releases its hold on the item's cached ranges on {@link #close()}.
   */
  public final class RegisteredStream implements AutoCloseable {

    private final GcsItemId itemId;
    private final AtomicBoolean closed = new AtomicBoolean();

    private RegisteredStream(GcsItemId itemId) {
      this.itemId = itemId;
    }

    /**
     * Offers the in-flight or completed range {@code [startOffset, startOffset + length)} to the
     * cache. {@code promotionAction} runs when a reader starts waiting on the range.
     *
     * <p>A stored range counts {@code length} bytes toward the cache's byte limit until it leaves
     * the cache. The cache never cancels {@code future}.
     *
     * @return {@code true} if the range was stored, or {@code false} if a stored range already
     *     covers these bytes, the range does not fit in the byte limit, {@code future} has already
     *     completed without the whole range, or this stream is closed
     */
    public boolean registerRange(
        long startOffset,
        int length,
        CompletableFuture<ByteBuffer> future,
        Runnable promotionAction) {
      checkArgument(length > 0, "length %s must be positive", length);
      CachedRange range =
          CachedRange.create(startOffset, startOffset + length, future, promotionAction);
      return offer(new Entry(itemId, range, nanoClock.getAsLong()), closed);
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        closeStream(itemId);
      }
    }
  }

  private static final class ItemState {

    private final ConcurrentSkipListMap<RangeKey, Entry> index =
        new ConcurrentSkipListMap<>(BY_START_THEN_END);
    private volatile int maxRangeLength;
    private int openStreams;
  }

  private static final class Entry {

    private final GcsItemId itemId;
    private final RangeKey key;
    private final CachedRange range;
    private volatile long lastUsedNanos;

    private Entry(GcsItemId itemId, CachedRange range, long lastUsedNanos) {
      this.itemId = itemId;
      this.key = RangeKey.create(range.getStartOffset(), range.getEndOffset());
      this.range = range;
      this.lastUsedNanos = lastUsedNanos;
    }
  }

  /** The bytes {@code [offset, offset + length)} of a vectored read that {@code entry} holds. */
  private static final class Segment {

    private final Entry entry;
    private final long offset;
    private final int length;

    private Segment(Entry entry, long offset, int length) {
      this.entry = entry;
      this.offset = offset;
      this.length = length;
    }
  }

  @AutoValue
  abstract static class RangeKey {

    abstract long getStartOffset();

    abstract long getEndOffset();

    final long getLength() {
      return getEndOffset() - getStartOffset();
    }

    static RangeKey create(long startOffset, long endOffset) {
      return new AutoValue_PrefetchBufferCache_RangeKey(startOffset, endOffset);
    }
  }
}
