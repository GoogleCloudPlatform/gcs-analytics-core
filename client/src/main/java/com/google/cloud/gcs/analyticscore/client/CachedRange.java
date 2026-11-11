/*
 * Copyright 2026 Google LLC
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.nio.ByteBuffer;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import javax.annotation.Nullable;

/**
 * Represents a byte range {@code [startOffset, endOffset)} in the prefetch cache.
 *
 * <p>The underlying bytes are held as a {@link CompletableFuture} so that a single entry represents
 * both an in-flight background download and a completed resident buffer.
 *
 * <p>Each entry tracks its own consumed and served bytes, so two entries are equal only if they are
 * the same instance.
 */
public final class CachedRange {

  private final long startOffset;
  private final long endOffset;
  private final CompletableFuture<ByteBuffer> future;
  private final Runnable promotionAction;
  private final RangeSet<Long> consumedRanges = TreeRangeSet.create();
  private final AtomicInteger servedBytes = new AtomicInteger();

  private CachedRange(
      long startOffset,
      long endOffset,
      CompletableFuture<ByteBuffer> future,
      Runnable promotionAction) {
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.future = future;
    this.promotionAction = promotionAction;
  }

  /** Returns the inclusive start offset of this range within the GCS object. */
  public long getStartOffset() {
    return startOffset;
  }

  /** Returns the exclusive end offset of this range within the GCS object. */
  public long getEndOffset() {
    return endOffset;
  }

  /**
   * Returns the future that completes with the buffer whose remaining bytes are the bytes of this
   * range. Callers must not modify the buffer.
   */
  public CompletableFuture<ByteBuffer> getFuture() {
    return future;
  }

  /**
   * Creates a new {@link CachedRange} for {@code [startOffset, endOffset)} that runs {@code
   * promotionAction} when a reader starts waiting on it.
   */
  public static CachedRange create(
      long startOffset,
      long endOffset,
      CompletableFuture<ByteBuffer> future,
      Runnable promotionAction) {
    checkArgument(startOffset >= 0, "startOffset %s must be non-negative", startOffset);
    checkArgument(
        endOffset > startOffset,
        "endOffset %s must be greater than startOffset %s",
        endOffset,
        startOffset);
    checkArgument(
        endOffset - startOffset <= Integer.MAX_VALUE,
        "range [%s, %s) must not be longer than %s bytes",
        startOffset,
        endOffset,
        Integer.MAX_VALUE);
    checkNotNull(future, "future cannot be null");
    checkNotNull(promotionAction, "promotionAction cannot be null");
    return new CachedRange(startOffset, endOffset, future, promotionAction);
  }

  /** Signals that a reader now waits on this range by running its promotion action. */
  public void promote() {
    promotionAction.run();
  }

  /** Returns the length of this range in bytes. */
  public int getLength() {
    return (int) (endOffset - startOffset);
  }

  /**
   * Records {@code [offset, offset + length)} as consumed and returns {@code true} once every byte
   * of this range has been consumed at least once. Bytes read more than once count only once.
   */
  synchronized boolean recordBytesConsumed(long offset, int length) {
    consumedRanges.add(Range.closedOpen(offset, offset + length));
    return consumedRanges.encloses(Range.closedOpen(startOffset, endOffset));
  }

  /**
   * Records {@code bytes} as served for metrics and returns how many of them are newly counted.
   * Bytes served more than once are counted again until the total counted for this range reaches
   * {@link #getLength()}, which it never exceeds.
   */
  int recordBytesServed(int bytes) {
    checkArgument(bytes >= 0, "bytes %s must be non-negative", bytes);
    int previous = servedBytes.getAndAccumulate(bytes, this::cappedServedBytes);
    return cappedServedBytes(previous, bytes) - previous;
  }

  private int cappedServedBytes(int total, int bytes) {
    return (int) Math.min(getLength(), (long) total + bytes);
  }

  /**
   * Waits for this range's buffer if necessary and copies as many available bytes starting at
   * {@code position} into {@code dst} as fit. Returns {@code 0} if the range cannot be read: the
   * download failed or was cancelled, the caller was interrupted, the buffer does not hold the
   * whole range, or {@code position} is outside the range.
   */
  public int copyInto(long position, ByteBuffer dst) {
    checkNotNull(dst, "dst cannot be null");
    if (position < startOffset || position >= endOffset || !dst.hasRemaining()) {
      return 0;
    }
    promoteIfInFlight();
    try {
      ByteBuffer buffer = future.get();
      if (!holdsWholeRange(buffer)) {
        return 0;
      }
      int relativeOffset = (int) (position - startOffset);
      int bytesToCopy = Math.min(dst.remaining(), getLength() - relativeOffset);
      dst.put(sliceBuffer(buffer, relativeOffset, bytesToCopy));
      return bytesToCopy;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return 0;
    } catch (ExecutionException | CancellationException e) {
      return 0;
    }
  }

  /**
   * Returns a future that copies {@code [offset, offset + length)} from this range's buffer into a
   * buffer allocated via {@code allocate}. The future fails if this range's buffer does not hold
   * the whole range.
   */
  CompletableFuture<ByteBuffer> copyIntoAsync(
      long offset, int length, IntFunction<ByteBuffer> allocate) {
    checkNotNull(allocate, "allocate cannot be null");
    int relativeOffset = relativeOffsetOf(offset, length);
    promoteIfInFlight();
    return future.thenApply(
        buffer ->
            copyToAllocated(
                sliceBuffer(checkHoldsWholeRange(buffer), relativeOffset, length),
                length,
                allocate));
  }

  /**
   * Returns a future holding a read-only view of {@code [offset, offset + length)} within this
   * range's buffer. The future fails if this range's buffer does not hold the whole range.
   */
  CompletableFuture<ByteBuffer> sliceAsync(long offset, int length) {
    int relativeOffset = relativeOffsetOf(offset, length);
    promoteIfInFlight();
    return future.thenApply(
        buffer -> sliceBuffer(checkHoldsWholeRange(buffer), relativeOffset, length));
  }

  private void promoteIfInFlight() {
    if (!future.isDone()) {
      promote();
    }
  }

  private int relativeOffsetOf(long offset, int length) {
    checkArgument(length >= 0, "length %s must be non-negative", length);
    checkArgument(
        offset >= startOffset && offset <= endOffset - length,
        "%s bytes at offset %s must be within range [%s, %s)",
        length,
        offset,
        startOffset,
        endOffset);
    return (int) (offset - startOffset);
  }

  private boolean holdsWholeRange(@Nullable ByteBuffer buffer) {
    return buffer != null && buffer.remaining() == getLength();
  }

  private ByteBuffer checkHoldsWholeRange(@Nullable ByteBuffer buffer) {
    checkState(
        holdsWholeRange(buffer),
        "buffer of range [%s, %s) must hold %s bytes",
        startOffset,
        endOffset,
        getLength());
    return buffer;
  }

  private static ByteBuffer sliceBuffer(ByteBuffer source, int relativeOffset, int length) {
    ByteBuffer view = source.asReadOnlyBuffer();
    view.position(view.position() + relativeOffset);
    view.limit(view.position() + length);
    return view.slice();
  }

  @Nullable
  private static ByteBuffer copyToAllocated(
      ByteBuffer source, int length, IntFunction<ByteBuffer> allocate) {
    ByteBuffer target = allocate.apply(length);
    if (target == null) {
      return null;
    }
    target.put(source);
    target.flip();
    return target;
  }
}
