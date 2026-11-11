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

import com.google.auto.value.AutoValue;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.nio.ByteBuffer;
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
 */
@AutoValue
public abstract class CachedRange {

  private final RangeSet<Long> consumedRanges = TreeRangeSet.create();
  private final AtomicInteger servedBytes = new AtomicInteger();
  private Runnable promotionAction;

  /** Returns the inclusive start offset of this range within the GCS object. */
  public abstract long getStartOffset();

  /** Returns the exclusive end offset of this range within the GCS object. */
  public abstract long getEndOffset();

  /** Returns the future that completes with the read-only byte buffer for this range. */
  public abstract CompletableFuture<ByteBuffer> getFuture();

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
    checkNotNull(future, "future cannot be null");
    checkNotNull(promotionAction, "promotionAction cannot be null");
    CachedRange range = new AutoValue_CachedRange(startOffset, endOffset, future);
    range.promotionAction = promotionAction;
    return range;
  }

  /** Signals that a reader now waits on this range by running its promotion action. */
  public void promote() {
    promotionAction.run();
  }

  /** Returns the length of this range in bytes. */
  public int getLength() {
    return (int) (getEndOffset() - getStartOffset());
  }

  /**
   * Records {@code [offset, offset + length)} as consumed and returns {@code true} once every byte
   * of this range has been consumed at least once. Bytes read more than once count only once.
   */
  synchronized boolean recordBytesConsumed(long offset, int length) {
    consumedRanges.add(Range.closedOpen(offset, offset + length));
    return consumedRanges.encloses(Range.closedOpen(getStartOffset(), getEndOffset()));
  }

  /**
   * Records {@code bytes} as served for metrics and returns how many of them are newly counted, so
   * the total counted for this range never exceeds {@link #getLength()}.
   */
  int recordBytesServed(int bytes) {
    int previous =
        servedBytes.getAndAccumulate(bytes, (total, add) -> Math.min(getLength(), total + add));
    return Math.min(getLength(), previous + bytes) - previous;
  }

  /**
   * Waits for this range's buffer if necessary and copies as many available bytes starting at
   * {@code position} into {@code dst} as fit, or returns {@code 0} if the range cannot be read.
   */
  public int copyInto(long position, ByteBuffer dst) {
    checkNotNull(dst, "dst cannot be null");
    if (position < getStartOffset() || position >= getEndOffset() || !dst.hasRemaining()) {
      return 0;
    }
    promote();
    try {
      ByteBuffer buffer = getFuture().get();
      if (buffer == null) {
        return 0;
      }
      int relativeOffset = (int) (position - getStartOffset());
      int available = getLength() - relativeOffset;
      int bytesToCopy = Math.min(dst.remaining(), available);
      ByteBuffer view = buffer.asReadOnlyBuffer();
      view.position(view.position() + relativeOffset);
      view.limit(view.position() + bytesToCopy);
      dst.put(view);
      return bytesToCopy;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return 0;
    } catch (ExecutionException | RuntimeException e) {
      return 0;
    }
  }

  /**
   * Returns a future that slices {@code [offset, offset + length)} from this range's buffer,
   * reusing the backing array on an exact match or copying into a buffer allocated via {@code
   * allocate}.
   */
  CompletableFuture<ByteBuffer> copyIntoAsync(
      long offset, int length, IntFunction<ByteBuffer> allocate) {
    promote();
    int relativeOffset = (int) (offset - getStartOffset());
    boolean exactMatch = relativeOffset == 0 && getLength() == length;
    return getFuture()
        .thenApply(
            buf -> {
              if (exactMatch && !buf.isReadOnly() && buf.hasArray() && buf.arrayOffset() == 0) {
                return buf.duplicate();
              }
              return copyToAllocated(sliceBuffer(buf, relativeOffset, length), length, allocate);
            });
  }

  /**
   * Returns a future holding a read-only view of {@code [offset, offset + length)} within this
   * range's buffer.
   */
  CompletableFuture<ByteBuffer> sliceAsync(long offset, int length) {
    promote();
    int relativeOffset = (int) (offset - getStartOffset());
    return getFuture().thenApply(buf -> sliceBuffer(buf, relativeOffset, length));
  }

  private static ByteBuffer sliceBuffer(ByteBuffer source, int relativeOffset, int length) {
    ByteBuffer view = source.duplicate();
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
