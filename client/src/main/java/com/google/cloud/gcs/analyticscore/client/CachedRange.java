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
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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

  /** Returns the inclusive start offset of this range within the GCS object. */
  public abstract long getStartOffset();

  /** Returns the exclusive end offset of this range within the GCS object. */
  public abstract long getEndOffset();

  /** Returns the future that completes with the read-only byte buffer for this range. */
  public abstract CompletableFuture<ByteBuffer> getFuture();

  /** Creates a new {@link CachedRange} for {@code [startOffset, endOffset)}. */
  public static CachedRange create(
      long startOffset, long endOffset, CompletableFuture<ByteBuffer> future) {
    checkArgument(startOffset >= 0, "startOffset %s must be non-negative", startOffset);
    checkArgument(
        endOffset > startOffset,
        "endOffset %s must be greater than startOffset %s",
        endOffset,
        startOffset);
    checkNotNull(future, "future cannot be null");
    return new AutoValue_CachedRange(startOffset, endOffset, future);
  }

  /** Returns the length of this range in bytes. */
  public int getLength() {
    return (int) (getEndOffset() - getStartOffset());
  }

  /** Returns whether this range completely covers {@code [offset, offset + length)}. */
  public boolean contains(long offset, int length) {
    return length >= 0 && offset >= getStartOffset() && (offset + length) <= getEndOffset();
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
    int relativeOffset = (int) (offset - getStartOffset());
    boolean exactMatch = relativeOffset == 0 && getLength() == length;
    return getFuture()
        .thenApply(
            buf -> {
              if (exactMatch && !buf.isReadOnly() && buf.hasArray() && buf.arrayOffset() == 0) {
                return buf.duplicate();
              }
              ByteBuffer view = buf.duplicate();
              view.position(view.position() + relativeOffset);
              view.limit(view.position() + length);
              return copyToAllocated(view, length, allocate);
            });
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
