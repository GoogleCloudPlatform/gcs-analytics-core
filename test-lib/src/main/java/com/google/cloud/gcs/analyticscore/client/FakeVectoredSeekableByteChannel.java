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

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

/**
 * An in-memory {@link VectoredSeekableByteChannel} that serves reads from a byte array.
 *
 * <p>Vectored reads complete their futures before {@link #readVectored} returns, so a test can
 * assert on the outcome of a speculative fetch without waiting on a background thread.
 *
 * <p>Every requested range is recorded, which lets a test verify what a component chose to fetch in
 * addition to what it ultimately served.
 */
public final class FakeVectoredSeekableByteChannel implements VectoredSeekableByteChannel {

  private final byte[] content;
  private final List<GcsObjectRange> requestedRanges = new ArrayList<>();

  private long position;
  private boolean open = true;
  private boolean deferVectoredCompletion;
  private IOException vectoredFailure;

  public FakeVectoredSeekableByteChannel(byte[] content) {
    this.content = content.clone();
  }

  /** Leaves vectored futures uncompleted so that in-flight bookkeeping can be observed. */
  public void deferVectoredCompletion() {
    this.deferVectoredCompletion = true;
  }

  /** Makes every subsequent {@link #readVectored} call throw {@code failure}. */
  public void failVectoredReadsWith(IOException failure) {
    this.vectoredFailure = failure;
  }

  /** Returns the ranges passed to {@link #readVectored}, in request order. */
  public ImmutableList<GcsObjectRange> getRequestedRanges() {
    return ImmutableList.copyOf(requestedRanges);
  }

  /** Returns the offsets passed to {@link #readVectored}, in request order. */
  public ImmutableList<Long> getRequestedOffsets() {
    return requestedRanges.stream()
        .map(GcsObjectRange::getOffset)
        .collect(ImmutableList.toImmutableList());
  }

  /** Returns a copy of {@code length} bytes starting at {@code offset}. */
  public byte[] sliceContent(long offset, int length) {
    byte[] slice = new byte[length];
    System.arraycopy(content, (int) offset, slice, 0, length);
    return slice;
  }

  /** Completes every requested range whose future is still pending. */
  public void completeDeferredRanges() {
    for (GcsObjectRange range : ImmutableList.copyOf(requestedRanges)) {
      if (range.getByteBufferFuture().isDone()) {
        continue;
      }
      ByteBuffer target = ByteBuffer.allocate(range.getLength());
      target.put(sliceContent(range.getOffset(), range.getLength()));
      target.flip();
      range.getByteBufferFuture().complete(target);
    }
  }

  @Override
  public void readVectored(List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    requestedRanges.addAll(ranges);
    if (vectoredFailure != null) {
      throw vectoredFailure;
    }
    if (deferVectoredCompletion) {
      return;
    }
    for (GcsObjectRange range : ranges) {
      ByteBuffer target = allocate.apply(range.getLength());
      target.put(sliceContent(range.getOffset(), range.getLength()));
      target.flip();
      range.getByteBufferFuture().complete(target);
    }
  }

  @Override
  public int read(ByteBuffer dst) {
    if (position >= content.length) {
      return -1;
    }
    int readableBytes = Math.min(dst.remaining(), (int) (content.length - position));
    dst.put(content, (int) position, readableBytes);
    position += readableBytes;
    return readableBytes;
  }

  @Override
  public int write(ByteBuffer src) {
    throw new UnsupportedOperationException("write is not supported");
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  public FakeVectoredSeekableByteChannel position(long newPosition) {
    this.position = newPosition;
    return this;
  }

  @Override
  public long size() {
    return content.length;
  }

  @Override
  public FakeVectoredSeekableByteChannel truncate(long size) {
    throw new UnsupportedOperationException("truncate is not supported");
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void close() {
    open = false;
  }
}
