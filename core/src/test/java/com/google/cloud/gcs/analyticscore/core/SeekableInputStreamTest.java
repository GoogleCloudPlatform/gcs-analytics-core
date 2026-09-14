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
package com.google.cloud.gcs.analyticscore.core;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import org.junit.jupiter.api.Test;

class SeekableInputStreamTest {

  private static class FakeSeekableInputStream extends SeekableInputStream {
    private boolean legacyCalled = false;
    private List<GcsObjectRange> lastRanges;
    private IntFunction<ByteBuffer> lastAllocate;

    @Override
    public void readVectored(List<GcsObjectRange> fileRanges, IntFunction<ByteBuffer> alloc) {
      this.legacyCalled = true;
      this.lastRanges = fileRanges;
      this.lastAllocate = alloc;
    }

    @Override
    public int read() {
      return -1;
    }

    @Override
    public long getPos() {
      return 0;
    }

    @Override
    public int read(ByteBuffer byteBuffer) {
      return 0;
    }

    @Override
    public void seek(long position) {}

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) {}

    @Override
    public int readTail(byte[] buffer, int offset, int n) {
      return 0;
    }
  }

  @Test
  void readVectored_withRelease_delegatesToLegacyImplementation() throws Exception {
    FakeSeekableInputStream stream = new FakeSeekableInputStream();
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(1)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    List<GcsObjectRange> ranges = List.of(range);
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;
    AtomicInteger releaseCount = new AtomicInteger();

    stream.readVectored(ranges, allocate, buffer -> releaseCount.incrementAndGet());

    assertThat(stream.legacyCalled).isTrue();
    assertThat(stream.lastRanges).isSameInstanceAs(ranges);
    assertThat(stream.lastAllocate).isSameInstanceAs(allocate);
    assertThat(releaseCount.get()).isEqualTo(0);
  }

  @Test
  void readVectored_nullRelease_rejectsBeforeDelegation() {
    FakeSeekableInputStream stream = new FakeSeekableInputStream();
    List<GcsObjectRange> ranges = List.of();
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    assertThrows(NullPointerException.class, () -> stream.readVectored(ranges, allocate, null));

    assertThat(stream.legacyCalled).isFalse();
  }
}
