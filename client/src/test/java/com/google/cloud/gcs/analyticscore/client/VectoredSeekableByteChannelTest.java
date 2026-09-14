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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.function.IntFunction;
import org.junit.jupiter.api.Test;

class VectoredSeekableByteChannelTest {

  private static class FakeChannel implements VectoredSeekableByteChannel {
    private boolean legacyCalled = false;

    @Override
    public void readVectored(List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate) {
      this.legacyCalled = true;
    }

    @Override
    public int read(ByteBuffer dst) {
      return 0;
    }

    @Override
    public int write(ByteBuffer src) {
      return 0;
    }

    @Override
    public long position() {
      return 0;
    }

    @Override
    public SeekableByteChannel position(long newPosition) {
      return this;
    }

    @Override
    public long size() {
      return 0;
    }

    @Override
    public SeekableByteChannel truncate(long size) {
      return this;
    }

    @Override
    public boolean isOpen() {
      return true;
    }

    @Override
    public void close() {}
  }

  @Test
  void readVectored_withRelease_requiresImplementationOverride() {
    FakeChannel channel = new FakeChannel();
    List<GcsObjectRange> ranges = List.of();
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    assertThrows(
        UnsupportedOperationException.class,
        () -> channel.readVectored(ranges, allocate, buffer -> {}));

    assertFalse(channel.legacyCalled);
  }

  @Test
  void readVectored_nullRelease_rejectsBeforeDelegation() {
    FakeChannel channel = new FakeChannel();
    List<GcsObjectRange> ranges = List.of();
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    assertThrows(NullPointerException.class, () -> channel.readVectored(ranges, allocate, null));

    assertFalse(channel.legacyCalled);
  }
}
