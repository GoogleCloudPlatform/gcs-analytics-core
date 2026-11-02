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

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.function.IntFunction;
import org.junit.jupiter.api.Test;

class VectoredSeekableByteChannelTest {

  @Test
  void getItemInfo_defaultImplementation_returnsNull() throws Exception {
    try (VectoredSeekableByteChannel channel = new NoOpVectoredChannel()) {
      assertThat(channel.getItemInfo()).isNull();
    }
  }

  private static class NoOpVectoredChannel implements VectoredSeekableByteChannel {

    @Override
    public void readVectored(List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate)
        throws IOException {}

    @Override
    public int read(ByteBuffer dst) throws IOException {
      return -1;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      return 0;
    }

    @Override
    public long position() throws IOException {
      return 0;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
      return this;
    }

    @Override
    public long size() throws IOException {
      return 0;
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
      return this;
    }

    @Override
    public boolean isOpen() {
      return true;
    }

    @Override
    public void close() throws IOException {}
  }
}
