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

import java.io.EOFException;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Adapts our WritableByteChannel to the Parquet OutputFile interface.
 */
public class TestOutputStreamOutputFile implements OutputFile {
  private final WritableByteChannel channel;

  public TestOutputStreamOutputFile(WritableByteChannel channel) {
    this.channel = channel;
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) throws IOException {
    return new PositionOutputStream() {
      private long position = 0;

      @Override
      public long getPos() {
        return position;
      }

      public void write(int b) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) b).flip();
        while (buffer.hasRemaining()) {
          int written = channel.write(buffer);
          if (written < 0) throw new EOFException("Channel closed unexpectedly");
          position += written;
        }
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(b, off, len);
        // CRITICAL FIX: Loop until all bytes are consumed by the channel
        while (buffer.hasRemaining()) {
          int written = channel.write(buffer);
          if (written < 0) throw new EOFException("Channel closed unexpectedly");
          position += written;
        }
      }

      @Override
      public void close() throws IOException {
        channel.close();
      }
    };
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
    return create(blockSizeHint);
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return 0;
  }
}
