/*
 * Copyright 2025 Google LLC
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

import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;

/**
 * SeekableInputStream is similar to {@link InputStream} with additional support for: {@link
 * #seek(long)} and {@link #getPos()}. This is primarily used for random data access.
 *
 * <p>This class is based on Parquet's SeekableInputStream.
 */
public abstract class SeekableInputStream extends InputStream {

  /**
   * Returns the current position in the input stream.
   *
   * @return the current position from the beginning of the stream.
   */
  public abstract long getPos();

  /**
   * This method will copy available bytes into the buffer, reading at most buf.remaining() bytes.
   * The number of bytes actually copied is returned by the method, or -1 is returned to signal that
   * the end of the underlying stream has been reached.
   *
   * @param byteBuffer a byte buffer to fill with data from the stream
   * @return the number of bytes read or -1 if the stream ended
   * @throws IOException If the underlying stream throws IOException
   */
  public abstract int read(ByteBuffer byteBuffer) throws IOException;

  /**
   * Moves the read position to a new offset within the stream, at which the next read happens.
   *
   * @param position the absolute position (in bytes) to move to.
   * @throws IOException if the underlying stream throws IOException.
   */
  public abstract void seek(long position) throws IOException;

  /**
   * Fill the provided buffer with the contents of the input source starting at {@code position} for
   * the given {@code offset} and {@code length}.
   *
   * @param position the starting position in the stream from which to read.
   * @param buffer the destination buffer for the data.
   * @param offset the starting offset in the destination buffer.
   * @param length the number of bytes to read.
   * @throws IOException if an I/O error occurs.
   */
  public abstract void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException;

  /**
   * Reads a specified number of bytes from the very end of the stream. This operation is blocking
   * and does not change the current stream position.
   *
   * @param buffer the buffer into which the data is read.
   * @param offset the starting offset in the buffer.
   * @param n the number of bytes to read from the tail of the stream.
   * @return the total number of bytes copied into the buffer.
   * @throws IOException if an error occurs while reading.
   */
  public abstract int readTail(byte[] buffer, int offset, int n) throws IOException;

  /**
   * Performs a vectored read, fetching multiple ranges in parallel.Buffers for the data are
   * supplied by the provided allocation function.
   *
   * @param fileRanges a list of {@link GcsObjectRange} ranges to be read in parallel.
   * @param alloc a function that allocates a {@link ByteBuffer} of a given size.
   * @throws IOException if any I/O error occurs during the reads.
   */
  public abstract void readVectored(
      List<GcsObjectRange> fileRanges, final IntFunction<ByteBuffer> alloc) throws IOException;
}
