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

package com.google.cloud.gcs.analyticscore.core.prefetch;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Reads the remaining bytes of a {@link ByteBuffer} in place, without copying them.
 *
 * <p>Works for heap, read-only, and direct buffers alike. The given buffer is left untouched.
 */
final class ByteBufferInputStream extends InputStream {

  private final ByteBuffer buffer;

  ByteBufferInputStream(ByteBuffer buffer) {
    this.buffer = checkNotNull(buffer, "buffer cannot be null").duplicate();
  }

  @Override
  public int read() {
    return buffer.hasRemaining() ? (buffer.get() & 0xFF) : -1;
  }

  @Override
  public int read(byte[] bytes, int offset, int length) {
    Objects.checkFromIndexSize(offset, length, bytes.length);
    if (length == 0) {
      return 0;
    }
    if (!buffer.hasRemaining()) {
      return -1;
    }
    int readLength = Math.min(length, buffer.remaining());
    buffer.get(bytes, offset, readLength);
    return readLength;
  }
}
