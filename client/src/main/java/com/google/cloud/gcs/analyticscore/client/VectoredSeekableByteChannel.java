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
package com.google.cloud.gcs.analyticscore.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntFunction;

public interface VectoredSeekableByteChannel extends SeekableByteChannel {
  /**
   * Reads the list of provided ranges in parallel.
   *
   * @param ranges Ranges to be fetched in parallel
   * @param allocate the function to allocate ByteBuffer
   * @throws IOException on any IO failure
   */
  void readVectored(List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException;

  /**
   * Reads the list of provided ranges in parallel.
   *
   * <p>If a read fails after a buffer has been allocated, the implementation may invoke {@code
   * release} with the allocated buffer before completing the relevant range futures exceptionally.
   *
   * @param ranges Ranges to be fetched in parallel
   * @param allocate the function to allocate ByteBuffer
   * @param release the function to release allocated ByteBuffer instances on failure
   * @throws IOException on any IO failure
   */
  default void readVectored(
      List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate, Consumer<ByteBuffer> release)
      throws IOException {
    Objects.requireNonNull(release, "Buffer release function must not be null");
    readVectored(ranges, allocate);
  }
}
