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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import javax.annotation.Nullable;

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
   * <p>Implementations supporting release invoke the callback at most once per allocated buffer,
   * only on failure and before completing the affected futures exceptionally. The callback is never
   * invoked on success, or for a buffer whose contents have been published to any range future,
   * including through a slice. The callback must be thread-safe: vectored-read executor threads can
   * invoke it concurrently for different combined ranges, including ranges from the same call.
   *
   * <p>Successful merged reads can return slices sharing one allocated parent buffer, rather than
   * the parent itself. This API does not return that parent or notify the allocator when callers
   * finish using its slices. It therefore does not provide successful-read reclamation for buffer
   * pools that require the original allocation to be returned. Such allocators need an external
   * ownership mechanism that keeps the parent alive until all its slices are no longer in use.
   * Returning individual slices to a pool is not equivalent to returning the original allocation.
   *
   * <p>The default implementation delegates to the two-argument overload and ignores {@code
   * release}. Implementations must override this overload to provide failure cleanup.
   *
   * @param ranges Ranges to be fetched in parallel
   * @param allocate the function to allocate ByteBuffer
   * @param release the function to release allocated ByteBuffer instances on failure
   * @throws IOException on any IO failure
   */
  default void readVectored(
      List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate, Consumer<ByteBuffer> release)
      throws IOException {
    checkNotNull(release, "Buffer release function must not be null");
    readVectored(ranges, allocate);
  }

  /** Returns the item metadata info if available, or null. */
  @Nullable
  default GcsItemInfo getItemInfo() {
    return null;
  }
}
