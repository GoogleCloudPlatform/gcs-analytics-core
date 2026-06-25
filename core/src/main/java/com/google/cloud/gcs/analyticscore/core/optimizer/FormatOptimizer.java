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

package com.google.cloud.gcs.analyticscore.core.optimizer;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;

/** Defines the contract for format-specific optimizations (e.g., Parquet footer caching). */
public interface FormatOptimizer {

  /** Returns whether this optimizer is applicable based on the item ID. */
  boolean isApplicable(GcsItemId itemId);

  /** Returns whether this optimizer is applicable based on the file metadata. */
  default boolean isApplicable(GcsFileInfo fileInfo) {
    return isApplicable(fileInfo.getItemInfo().getItemId());
  }

  /** Invoked when the channel is opened with only an item ID. */
  void onOpen(GcsItemId itemId, AnalyticsCacheManager cacheManager) throws IOException;

  /** Invoked when the channel is opened with full file metadata. */
  default void onOpen(GcsFileInfo fileInfo, AnalyticsCacheManager cacheManager) throws IOException {
    onOpen(fileInfo.getItemInfo().getItemId(), cacheManager);
  }

  /**
   * Intercepts read operations to serve data from cache or optimize fetching. Returns the number of
   * bytes read, or 0 if this optimizer cannot serve the read.
   */
  int read(long position, ByteBuffer dst, VectoredSeekableByteChannel delegate) throws IOException;

  /**
   * Intercepts vectored read operations.
   *
   * @param ranges The list of ranges requested.
   * @param allocate Function to allocate ByteBuffers for satisfied ranges.
   * @return The list of ranges that were NOT satisfied and still need to be read from source.
   */
  default List<GcsObjectRange> readVectored(
      List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate) throws IOException {
    return ranges;
  }

  /** Invoked when the channel is closed. */
  default void onClose() throws IOException {}
}
