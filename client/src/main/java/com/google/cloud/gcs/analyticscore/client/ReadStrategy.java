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

import com.google.cloud.ReadChannel;
import java.io.IOException;

/** Strategy for reading data from Google Cloud Storage. */
interface ReadStrategy {
  /**
   * Returns a {@link ReadChannel} prepared for reading from the specified position.
   *
   * @param requestedPosition the position in the file where reading will start
   * @param bytesToRead the estimated number of bytes to be read
   * @return a {@code ReadChannel} positioned at {@code requestedPosition}
   * @throws IOException if an I/O error occurs while acquiring the channel
   */
  ReadChannel getReadChannel(long requestedPosition, int bytesToRead) throws IOException;

  /**
   * Updates the strategy's current read position.
   *
   * @param newPosition the new read position
   */
  void position(long newPosition);

  /**
   * Returns the limit up to which data can be read by this strategy.
   *
   * @return the read limit
   */
  long getLimit();

  /**
   * Checks if the specified position is at or beyond the End-Of-File.
   *
   * @param position the position to check
   * @return {@code true} if the position is at or beyond EOF, {@code false} otherwise
   */
  boolean isEof(long position);

  /**
   * Closes this read strategy and releases any held resources, such as open channels.
   *
   * @throws IOException if an I/O error occurs during close
   */
  void close() throws IOException;
}
