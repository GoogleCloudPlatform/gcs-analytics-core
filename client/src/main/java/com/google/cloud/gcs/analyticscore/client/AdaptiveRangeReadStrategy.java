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

import static java.lang.Math.max;
import static java.lang.Math.min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Strategy class to handle adaptive range reads. */
class AdaptiveRangeReadStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(AdaptiveRangeReadStrategy.class);

  private final GcsReadOptions readOptions;
  private final GcsItemInfo itemInfo;
  private boolean randomAccess;

  private long lastRangeRequestEnd = -1;
  private int sequentialReadCount = 0;
  private final int sequentialRangeReadThreshold;

  AdaptiveRangeReadStrategy(GcsReadOptions readOptions, GcsItemInfo itemInfo) {
    this.readOptions = readOptions;
    this.itemInfo = itemInfo;
    this.randomAccess = readOptions.getFileAccessPattern() == FileAccessPattern.RANDOM;
    this.sequentialRangeReadThreshold = readOptions.getSequentialRangeReadThreshold();
  }

  long calculateRangeRequestEnd(long currentPosition, long bytesToRead, long objectSize) {
    if (randomAccess) {
      if (currentPosition == lastRangeRequestEnd) {
        sequentialReadCount++;
        if (sequentialReadCount > sequentialRangeReadThreshold
            && readOptions.getFileAccessPattern() == FileAccessPattern.AUTO) {
          randomAccess = false;
          LOG.debug(
              "Detected sequential read pattern, switching to sequential IO for '{}'",
              itemInfo.getItemId());
        }
      } else {
        sequentialReadCount = 0;
      }
    }

    long endPosition = objectSize;
    if (randomAccess) {
      // In random access mode, we only read what is requested plus a minimum size,
      // rather than reading until the end of the file.
      endPosition = currentPosition + max(bytesToRead, readOptions.getMinRangeRequestSize());
    }
    long result = min(endPosition, objectSize);
    lastRangeRequestEnd = result;

    return result;
  }

  void detectRandomAccess(long currentPosition, long contentChannelCurrentPosition) {
    if (shouldDetectRandomAccess()) {
      if (currentPosition < contentChannelCurrentPosition) {
        LOG.debug(
            "Detected backward read from {} to {} position, switching to random IO for '{}'",
            contentChannelCurrentPosition,
            currentPosition,
            itemInfo.getItemId());
        randomAccess = true;
      } else if (contentChannelCurrentPosition >= 0
          && contentChannelCurrentPosition + readOptions.getInplaceSeekLimit() < currentPosition) {
        LOG.debug(
            "Detected forward read from {} to {} position over {} threshold, switching to random IO for '{}'",
            contentChannelCurrentPosition,
            currentPosition,
            readOptions.getInplaceSeekLimit(),
            itemInfo.getItemId());
        randomAccess = true;
      }
    }
  }

  boolean shouldSeekInPlace(
      long currentPosition, long contentChannelCurrentPosition, long contentChannelEnd) {
    long seekDistance = currentPosition - contentChannelCurrentPosition;
    boolean result =
        seekDistance > 0
            && seekDistance <= readOptions.getInplaceSeekLimit()
            && currentPosition < contentChannelEnd;
    return result;
  }

  boolean shouldDetectRandomAccess() {
    return !randomAccess && readOptions.getFileAccessPattern() == FileAccessPattern.AUTO;
  }

  boolean isRandomAccess() {
    return randomAccess;
  }
}
