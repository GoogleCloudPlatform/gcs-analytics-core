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

  private long lastAdaptiveReadSessionEnd = -1;
  private int sequentialReadSessionCount = 0;
  private final int sequentialReadSessionThreshold;

  AdaptiveRangeReadStrategy(GcsReadOptions readOptions, GcsItemInfo itemInfo) {
    this.readOptions = readOptions;
    this.itemInfo = itemInfo;
    this.randomAccess = readOptions.getFileAccessPattern() == FileAccessPattern.RANDOM;
    this.sequentialReadSessionThreshold = readOptions.getSequentialReadSessionThreshold();
  }

  long calculateAdaptiveReadSessionEnd(
      long readSessionPosition, long bytesToRead, long objectSize) {
    long endPosition = objectSize;
    if (randomAccess) {
      // In random access mode, we only read what is requested plus a minimum size,
      // rather than reading until the end of the file.
      endPosition = readSessionPosition + max(bytesToRead, readOptions.getMinRangeRequestSize());
    }
    long newEndPosition = min(endPosition, objectSize);
    lastAdaptiveReadSessionEnd = newEndPosition;

    return newEndPosition;
  }

  void detectSequentialAccess(long readChannelPosition) {
    sequentialReadSessionCount =
        (readChannelPosition == lastAdaptiveReadSessionEnd) ? sequentialReadSessionCount + 1 : 0;
    if (!shouldDetectSequentialAccess()) {
      return;
    }
    LOG.debug(
        "Detected sequential read pattern, switching to sequential IO for '{}'",
        itemInfo.getItemId());
    randomAccess = false;
  }

  void detectRandomAccess(long readChannelPosition, long readSessionPosition) {
    if (!shouldDetectRandomAccess()) {
      return;
    }
    if (readChannelPosition < readSessionPosition) {
      LOG.debug(
          "Detected backward read from {} to {} position, switching to random IO for '{}'",
          readSessionPosition,
          readChannelPosition,
          itemInfo.getItemId());
      randomAccess = true;
    } else if (readSessionPosition >= 0
        && readSessionPosition + readOptions.getInplaceSeekLimit() < readChannelPosition) {
      LOG.debug(
          "Detected forward read from {} to {} position over {} threshold, switching to random IO for '{}'",
          readSessionPosition,
          readChannelPosition,
          readOptions.getInplaceSeekLimit(),
          itemInfo.getItemId());
      randomAccess = true;
    }
  }

  boolean shouldSeekInPlace(long channelPosition, long sessionPosition, long sessionEnd) {
    long seekDistance = channelPosition - sessionPosition;
    boolean result =
        seekDistance > 0
            && seekDistance <= readOptions.getInplaceSeekLimit()
            && channelPosition < sessionEnd;
    return result;
  }

  private boolean shouldDetectRandomAccess() {
    return !randomAccess && readOptions.getFileAccessPattern() == FileAccessPattern.AUTO;
  }

  private boolean shouldDetectSequentialAccess() {
    return randomAccess
        && readOptions.getFileAccessPattern() == FileAccessPattern.AUTO
        && sequentialReadSessionCount >= sequentialReadSessionThreshold;
  }

  boolean isRandomAccess() {
    return randomAccess;
  }
}
