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
import com.google.cloud.storage.Storage;
import java.io.IOException;

class RandomReadStrategy extends AbstractReadStrategy {
  private long currentLimit = -1;

  RandomReadStrategy(
      Storage storage, GcsItemId itemId, GcsReadOptions options, GcsItemInfo itemInfo) {
    super(storage, itemId, options, itemInfo);
  }

  @Override
  public ReadChannel getReadChannel(long requestedPosition, int bytesToRead) throws IOException {
    long requestedEndPosition = requestedPosition + bytesToRead;
    if (canReuseChannel(requestedPosition, requestedEndPosition)
        && performPendingSeeks(requestedPosition)) {
      return channel;
    }

    return openBoundedReadChannel(requestedPosition, bytesToRead);
  }

  private boolean canReuseChannel(long requestedPosition, long requestedEndPosition) {
    return channel != null && requestedPosition >= position && requestedEndPosition <= currentLimit;
  }

  private ReadChannel openBoundedReadChannel(long requestedPosition, int bytesToRead)
      throws IOException {
    if (channel != null && channel.isOpen()) {
      channel.close();
    }
    currentLimit = requestedPosition + bytesToRead;
    channel = openSdkReadChannel();
    channel.setChunkSize(0);
    channel.limit(currentLimit);
    channel.seek(requestedPosition);
    position = requestedPosition;

    return channel;
  }

  @Override
  public long getLimit() {
    return currentLimit;
  }
}
