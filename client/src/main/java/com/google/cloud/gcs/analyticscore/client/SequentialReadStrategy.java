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

class SequentialReadStrategy extends AbstractReadStrategy {

  SequentialReadStrategy(
      Storage storage, GcsItemId itemId, GcsReadOptions options, GcsItemInfo itemInfo)
      throws IOException {
    super(storage, itemId, options, itemInfo);
    this.channel = openSdkReadChannel();
  }

  @Override
  public ReadChannel getReadChannel(long requestedPosition, int bytesToRead) throws IOException {
    if (channel == null) {
      openUnboundedReadChannel(requestedPosition);
    }
    // Reopen the channel if in-place seek fails.
    if (!performPendingSeeks(requestedPosition)) {
      openUnboundedReadChannel(requestedPosition);
    }

    return channel;
  }

  private void openUnboundedReadChannel(long requestedPosition) throws IOException {
    if (channel != null && channel.isOpen()) {
      channel.close();
    }
    channel = openSdkReadChannel();
    channel.seek(requestedPosition);
    position = requestedPosition;
  }

  @Override
  public long getLimit() {
    return Long.MAX_VALUE;
  }
}
