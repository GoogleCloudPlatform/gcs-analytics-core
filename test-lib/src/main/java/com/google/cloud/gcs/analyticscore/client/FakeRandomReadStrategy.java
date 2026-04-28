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
import java.util.ArrayList;
import java.util.List;

public class FakeRandomReadStrategy extends RandomReadStrategy {
  private final List<TrackingReadChannel> createdChannels = new ArrayList<>();

  public FakeRandomReadStrategy(
      Storage storage, GcsItemId itemId, GcsReadOptions options, GcsItemInfo itemInfo) {
    super(storage, itemId, options, itemInfo);
  }

  @Override
  protected ReadChannel openSdkReadChannel() throws IOException {
    ReadChannel channel = super.openSdkReadChannel();
    TrackingReadChannel trackingChannel = new TrackingReadChannel(channel);
    createdChannels.add(trackingChannel);
    return trackingChannel;
  }

  public List<TrackingReadChannel> getCreatedChannels() {
    return createdChannels;
  }
}
