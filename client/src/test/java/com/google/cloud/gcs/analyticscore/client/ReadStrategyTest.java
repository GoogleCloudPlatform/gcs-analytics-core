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
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;

class ReadStrategyTest {

  private static final long KB = 1024L;

  @Test
  void updateItemInfo_defaultImplementation_doesNotThrow() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-obj").build();
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(KB).build();
    ReadStrategy strategy = new NoOpReadStrategy();

    strategy.updateItemInfo(itemInfo);
  }

  private static class NoOpReadStrategy implements ReadStrategy {

    @Override
    public ReadChannel getReadChannel(long requestedPosition, int bytesToRead) throws IOException {
      return null;
    }

    @Nullable
    @Override
    public ReadChannel getSdkReadChannel() {
      return null;
    }

    @Override
    public void position(long newPosition) {}

    @Override
    public long getLimit() {
      return 0L;
    }

    @Override
    public boolean isEof(long position) {
      return false;
    }

    @Override
    public void close() throws IOException {}
  }
}
