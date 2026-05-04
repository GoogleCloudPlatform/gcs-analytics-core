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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FakeSequentialReadStrategyTest {

  private Storage storage;
  private FakeSequentialReadStrategy strategy;

  @BeforeEach
  void setUp() {
    storage = LocalStorageHelper.getOptions().getService();
  }

  @AfterEach
  void tearDown() throws IOException {
    if (strategy != null) {
      strategy.close();
    }
  }

  @Test
  void constructor_createsInitialChannel() throws IOException {
    GcsItemId itemId = GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsReadOptions options = GcsReadOptions.builder().build();
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(1000L).build();
    StorageTestUtils.createBlobInStorage(storage, itemId, "A".repeat(1000));

    strategy = new FakeSequentialReadStrategy(storage, itemId, options, itemInfo);

    assertEquals(1, strategy.getCreatedChannels().size());
  }

  @Test
  void getReadChannel_reusesChannelIfNoSeek() throws IOException {
    GcsItemId itemId = GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsReadOptions options = GcsReadOptions.builder().build();
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(1000L).build();
    StorageTestUtils.createBlobInStorage(storage, itemId, "A".repeat(1000));
    strategy = new FakeSequentialReadStrategy(storage, itemId, options, itemInfo);

    strategy.getReadChannel(0, 10);

    assertEquals(1, strategy.getCreatedChannels().size());
  }

  @Test
  void getReadChannel_createsNewChannelOnSkipFailure() throws IOException {
    GcsItemId itemId = GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsReadOptions options = GcsReadOptions.builder().setInplaceSeekLimit(100).build();
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(1000L).build();
    StorageTestUtils.createBlobInStorage(storage, itemId, "A".repeat(1000));
    strategy = new FakeSequentialReadStrategy(storage, itemId, options, itemInfo);
    strategy.getCreatedChannels().get(0).setEofAtCall(1);

    strategy.getReadChannel(10, 10);

    assertEquals(2, strategy.getCreatedChannels().size());
  }
}
