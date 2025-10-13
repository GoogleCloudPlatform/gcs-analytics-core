/*
 * Copyright 2025 Google LLC
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

import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Suppliers;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FakeGcsReadChannelTest {

  private FakeGcsReadChannel fakeGcsReadChannel;
  private GcsItemInfo itemInfo;
  private GcsReadOptions readOptions;

  @BeforeEach
  void createDefaultInstances() throws Exception {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(100L).build();
    readOptions = GcsReadOptions.builder().build();
    fakeGcsReadChannel =
        new FakeGcsReadChannel(
            StorageOptions.newBuilder().build().getService(),
            itemInfo,
            readOptions,
            Suppliers.ofInstance(Executors.newSingleThreadExecutor()));
    FakeGcsReadChannel.resetCounts();
  }

  @Test
  void openReadChannel_incrementsOpenReadChannelCount() throws Exception {
    ReadChannel readChannel = fakeGcsReadChannel.openReadChannel(itemInfo, readOptions);

    assertEquals(1, FakeGcsReadChannel.getOpenReadChannelCount());
  }

  @Test
  void openReadChannel_returnsInstanceOfInmemoryReadChannel() throws Exception {
    ReadChannel readChannel = fakeGcsReadChannel.openReadChannel(itemInfo, readOptions);

    assertTrue(readChannel instanceof InmemoryReadChannel);
  }
}
