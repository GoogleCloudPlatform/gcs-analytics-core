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

import com.google.common.base.Suppliers;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FakeGcsClientImplTest {

  private FakeGcsClientImpl fakeGcsClient;
  private Map<GcsItemId, Long> itemIdToSizeMap;

  @BeforeEach
  void setUp() {
    itemIdToSizeMap = new HashMap<>();
    fakeGcsClient =
        new FakeGcsClientImpl(
            itemIdToSizeMap, Suppliers.ofInstance(Executors.newSingleThreadExecutor()));
    FakeGcsClientImpl.resetCounts();
  }

  @Test
  void openReadChannel_incrementsOpenReadChannelCount() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(100L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().build();

    assertNotNull(fakeGcsClient.openReadChannel(itemInfo, readOptions));
    assertEquals(1, FakeGcsClientImpl.getOpenReadChannelCount());
  }

  @Test
  void getGcsItemInfo_objectExists_pass() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    itemIdToSizeMap.put(itemId, 100L);

    GcsItemInfo itemInfo = fakeGcsClient.getGcsItemInfo(itemId);

    assertNotNull(itemInfo);
    assertEquals(itemId, itemInfo.getItemId());
    assertEquals(100L, itemInfo.getSize());
    assertEquals(1, FakeGcsClientImpl.getGetGcsItemInfoCount());
  }

  @Test
  void getGcsItemInfo_objectDoesNotExists_throws() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("not-exists").build();
    assertThrows(IOException.class, () -> fakeGcsClient.getGcsItemInfo(itemId));
    assertEquals(1, FakeGcsClientImpl.getGetGcsItemInfoCount());
  }

  @Test
  void getGcsItemInfo_itemIsNotObject_throws() {
    GcsItemId itemId = GcsItemId.builder().setBucketName("test-bucket").build(); // No object name
    itemIdToSizeMap.put(itemId, 100L);

    assertThrows(UnsupportedOperationException.class, () -> fakeGcsClient.getGcsItemInfo(itemId));
    assertEquals(1, FakeGcsClientImpl.getGetGcsItemInfoCount());
  }

  @Test
  void close_incrementsCloseCount() {
    fakeGcsClient.close();
    assertEquals(1, FakeGcsClientImpl.getCloseCount());
  }
}
