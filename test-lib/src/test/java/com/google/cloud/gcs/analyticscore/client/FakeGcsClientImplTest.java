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
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FakeGcsClientImplTest {

  private FakeGcsClientImpl fakeGcsClient;

  @BeforeEach
  void setUp() {
    GcsFileSystemOptions options = GcsFileSystemOptions.builder().build();
    fakeGcsClient =
        new FakeGcsClientImpl(
            options.getGcsClientOptions(),
            Suppliers.ofInstance(Executors.newSingleThreadExecutor()));
    FakeGcsClientImpl.resetCounts();
  }

  @Test
  void openReadChannel_incrementsOpenReadChannelCount() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(100L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    TestDataGenerator.createGcsData(itemId, 100);

    VectoredSeekableByteChannel channel = fakeGcsClient.openReadChannel(itemInfo, readOptions);

    assertNotNull(channel);
    assertEquals(1, FakeGcsClientImpl.getOpenReadChannelCount());
  }

  @Test
  void getGcsItemInfo_objectExists_pass() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    TestDataGenerator.createGcsData(itemId, 100);

    GcsItemInfo itemInfo = fakeGcsClient.getGcsItemInfo(itemId);

    assertNotNull(itemInfo);
    assertEquals(itemId, itemInfo.getItemId());
    assertEquals(100L, itemInfo.getSize());
  }

  @Test
  void getGcsItemInfo_objectDoesNotExists_throws() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("not-exists").build();
    assertThrows(IOException.class, () -> fakeGcsClient.getGcsItemInfo(itemId));
  }

  @Test
  void getGcsItemInfo_itemIsNotObject_throws() {
    GcsItemId itemId = GcsItemId.builder().setBucketName("test-bucket").build(); // No object name

    assertThrows(UnsupportedOperationException.class, () -> fakeGcsClient.getGcsItemInfo(itemId));
  }

  @Test
  void close_incrementsCloseCount() {
    fakeGcsClient.close();
    assertEquals(1, FakeGcsClientImpl.getCloseCount());
  }
}
