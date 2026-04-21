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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.ReadChannel;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FakeGcsReadChannelTest {

  private FakeGcsReadChannel fakeGcsReadChannel;
  private GcsItemInfo itemInfo;
  private GcsReadOptions readOptions;
  private Storage storage;

  @BeforeEach
  void createDefaultInstances() throws Exception {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(100L).build();
    readOptions = GcsReadOptions.builder().build();
    byte[] data = TestDataGenerator.generateSeededRandomBytes(100, 1);
    storage = LocalStorageHelper.getOptions().getService();
    storage.create(
        BlobInfo.newBuilder(itemId.getBucketName(), itemId.getObjectName().get()).build(), data);
    fakeGcsReadChannel =
        new FakeGcsReadChannel(
            storage,
            itemInfo,
            readOptions,
            Suppliers.ofInstance(Executors.newSingleThreadExecutor()),
            new Telemetry(ImmutableList.of()));
    FakeGcsReadChannel.resetCounts();
  }

  @Test
  void openSdkReadChannel_incrementsOpenReadChannelCount() throws Exception {
    fakeGcsReadChannel.openSdkReadChannel(itemInfo.getItemId(), readOptions);

    assertThat(FakeGcsReadChannel.getOpenReadChannelCount()).isEqualTo(1);
  }

  @Test
  void getTrackingReadChannel_returnsAutoCreatedWrapper() {
    assertThat(fakeGcsReadChannel.getTrackingReadChannel()).isNotNull();
  }

  @Test
  void openSdkReadChannel_createsTrackingReadChannelThatReadsFromStorage() throws Exception {
    TrackingReadChannel tracking = fakeGcsReadChannel.getTrackingReadChannel();
    ByteBuffer dst = ByteBuffer.allocate(100);

    int bytesRead = tracking.read(dst);

    assertThat(bytesRead).isEqualTo(100);
  }

  @Test
  void openSdkReadChannel_setsDefaultEofAtCall() throws Exception {
    fakeGcsReadChannel.setDefaultEofAtCall(1);

    ReadChannel channel = fakeGcsReadChannel.openSdkReadChannel(itemInfo.getItemId(), readOptions);
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = channel.read(dst);

    assertThat(bytesRead).isEqualTo(-1);
  }
}
