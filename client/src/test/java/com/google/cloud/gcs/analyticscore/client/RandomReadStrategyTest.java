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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class RandomReadStrategyTest {

  private final Storage storage = LocalStorageHelper.getOptions().getService();
  private final GcsItemId itemId =
      GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
  private final GcsReadOptions options = GcsReadOptions.builder().build();
  private final GcsItemInfo itemInfo =
      GcsItemInfo.builder().setItemId(itemId).setSize(1000).setContentGeneration(0L).build();

  @Test
  void constructor_doesNotOpenChannel() {
    FakeRandomReadStrategy strategy =
        new FakeRandomReadStrategy(storage, itemId, options, itemInfo);

    assertThat(strategy.channel).isNull();
  }

  @Test
  void getReadChannel_firstCall_createsChannelWithLimit() throws IOException {
    createBlobInStorage("a".repeat(1000));
    FakeRandomReadStrategy strategy =
        new FakeRandomReadStrategy(storage, itemId, options, itemInfo);

    strategy.getReadChannel(10, 20);

    TrackingReadChannel trackingChannel = strategy.getCreatedChannels().get(0);
    assertThat(trackingChannel.getLastLimit()).isEqualTo(30L);
    assertThat(trackingChannel.getLastChunkSize()).isEqualTo(0);
    assertThat(trackingChannel.getSeekCalls()).isEqualTo(1);
  }

  @Test
  void getReadChannel_reuseChannel_whenWithinLimit() throws IOException {
    createBlobInStorage("a".repeat(1000));
    GcsReadOptions readOptions = GcsReadOptions.builder().setInplaceSeekLimit(100).build();
    FakeRandomReadStrategy strategy =
        new FakeRandomReadStrategy(storage, itemId, readOptions, itemInfo);
    strategy.getReadChannel(10, 50);

    strategy.getReadChannel(20, 10);

    TrackingReadChannel trackingChannel = strategy.getCreatedChannels().get(0);
    assertThat(trackingChannel.getSeekCalls()).isEqualTo(1);
    assertThat(trackingChannel.getReadCalls()).isGreaterThan(0);
  }

  @Test
  void getReadChannel_reuseChannel_failsPerformPendingSeeks_recreatesChannel() throws IOException {
    createBlobInStorage("a".repeat(100));
    GcsReadOptions readOptions = GcsReadOptions.builder().setInplaceSeekLimit(100).build();
    FakeRandomReadStrategy strategy =
        new FakeRandomReadStrategy(storage, itemId, readOptions, itemInfo);
    strategy.getReadChannel(0, 20);
    TrackingReadChannel trackingChannel = strategy.getCreatedChannels().get(0);
    trackingChannel.setEofAtCall(1);

    strategy.getReadChannel(10, 5);

    assertThat(strategy.getCreatedChannels()).hasSize(2);
    assertThat(trackingChannel.getCloseCalls()).isEqualTo(1);
    TrackingReadChannel trackingChannel2 = strategy.getCreatedChannels().get(1);
    assertThat(trackingChannel2.getLastLimit()).isEqualTo(15L);
  }

  @Test
  void getReadChannel_channelOpen_notReusable_closesOldChannel() throws IOException {
    createBlobInStorage("a".repeat(1000));
    FakeRandomReadStrategy strategy =
        new FakeRandomReadStrategy(storage, itemId, options, itemInfo);
    strategy.getReadChannel(0, 20);
    TrackingReadChannel trackingChannel = strategy.getCreatedChannels().get(0);

    strategy.getReadChannel(40, 10);

    assertThat(strategy.getCreatedChannels()).hasSize(2);
    assertThat(trackingChannel.getCloseCalls()).isEqualTo(1);
  }

  @Test
  void getReadChannel_recreatesChannel_whenPastLimit() throws IOException {
    createBlobInStorage("a".repeat(1000));
    FakeRandomReadStrategy strategy =
        new FakeRandomReadStrategy(storage, itemId, options, itemInfo);
    strategy.getReadChannel(10, 20);

    strategy.getReadChannel(40, 10);

    assertThat(strategy.getCreatedChannels()).hasSize(2);
    TrackingReadChannel trackingChannel2 = strategy.getCreatedChannels().get(1);
    assertThat(trackingChannel2.getLastLimit()).isEqualTo(50L);
    assertThat(trackingChannel2.getSeekCalls()).isEqualTo(1);
  }

  @Test
  void getReadChannel_seekBeyondLimit_closesChannel() throws IOException {
    createBlobInStorage("a".repeat(100));
    FakeRandomReadStrategy strategy =
        new FakeRandomReadStrategy(storage, itemId, options, itemInfo);
    strategy.getReadChannel(0, 10);

    strategy.getReadChannel(20, 5);

    TrackingReadChannel trackingChannel = strategy.getCreatedChannels().get(0);
    assertThat(trackingChannel.getCloseCalls()).isEqualTo(1);
  }

  @Test
  void getLimit_returnsCurrentLimit() throws IOException {
    createBlobInStorage("a".repeat(1000));
    FakeRandomReadStrategy strategy =
        new FakeRandomReadStrategy(storage, itemId, options, itemInfo);
    strategy.getReadChannel(10, 20);

    long limit = strategy.getLimit();

    assertThat(limit).isEqualTo(30);
  }

  @Test
  void getReadChannel_backwardSeek_recreatesChannel() throws IOException {
    createBlobInStorage("a".repeat(100));
    FakeRandomReadStrategy strategy =
        new FakeRandomReadStrategy(storage, itemId, options, itemInfo);
    strategy.getReadChannel(0, 10);
    strategy.position(10);

    strategy.getReadChannel(2, 2);

    assertThat(strategy.getCreatedChannels()).hasSize(2);
    TrackingReadChannel trackingChannel1 = strategy.getCreatedChannels().get(0);
    TrackingReadChannel trackingChannel2 = strategy.getCreatedChannels().get(1);
    assertThat(trackingChannel1.getCloseCalls()).isEqualTo(1);
    assertThat(trackingChannel2.getSeekCalls()).isEqualTo(1);
  }

  @Test
  void getReadChannel_hitsLimit_opensNewChannel() throws IOException {
    createBlobInStorage("a".repeat(100));
    FakeRandomReadStrategy strategy =
        new FakeRandomReadStrategy(storage, itemId, options, itemInfo);
    strategy.getReadChannel(0, 5);
    strategy.position(5);

    strategy.getReadChannel(5, 5);

    assertThat(strategy.getCreatedChannels()).hasSize(2);
    TrackingReadChannel trackingChannel1 = strategy.getCreatedChannels().get(0);
    assertThat(trackingChannel1.getCloseCalls()).isEqualTo(1);
  }

  @Test
  void getReadChannel_seekPastLimit_readsSuccessfully() throws IOException {
    String objectData = "abcdefghijklmnopqrstuvwxyz";
    createBlobInStorage(objectData);
    GcsReadOptions readOptions = GcsReadOptions.builder().setInplaceSeekLimit(0).build();
    RandomReadStrategy strategy = new RandomReadStrategy(storage, itemId, readOptions, itemInfo);
    strategy.getReadChannel(0, 5);

    ReadChannel channel = strategy.getReadChannel(10, 5);
    ByteBuffer buffer = ByteBuffer.allocate(5);
    int bytesRead = channel.read(buffer);

    assertThat(bytesRead).isEqualTo(5);
    assertThat(new String(buffer.array(), StandardCharsets.UTF_8)).isEqualTo("klmno");
  }

  private void createBlobInStorage(String content) {
    BlobId blobId = BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, content.getBytes(StandardCharsets.UTF_8));
  }
}
