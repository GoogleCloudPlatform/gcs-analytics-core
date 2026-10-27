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
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class SequentialReadStrategyTest {

  private final Storage storage = LocalStorageHelper.getOptions().getService();
  private final GcsItemId itemId =
      GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
  private final GcsReadOptions options = GcsReadOptions.builder().setInplaceSeekLimit(100).build();
  private final GcsItemInfo itemInfo =
      GcsItemInfo.builder().setItemId(itemId).setSize(1000).setContentGeneration(0L).build();

  @Test
  void constructor_opensSdkReadChannel() throws IOException {
    StorageTestUtils.createBlobInStorage(storage, itemId, "a".repeat(1000));

    SequentialReadStrategy strategy =
        new SequentialReadStrategy(storage, itemId, options, itemInfo);

    assertThat(strategy.channel).isNotNull();
  }

  @Test
  void getReadChannel_returnsOpenChannel() throws IOException {
    StorageTestUtils.createBlobInStorage(storage, itemId, "a".repeat(1000));
    SequentialReadStrategy strategy =
        new SequentialReadStrategy(storage, itemId, options, itemInfo);

    ReadChannel channel = strategy.getReadChannel(0, 10);

    assertThat(channel).isNotNull();
    assertThat(channel.isOpen()).isTrue();
  }

  @Test
  void getReadChannel_reopensIfNull() throws IOException {
    StorageTestUtils.createBlobInStorage(storage, itemId, "a".repeat(1000));
    SequentialReadStrategy strategy =
        new SequentialReadStrategy(storage, itemId, options, itemInfo);
    strategy.channel = null;

    ReadChannel channel = strategy.getReadChannel(0, 10);

    assertThat(channel).isNotNull();
    assertThat(strategy.channel).isNotNull();
  }

  @Test
  void getLimit_returnsMaxValue() throws IOException {
    StorageTestUtils.createBlobInStorage(storage, itemId, "a".repeat(1000));
    SequentialReadStrategy strategy =
        new SequentialReadStrategy(storage, itemId, options, itemInfo);

    long limit = strategy.getLimit();

    assertThat(limit).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void isEof_returnsTrueWhenPositionAtSize() throws IOException {
    StorageTestUtils.createBlobInStorage(storage, itemId, "a".repeat(1000));
    SequentialReadStrategy strategy =
        new SequentialReadStrategy(storage, itemId, options, itemInfo);

    boolean eof = strategy.isEof(1000);

    assertThat(eof).isTrue();
  }

  @Test
  void isEof_returnsFalseWhenPositionBeforeSize() throws IOException {
    StorageTestUtils.createBlobInStorage(storage, itemId, "a".repeat(1000));
    SequentialReadStrategy strategy =
        new SequentialReadStrategy(storage, itemId, options, itemInfo);

    boolean eof = strategy.isEof(999);

    assertThat(eof).isFalse();
  }

  @Test
  void close_closesChannel() throws IOException {
    StorageTestUtils.createBlobInStorage(storage, itemId, "a".repeat(1000));
    SequentialReadStrategy strategy =
        new SequentialReadStrategy(storage, itemId, options, itemInfo);
    ReadChannel channel = strategy.channel;

    strategy.close();

    assertThat(channel.isOpen()).isFalse();
    assertThat(strategy.channel).isNull();
  }

  @Test
  void getReadChannel_smallForwardSeek_usesSkipInPlace() throws IOException {
    StorageTestUtils.createBlobInStorage(storage, itemId, "a".repeat(1000));
    FakeSequentialReadStrategy strategy =
        new FakeSequentialReadStrategy(storage, itemId, options, itemInfo);

    strategy.getReadChannel(10, 10);

    TrackingReadChannel trackingChannel = strategy.getCreatedChannels().get(0);
    assertThat(trackingChannel.getSeekCalls()).isEqualTo(0);
    assertThat(trackingChannel.getReadCalls()).isGreaterThan(0);
    assertThat(trackingChannel.getLastChunkSize()).isNull();
    assertThat(trackingChannel.getLastLimit()).isNull();
  }

  @Test
  void getReadChannel_largeForwardSeek_usesChannelSeek() throws IOException {
    StorageTestUtils.createBlobInStorage(storage, itemId, "a".repeat(1000));
    FakeSequentialReadStrategy strategy =
        new FakeSequentialReadStrategy(storage, itemId, options, itemInfo);

    strategy.getReadChannel(200, 10);

    TrackingReadChannel trackingChannel = strategy.getCreatedChannels().get(0);
    assertThat(trackingChannel.getSeekCalls()).isEqualTo(1);
  }

  @Test
  void getReadChannel_multipleSmallForwardSeeks_usesSkipInPlace() throws IOException {
    StorageTestUtils.createBlobInStorage(storage, itemId, "a".repeat(1000));
    FakeSequentialReadStrategy strategy =
        new FakeSequentialReadStrategy(storage, itemId, options, itemInfo);

    strategy.getReadChannel(0, 5).read(ByteBuffer.allocate(5));
    strategy.getReadChannel(10, 5).read(ByteBuffer.allocate(5));
    strategy.getReadChannel(20, 5).read(ByteBuffer.allocate(5));

    TrackingReadChannel trackingChannel = strategy.getCreatedChannels().get(0);
    assertThat(trackingChannel.getSeekCalls()).isEqualTo(0);
  }

  @Test
  void getReadChannel_skipInPlaceHitsEof_fallsBackToHardSeek() throws IOException {
    StorageTestUtils.createBlobInStorage(storage, itemId, "a".repeat(100));
    FakeSequentialReadStrategy strategy =
        new FakeSequentialReadStrategy(storage, itemId, options, itemInfo);
    TrackingReadChannel channel1 = strategy.getCreatedChannels().get(0);
    channel1.setEofAtCall(2);

    strategy.getReadChannel(0, 5).read(ByteBuffer.allocate(5));
    strategy.getReadChannel(10, 5);

    assertThat(strategy.getCreatedChannels()).hasSize(2);
    TrackingReadChannel channel2 = strategy.getCreatedChannels().get(1);
    assertThat(channel1.getCloseCalls()).isEqualTo(1);
    assertThat(channel2.getSeekCalls()).isEqualTo(1);
  }

  @Test
  void getReadChannel_skipInPlaceLargerThanBufferSize_loopsAndReads() throws IOException {
    StorageTestUtils.createBlobInStorage(storage, itemId, "a".repeat(500 * 1024));
    GcsReadOptions customOptions = GcsReadOptions.builder().setInplaceSeekLimit(500 * 1024).build();
    FakeSequentialReadStrategy strategy =
        new FakeSequentialReadStrategy(storage, itemId, customOptions, itemInfo);

    strategy.getReadChannel(0, 5).read(ByteBuffer.allocate(5));
    strategy.getReadChannel(300 * 1024, 5);

    TrackingReadChannel trackingChannel = strategy.getCreatedChannels().get(0);
    assertThat(trackingChannel.getReadCalls()).isGreaterThan(2);
    assertThat(trackingChannel.getSeekCalls()).isEqualTo(0);
  }

  @Test
  void getReadChannel_backwardSeek_fallsBackToHardSeek() throws IOException {
    StorageTestUtils.createBlobInStorage(storage, itemId, "a".repeat(100));
    FakeSequentialReadStrategy strategy =
        new FakeSequentialReadStrategy(storage, itemId, options, itemInfo);

    strategy.getReadChannel(10, 5).read(ByteBuffer.allocate(5));
    strategy.getReadChannel(2, 5);

    assertThat(strategy.getCreatedChannels()).hasSize(1);
    TrackingReadChannel channel = strategy.getCreatedChannels().get(0);
    assertThat(channel.getSeekCalls()).isEqualTo(1);
  }

  @Test
  void getReadChannel_skipInPlaceFailsWithoutClosing_closesOldChannel() throws IOException {
    StorageTestUtils.createBlobInStorage(storage, itemId, "a".repeat(1000));
    SequentialReadStrategy strategy =
        new SequentialReadStrategy(storage, itemId, options, itemInfo) {
          @Override
          protected boolean skipInPlace(long seekDistance) throws IOException {
            return false;
          }
        };
    ReadChannel oldChannel = strategy.channel;

    strategy.getReadChannel(10, 10);

    assertThat(oldChannel.isOpen()).isFalse();
    assertThat(strategy.channel).isNotNull();
    assertThat(strategy.channel).isNotEqualTo(oldChannel);
  }

  @Test
  void skipInPlace_reusesThreadLocalBufferAcrossInstancesAndReallocatesWhenCleared()
      throws IOException {
    StorageTestUtils.createBlobInStorage(storage, itemId, "0123456789".repeat(100));
    AbstractReadStrategy.SKIP_BUFFER.remove();

    FakeSequentialReadStrategy strategy1 =
        new FakeSequentialReadStrategy(storage, itemId, options, itemInfo);
    ByteBuffer dst1 = ByteBuffer.allocate(5);
    assertThat(strategy1.getReadChannel(13, 5).read(dst1)).isEqualTo(5);
    assertThat(new String(dst1.array(), StandardCharsets.UTF_8)).isEqualTo("34567");
    ByteBuffer firstBuffer = AbstractReadStrategy.SKIP_BUFFER.get().get();
    assertThat(firstBuffer).isNotNull();
    assertThat(firstBuffer.capacity()).isEqualTo(128 * 1024);

    FakeSequentialReadStrategy strategy2 =
        new FakeSequentialReadStrategy(storage, itemId, options, itemInfo);
    ByteBuffer dst2 = ByteBuffer.allocate(5);
    assertThat(strategy2.getReadChannel(27, 5).read(dst2)).isEqualTo(5);
    assertThat(new String(dst2.array(), StandardCharsets.UTF_8)).isEqualTo("78901");
    ByteBuffer secondBuffer = AbstractReadStrategy.SKIP_BUFFER.get().get();
    assertThat(secondBuffer).isSameInstanceAs(firstBuffer);

    AbstractReadStrategy.SKIP_BUFFER.get().clear();
    FakeSequentialReadStrategy strategy3 =
        new FakeSequentialReadStrategy(storage, itemId, options, itemInfo);
    ByteBuffer dst3 = ByteBuffer.allocate(5);
    assertThat(strategy3.getReadChannel(34, 5).read(dst3)).isEqualTo(5);
    assertThat(new String(dst3.array(), StandardCharsets.UTF_8)).isEqualTo("45678");
    ByteBuffer thirdBuffer = AbstractReadStrategy.SKIP_BUFFER.get().get();
    assertThat(thirdBuffer).isNotNull();
    assertThat(thirdBuffer).isNotSameInstanceAs(firstBuffer);
  }

  @Test
  void skipInPlace_allocatesDistinctBufferPerThread() throws Exception {
    StorageTestUtils.createBlobInStorage(storage, itemId, "0123456789".repeat(100));
    FakeSequentialReadStrategy mainStrategy =
        new FakeSequentialReadStrategy(storage, itemId, options, itemInfo);
    ByteBuffer mainDst = ByteBuffer.allocate(5);
    assertThat(mainStrategy.getReadChannel(13, 5).read(mainDst)).isEqualTo(5);
    assertThat(new String(mainDst.array(), StandardCharsets.UTF_8)).isEqualTo("34567");
    ByteBuffer mainThreadBuffer = AbstractReadStrategy.SKIP_BUFFER.get().get();
    assertThat(mainThreadBuffer).isNotNull();

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      ByteBuffer workerThreadBuffer =
          executor
              .submit(
                  () -> {
                    FakeSequentialReadStrategy workerStrategy =
                        new FakeSequentialReadStrategy(storage, itemId, options, itemInfo);
                    ByteBuffer workerDst = ByteBuffer.allocate(5);
                    assertThat(workerStrategy.getReadChannel(27, 5).read(workerDst)).isEqualTo(5);
                    assertThat(new String(workerDst.array(), StandardCharsets.UTF_8))
                        .isEqualTo("78901");
                    return AbstractReadStrategy.SKIP_BUFFER.get().get();
                  })
              .get();
      assertThat(workerThreadBuffer).isNotNull();
      assertThat(workerThreadBuffer).isNotSameInstanceAs(mainThreadBuffer);
    } finally {
      executor.shutdownNow();
    }
  }
}
