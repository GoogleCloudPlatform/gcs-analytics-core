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

package com.google.cloud.gcs.analyticscore.core.prefetch;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.gcs.analyticscore.client.FakeVectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.PrefetchBufferCache;
import com.google.cloud.gcs.analyticscore.common.telemetry.RecordingOperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PrefetchSchedulerTest {

  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName("bucket").setObjectName("data.parquet").build();
  private static final int CONTENT_LENGTH = 512;
  private static final int RANGE_LENGTH = 32;
  private static final long RANGE_OFFSET = 64;

  private FakeVectoredSeekableByteChannel channel;
  private PrefetchBufferCache bufferCache;
  private RecordingOperationListener metricListener;
  private Telemetry telemetry;
  private PrefetchScheduler scheduler;

  @BeforeEach
  void createScheduler() {
    channel = new FakeVectoredSeekableByteChannel(createContent());
    metricListener = new RecordingOperationListener();
    telemetry = new Telemetry(ImmutableList.of(metricListener));
    bufferCache = new PrefetchBufferCache(CONTENT_LENGTH, 60, telemetry);
    scheduler = new PrefetchScheduler(ITEM_ID, bufferCache);
  }

  @AfterEach
  void closeScheduler() {
    scheduler.close();
    telemetry.close();
    channel.close();
  }

  @Test
  void schedule_uncachedRange_requestsTheRange() {
    schedule(RANGE_OFFSET);

    assertThat(channel.getRequestedOffsets()).containsExactly(RANGE_OFFSET);
  }

  @Test
  void schedule_uncachedRange_publishesContentIntoCache() {
    schedule(RANGE_OFFSET);

    assertThat(readCachedRange(RANGE_OFFSET, RANGE_LENGTH))
        .isEqualTo(channel.sliceContent(RANGE_OFFSET, RANGE_LENGTH));
  }

  @Test
  void schedule_touchingRanges_requestsThemAsOneMergedRange() {
    schedule(0, RANGE_LENGTH);

    GcsObjectRange request = Iterables.getOnlyElement(channel.getRequestedRanges());
    assertThat(Range.closedOpen(request.getOffset(), request.getOffset() + request.getLength()))
        .isEqualTo(Range.closedOpen(0L, 2L * RANGE_LENGTH));
  }

  @Test
  void schedule_rangesWithGap_requestsEachRangeAsItsOwnRange() {
    schedule(0, 2 * RANGE_LENGTH);

    assertThat(channel.getRequestedOffsets()).containsExactly(0L, 2L * RANGE_LENGTH).inOrder();
  }

  @Test
  void schedule_rangeAlreadyCached_doesNotRequestTheRange() {
    try (PrefetchBufferCache.RegisteredStream otherStream = bufferCache.registerStream(ITEM_ID)) {
      otherStream.registerRange(
          RANGE_OFFSET,
          RANGE_LENGTH,
          CompletableFuture.completedFuture(
              ByteBuffer.wrap(channel.sliceContent(RANGE_OFFSET, RANGE_LENGTH))),
          () -> {});

      schedule(RANGE_OFFSET);

      assertThat(channel.getRequestedOffsets()).isEmpty();
    }
  }

  @Test
  void schedule_rangeAlreadyInFlight_doesNotRequestTheRange() {
    try (PrefetchBufferCache.RegisteredStream otherStream = bufferCache.registerStream(ITEM_ID)) {
      otherStream.registerRange(RANGE_OFFSET, RANGE_LENGTH, new CompletableFuture<>(), () -> {});

      schedule(RANGE_OFFSET);

      assertThat(channel.getRequestedOffsets()).isEmpty();
    }
  }

  @Test
  void schedule_sameRangeTwice_requestsTheRangeOnlyOnce() {
    channel.deferVectoredCompletion();

    schedule(RANGE_OFFSET);
    schedule(RANGE_OFFSET);

    assertThat(channel.getRequestedOffsets()).containsExactly(RANGE_OFFSET);
  }

  @Test
  void schedule_rangeStartingBeyondFileSize_doesNotRequestTheRange() {
    scheduler.schedule(
        channel,
        ImmutableList.of(
            Range.closedOpen((long) CONTENT_LENGTH, (long) CONTENT_LENGTH + RANGE_LENGTH)),
        CONTENT_LENGTH);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void schedule_finalRangeExtendingBeyondFileSize_requestsOnlyTheRemainingBytes() {
    scheduler.schedule(
        channel,
        ImmutableList.of(Range.closedOpen(RANGE_OFFSET, RANGE_OFFSET + RANGE_LENGTH)),
        RANGE_OFFSET + 8);

    assertThat(readCachedRange(RANGE_OFFSET, 8)).isEqualTo(channel.sliceContent(RANGE_OFFSET, 8));
  }

  @Test
  void schedule_emptyRangeList_doesNotRequestAnyRange() {
    scheduler.schedule(channel, ImmutableList.of(), CONTENT_LENGTH);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void schedule_channelFails_removesRangeFromCache() {
    channel.failVectoredReadsWith(new IOException("vectored read rejected"));

    schedule(RANGE_OFFSET);

    assertThat(bufferCache.isCovered(ITEM_ID, RANGE_OFFSET, RANGE_LENGTH)).isFalse();
  }

  @Test
  void schedule_requestNotSettled_registersPendingRangeInCache() {
    channel.deferVectoredCompletion();
    schedule(RANGE_OFFSET);

    assertThat(bufferCache.isCovered(ITEM_ID, RANGE_OFFSET, RANGE_LENGTH)).isTrue();
  }

  @Test
  void schedule_settledRequest_completesTheCachedRangeFuture() {
    channel.deferVectoredCompletion();
    schedule(RANGE_OFFSET);
    CompletableFuture<ByteBuffer> future =
        channel.getRequestedRanges().get(0).getByteBufferFuture();

    channel.completeDeferredRanges();

    assertThat(future.thenApply(data -> bufferCache.isCovered(ITEM_ID, RANGE_OFFSET, 1)).join())
        .isTrue();
  }

  @Test
  void close_withPendingRequest_cancelsTheRequest() {
    channel.deferVectoredCompletion();
    schedule(RANGE_OFFSET);

    scheduler.close();

    assertThat(channel.getRequestedRanges().get(0).getByteBufferFuture().isCancelled()).isTrue();
  }

  @Test
  void cancelAll_withPendingRequest_leavesTheRangeUncached() {
    channel.deferVectoredCompletion();
    schedule(RANGE_OFFSET);

    scheduler.cancelAll();

    assertThat(bufferCache.isCovered(ITEM_ID, RANGE_OFFSET, 1)).isFalse();
  }

  @Test
  void cancelAll_afterRangePromoted_doesNotCancelPromotedRequest() {
    channel.deferVectoredCompletion();
    schedule(RANGE_OFFSET);
    List<GcsObjectRange> unused =
        bufferCache.serveVectoredFromCache(
            ITEM_ID,
            ImmutableList.of(
                GcsObjectRange.builder()
                    .setOffset(RANGE_OFFSET)
                    .setLength(RANGE_LENGTH)
                    .setByteBufferFuture(new CompletableFuture<>())
                    .build()),
            ByteBuffer::allocate,
            range -> {});

    scheduler.cancelAll();

    assertThat(channel.getRequestedRanges().get(0).getByteBufferFuture().isCancelled()).isFalse();
  }

  @Test
  void close_lastOpenScheduler_evictsCompletedRange() {
    schedule(RANGE_OFFSET);

    scheduler.close();

    assertThat(bufferCache.isCovered(ITEM_ID, RANGE_OFFSET, RANGE_LENGTH)).isFalse();
  }

  @Test
  void close_anotherSchedulerStillOpen_keepsCompletedRange() {
    try (PrefetchScheduler otherScheduler = new PrefetchScheduler(ITEM_ID, bufferCache)) {
      otherScheduler.schedule(
          channel,
          ImmutableList.of(Range.closedOpen(RANGE_OFFSET, RANGE_OFFSET + RANGE_LENGTH)),
          CONTENT_LENGTH);

      scheduler.close();

      assertThat(bufferCache.isCovered(ITEM_ID, RANGE_OFFSET, RANGE_LENGTH)).isTrue();
    }
  }

  @Test
  void close_calledTwiceWithAnotherSchedulerOpen_keepsCompletedRange() {
    try (PrefetchScheduler otherScheduler = new PrefetchScheduler(ITEM_ID, bufferCache)) {
      otherScheduler.schedule(
          channel,
          ImmutableList.of(Range.closedOpen(RANGE_OFFSET, RANGE_OFFSET + RANGE_LENGTH)),
          CONTENT_LENGTH);

      scheduler.close();
      scheduler.close();

      assertThat(bufferCache.isCovered(ITEM_ID, RANGE_OFFSET, RANGE_LENGTH)).isTrue();
    }
  }

  private static byte[] createContent() {
    byte[] content = new byte[CONTENT_LENGTH];
    for (int index = 0; index < CONTENT_LENGTH; index++) {
      content[index] = (byte) (index % 251);
    }
    return content;
  }

  private void schedule(long... rangeOffsets) {
    ImmutableList.Builder<Range<Long>> ranges = ImmutableList.builder();
    for (long offset : rangeOffsets) {
      ranges.add(Range.closedOpen(offset, offset + RANGE_LENGTH));
    }
    scheduler.schedule(channel, ranges.build(), CONTENT_LENGTH);
  }

  private byte[] readCachedRange(long offset, int length) {
    ByteBuffer destination = ByteBuffer.allocate(length);
    int copiedBytes = bufferCache.serveFromCache(ITEM_ID, offset, destination);
    byte[] bytes = new byte[copiedBytes];
    destination.flip();
    destination.get(bytes);
    return bytes;
  }
}
