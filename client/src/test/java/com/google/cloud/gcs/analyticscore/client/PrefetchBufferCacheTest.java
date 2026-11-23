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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.gcs.analyticscore.client.PrefetchBufferCache.RegisteredStream;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants;
import com.google.cloud.gcs.analyticscore.common.telemetry.OperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.RecordingOperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class PrefetchBufferCacheTest {

  private static final long MAX_SIZE_BYTES = 1024;
  private static final Runnable NO_PROMOTION = () -> {};
  private static final long TTL_SECONDS = 60;
  private static final long FIRST_RANGE_OFFSET = 0;
  private static final long SECOND_RANGE_OFFSET = 4;
  private static final byte[] FIRST_RANGE = {1, 2, 3, 4};
  private static final byte[] SECOND_RANGE = {5, 6, 7, 8};
  private static final byte[] COMBINED_RANGES = {1, 2, 3, 4, 5, 6, 7, 8};
  private static final int WORKER_THREADS = 8;
  private static final long WAIT_TIMEOUT_SECONDS = 30;

  private final AtomicLong fakeClockNanos = new AtomicLong();

  @AfterEach
  void clearInterruptStatus() {
    Thread.interrupted();
  }

  @ParameterizedTest
  @CsvSource({"0, 60, maxSizeBytes must be positive", "1024, 0, ttlSeconds must be positive"})
  void constructor_nonPositiveArgument_throwsIllegalArgumentException(
      long maxSizeBytes, long ttlSeconds, String expectedMessage) {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new PrefetchBufferCache(
                    maxSizeBytes, ttlSeconds, new Telemetry(ImmutableList.of())));

    assertThat(exception).hasMessageThat().isEqualTo(expectedMessage);
  }

  @Test
  void serveFromCache_absentRange_returnsZero() {
    PrefetchBufferCache cache = newCache();
    ByteBuffer destination = ByteBuffer.allocate(FIRST_RANGE.length);

    int servedBytes = cache.serveFromCache(itemId("object"), FIRST_RANGE_OFFSET, destination);

    assertThat(servedBytes).isEqualTo(0);
  }

  @Test
  void serveFromCache_completedRange_copiesStoredBytes() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), 13, FIRST_RANGE);
    ByteBuffer destination = ByteBuffer.allocate(FIRST_RANGE.length);

    int unusedServedBytes = cache.serveFromCache(itemId("object"), 13, destination);

    assertThat(writtenBytes(destination)).isEqualTo(FIRST_RANGE);
  }

  @Test
  void serveFromCache_positionInsideRange_copiesFromThatPosition() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);
    ByteBuffer destination = ByteBuffer.allocate(FIRST_RANGE.length);

    int unusedServedBytes = cache.serveFromCache(itemId("object"), 2, destination);

    assertThat(writtenBytes(destination)).isEqualTo(new byte[] {3, 4});
  }

  @Test
  void serveFromCache_acrossAdjacentRanges_servesCombinedBytes() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerCompleted(stream, FIRST_RANGE_OFFSET, FIRST_RANGE);
    registerCompleted(stream, SECOND_RANGE_OFFSET, SECOND_RANGE);
    ByteBuffer destination = ByteBuffer.allocate(COMBINED_RANGES.length);

    int unusedServedBytes = cache.serveFromCache(itemId("object"), FIRST_RANGE_OFFSET, destination);

    assertThat(writtenBytes(destination)).isEqualTo(COMBINED_RANGES);
  }

  @Test
  void serveFromCache_differentItemIdsSameOffset_returnsIndependentRanges() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);
    registerCompleted(
        cache.registerStream(itemId("other-object")), FIRST_RANGE_OFFSET, SECOND_RANGE);
    ByteBuffer destination = ByteBuffer.allocate(FIRST_RANGE.length);

    int unusedServedBytes = cache.serveFromCache(itemId("object"), FIRST_RANGE_OFFSET, destination);

    assertThat(writtenBytes(destination)).isEqualTo(FIRST_RANGE);
  }

  @Test
  void serveFromCache_negativePosition_throwsIllegalArgumentException() {
    PrefetchBufferCache cache = newCache();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> cache.serveFromCache(itemId("object"), -1, ByteBuffer.allocate(4)));

    assertThat(exception).hasMessageThat().isEqualTo("position -1 must be non-negative");
  }

  @Test
  void serveFromCache_emptyDestination_returnsZero() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);

    int servedBytes =
        cache.serveFromCache(itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(0));

    assertThat(servedBytes).isEqualTo(0);
  }

  @Test
  void serveFromCache_positionNearLongMaxValue_returnsZero() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);

    int servedBytes =
        cache.serveFromCache(itemId("object"), Long.MAX_VALUE - 2, ByteBuffer.allocate(10));

    assertThat(servedBytes).isEqualTo(0);
  }

  @Test
  void serveFromCache_interruptedWhileRangeInFlight_returnsZero() {
    PrefetchBufferCache cache = newCache();
    registerInFlight(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, 4);
    Thread.currentThread().interrupt();

    int servedBytes =
        cache.serveFromCache(itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(4));

    assertThat(servedBytes).isEqualTo(0);
  }

  @Test
  void serveFromCache_interruptedWhileRangeInFlight_keepsRange() {
    PrefetchBufferCache cache = newCache();
    registerInFlight(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, 4);
    Thread.currentThread().interrupt();

    int unusedServedBytes =
        cache.serveFromCache(itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(4));

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 4)).isTrue();
  }

  @Test
  void serveFromCache_nestedInFlightRangeFails_servesFromEnclosingRange() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    CompletableFuture<ByteBuffer> nestedFuture = new CompletableFuture<>();
    stream.registerRange(
        SECOND_RANGE_OFFSET,
        SECOND_RANGE.length,
        nestedFuture,
        () -> nestedFuture.completeExceptionally(new RuntimeException("network error")));
    registerCompleted(stream, FIRST_RANGE_OFFSET, COMBINED_RANGES);
    ByteBuffer destination = ByteBuffer.allocate(SECOND_RANGE.length);

    int unusedServedBytes =
        cache.serveFromCache(itemId("object"), SECOND_RANGE_OFFSET, destination);

    assertThat(writtenBytes(destination)).isEqualTo(SECOND_RANGE);
  }

  @Test
  void serveFromCache_windowMatchesSmallerOfNestedRanges_evictsOnlySmallerRange() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerCompleted(stream, SECOND_RANGE_OFFSET, SECOND_RANGE);
    registerCompleted(stream, FIRST_RANGE_OFFSET, COMBINED_RANGES);

    int unusedServedBytes =
        cache.serveFromCache(
            itemId("object"), SECOND_RANGE_OFFSET, ByteBuffer.allocate(SECOND_RANGE.length));

    assertThat(cache.getReservedBytes()).isEqualTo(COMBINED_RANGES.length);
  }

  @Test
  void serveFromCache_wholeEnclosingRangeRead_evictsNestedRange() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerCompleted(stream, SECOND_RANGE_OFFSET, SECOND_RANGE);
    registerCompleted(stream, FIRST_RANGE_OFFSET, COMBINED_RANGES);

    int unusedServedBytes =
        cache.serveFromCache(
            itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(COMBINED_RANGES.length));

    assertThat(cache.isCovered(itemId("object"), SECOND_RANGE_OFFSET, 1)).isFalse();
  }

  @Test
  void serveFromCache_enclosingRangeReadThroughNestedRange_evictsEnclosingRange() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerCompleted(stream, SECOND_RANGE_OFFSET, SECOND_RANGE);
    registerCompleted(stream, FIRST_RANGE_OFFSET, COMBINED_RANGES);
    int unusedHeadBytes =
        cache.serveFromCache(itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(4));

    int unusedTailBytes =
        cache.serveFromCache(itemId("object"), SECOND_RANGE_OFFSET, ByteBuffer.allocate(4));

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 1)).isFalse();
  }

  @Test
  void serveFromCache_overlappingReads_keepsRangeUntilEveryByteIsRead() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, COMBINED_RANGES);
    int unusedHeadBytes =
        cache.serveFromCache(itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(4));

    int unusedOverlappingBytes =
        cache.serveFromCache(itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(6));

    assertThat(cache.isCovered(itemId("object"), 6, 2)).isTrue();
  }

  @Test
  void serveFromCache_segmentExhausted_evictsSegment() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);

    int unusedServedBytes =
        cache.serveFromCache(
            itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(FIRST_RANGE.length));

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 1)).isFalse();
  }

  @Test
  void serveFromCache_tailReadBeforeHead_keepsRange() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), 10, COMBINED_RANGES);

    int unusedTailBytes = cache.serveFromCache(itemId("object"), 14, ByteBuffer.allocate(4));

    assertThat(cache.isCovered(itemId("object"), 10, 4)).isTrue();
  }

  @Test
  void serveFromCache_headReadAfterTail_evictsRange() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), 10, COMBINED_RANGES);
    int unusedTailBytes = cache.serveFromCache(itemId("object"), 14, ByteBuffer.allocate(4));

    int unusedHeadBytes = cache.serveFromCache(itemId("object"), 10, ByteBuffer.allocate(4));

    assertThat(cache.isCovered(itemId("object"), 10, 1)).isFalse();
  }

  @Test
  void serveFromCache_noSingleRangeCoversWindow_readsOnlyFromRangeReachingFurthest() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    AtomicInteger otherRangePromotions = new AtomicInteger();
    registerInFlightCountingPromotions(stream, 4, new byte[2], otherRangePromotions);
    registerCompleted(stream, 3, new byte[6]);
    registerInFlightCountingPromotions(
        stream, FIRST_RANGE_OFFSET, COMBINED_RANGES, otherRangePromotions);

    int unusedServedBytes = cache.serveFromCache(itemId("object"), 5, ByteBuffer.allocate(10));

    assertThat(otherRangePromotions.get()).isEqualTo(0);
  }

  @Test
  void serveFromCache_windowCoveredByNestedAndEnclosingRanges_readsOnlyFromSmallerRange() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    AtomicInteger enclosingRangePromotions = new AtomicInteger();
    registerCompleted(stream, SECOND_RANGE_OFFSET, SECOND_RANGE);
    registerInFlightCountingPromotions(
        stream, FIRST_RANGE_OFFSET, COMBINED_RANGES, enclosingRangePromotions);

    int unusedServedBytes =
        cache.serveFromCache(
            itemId("object"), SECOND_RANGE_OFFSET, ByteBuffer.allocate(SECOND_RANGE.length));

    assertThat(enclosingRangePromotions.get()).isEqualTo(0);
  }

  @Test
  void serveFromCache_downloadedEnclosingAndInFlightNestedRanges_readsOnlyFromDownloadedRange() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    AtomicInteger nestedRangePromotions = new AtomicInteger();
    registerInFlightCountingPromotions(
        stream, SECOND_RANGE_OFFSET, SECOND_RANGE, nestedRangePromotions);
    registerCompleted(stream, FIRST_RANGE_OFFSET, COMBINED_RANGES);

    int unusedServedBytes =
        cache.serveFromCache(
            itemId("object"), SECOND_RANGE_OFFSET, ByteBuffer.allocate(SECOND_RANGE.length));

    assertThat(nestedRangePromotions.get()).isEqualTo(0);
  }

  @Test
  void serveFromCache_inFlightRangesWithSameStart_readsOnlyFromSmallerRange() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    AtomicInteger largerRangePromotions = new AtomicInteger();
    registerInFlightCountingPromotions(
        stream, FIRST_RANGE_OFFSET, FIRST_RANGE, new AtomicInteger());
    registerInFlightCountingPromotions(
        stream, FIRST_RANGE_OFFSET, COMBINED_RANGES, largerRangePromotions);

    int unusedServedBytes =
        cache.serveFromCache(
            itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(FIRST_RANGE.length));

    assertThat(largerRangePromotions.get()).isEqualTo(0);
  }

  @Test
  void serveFromCache_earlierRangeEndsBeforeRead_keepsEarlierRange() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerCompleted(stream, FIRST_RANGE_OFFSET, new byte[] {1, 2});
    registerCompleted(stream, 2, new byte[] {3, 4, 5, 6, 7, 8});

    int unusedServedBytes = cache.serveFromCache(itemId("object"), 4, ByteBuffer.allocate(2));

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 2)).isTrue();
  }

  @Test
  void serveFromCache_inFlightRange_runsPromotionCallback() {
    PrefetchBufferCache cache = newCache();
    AtomicBoolean promoted = new AtomicBoolean();
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    cache
        .registerStream(itemId("object"))
        .registerRange(
            FIRST_RANGE_OFFSET,
            FIRST_RANGE.length,
            future,
            () -> {
              promoted.set(true);
              future.complete(bufferOf(FIRST_RANGE));
            });

    int unusedServedBytes =
        cache.serveFromCache(
            itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(FIRST_RANGE.length));

    assertThat(promoted.get()).isTrue();
  }

  @Test
  void serveFromCache_completedRange_recordsCacheHit() {
    RecordingOperationListener listener = new RecordingOperationListener();
    PrefetchBufferCache cache = newCache(listener);
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);

    int unusedServedBytes =
        cache.serveFromCache(itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(4));

    assertThat(listener.getTotal(GcsAnalyticsCoreTelemetryConstants.Metric.PREFETCH_CACHE_HIT))
        .isEqualTo(1);
  }

  @Test
  void serveFromCache_absentRange_recordsNoCacheHit() {
    RecordingOperationListener listener = new RecordingOperationListener();
    PrefetchBufferCache cache = newCache(listener);

    int unusedServedBytes =
        cache.serveFromCache(itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(4));

    assertThat(listener.getTotal(GcsAnalyticsCoreTelemetryConstants.Metric.PREFETCH_CACHE_HIT))
        .isEqualTo(0);
  }

  @Test
  void recordCacheMiss_recordsOneCacheMiss() {
    RecordingOperationListener listener = new RecordingOperationListener();
    PrefetchBufferCache cache = newCache(listener);

    cache.recordCacheMiss();

    assertThat(listener.getTotal(GcsAnalyticsCoreTelemetryConstants.Metric.PREFETCH_CACHE_MISS))
        .isEqualTo(1);
  }

  @Test
  void serveFromCache_overlappingReadsOfSameRange_countsConsumedBytesOnce() {
    RecordingOperationListener listener = new RecordingOperationListener();
    PrefetchBufferCache cache = newCache(listener);
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, COMBINED_RANGES);
    int unusedHeadBytes =
        cache.serveFromCache(itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(4));

    int unusedAllBytes =
        cache.serveFromCache(
            itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(COMBINED_RANGES.length));

    assertThat(listener.getTotal(GcsAnalyticsCoreTelemetryConstants.Metric.PREFETCH_BYTES_CONSUMED))
        .isEqualTo(COMBINED_RANGES.length);
  }

  @Test
  void serveFromCache_bytesReadThroughNestedAndEnclosingRanges_countsConsumedBytesOnce() {
    RecordingOperationListener listener = new RecordingOperationListener();
    PrefetchBufferCache cache = newCache(listener);
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerCompleted(stream, SECOND_RANGE_OFFSET, SECOND_RANGE);
    registerCompleted(stream, FIRST_RANGE_OFFSET, COMBINED_RANGES);
    int unusedNestedBytes =
        cache.serveFromCache(itemId("object"), SECOND_RANGE_OFFSET, ByteBuffer.allocate(4));

    int unusedAllBytes =
        cache.serveFromCache(
            itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(COMBINED_RANGES.length));

    assertThat(listener.getTotal(GcsAnalyticsCoreTelemetryConstants.Metric.PREFETCH_BYTES_CONSUMED))
        .isEqualTo(COMBINED_RANGES.length);
  }

  @Test
  void serveFromCache_bytesAlreadyConsumed_skipsConsumedBytesMetric() {
    RecordingOperationListener listener = new RecordingOperationListener();
    PrefetchBufferCache cache = newCache(listener);
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, COMBINED_RANGES);
    int unusedFirstHeadBytes =
        cache.serveFromCache(itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(4));

    int unusedSecondHeadBytes =
        cache.serveFromCache(itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(4));

    assertThat(
            listener.getValues(GcsAnalyticsCoreTelemetryConstants.Metric.PREFETCH_BYTES_CONSUMED))
        .containsExactly(4L);
  }

  @Test
  void registerRange_zeroLength_throwsIllegalArgumentException() {
    RegisteredStream stream = newCache().registerStream(itemId("object"));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                stream.registerRange(
                    FIRST_RANGE_OFFSET, 0, new CompletableFuture<>(), NO_PROMOTION));

    assertThat(exception).hasMessageThat().isEqualTo("length 0 must be positive");
  }

  @Test
  void registerRange_unregisteredRange_returnsTrue() {
    RegisteredStream stream = newCache().registerStream(itemId("object"));

    boolean registered = registerInFlight(stream, FIRST_RANGE_OFFSET, 4);

    assertThat(registered).isTrue();
  }

  @Test
  void registerRange_alreadyCoveredByInFlightRange_returnsFalse() {
    RegisteredStream stream = newCache().registerStream(itemId("object"));
    registerInFlight(stream, 10, 8);

    boolean registered = registerInFlight(stream, 12, 4);

    assertThat(registered).isFalse();
  }

  @Test
  void registerRange_coveredByRangeOfAnotherStream_returnsFalse() {
    PrefetchBufferCache cache = newCache();
    registerInFlight(cache.registerStream(itemId("object")), 10, 8);

    boolean registered = registerInFlight(cache.registerStream(itemId("object")), 12, 4);

    assertThat(registered).isFalse();
  }

  @Test
  void registerRange_closedStream_returnsFalse() {
    RegisteredStream stream = newCache().registerStream(itemId("object"));
    stream.close();

    boolean registered = registerInFlight(stream, FIRST_RANGE_OFFSET, 4);

    assertThat(registered).isFalse();
  }

  @Test
  void registerRange_closedStreamWhileAnotherStreamIsOpen_returnsFalse() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream closedStream = cache.registerStream(itemId("object"));
    RegisteredStream unusedOpenStream = cache.registerStream(itemId("object"));
    closedStream.close();

    boolean registered = registerInFlight(closedStream, FIRST_RANGE_OFFSET, 4);

    assertThat(registered).isFalse();
  }

  @Test
  void registerRange_newStreamAfterLastStreamClosed_returnsTrue() {
    PrefetchBufferCache cache = newCache(/* maxSizeBytes= */ 4);
    RegisteredStream closedStream = cache.registerStream(itemId("object"));
    registerInFlight(closedStream, FIRST_RANGE_OFFSET, 4);
    closedStream.close();

    boolean registered =
        registerInFlight(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, 4);

    assertThat(registered).isTrue();
  }

  @ParameterizedTest
  @MethodSource("badDownloads")
  void registerRange_downloadEndsWithoutWholeRange_evictsRange(
      Consumer<CompletableFuture<ByteBuffer>> badDownload) {
    PrefetchBufferCache cache = newCache();
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    cache
        .registerStream(itemId("object"))
        .registerRange(FIRST_RANGE_OFFSET, 4, future, NO_PROMOTION);

    badDownload.accept(future);

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 4)).isFalse();
  }

  @ParameterizedTest
  @MethodSource("badDownloads")
  void registerRange_downloadAlreadyEndedWithoutWholeRange_returnsFalse(
      Consumer<CompletableFuture<ByteBuffer>> badDownload) {
    RegisteredStream stream = newCache().registerStream(itemId("object"));
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    badDownload.accept(future);

    boolean registered = stream.registerRange(FIRST_RANGE_OFFSET, 4, future, NO_PROMOTION);

    assertThat(registered).isFalse();
  }

  @Test
  void registerRange_sameRangeAfterShortBuffer_returnsTrue() {
    RegisteredStream stream = newCache(/* maxSizeBytes= */ 4).registerStream(itemId("object"));
    CompletableFuture<ByteBuffer> shortDownload = new CompletableFuture<>();
    stream.registerRange(FIRST_RANGE_OFFSET, 4, shortDownload, NO_PROMOTION);
    shortDownload.complete(bufferOf(new byte[] {1, 2}));

    boolean registered = registerCompleted(stream, FIRST_RANGE_OFFSET, FIRST_RANGE);

    assertThat(registered).isTrue();
  }

  @Test
  void registerRange_evictedFutureFailsAfterReRegistration_keepsNewRange() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream closedStream = cache.registerStream(itemId("object"));
    CompletableFuture<ByteBuffer> evictedFuture = new CompletableFuture<>();
    closedStream.registerRange(FIRST_RANGE_OFFSET, 4, evictedFuture, NO_PROMOTION);
    closedStream.close();
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);

    evictedFuture.completeExceptionally(new RuntimeException("cancelled"));

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 4)).isTrue();
  }

  @Test
  void registerRange_largerRangeWithSameStart_keepsBothRanges() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerCompleted(stream, FIRST_RANGE_OFFSET, FIRST_RANGE);

    registerCompleted(stream, FIRST_RANGE_OFFSET, COMBINED_RANGES);

    assertThat(cache.getReservedBytes()).isEqualTo(FIRST_RANGE.length + COMBINED_RANGES.length);
  }

  @Test
  void registerRange_rangeLongerThanMaxSizeBytes_returnsFalse() {
    RegisteredStream stream = newCache(/* maxSizeBytes= */ 4).registerStream(itemId("object"));

    boolean registered = registerInFlight(stream, FIRST_RANGE_OFFSET, 8);

    assertThat(registered).isFalse();
  }

  @Test
  void registerRange_rangeLongerThanMaxSizeBytes_keepsIdleRanges() {
    PrefetchBufferCache cache = newCacheWithFakeClock(/* maxSizeBytes= */ 8);
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerInFlight(stream, FIRST_RANGE_OFFSET, 8);
    advanceFakeClock(TTL_SECONDS + 1);

    registerInFlight(stream, 8, 16);

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 8)).isTrue();
  }

  @Test
  void registerRange_coveredRangeWhenCacheIsFull_keepsIdleRanges() {
    PrefetchBufferCache cache = newCacheWithFakeClock(/* maxSizeBytes= */ 8);
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerInFlight(stream, FIRST_RANGE_OFFSET, 8);
    advanceFakeClock(TTL_SECONDS + 1);

    registerInFlight(stream, 2, 4);

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 8)).isTrue();
  }

  @Test
  void registerRange_rangeFillsCacheExactly_returnsTrue() {
    RegisteredStream stream = newCache(/* maxSizeBytes= */ 8).registerStream(itemId("object"));
    registerInFlight(stream, FIRST_RANGE_OFFSET, 4);

    boolean registered = registerInFlight(stream, SECOND_RANGE_OFFSET, 4);

    assertThat(registered).isTrue();
  }

  @Test
  void registerRange_cacheFullOfInFlightRanges_returnsFalse() {
    RegisteredStream stream = newCache(/* maxSizeBytes= */ 8).registerStream(itemId("object"));
    registerInFlight(stream, FIRST_RANGE_OFFSET, 8);

    boolean registered = registerInFlight(stream, 8, 4);

    assertThat(registered).isFalse();
  }

  @Test
  void registerRange_cacheFullOfRangesOfAnotherItem_returnsFalse() {
    PrefetchBufferCache cache = newCache(/* maxSizeBytes= */ 8);
    registerInFlight(cache.registerStream(itemId("other-object")), FIRST_RANGE_OFFSET, 8);

    boolean registered =
        registerInFlight(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, 4);

    assertThat(registered).isFalse();
  }

  @Test
  void registerRange_rejectedRange_isNotCached() {
    PrefetchBufferCache cache = newCache(/* maxSizeBytes= */ 8);
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerInFlight(stream, FIRST_RANGE_OFFSET, 8);

    registerInFlight(stream, 8, 4);

    assertThat(cache.isCovered(itemId("object"), 8, 4)).isFalse();
  }

  @Test
  void registerRange_rejectedRange_keepsAdmittedRanges() {
    PrefetchBufferCache cache = newCache(/* maxSizeBytes= */ 8);
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerCompleted(stream, FIRST_RANGE_OFFSET, COMBINED_RANGES);

    registerInFlight(stream, 8, 4);

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, COMBINED_RANGES.length))
        .isTrue();
  }

  @Test
  void registerRange_afterFullRangeWasConsumed_returnsTrue() {
    PrefetchBufferCache cache = newCache(/* maxSizeBytes= */ 4);
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerCompleted(stream, FIRST_RANGE_OFFSET, FIRST_RANGE);
    int unusedServedBytes =
        cache.serveFromCache(itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(4));

    boolean registered = registerInFlight(stream, SECOND_RANGE_OFFSET, 4);

    assertThat(registered).isTrue();
  }

  @Test
  void registerRange_concurrentOffersBeyondMaxSizeBytes_admitsOnlyRangesThatFit() throws Exception {
    PrefetchBufferCache cache = newCache(/* maxSizeBytes= */ 64);
    RegisteredStream stream = cache.registerStream(itemId("object"));
    ExecutorService executor = Executors.newFixedThreadPool(WORKER_THREADS);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<Boolean>> admissions = new ArrayList<>();
    for (int i = 0; i < 400; i++) {
      long offset = i * 4L;
      admissions.add(
          executor.submit(
              () -> {
                start.await();
                return registerInFlight(stream, offset, 4);
              }));
    }

    start.countDown();
    int admitted = countAdmitted(admissions);
    executor.shutdownNow();

    assertThat(admitted).isEqualTo(16);
  }

  @Test
  void isCovered_rangeInsideInFlightRange_returnsTrue() {
    PrefetchBufferCache cache = newCache();
    registerInFlight(cache.registerStream(itemId("object")), 10, 8);

    assertThat(cache.isCovered(itemId("object"), 12, 4)).isTrue();
  }

  @Test
  void isCovered_itemWithoutOpenStream_returnsFalse() {
    PrefetchBufferCache cache = newCache();

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 4)).isFalse();
  }

  @Test
  void isCovered_acrossRangesWithGap_returnsFalse() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerCompleted(stream, FIRST_RANGE_OFFSET, FIRST_RANGE);
    registerCompleted(stream, 6, SECOND_RANGE);

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 10)).isFalse();
  }

  @Test
  void isCovered_negativeLength_returnsFalse() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);

    assertThat(cache.isCovered(itemId("object"), 2, -1)).isFalse();
  }

  @Test
  void isCovered_offsetAtLongMaxValue_returnsFalse() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);

    assertThat(cache.isCovered(itemId("object"), Long.MAX_VALUE, 10)).isFalse();
  }

  @Test
  void registerRange_cacheFullOfRangesIdleLongerThanTtl_returnsTrue() {
    RegisteredStream stream =
        newCacheWithFakeClock(/* maxSizeBytes= */ 8).registerStream(itemId("object"));
    registerInFlight(stream, FIRST_RANGE_OFFSET, 8);
    advanceFakeClock(TTL_SECONDS + 1);

    boolean registered = registerInFlight(stream, 8, 4);

    assertThat(registered).isTrue();
  }

  @Test
  void registerRange_cacheFullOfRangesIdleShorterThanTtl_returnsFalse() {
    RegisteredStream stream =
        newCacheWithFakeClock(/* maxSizeBytes= */ 8).registerStream(itemId("object"));
    registerInFlight(stream, FIRST_RANGE_OFFSET, 8);
    advanceFakeClock(TTL_SECONDS - 1);

    boolean registered = registerInFlight(stream, 8, 4);

    assertThat(registered).isFalse();
  }

  @Test
  void registerRange_cacheFullOfRangesIdleLongerThanTtl_evictsIdleRanges() {
    PrefetchBufferCache cache = newCacheWithFakeClock(/* maxSizeBytes= */ 8);
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerInFlight(stream, FIRST_RANGE_OFFSET, 8);
    advanceFakeClock(TTL_SECONDS + 1);

    registerInFlight(stream, 8, 4);

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 8)).isFalse();
  }

  @Test
  void registerRange_cacheHasRoom_keepsRangesIdleLongerThanTtl() {
    PrefetchBufferCache cache = newCacheWithFakeClock(MAX_SIZE_BYTES);
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerInFlight(stream, FIRST_RANGE_OFFSET, 4);
    advanceFakeClock(TTL_SECONDS + 1);

    registerInFlight(stream, SECOND_RANGE_OFFSET, 4);

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 4)).isTrue();
  }

  @Test
  void registerRange_coveredOfferBeforeTtl_doesNotExtendIdleTime() {
    PrefetchBufferCache cache = newCacheWithFakeClock(/* maxSizeBytes= */ 8);
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerCompleted(stream, FIRST_RANGE_OFFSET, COMBINED_RANGES);
    advanceFakeClock(TTL_SECONDS - 1);
    registerInFlight(stream, 2, 4);
    advanceFakeClock(2);

    registerInFlight(stream, 8, 4);

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 4)).isFalse();
  }

  @Test
  void registerRange_downloadCompletesBeforeTtl_extendsIdleTime() {
    PrefetchBufferCache cache = newCacheWithFakeClock(/* maxSizeBytes= */ 4);
    RegisteredStream stream = cache.registerStream(itemId("object"));
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    stream.registerRange(FIRST_RANGE_OFFSET, 4, future, NO_PROMOTION);
    advanceFakeClock(TTL_SECONDS - 1);
    future.complete(bufferOf(FIRST_RANGE));
    advanceFakeClock(2);

    registerInFlight(stream, SECOND_RANGE_OFFSET, 4);

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 4)).isTrue();
  }

  @Test
  void serveFromCache_readBeforeTtl_extendsIdleTime() {
    PrefetchBufferCache cache = newCacheWithFakeClock(/* maxSizeBytes= */ 4);
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerCompleted(stream, FIRST_RANGE_OFFSET, FIRST_RANGE);
    advanceFakeClock(TTL_SECONDS - 1);
    int unusedServedBytes =
        cache.serveFromCache(itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(2));
    advanceFakeClock(2);

    registerInFlight(stream, SECOND_RANGE_OFFSET, 4);

    assertThat(cache.isCovered(itemId("object"), 2, 2)).isTrue();
  }

  @Test
  void serveFromCache_readOfNeighbouringRange_doesNotExtendIdleTime() {
    PrefetchBufferCache cache = newCacheWithFakeClock(/* maxSizeBytes= */ 8);
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerCompleted(stream, FIRST_RANGE_OFFSET, FIRST_RANGE);
    registerCompleted(stream, SECOND_RANGE_OFFSET, SECOND_RANGE);
    advanceFakeClock(TTL_SECONDS - 1);
    int unusedServedBytes =
        cache.serveFromCache(itemId("object"), SECOND_RANGE_OFFSET + 1, ByteBuffer.allocate(1));
    advanceFakeClock(2);

    registerInFlight(stream, 8, 4);

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 4)).isFalse();
  }

  @Test
  void registerStream_otherItemCached_keepsOtherItemRangesOnClose() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerInFlight(stream, FIRST_RANGE_OFFSET, 4);
    registerInFlight(cache.registerStream(itemId("other")), FIRST_RANGE_OFFSET, 4);

    stream.close();

    assertThat(cache.isCovered(itemId("other"), FIRST_RANGE_OFFSET, 4)).isTrue();
  }

  @Test
  void registerStream_lastStreamClosed_evictsEveryRange() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerInFlight(stream, FIRST_RANGE_OFFSET, 4);
    registerInFlight(stream, SECOND_RANGE_OFFSET, 4);

    stream.close();

    assertThat(cache.isCovered(itemId("object"), SECOND_RANGE_OFFSET, 4)).isFalse();
  }

  @Test
  void registerStream_anotherStreamStillOpen_keepsRangesOnClose() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream firstStream = cache.registerStream(itemId("object"));
    RegisteredStream unusedSecondStream = cache.registerStream(itemId("object"));
    registerInFlight(firstStream, SECOND_RANGE_OFFSET, 4);

    firstStream.close();

    assertThat(cache.isCovered(itemId("object"), SECOND_RANGE_OFFSET, 4)).isTrue();
  }

  @Test
  void registerStream_closedTwice_doesNotEvictOtherOpenStreamRanges() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream firstStream = cache.registerStream(itemId("object"));
    RegisteredStream secondStream = cache.registerStream(itemId("object"));
    registerInFlight(secondStream, SECOND_RANGE_OFFSET, 4);

    firstStream.close();
    firstStream.close();

    assertThat(cache.isCovered(itemId("object"), SECOND_RANGE_OFFSET, 4)).isTrue();
  }

  @Test
  void close_concurrentRegistrationsReadsAndEvictions_releasesAllReservedBytes() throws Exception {
    PrefetchBufferCache cache = newCache(/* maxSizeBytes= */ 64);

    runOnAllWorkers(() -> registerReadAndCloseRepeatedly(cache));

    assertThat(cache.getReservedBytes()).isEqualTo(0);
  }

  @Test
  void invalidateAll_discardsInFlightRanges() {
    PrefetchBufferCache cache = newCache();
    registerInFlight(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, 4);

    cache.invalidateAll();

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 4)).isFalse();
  }

  @Test
  void invalidateAll_openStream_stillRegistersRanges() {
    PrefetchBufferCache cache = newCache(/* maxSizeBytes= */ 4);
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerInFlight(stream, FIRST_RANGE_OFFSET, 4);
    cache.invalidateAll();

    boolean registered = registerInFlight(stream, FIRST_RANGE_OFFSET, 4);

    assertThat(registered).isTrue();
  }

  @Test
  void registerRange_streamClosesDuringCall_returnsFalse() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream unusedOpenStream = cache.registerStream(itemId("object"));
    RegisteredStream closingStream = cache.registerStream(itemId("object"));
    // The cache checks the download after it first checks the stream, so this future closes the
    // stream in the middle of the call.
    CompletableFuture<ByteBuffer> futureThatClosesStream =
        new CompletableFuture<ByteBuffer>() {
          @Override
          public boolean isDone() {
            closingStream.close();
            return super.isDone();
          }
        };

    boolean registered =
        closingStream.registerRange(FIRST_RANGE_OFFSET, 4, futureThatClosesStream, NO_PROMOTION);

    assertThat(registered).isFalse();
  }

  @Test
  void registerRange_closedStreamWithFullCache_keepsIdleRangesOfOtherStreams() {
    PrefetchBufferCache cache = newCacheWithFakeClock(/* maxSizeBytes= */ 8);
    registerInFlight(cache.registerStream(itemId("other-object")), FIRST_RANGE_OFFSET, 8);
    RegisteredStream closedStream = cache.registerStream(itemId("object"));
    closedStream.close();
    advanceFakeClock(TTL_SECONDS + 1);

    registerInFlight(closedStream, FIRST_RANGE_OFFSET, 4);

    assertThat(cache.isCovered(itemId("other-object"), FIRST_RANGE_OFFSET, 8)).isTrue();
  }

  @Test
  void serveVectoredFromCache_completedRange_completesRangeWithItsBytes() throws Exception {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);
    GcsObjectRange range = vectoredRange(FIRST_RANGE_OFFSET, 4);

    serveVectored(cache, range);

    assertThat(readBytes(range)).isEqualTo(FIRST_RANGE);
  }

  @Test
  void serveVectoredFromCache_completedRange_returnsNoUnservedRanges() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);

    List<GcsObjectRange> unservedRanges =
        serveVectored(cache, vectoredRange(FIRST_RANGE_OFFSET, 4));

    assertThat(unservedRanges).isEmpty();
  }

  @Test
  void serveVectoredFromCache_rangeInsideCachedRange_completesRangeWithItsBytes() throws Exception {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);
    GcsObjectRange range = vectoredRange(1, 2);

    serveVectored(cache, range);

    assertThat(readBytes(range)).isEqualTo(new byte[] {2, 3});
  }

  @Test
  void serveVectoredFromCache_rangeSpanningAdjacentRanges_stitchesTheirBytes() throws Exception {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerCompleted(stream, FIRST_RANGE_OFFSET, FIRST_RANGE);
    registerCompleted(stream, SECOND_RANGE_OFFSET, SECOND_RANGE);
    GcsObjectRange range = vectoredRange(FIRST_RANGE_OFFSET, 8);

    serveVectored(cache, range);

    assertThat(readBytes(range)).isEqualTo(COMBINED_RANGES);
  }

  @Test
  void serveVectoredFromCache_uncachedAndCachedRanges_returnsOnlyUncachedRange() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);
    GcsObjectRange uncachedRange = vectoredRange(SECOND_RANGE_OFFSET, 4);

    List<GcsObjectRange> unservedRanges =
        serveVectored(cache, vectoredRange(FIRST_RANGE_OFFSET, 4), uncachedRange);

    assertThat(unservedRanges).containsExactly(uncachedRange);
  }

  @Test
  void serveVectoredFromCache_partlyCachedRange_returnsRange() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);
    GcsObjectRange range = vectoredRange(2, 4);

    List<GcsObjectRange> unservedRanges = serveVectored(cache, range);

    assertThat(unservedRanges).containsExactly(range);
  }

  @Test
  void serveVectoredFromCache_itemWithoutOpenStream_returnsAllRanges() {
    PrefetchBufferCache cache = newCache();
    GcsObjectRange firstRange = vectoredRange(FIRST_RANGE_OFFSET, 4);
    GcsObjectRange secondRange = vectoredRange(SECOND_RANGE_OFFSET, 3);

    List<GcsObjectRange> unservedRanges = serveVectored(cache, firstRange, secondRange);

    assertThat(unservedRanges).containsExactly(firstRange, secondRange).inOrder();
  }

  @Test
  void serveVectoredFromCache_emptyRange_returnsRange() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);
    GcsObjectRange range = vectoredRange(1, 0);

    List<GcsObjectRange> unservedRanges = serveVectored(cache, range);

    assertThat(unservedRanges).containsExactly(range);
  }

  @ParameterizedTest
  @ValueSource(longs = {Long.MAX_VALUE - 2, Long.MAX_VALUE})
  void serveVectoredFromCache_rangeEndsPastLongMaxValue_returnsRange(long offset) {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), Long.MAX_VALUE - 4, FIRST_RANGE);
    GcsObjectRange range = vectoredRange(offset, 10);

    List<GcsObjectRange> unservedRanges = serveVectored(cache, range);

    assertThat(unservedRanges).containsExactly(range);
  }

  @Test
  void serveVectoredFromCache_rangeAlreadyDone_returnsNoUnservedRanges() {
    PrefetchBufferCache cache = newCache();
    GcsObjectRange range = vectoredRange(FIRST_RANGE_OFFSET, 4);
    range.getByteBufferFuture().cancel(false);

    List<GcsObjectRange> unservedRanges = serveVectored(cache, range);

    assertThat(unservedRanges).isEmpty();
  }

  @Test
  void serveVectoredFromCache_rangeAlreadyDone_doesNotPromoteDownload() {
    PrefetchBufferCache cache = newCache();
    AtomicInteger promotions = new AtomicInteger();
    registerInFlightCountingPromotions(
        cache.registerStream(itemId("object")), SECOND_RANGE_OFFSET, SECOND_RANGE, promotions);
    GcsObjectRange range = vectoredRange(SECOND_RANGE_OFFSET, 4);
    range.getByteBufferFuture().cancel(false);

    serveVectored(cache, range);

    assertThat(promotions.get()).isEqualTo(0);
  }

  @Test
  void serveVectoredFromCache_inFlightRange_completesRangeWhenDownloadFinishes() throws Exception {
    PrefetchBufferCache cache = newCache();
    CompletableFuture<ByteBuffer> download = new CompletableFuture<>();
    cache
        .registerStream(itemId("object"))
        .registerRange(SECOND_RANGE_OFFSET, 4, download, NO_PROMOTION);
    GcsObjectRange range = vectoredRange(SECOND_RANGE_OFFSET, 4);
    serveVectored(cache, range);

    download.complete(bufferOf(SECOND_RANGE));

    assertThat(readBytes(range)).isEqualTo(SECOND_RANGE);
  }

  @Test
  void serveVectoredFromCache_inFlightRange_promotesDownload() {
    PrefetchBufferCache cache = newCache();
    AtomicInteger promotions = new AtomicInteger();
    registerInFlightCountingPromotions(
        cache.registerStream(itemId("object")), SECOND_RANGE_OFFSET, SECOND_RANGE, promotions);

    serveVectored(cache, vectoredRange(SECOND_RANGE_OFFSET, 4));

    assertThat(promotions.get()).isEqualTo(1);
  }

  @ParameterizedTest
  @MethodSource("badDownloads")
  void serveVectoredFromCache_badDownload_passesRangeToFallback(
      Consumer<CompletableFuture<ByteBuffer>> badDownload) {
    PrefetchBufferCache cache = newCache();
    CompletableFuture<ByteBuffer> download = new CompletableFuture<>();
    cache
        .registerStream(itemId("object"))
        .registerRange(FIRST_RANGE_OFFSET, 4, download, NO_PROMOTION);
    GcsObjectRange range = vectoredRange(FIRST_RANGE_OFFSET, 4);
    List<GcsObjectRange> fallbackRanges = new ArrayList<>();
    List<GcsObjectRange> unusedUnservedRanges =
        cache.serveVectoredFromCache(
            itemId("object"), ImmutableList.of(range), ByteBuffer::allocate, fallbackRanges::add);

    badDownload.accept(download);

    assertThat(fallbackRanges).containsExactly(range);
  }

  @Test
  void serveVectoredFromCache_fallbackThrows_failsRangeWithThatException() {
    PrefetchBufferCache cache = newCache();
    CompletableFuture<ByteBuffer> download = new CompletableFuture<>();
    cache
        .registerStream(itemId("object"))
        .registerRange(FIRST_RANGE_OFFSET, 4, download, NO_PROMOTION);
    GcsObjectRange range = vectoredRange(FIRST_RANGE_OFFSET, 4);
    IllegalStateException fallbackError = new IllegalStateException("channel closed");
    List<GcsObjectRange> unusedUnservedRanges =
        cache.serveVectoredFromCache(
            itemId("object"),
            ImmutableList.of(range),
            ByteBuffer::allocate,
            failedRange -> {
              throw fallbackError;
            });

    download.completeExceptionally(new RuntimeException("network error"));

    ExecutionException thrown =
        assertThrows(ExecutionException.class, () -> range.getByteBufferFuture().get());
    assertThat(thrown).hasCauseThat().isSameInstanceAs(fallbackError);
  }

  @Test
  void serveVectoredFromCache_fallbackThrowsError_failsRange() {
    PrefetchBufferCache cache = newCache();
    CompletableFuture<ByteBuffer> download = new CompletableFuture<>();
    cache
        .registerStream(itemId("object"))
        .registerRange(FIRST_RANGE_OFFSET, 4, download, NO_PROMOTION);
    GcsObjectRange range = vectoredRange(FIRST_RANGE_OFFSET, 4);
    List<GcsObjectRange> unusedUnservedRanges =
        cache.serveVectoredFromCache(
            itemId("object"),
            ImmutableList.of(range),
            ByteBuffer::allocate,
            failedRange -> {
              throw new OutOfMemoryError("heap exhausted");
            });

    download.completeExceptionally(new RuntimeException("network error"));

    assertThat(range.getByteBufferFuture().isCompletedExceptionally()).isTrue();
  }

  @Test
  void serveVectoredFromCache_nestedInFlightRangeFails_servesFromEnclosingRange() throws Exception {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    CompletableFuture<ByteBuffer> nestedDownload = new CompletableFuture<>();
    CompletableFuture<ByteBuffer> enclosingDownload = new CompletableFuture<>();
    stream.registerRange(SECOND_RANGE_OFFSET, 4, nestedDownload, NO_PROMOTION);
    stream.registerRange(FIRST_RANGE_OFFSET, 8, enclosingDownload, NO_PROMOTION);
    GcsObjectRange range = vectoredRange(SECOND_RANGE_OFFSET, 4);
    serveVectored(cache, range);
    enclosingDownload.complete(bufferOf(COMBINED_RANGES));

    nestedDownload.completeExceptionally(new RuntimeException("network error"));

    assertThat(readBytes(range)).isEqualTo(SECOND_RANGE);
  }

  @Test
  void serveVectoredFromCache_promotionThrowsAfterFailedDownload_failsRangeWithThatException() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    CompletableFuture<ByteBuffer> nestedDownload = new CompletableFuture<>();
    RejectedExecutionException promotionError = new RejectedExecutionException("pool shut down");
    stream.registerRange(SECOND_RANGE_OFFSET, 4, nestedDownload, NO_PROMOTION);
    stream.registerRange(
        FIRST_RANGE_OFFSET,
        8,
        new CompletableFuture<>(),
        () -> {
          throw promotionError;
        });
    GcsObjectRange range = vectoredRange(SECOND_RANGE_OFFSET, 4);
    serveVectored(cache, range);

    nestedDownload.completeExceptionally(new RuntimeException("network error"));

    CompletionException thrown =
        assertThrows(CompletionException.class, () -> range.getByteBufferFuture().getNow(null));
    assertThat(thrown).hasCauseThat().isSameInstanceAs(promotionError);
  }

  @Test
  void serveVectoredFromCache_oneStitchedDownloadFails_passesRangeToFallback() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerCompleted(stream, FIRST_RANGE_OFFSET, FIRST_RANGE);
    CompletableFuture<ByteBuffer> download = new CompletableFuture<>();
    stream.registerRange(SECOND_RANGE_OFFSET, 4, download, NO_PROMOTION);
    GcsObjectRange range = vectoredRange(FIRST_RANGE_OFFSET, 8);
    List<GcsObjectRange> fallbackRanges = new ArrayList<>();
    List<GcsObjectRange> unusedUnservedRanges =
        cache.serveVectoredFromCache(
            itemId("object"), ImmutableList.of(range), ByteBuffer::allocate, fallbackRanges::add);

    download.completeExceptionally(new RuntimeException("network error"));

    assertThat(fallbackRanges).containsExactly(range);
  }

  @Test
  void serveVectoredFromCache_allocatorReturnsNull_failsRange() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);
    GcsObjectRange range = vectoredRange(FIRST_RANGE_OFFSET, 4);

    List<GcsObjectRange> unusedUnservedRanges =
        cache.serveVectoredFromCache(
            itemId("object"), ImmutableList.of(range), length -> null, failedRange -> {});

    assertThat(range.getByteBufferFuture().isCompletedExceptionally()).isTrue();
  }

  @Test
  void serveVectoredFromCache_rangeCancelledWhileDownloading_keepsCachedRange() {
    PrefetchBufferCache cache = newCache();
    CompletableFuture<ByteBuffer> download = new CompletableFuture<>();
    cache
        .registerStream(itemId("object"))
        .registerRange(FIRST_RANGE_OFFSET, 4, download, NO_PROMOTION);
    GcsObjectRange range = vectoredRange(FIRST_RANGE_OFFSET, 4);
    serveVectored(cache, range);
    range.getByteBufferFuture().cancel(false);

    download.complete(bufferOf(FIRST_RANGE));

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 4)).isTrue();
  }

  @Test
  void serveVectoredFromCache_rangeCancelledWhileDownloading_allocatesNoBuffer() {
    PrefetchBufferCache cache = newCache();
    CompletableFuture<ByteBuffer> download = new CompletableFuture<>();
    cache
        .registerStream(itemId("object"))
        .registerRange(FIRST_RANGE_OFFSET, 4, download, NO_PROMOTION);
    GcsObjectRange range = vectoredRange(FIRST_RANGE_OFFSET, 4);
    AtomicInteger allocations = new AtomicInteger();
    List<GcsObjectRange> unusedUnservedRanges =
        cache.serveVectoredFromCache(
            itemId("object"),
            ImmutableList.of(range),
            length -> {
              allocations.incrementAndGet();
              return ByteBuffer.allocate(length);
            },
            failedRange -> {});
    range.getByteBufferFuture().cancel(false);

    download.complete(bufferOf(FIRST_RANGE));

    assertThat(allocations.get()).isEqualTo(0);
  }

  @Test
  void serveVectoredFromCache_readBeforeTtl_extendsIdleTime() {
    PrefetchBufferCache cache = newCacheWithFakeClock(/* maxSizeBytes= */ 4);
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerCompleted(stream, FIRST_RANGE_OFFSET, FIRST_RANGE);
    advanceFakeClock(TTL_SECONDS - 1);
    serveVectored(cache, vectoredRange(FIRST_RANGE_OFFSET, 2));
    advanceFakeClock(2);

    registerInFlight(stream, SECOND_RANGE_OFFSET, 4);

    assertThat(cache.isCovered(itemId("object"), 2, 2)).isTrue();
  }

  @Test
  void serveVectoredFromCache_servedRange_recordsCacheHit() {
    RecordingOperationListener listener = new RecordingOperationListener();
    PrefetchBufferCache cache = newCache(listener);
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);

    serveVectored(cache, vectoredRange(FIRST_RANGE_OFFSET, 4));

    assertThat(listener.getTotal(GcsAnalyticsCoreTelemetryConstants.Metric.PREFETCH_CACHE_HIT))
        .isEqualTo(1);
  }

  @Test
  void serveVectoredFromCache_uncachedRange_recordsNoCacheMiss() {
    RecordingOperationListener listener = new RecordingOperationListener();
    PrefetchBufferCache cache = newCache(listener);

    serveVectored(cache, vectoredRange(FIRST_RANGE_OFFSET, 4));

    assertThat(listener.getTotal(GcsAnalyticsCoreTelemetryConstants.Metric.PREFETCH_CACHE_MISS))
        .isEqualTo(0);
  }

  @Test
  void serveVectoredFromCache_failedDownload_recordsNoCacheHit() {
    RecordingOperationListener listener = new RecordingOperationListener();
    PrefetchBufferCache cache = newCache(listener);
    CompletableFuture<ByteBuffer> download = new CompletableFuture<>();
    cache
        .registerStream(itemId("object"))
        .registerRange(FIRST_RANGE_OFFSET, 4, download, NO_PROMOTION);
    serveVectored(cache, vectoredRange(FIRST_RANGE_OFFSET, 4));

    download.completeExceptionally(new RuntimeException("network error"));

    assertThat(listener.getTotal(GcsAnalyticsCoreTelemetryConstants.Metric.PREFETCH_CACHE_HIT))
        .isEqualTo(0);
  }

  @Test
  void serveVectoredFromCache_servedRange_recordsConsumedBytes() {
    RecordingOperationListener listener = new RecordingOperationListener();
    PrefetchBufferCache cache = newCache(listener);
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);

    serveVectored(cache, vectoredRange(1, 3));

    assertThat(listener.getTotal(GcsAnalyticsCoreTelemetryConstants.Metric.PREFETCH_BYTES_CONSUMED))
        .isEqualTo(3);
  }

  @Test
  void serveVectoredFromCache_everyByteRead_evictsRange() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, FIRST_RANGE);

    serveVectored(cache, vectoredRange(FIRST_RANGE_OFFSET, 4));

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 1)).isFalse();
  }

  @Test
  void serveVectoredFromCache_downloadNotFinished_keepsRange() {
    PrefetchBufferCache cache = newCache();
    registerInFlight(cache.registerStream(itemId("object")), FIRST_RANGE_OFFSET, 4);

    serveVectored(cache, vectoredRange(FIRST_RANGE_OFFSET, 4));

    assertThat(cache.isCovered(itemId("object"), FIRST_RANGE_OFFSET, 4)).isTrue();
  }

  @Test
  void serveVectoredFromCache_rangeReadThroughEnclosingRange_evictsNestedRange() {
    PrefetchBufferCache cache = newCache();
    RegisteredStream stream = cache.registerStream(itemId("object"));
    registerCompleted(stream, 2, new byte[] {3, 4});
    registerCompleted(stream, FIRST_RANGE_OFFSET, COMBINED_RANGES);

    serveVectored(cache, vectoredRange(FIRST_RANGE_OFFSET, 8));

    assertThat(cache.isCovered(itemId("object"), 2, 2)).isFalse();
  }

  @Test
  void signatures_doNotExposeGuavaTypes() {
    ApiSignatureTestUtils.assertNoGuavaTypesInSignatures(
        PrefetchBufferCache.class, RegisteredStream.class);
  }

  private static PrefetchBufferCache newCache() {
    return newCache(MAX_SIZE_BYTES);
  }

  private static PrefetchBufferCache newCache(long maxSizeBytes) {
    return new PrefetchBufferCache(maxSizeBytes, TTL_SECONDS, new Telemetry(ImmutableList.of()));
  }

  private static PrefetchBufferCache newCache(OperationListener listener) {
    return new PrefetchBufferCache(
        MAX_SIZE_BYTES, TTL_SECONDS, new Telemetry(ImmutableList.of(listener)));
  }

  private PrefetchBufferCache newCacheWithFakeClock(long maxSizeBytes) {
    return new PrefetchBufferCache(
        maxSizeBytes, TTL_SECONDS, new Telemetry(ImmutableList.of()), fakeClockNanos::get);
  }

  private void advanceFakeClock(long seconds) {
    fakeClockNanos.addAndGet(TimeUnit.SECONDS.toNanos(seconds));
  }

  private static boolean registerCompleted(RegisteredStream stream, long offset, byte[] bytes) {
    return stream.registerRange(
        offset, bytes.length, CompletableFuture.completedFuture(bufferOf(bytes)), NO_PROMOTION);
  }

  private static boolean registerInFlight(RegisteredStream stream, long offset, int length) {
    return stream.registerRange(offset, length, new CompletableFuture<>(), NO_PROMOTION);
  }

  /**
   * Registers a range that is still downloading. When a reader starts waiting on the range, it
   * counts the promotion and completes with {@code bytes}, so the reader never blocks.
   */
  private static void registerInFlightCountingPromotions(
      RegisteredStream stream, long offset, byte[] bytes, AtomicInteger promotions) {
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    stream.registerRange(
        offset,
        bytes.length,
        future,
        () -> {
          promotions.incrementAndGet();
          future.complete(bufferOf(bytes));
        });
  }

  private static void runOnAllWorkers(Runnable work) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(WORKER_THREADS);
    try {
      List<Future<?>> workers = new ArrayList<>();
      for (int worker = 0; worker < WORKER_THREADS; worker++) {
        workers.add(executor.submit(work));
      }
      for (Future<?> worker : workers) {
        worker.get(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private static int countAdmitted(List<Future<Boolean>> admissions) throws Exception {
    int admitted = 0;
    for (Future<Boolean> admission : admissions) {
      if (admission.get(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        admitted++;
      }
    }
    return admitted;
  }

  private static void registerReadAndCloseRepeatedly(PrefetchBufferCache cache) {
    for (int iteration = 0; iteration < 500; iteration++) {
      GcsItemId itemId = itemId("object-" + iteration % 3);
      long offset = (iteration % 5) * 4L;
      CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
      try (RegisteredStream stream = cache.registerStream(itemId)) {
        stream.registerRange(offset, 4, future, NO_PROMOTION);
        if (iteration % 7 == 0) {
          future.completeExceptionally(new RuntimeException("network error"));
        } else {
          future.complete(bufferOf(FIRST_RANGE));
        }
        int unusedServedBytes =
            cache.serveFromCache(itemId, offset, ByteBuffer.allocate(2 + iteration % 3));
        if (iteration % 11 == 0) {
          cache.invalidateAll();
        }
      }
    }
  }

  private static GcsItemId itemId(String objectName) {
    return GcsItemId.builder().setBucketName("test-bucket").setObjectName(objectName).build();
  }

  private static GcsObjectRange vectoredRange(long offset, int length) {
    return GcsObjectRange.builder()
        .setOffset(offset)
        .setLength(length)
        .setByteBufferFuture(new CompletableFuture<>())
        .build();
  }

  /** Serves {@code ranges} of "object" with a fallback that is never expected to run. */
  private static List<GcsObjectRange> serveVectored(
      PrefetchBufferCache cache, GcsObjectRange... ranges) {
    return cache.serveVectoredFromCache(
        itemId("object"),
        ImmutableList.copyOf(ranges),
        ByteBuffer::allocate,
        failedRange -> failedRange.getByteBufferFuture().cancel(false));
  }

  private static byte[] readBytes(GcsObjectRange range) throws Exception {
    ByteBuffer buffer = range.getByteBufferFuture().get(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }

  private static Stream<Named<Consumer<CompletableFuture<ByteBuffer>>>> badDownloads() {
    return Stream.of(
        Named.of(
            "fails", future -> future.completeExceptionally(new RuntimeException("network error"))),
        Named.of("returns no buffer", future -> future.complete(null)),
        Named.of("returns a short buffer", future -> future.complete(bufferOf(new byte[] {1, 2}))));
  }

  private static ByteBuffer bufferOf(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
    buffer.put(bytes);
    buffer.flip();
    return buffer;
  }

  private static byte[] writtenBytes(ByteBuffer buffer) {
    ByteBuffer view = buffer.duplicate();
    view.flip();
    byte[] bytes = new byte[view.remaining()];
    view.get(bytes);
    return bytes;
  }
}
