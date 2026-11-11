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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class CachedRangeTest {

  private static final long RANGE_OFFSET = 10;
  private static final byte[] BYTES = {1, 2, 3, 4};

  @AfterEach
  void clearInterruptStatus() {
    Thread.interrupted();
  }

  @Test
  void create_negativeStartOffset_throwsIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> CachedRange.create(-1, BYTES.length, new CompletableFuture<>(), () -> {}));

    assertThat(exception).hasMessageThat().isEqualTo("startOffset -1 must be non-negative");
  }

  @Test
  void create_endOffsetEqualToStartOffset_throwsIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                CachedRange.create(
                    RANGE_OFFSET, RANGE_OFFSET, new CompletableFuture<>(), () -> {}));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo("endOffset 10 must be greater than startOffset 10");
  }

  @Test
  void create_rangeLongerThanMaxInt_throwsIllegalArgumentException() {
    long endOffset = RANGE_OFFSET + Integer.MAX_VALUE + 1L;

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> CachedRange.create(RANGE_OFFSET, endOffset, new CompletableFuture<>(), () -> {}));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo("range [10, 2147483658) must not be longer than 2147483647 bytes");
  }

  @Test
  void getLength_returnsByteCountOfRange() {
    CachedRange range = rangeOf(new CompletableFuture<>());

    int length = range.getLength();

    assertThat(length).isEqualTo(BYTES.length);
  }

  @Test
  void promote_runsPromotionAction() {
    AtomicInteger promotions = new AtomicInteger();
    CachedRange range = rangeOf(new CompletableFuture<>(), promotions::incrementAndGet);

    range.promote();

    assertThat(promotions.get()).isEqualTo(1);
  }

  @Test
  void recordBytesConsumed_partOfRange_returnsFalse() {
    CachedRange range = rangeOf(new CompletableFuture<>());

    boolean fullyConsumed = range.recordBytesConsumed(RANGE_OFFSET, BYTES.length - 1);

    assertThat(fullyConsumed).isFalse();
  }

  @Test
  void recordBytesConsumed_wholeRangeInTwoParts_returnsTrue() {
    CachedRange range = rangeOf(new CompletableFuture<>());
    range.recordBytesConsumed(RANGE_OFFSET + 2, 2);

    boolean fullyConsumed = range.recordBytesConsumed(RANGE_OFFSET, 2);

    assertThat(fullyConsumed).isTrue();
  }

  @Test
  void recordBytesConsumed_sameBytesTwice_returnsFalse() {
    CachedRange range = rangeOf(new CompletableFuture<>());
    range.recordBytesConsumed(RANGE_OFFSET, 2);

    boolean fullyConsumed = range.recordBytesConsumed(RANGE_OFFSET, 2);

    assertThat(fullyConsumed).isFalse();
  }

  @Test
  void recordBytesServed_bytesWithinRange_returnsAllBytes() {
    CachedRange range = rangeOf(new CompletableFuture<>());

    int newlyServed = range.recordBytesServed(3);

    assertThat(newlyServed).isEqualTo(3);
  }

  @Test
  void recordBytesServed_totalExceedsRangeLength_returnsOnlyBytesUpToRangeLength() {
    CachedRange range = rangeOf(new CompletableFuture<>());
    range.recordBytesServed(3);

    int newlyServed = range.recordBytesServed(Integer.MAX_VALUE);

    assertThat(newlyServed).isEqualTo(1);
  }

  @Test
  void recordBytesServed_rangeAlreadyFullyServed_returnsZero() {
    CachedRange range = rangeOf(new CompletableFuture<>());
    range.recordBytesServed(BYTES.length);

    int newlyServed = range.recordBytesServed(2);

    assertThat(newlyServed).isEqualTo(0);
  }

  @Test
  void recordBytesServed_negativeBytes_throwsIllegalArgumentException() {
    CachedRange range = rangeOf(new CompletableFuture<>());

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> range.recordBytesServed(-1));

    assertThat(exception).hasMessageThat().isEqualTo("bytes -1 must be non-negative");
  }

  @Test
  void copyInto_positionAtRangeStart_copiesWholeRange() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));
    ByteBuffer dst = ByteBuffer.allocate(BYTES.length);

    range.copyInto(RANGE_OFFSET, dst);

    assertThat(dst.array()).isEqualTo(BYTES);
  }

  @Test
  void copyInto_positionInsideRange_copiesBytesFromPosition() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));
    ByteBuffer dst = ByteBuffer.allocate(2);

    range.copyInto(RANGE_OFFSET + 1, dst);

    assertThat(dst.array()).isEqualTo(new byte[] {2, 3});
  }

  @Test
  void copyInto_destinationLargerThanAvailableBytes_returnsAvailableByteCount() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));

    int copiedBytes = range.copyInto(RANGE_OFFSET + 1, ByteBuffer.allocate(BYTES.length * 2));

    assertThat(copiedBytes).isEqualTo(BYTES.length - 1);
  }

  @Test
  void copyInto_destinationSmallerThanAvailableBytes_returnsDestinationSize() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));

    int copiedBytes = range.copyInto(RANGE_OFFSET, ByteBuffer.allocate(3));

    assertThat(copiedBytes).isEqualTo(3);
  }

  @Test
  void copyInto_bufferInsideLargerArray_copiesOnlyRangeBytes() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferInsideLargerArray()));
    ByteBuffer dst = ByteBuffer.allocate(BYTES.length);

    range.copyInto(RANGE_OFFSET, dst);

    assertThat(dst.array()).isEqualTo(BYTES);
  }

  @Test
  void copyInto_calledTwice_copiesSameBytes() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));
    range.copyInto(RANGE_OFFSET, ByteBuffer.allocate(BYTES.length));
    ByteBuffer dst = ByteBuffer.allocate(BYTES.length);

    range.copyInto(RANGE_OFFSET, dst);

    assertThat(dst.array()).isEqualTo(BYTES);
  }

  @Test
  void copyInto_bufferShorterThanRange_returnsZero() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(new byte[] {1, 2})));

    int copiedBytes = range.copyInto(RANGE_OFFSET, ByteBuffer.allocate(BYTES.length));

    assertThat(copiedBytes).isEqualTo(0);
  }

  @Test
  void copyInto_cancelledFuture_returnsZero() {
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    future.cancel(true);
    CachedRange range = rangeOf(future);

    int copiedBytes = range.copyInto(RANGE_OFFSET, ByteBuffer.allocate(BYTES.length));

    assertThat(copiedBytes).isEqualTo(0);
  }

  @Test
  void copyInto_readOnlyDestination_throwsReadOnlyBufferException() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));
    ByteBuffer readOnlyDst = ByteBuffer.allocate(BYTES.length).asReadOnlyBuffer();

    assertThrows(ReadOnlyBufferException.class, () -> range.copyInto(RANGE_OFFSET, readOnlyDst));
  }

  @Test
  void copyInto_inFlightFuture_runsPromotionAction() {
    AtomicInteger promotions = new AtomicInteger();
    CachedRange range = rangeOf(new CompletableFuture<>(), promotions::incrementAndGet);
    Thread.currentThread().interrupt();

    int unusedCopiedBytes = range.copyInto(RANGE_OFFSET, ByteBuffer.allocate(BYTES.length));

    assertThat(promotions.get()).isEqualTo(1);
  }

  @Test
  void copyInto_completedFuture_doesNotRunPromotionAction() {
    AtomicInteger promotions = new AtomicInteger();
    CachedRange range =
        rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)), promotions::incrementAndGet);

    int unusedCopiedBytes = range.copyInto(RANGE_OFFSET, ByteBuffer.allocate(BYTES.length));

    assertThat(promotions.get()).isEqualTo(0);
  }

  @Test
  void copyInto_positionBeforeRange_returnsZero() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));

    int copiedBytes = range.copyInto(RANGE_OFFSET - 1, ByteBuffer.allocate(BYTES.length));

    assertThat(copiedBytes).isEqualTo(0);
  }

  @Test
  void copyInto_positionAtRangeEnd_returnsZero() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));

    int copiedBytes =
        range.copyInto(RANGE_OFFSET + BYTES.length, ByteBuffer.allocate(BYTES.length));

    assertThat(copiedBytes).isEqualTo(0);
  }

  @Test
  void copyInto_destinationWithoutRemainingSpace_returnsZero() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));

    int copiedBytes = range.copyInto(RANGE_OFFSET, ByteBuffer.allocate(0));

    assertThat(copiedBytes).isEqualTo(0);
  }

  @Test
  void copyInto_futureCompletedWithNull_returnsZero() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(null));

    int copiedBytes = range.copyInto(RANGE_OFFSET, ByteBuffer.allocate(BYTES.length));

    assertThat(copiedBytes).isEqualTo(0);
  }

  @Test
  void copyInto_failedFuture_returnsZero() {
    CachedRange range = rangeOf(CompletableFuture.failedFuture(new IOException("prefetch failed")));

    int copiedBytes = range.copyInto(RANGE_OFFSET, ByteBuffer.allocate(BYTES.length));

    assertThat(copiedBytes).isEqualTo(0);
  }

  @Test
  void copyInto_callerInterruptedWhileInFlight_returnsZero() {
    CachedRange range = rangeOf(new CompletableFuture<>());
    Thread.currentThread().interrupt();

    int copiedBytes = range.copyInto(RANGE_OFFSET, ByteBuffer.allocate(BYTES.length));

    assertThat(copiedBytes).isEqualTo(0);
  }

  @Test
  void copyInto_callerInterruptedWhileInFlight_preservesInterruptStatus() {
    CachedRange range = rangeOf(new CompletableFuture<>());
    Thread.currentThread().interrupt();

    int unusedCopiedBytes = range.copyInto(RANGE_OFFSET, ByteBuffer.allocate(BYTES.length));

    assertThat(Thread.currentThread().isInterrupted()).isTrue();
  }

  @Test
  void copyIntoAsync_exactMatchOfReadOnlyBuffer_copiesIntoAllocatedBuffer() {
    CachedRange range =
        rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES).asReadOnlyBuffer()));
    ByteBuffer allocated = ByteBuffer.allocate(BYTES.length);

    ByteBuffer result = range.copyIntoAsync(RANGE_OFFSET, BYTES.length, length -> allocated).join();

    assertThat(result).isSameInstanceAs(allocated);
  }

  @Test
  void copyIntoAsync_exactMatchOfDirectBuffer_copiesIntoAllocatedBuffer() {
    ByteBuffer direct = ByteBuffer.allocateDirect(BYTES.length).put(BYTES).flip();
    CachedRange range = rangeOf(CompletableFuture.completedFuture(direct));
    ByteBuffer allocated = ByteBuffer.allocate(BYTES.length);

    ByteBuffer result = range.copyIntoAsync(RANGE_OFFSET, BYTES.length, length -> allocated).join();

    assertThat(result).isSameInstanceAs(allocated);
  }

  @Test
  void copyIntoAsync_exactMatchOfBufferWithArrayOffset_copiesIntoAllocatedBuffer() {
    ByteBuffer offsetSlice = ByteBuffer.wrap(new byte[] {0, 1, 2, 3, 4}, 1, BYTES.length).slice();
    CachedRange range = rangeOf(CompletableFuture.completedFuture(offsetSlice));
    ByteBuffer allocated = ByteBuffer.allocate(BYTES.length);

    ByteBuffer result = range.copyIntoAsync(RANGE_OFFSET, BYTES.length, length -> allocated).join();

    assertThat(result).isSameInstanceAs(allocated);
  }

  @Test
  void copyIntoAsync_allocatorReturnsNull_completesWithNull() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));

    ByteBuffer result = range.copyIntoAsync(RANGE_OFFSET + 1, 2, length -> null).join();

    assertThat(result).isNull();
  }

  @Test
  void copyIntoAsync_exactMatchOfWritableHeapBuffer_copiesIntoAllocatedBuffer() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));
    ByteBuffer allocated = ByteBuffer.allocate(BYTES.length);

    ByteBuffer result = range.copyIntoAsync(RANGE_OFFSET, BYTES.length, length -> allocated).join();

    assertThat(result).isSameInstanceAs(allocated);
  }

  @Test
  void copyIntoAsync_writeToResult_leavesCachedBytesUnchanged() {
    ByteBuffer cached = bufferOf(BYTES);
    CachedRange range = rangeOf(CompletableFuture.completedFuture(cached));
    ByteBuffer result =
        range.copyIntoAsync(RANGE_OFFSET, BYTES.length, ByteBuffer::allocate).join();

    result.put(0, (byte) 99);

    assertThat(cached.get(0)).isEqualTo(BYTES[0]);
  }

  @Test
  void copyIntoAsync_wholeRange_completesWithRangeBytes() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));

    ByteBuffer result =
        range.copyIntoAsync(RANGE_OFFSET, BYTES.length, ByteBuffer::allocate).join();

    assertThat(bytesOf(result)).isEqualTo(BYTES);
  }

  @Test
  void copyIntoAsync_subRange_completesWithRequestedBytes() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));

    ByteBuffer result = range.copyIntoAsync(RANGE_OFFSET + 1, 2, ByteBuffer::allocate).join();

    assertThat(bytesOf(result)).isEqualTo(new byte[] {2, 3});
  }

  @Test
  void copyIntoAsync_bufferInsideLargerArray_completesWithOnlyRangeBytes() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferInsideLargerArray()));

    ByteBuffer result =
        range.copyIntoAsync(RANGE_OFFSET, BYTES.length, ByteBuffer::allocate).join();

    assertThat(bytesOf(result)).isEqualTo(BYTES);
  }

  @Test
  void copyIntoAsync_futureCompletesLater_completesWithRangeBytes() {
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    CachedRange range = rangeOf(future);
    CompletableFuture<ByteBuffer> result =
        range.copyIntoAsync(RANGE_OFFSET, BYTES.length, ByteBuffer::allocate);

    future.complete(bufferOf(BYTES));

    assertThat(bytesOf(result.join())).isEqualTo(BYTES);
  }

  @Test
  void copyIntoAsync_inFlightFuture_runsPromotionAction() {
    AtomicInteger promotions = new AtomicInteger();
    CachedRange range = rangeOf(new CompletableFuture<>(), promotions::incrementAndGet);

    CompletableFuture<ByteBuffer> unusedResult =
        range.copyIntoAsync(RANGE_OFFSET, BYTES.length, ByteBuffer::allocate);

    assertThat(promotions.get()).isEqualTo(1);
  }

  @Test
  void copyIntoAsync_offsetBeforeRange_throwsIllegalArgumentException() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> range.copyIntoAsync(RANGE_OFFSET - 1, 2, ByteBuffer::allocate));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo("2 bytes at offset 9 must be within range [10, 14)");
  }

  @Test
  void copyIntoAsync_endAfterRange_throwsIllegalArgumentException() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> range.copyIntoAsync(RANGE_OFFSET + 2, BYTES.length, ByteBuffer::allocate));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo("4 bytes at offset 12 must be within range [10, 14)");
  }

  @Test
  void copyIntoAsync_nullAllocator_throwsNullPointerException() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));

    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> range.copyIntoAsync(RANGE_OFFSET, BYTES.length, null));

    assertThat(exception).hasMessageThat().isEqualTo("allocate cannot be null");
  }

  @Test
  void copyIntoAsync_bufferShorterThanRange_failsWithIllegalStateException() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(new byte[] {1, 2})));
    CompletableFuture<ByteBuffer> result =
        range.copyIntoAsync(RANGE_OFFSET, BYTES.length, ByteBuffer::allocate);

    CompletionException exception = assertThrows(CompletionException.class, result::join);

    assertThat(exception).hasCauseThat().isInstanceOf(IllegalStateException.class);
  }

  @Test
  void sliceAsync_subRange_completesWithRequestedBytes() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));

    ByteBuffer result = range.sliceAsync(RANGE_OFFSET + 1, 2).join();

    assertThat(bytesOf(result)).isEqualTo(new byte[] {2, 3});
  }

  @Test
  void sliceAsync_bufferInsideLargerArray_completesWithOnlyRangeBytes() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferInsideLargerArray()));

    ByteBuffer result = range.sliceAsync(RANGE_OFFSET, BYTES.length).join();

    assertThat(bytesOf(result)).isEqualTo(BYTES);
  }

  @Test
  void sliceAsync_writableBuffer_completesWithReadOnlyView() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));

    ByteBuffer result = range.sliceAsync(RANGE_OFFSET, BYTES.length).join();

    assertThat(result.isReadOnly()).isTrue();
  }

  @Test
  void sliceAsync_inFlightFuture_runsPromotionAction() {
    AtomicInteger promotions = new AtomicInteger();
    CachedRange range = rangeOf(new CompletableFuture<>(), promotions::incrementAndGet);

    CompletableFuture<ByteBuffer> unusedResult = range.sliceAsync(RANGE_OFFSET, BYTES.length);

    assertThat(promotions.get()).isEqualTo(1);
  }

  @Test
  void sliceAsync_offsetOverflowingInt_throwsIllegalArgumentException() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));
    long wrappingOffset = RANGE_OFFSET + (1L << 32);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> range.sliceAsync(wrappingOffset, 2));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo("2 bytes at offset 4294967306 must be within range [10, 14)");
  }

  @Test
  void sliceAsync_negativeLength_throwsIllegalArgumentException() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(BYTES)));

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> range.sliceAsync(RANGE_OFFSET, -1));

    assertThat(exception).hasMessageThat().isEqualTo("length -1 must be non-negative");
  }

  @Test
  void sliceAsync_bufferShorterThanRange_failsWithIllegalStateException() {
    CachedRange range = rangeOf(CompletableFuture.completedFuture(bufferOf(new byte[] {1, 2})));
    CompletableFuture<ByteBuffer> result = range.sliceAsync(RANGE_OFFSET, 2);

    CompletionException exception = assertThrows(CompletionException.class, result::join);

    assertThat(exception).hasCauseThat().isInstanceOf(IllegalStateException.class);
  }

  private static CachedRange rangeOf(CompletableFuture<ByteBuffer> future) {
    return rangeOf(future, () -> {});
  }

  private static CachedRange rangeOf(
      CompletableFuture<ByteBuffer> future, Runnable promotionAction) {
    return CachedRange.create(RANGE_OFFSET, RANGE_OFFSET + BYTES.length, future, promotionAction);
  }

  /** Returns a buffer shaped like one range of a merged vectored read. */
  private static ByteBuffer bufferInsideLargerArray() {
    return ByteBuffer.wrap(new byte[] {9, 9, 1, 2, 3, 4, 8, 8}, 2, BYTES.length);
  }

  private static byte[] bytesOf(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }

  private static ByteBuffer bufferOf(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
    buffer.put(bytes);
    buffer.flip();
    return buffer;
  }
}
