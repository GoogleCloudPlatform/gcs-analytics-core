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
import java.util.concurrent.CompletableFuture;
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

  private static CachedRange rangeOf(CompletableFuture<ByteBuffer> future) {
    return CachedRange.create(RANGE_OFFSET, RANGE_OFFSET + BYTES.length, future, () -> {});
  }

  private static ByteBuffer bufferOf(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
    buffer.put(bytes);
    buffer.flip();
    return buffer;
  }
}
