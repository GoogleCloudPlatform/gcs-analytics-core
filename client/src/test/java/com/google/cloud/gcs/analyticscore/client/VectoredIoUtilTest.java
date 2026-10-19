/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;

class VectoredIoUtilTest {
  private static final GcsItemId TEST_ITEM_ID =
      GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();

  @Test
  void sortGcsObjectRanges_withSingleRange_returnsSingleRange() {
    ImmutableList<GcsObjectRange> ranges = createRanges(ImmutableMap.of(100L, 10));

    ImmutableList<GcsObjectRange> sortedRanges = VectoredIoUtil.sortGcsObjectRanges(ranges);

    assertThat(sortedRanges.get(0)).isEqualTo(ranges.get(0));
  }

  @Test
  void sortGcsObjectRanges_withUnsortedRanges_returnsRangesSortedByOffset() {
    ImmutableList<GcsObjectRange> ranges = createRanges(ImmutableMap.of(100L, 10, 0L, 10, 50L, 10));

    ImmutableList<GcsObjectRange> sortedRanges = VectoredIoUtil.sortGcsObjectRanges(ranges);

    assertThat(sortedRanges.get(0).getOffset()).isEqualTo(0);
    assertThat(sortedRanges.get(1).getOffset()).isEqualTo(50);
    assertThat(sortedRanges.get(2).getOffset()).isEqualTo(100);
  }

  @Test
  void mergeGcsObjectRanges_withEmptyList_returnsEmptyList() {
    List<GcsObjectCombinedRange> merged =
        VectoredIoUtil.mergeGcsObjectRanges(ImmutableList.of(), 10, 100);
    assertThat(merged).isEmpty();
  }

  @Test
  void mergeGcsObjectRanges_withNullList_returnsEmptyList() {
    List<GcsObjectCombinedRange> merged = VectoredIoUtil.mergeGcsObjectRanges(null, 10, 100);
    assertThat(merged).isEmpty();
  }

  @Test
  void mergeGcsObjectRanges_withRangesOutsideMergeThreshold_doesNotMergeRanges() {
    ImmutableList<GcsObjectRange> ranges = createRanges(ImmutableMap.of(0L, 10, 20L, 10, 40L, 10));

    List<GcsObjectCombinedRange> merged = VectoredIoUtil.mergeGcsObjectRanges(ranges, 5, 100);

    assertThat(merged).hasSize(3);
    assertThat(merged.get(0).getOffset()).isEqualTo(0);
    assertThat(merged.get(0).getLength()).isEqualTo(10);
    assertThat(merged.get(1).getOffset()).isEqualTo(20);
    assertThat(merged.get(1).getLength()).isEqualTo(10);
    assertThat(merged.get(2).getOffset()).isEqualTo(40);
    assertThat(merged.get(2).getLength()).isEqualTo(10);
  }

  @Test
  void mergeGcsObjectRanges_withAllRangesWithinMergeThreshold_mergesAllRanges() {
    ImmutableList<GcsObjectRange> ranges = createRanges(ImmutableMap.of(0L, 10, 12L, 10, 24L, 10));

    List<GcsObjectCombinedRange> merged = VectoredIoUtil.mergeGcsObjectRanges(ranges, 5, 100);

    assertThat(merged).hasSize(1);
    assertThat(merged.get(0).getOffset()).isEqualTo(0);
    assertThat(merged.get(0).getLength()).isEqualTo(34);
    assertThat(merged.get(0).getUnderlyingRanges()).hasSize(3);
  }

  @Test
  void mergeGcsObjectRanges_withSomeRangesWithinMergeThreshold_mergesEligibleRanges() {
    ImmutableList<GcsObjectRange> ranges = createRanges(ImmutableMap.of(0L, 10, 12L, 10, 32L, 10));

    List<GcsObjectCombinedRange> merged = VectoredIoUtil.mergeGcsObjectRanges(ranges, 5, 100);

    assertThat(merged).hasSize(2);
    assertThat(merged.get(0).getOffset()).isEqualTo(0);
    assertThat(merged.get(0).getLength()).isEqualTo(22);
    assertThat(merged.get(0).getUnderlyingRanges()).hasSize(2);
    assertThat(merged.get(1).getOffset()).isEqualTo(32);
    assertThat(merged.get(1).getLength()).isEqualTo(10);
    assertThat(merged.get(1).getUnderlyingRanges()).hasSize(1);
  }

  @Test
  void mergeGcsObjectRanges_rangeHitsMaxSizeConstraint_doesNotMerge() {
    ImmutableList<GcsObjectRange> ranges = createRanges(ImmutableMap.of(0L, 10, 12L, 10, 24L, 10));

    List<GcsObjectCombinedRange> merged = VectoredIoUtil.mergeGcsObjectRanges(ranges, 5, 30);

    assertThat(merged).hasSize(2);
    assertThat(merged.get(0).getOffset()).isEqualTo(0);
    assertThat(merged.get(0).getLength()).isEqualTo(22);
    assertThat(merged.get(1).getOffset()).isEqualTo(24);
    assertThat(merged.get(1).getLength()).isEqualTo(10);
  }

  @Test
  void mergeGcsObjectRanges_withOverlappingRanges_mergesRanges() {
    ImmutableList<GcsObjectRange> ranges = createRanges(ImmutableMap.of(0L, 20, 15L, 10));

    List<GcsObjectCombinedRange> merged = VectoredIoUtil.mergeGcsObjectRanges(ranges, 5, 100);

    assertThat(merged).hasSize(1);
    assertThat(merged.get(0).getOffset()).isEqualTo(0);
    assertThat(merged.get(0).getLength()).isEqualTo(25);
    assertThat(merged.get(0).getUnderlyingRanges()).hasSize(2);
  }

  @Test
  void fetchUnderlyingRangeData_withValidInputs_returnsCorrectDataSlice() {
    byte[] data = RandomUtils.nextBytes(100);
    ImmutableList<GcsObjectRange> children = createRanges(ImmutableMap.of(10L, 20, 40L, 30));
    GcsObjectRange underlyingRange2 = children.get(1);
    GcsObjectCombinedRange combinedRange =
        GcsObjectCombinedRange.builder()
            .setOffset(10)
            .setLength(60) // 10 to 70
            .setUnderlyingRanges(children)
            .build();
    ByteBuffer combinedDataBuffer = ByteBuffer.wrap(data, 10, 60);
    byte[] resultArray = new byte[30];
    byte[] expectedArray = new byte[30];

    ByteBuffer result =
        VectoredIoUtil.fetchUnderlyingRangeData(
            combinedDataBuffer, combinedRange, underlyingRange2);
    result.get(resultArray);
    System.arraycopy(data, 40, expectedArray, 0, 30);

    assertThat(resultArray).isEqualTo(expectedArray);
  }

  @Test
  void fetchUnderlyingRangeData_withZeroLengthChild_returnsEmptySlice() {
    GcsObjectRange emptyChild = createRange(15, 0);
    GcsObjectCombinedRange combinedRange =
        GcsObjectCombinedRange.builder()
            .setOffset(10)
            .setLength(20)
            .setUnderlyingRanges(ImmutableList.of(emptyChild))
            .build();

    ByteBuffer result =
        VectoredIoUtil.fetchUnderlyingRangeData(ByteBuffer.allocate(20), combinedRange, emptyChild);

    assertThat(result.remaining()).isEqualTo(0);
  }

  @Test
  void completeDegenerateRanges_zeroLength_completesWithEmptyBuffer() throws Exception {
    GcsObjectRange emptyRange = createRange(100, 0);

    ImmutableList<GcsObjectRange> pending =
        VectoredIoUtil.completeDegenerateRanges(ImmutableList.of(emptyRange), TEST_ITEM_ID);

    assertThat(pending).isEmpty();
    assertThat(emptyRange.getByteBufferFuture().get().remaining()).isEqualTo(0);
  }

  @Test
  void completeDegenerateRanges_negativeOffset_completesWithEofException() {
    GcsObjectRange negativeOffset = createRange(-1, 10);

    ImmutableList<GcsObjectRange> pending =
        VectoredIoUtil.completeDegenerateRanges(ImmutableList.of(negativeOffset), TEST_ITEM_ID);

    assertThat(pending).isEmpty();
    ExecutionException e =
        assertThrows(ExecutionException.class, () -> negativeOffset.getByteBufferFuture().get());
    assertThat(e).hasCauseThat().isInstanceOf(EOFException.class);
    assertThat(e).hasCauseThat().hasMessageThat().contains(TEST_ITEM_ID.toString());
  }

  @Test
  void completeDegenerateRanges_negativeLength_completesWithIllegalArgumentException() {
    GcsObjectRange negativeLength = createRange(0, -10);

    ImmutableList<GcsObjectRange> pending =
        VectoredIoUtil.completeDegenerateRanges(ImmutableList.of(negativeLength), TEST_ITEM_ID);

    assertThat(pending).isEmpty();
    ExecutionException e =
        assertThrows(ExecutionException.class, () -> negativeLength.getByteBufferFuture().get());
    assertThat(e).hasCauseThat().isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void completeDegenerateRanges_mixedRanges_returnsReadableRangesInOriginalOrder() {
    GcsObjectRange second = createRange(50, 10);
    GcsObjectRange empty = createRange(30, 0);
    GcsObjectRange first = createRange(0, 10);
    ImmutableList<GcsObjectRange> ranges = ImmutableList.of(second, empty, first);

    ImmutableList<GcsObjectRange> pending =
        VectoredIoUtil.completeDegenerateRanges(ranges, TEST_ITEM_ID);

    assertThat(pending).containsExactly(second, first).inOrder();
    assertThat(second.getByteBufferFuture().isDone()).isFalse();
    assertThat(first.getByteBufferFuture().isDone()).isFalse();
  }

  private GcsObjectRange createRange(long offset, int length) {
    return GcsObjectRange.builder()
        .setOffset(offset)
        .setLength(length)
        .setByteBufferFuture(new CompletableFuture<>())
        .build();
  }

  private ImmutableList<GcsObjectRange> createRanges(
      ImmutableMap<Long, Integer> offsetToLengthMap) {
    return offsetToLengthMap.entrySet().stream()
        .map(entry -> createRange(entry.getKey(), entry.getValue()))
        .collect(ImmutableList.toImmutableList());
  }
}
