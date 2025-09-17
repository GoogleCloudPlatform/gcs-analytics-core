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

import com.google.common.collect.ImmutableList;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class GcsCombinedObjectRangeTest {

  @Test
  void union_withDisjointRange_returnsCombinedRangeSpanningBoth() {
    GcsObjectRange range1 = createRange(/* offset= */ 100, /* length= */ 50);
    GcsObjectRange range2 = createRange(/* offset= */ 200, /* length= */ 50);
    GcsObjectCombinedRange combinedRange =
        GcsObjectCombinedRange.builder()
            .setUnderlyingRanges(ImmutableList.of(range1))
            .setOffset(range1.getOffset())
            .setLength(range1.getLength())
            .build();

    GcsObjectCombinedRange result = combinedRange.union(range2);

    assertThat(result.getUnderlyingRanges()).containsExactly(range1, range2);
    assertThat(result.getOffset()).isEqualTo(100);
    assertThat(result.getLength()).isEqualTo(150); // (200+50)-100
  }

  @Test
  void union_withOverlappingRange_returnsCombinedRange() {
    GcsObjectRange range1 = createRange(/* offset= */ 100, /* length= */ 100);
    GcsObjectRange range2 = createRange(/* offset= */ 150, /* length= */ 100);
    GcsObjectCombinedRange combinedRange =
        GcsObjectCombinedRange.builder()
            .setUnderlyingRanges(ImmutableList.of(range1))
            .setOffset(range1.getOffset())
            .setLength(range1.getLength())
            .build();

    GcsObjectCombinedRange result = combinedRange.union(range2);

    assertThat(result.getUnderlyingRanges()).containsExactly(range1, range2);
    assertThat(result.getOffset()).isEqualTo(100);
    assertThat(result.getLength()).isEqualTo(150); // (100+150)-100
  }

  @Test
  void union_withContainedRange_returnsContainingRange() {
    GcsObjectRange range1 = createRange(/* offset= */ 100, /* length= */ 200);
    GcsObjectRange range2 = createRange(/* offset= */ 150, /* length= */ 50);
    GcsObjectCombinedRange combinedRange =
        GcsObjectCombinedRange.builder()
            .setUnderlyingRanges(ImmutableList.of(range1))
            .setOffset(range1.getOffset())
            .setLength(range1.getLength())
            .build();

    GcsObjectCombinedRange result = combinedRange.union(range2);

    assertThat(result.getUnderlyingRanges()).containsExactly(range1, range2);
    assertThat(result.getOffset()).isEqualTo(100);
    assertThat(result.getLength()).isEqualTo(200); // (100 + 200) - 100
  }

  private GcsObjectRange createRange(long offset, int length) {
    return GcsObjectRange.builder()
        .setOffset(offset)
        .setLength(length)
        .setByteBufferFuture(new CompletableFuture<>())
        .build();
  }
}
