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

import org.junit.jupiter.api.Test;

class AdaptiveRangeReadStrategyTest {

  private static final long OBJECT_SIZE = 10000L;
  private static final int MIN_RANGE_REQUEST_SIZE = 100;
  private static final int INPLACE_SEEK_LIMIT = 50;

  @Test
  void calculateRangeRequestEnd_sequential_readsUntilEnd() {
    GcsReadOptions options = createOptions(FileAccessPattern.SEQUENTIAL);
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());

    long end = strategy.calculateRangeRequestEnd(0, 10, OBJECT_SIZE);

    assertThat(end).isEqualTo(OBJECT_SIZE);
  }

  @Test
  void calculateRangeRequestEnd_random_readsMinRange() {
    GcsReadOptions options = createOptions(FileAccessPattern.RANDOM);
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());

    long end = strategy.calculateRangeRequestEnd(0, 10, OBJECT_SIZE);

    assertThat(end).isEqualTo(100);
  }

  @Test
  void calculateRangeRequestEnd_random_readsRequestedIfLargerThanMin() {
    GcsReadOptions options = createOptions(FileAccessPattern.RANDOM);
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());

    long end = strategy.calculateRangeRequestEnd(0, 200, OBJECT_SIZE);

    assertThat(end).isEqualTo(200);
  }

  @Test
  void detectRandomAccess_backwardRead_switchesToRandom() {
    GcsReadOptions options = createOptions(FileAccessPattern.AUTO);
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());

    strategy.detectRandomAccess(50, 100);

    long end = strategy.calculateRangeRequestEnd(50, 10, OBJECT_SIZE);
    assertThat(end).isEqualTo(50 + MIN_RANGE_REQUEST_SIZE);
  }

  @Test
  void detectRandomAccess_forwardReadBeyondLimit_switchesToRandom() {
    GcsReadOptions options = createOptions(FileAccessPattern.AUTO);
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());

    strategy.detectRandomAccess(60, 0);
    long end = strategy.calculateRangeRequestEnd(60, 10, OBJECT_SIZE);

    assertThat(end).isEqualTo(60 + MIN_RANGE_REQUEST_SIZE);
  }

  @Test
  void shouldSeekInPlace_withinLimit_returnsTrue() {
    GcsReadOptions options = createOptions(FileAccessPattern.AUTO);
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());

    boolean result = strategy.shouldSeekInPlace(10, 0, OBJECT_SIZE);

    assertThat(result).isTrue();
  }

  @Test
  void shouldSeekInPlace_beyondLimit_returnsFalse() {
    GcsReadOptions options = createOptions(FileAccessPattern.AUTO);
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());

    boolean result = strategy.shouldSeekInPlace(60, 0, OBJECT_SIZE);

    assertThat(result).isFalse();
  }

  @Test
  void shouldSeekInPlace_backward_returnsFalse() {
    GcsReadOptions options = createOptions(FileAccessPattern.AUTO);
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());

    boolean result = strategy.shouldSeekInPlace(0, 10, OBJECT_SIZE);

    assertThat(result).isFalse();
  }

  @Test
  void calculateRangeRequestEnd_auto_switchesToSequentialAfterThreshold() {
    int threshold = 2;
    GcsReadOptions options =
        GcsReadOptions.builder()
            .setUserProjectId("test-project")
            .setFileAccessPattern(FileAccessPattern.AUTO)
            .setMinRangeRequestSize(MIN_RANGE_REQUEST_SIZE)
            .setInplaceSeekLimit(INPLACE_SEEK_LIMIT)
            .setSequentialRangeReadThreshold(threshold)
            .build();
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());
    strategy.detectRandomAccess(0, 100); // Force random access

    long end1 = strategy.calculateRangeRequestEnd(0, 10, OBJECT_SIZE);
    long end2 = strategy.calculateRangeRequestEnd(MIN_RANGE_REQUEST_SIZE, 10, OBJECT_SIZE);
    long end3 = strategy.calculateRangeRequestEnd(MIN_RANGE_REQUEST_SIZE * 2, 10, OBJECT_SIZE);
    long end4 = strategy.calculateRangeRequestEnd(MIN_RANGE_REQUEST_SIZE * 3, 10, OBJECT_SIZE);

    assertThat(strategy.isRandomAccess()).isFalse();
    assertThat(end1).isEqualTo(MIN_RANGE_REQUEST_SIZE);
    assertThat(end2).isEqualTo(MIN_RANGE_REQUEST_SIZE * 2);
    assertThat(end3).isEqualTo(MIN_RANGE_REQUEST_SIZE * 3);
    assertThat(end4).isEqualTo(OBJECT_SIZE);
  }

  @Test
  void calculateRangeRequestEnd_random_doesNotSwitchToSequential() {
    int threshold = 2;
    GcsReadOptions options =
        GcsReadOptions.builder()
            .setUserProjectId("test-project")
            .setFileAccessPattern(FileAccessPattern.RANDOM)
            .setMinRangeRequestSize(MIN_RANGE_REQUEST_SIZE)
            .setInplaceSeekLimit(INPLACE_SEEK_LIMIT)
            .setSequentialRangeReadThreshold(threshold)
            .build();
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());

    long end1 = strategy.calculateRangeRequestEnd(0, 10, OBJECT_SIZE);
    long end2 = strategy.calculateRangeRequestEnd(MIN_RANGE_REQUEST_SIZE, 10, OBJECT_SIZE);
    long end3 = strategy.calculateRangeRequestEnd(MIN_RANGE_REQUEST_SIZE * 2, 10, OBJECT_SIZE);
    long end4 = strategy.calculateRangeRequestEnd(MIN_RANGE_REQUEST_SIZE * 3, 10, OBJECT_SIZE);

    assertThat(strategy.isRandomAccess()).isTrue();
    assertThat(end1).isEqualTo(MIN_RANGE_REQUEST_SIZE);
    assertThat(end2).isEqualTo(MIN_RANGE_REQUEST_SIZE * 2);
    assertThat(end3).isEqualTo(MIN_RANGE_REQUEST_SIZE * 3);
    assertThat(end4).isEqualTo(MIN_RANGE_REQUEST_SIZE * 4);
  }

  private GcsReadOptions createOptions(FileAccessPattern fileAccessPattern) {
    return GcsReadOptions.builder()
        .setUserProjectId("test-project")
        .setFileAccessPattern(fileAccessPattern)
        .setMinRangeRequestSize(MIN_RANGE_REQUEST_SIZE)
        .setInplaceSeekLimit(INPLACE_SEEK_LIMIT)
        .build();
  }

  private GcsItemInfo createItemInfo() {
    return GcsItemInfo.builder()
        .setItemId(GcsItemId.builder().setBucketName("b").setObjectName("o").build())
        .setSize(OBJECT_SIZE)
        .setContentGeneration(1L)
        .build();
  }
}
