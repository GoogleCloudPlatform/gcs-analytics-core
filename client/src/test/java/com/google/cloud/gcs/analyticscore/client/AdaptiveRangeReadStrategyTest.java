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
  void detectRandomAccess_backwardRead_switchesToRandom() {
    GcsReadOptions options =
        GcsReadOptions.builder()
            .setUserProjectId("test-project")
            .setFileAccessPattern(FileAccessPattern.AUTO)
            .setMinRangeRequestSize(MIN_RANGE_REQUEST_SIZE)
            .setInplaceSeekLimit(INPLACE_SEEK_LIMIT)
            .build();
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());

    strategy.detectRandomAccess(50, 100);

    long end = strategy.calculateAdaptiveReadSessionEnd(50, 10, OBJECT_SIZE);
    assertThat(end).isEqualTo(50 + MIN_RANGE_REQUEST_SIZE);
  }

  @Test
  void detectRandomAccess_forwardReadBeyondLimit_switchesToRandom() {
    GcsReadOptions options =
        GcsReadOptions.builder()
            .setUserProjectId("test-project")
            .setFileAccessPattern(FileAccessPattern.AUTO)
            .setMinRangeRequestSize(MIN_RANGE_REQUEST_SIZE)
            .setInplaceSeekLimit(INPLACE_SEEK_LIMIT)
            .build();
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());

    strategy.detectRandomAccess(60, 0);
    long end = strategy.calculateAdaptiveReadSessionEnd(60, 10, OBJECT_SIZE);

    assertThat(end).isEqualTo(60 + MIN_RANGE_REQUEST_SIZE);
  }

  @Test
  void shouldSeekInPlace_withinLimit_returnsTrue() {
    GcsReadOptions options =
        GcsReadOptions.builder()
            .setUserProjectId("test-project")
            .setFileAccessPattern(FileAccessPattern.AUTO)
            .setMinRangeRequestSize(MIN_RANGE_REQUEST_SIZE)
            .setInplaceSeekLimit(INPLACE_SEEK_LIMIT)
            .build();
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());

    boolean result = strategy.shouldSeekInPlace(10, 0, OBJECT_SIZE);

    assertThat(result).isTrue();
  }

  @Test
  void shouldSeekInPlace_beyondLimit_returnsFalse() {
    GcsReadOptions options =
        GcsReadOptions.builder()
            .setUserProjectId("test-project")
            .setFileAccessPattern(FileAccessPattern.AUTO)
            .setMinRangeRequestSize(MIN_RANGE_REQUEST_SIZE)
            .setInplaceSeekLimit(INPLACE_SEEK_LIMIT)
            .build();
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());

    boolean result = strategy.shouldSeekInPlace(60, 0, OBJECT_SIZE);

    assertThat(result).isFalse();
  }

  @Test
  void shouldSeekInPlace_backward_returnsFalse() {
    GcsReadOptions options =
        GcsReadOptions.builder()
            .setUserProjectId("test-project")
            .setFileAccessPattern(FileAccessPattern.AUTO)
            .setMinRangeRequestSize(MIN_RANGE_REQUEST_SIZE)
            .setInplaceSeekLimit(INPLACE_SEEK_LIMIT)
            .build();
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());

    boolean result = strategy.shouldSeekInPlace(0, 10, OBJECT_SIZE);

    assertThat(result).isFalse();
  }

  @Test
  void detectSequentialAccess_switchesToSequential_afterThreshold() {
    GcsReadOptions options =
        GcsReadOptions.builder()
            .setUserProjectId("test-project")
            .setFileAccessPattern(FileAccessPattern.AUTO)
            .setMinRangeRequestSize(100)
            .setInplaceSeekLimit(50)
            .setSequentialReadSessionThreshold(2)
            .build();
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());
    boolean initialRandomAccess = strategy.isRandomAccess();
    // Forcing the random access to true.
    strategy.detectRandomAccess(100, 200);
    boolean randomAccessAfterForceSet = strategy.isRandomAccess();

    long firstSessionEnd =
        strategy.calculateAdaptiveReadSessionEnd(
            100, 10, OBJECT_SIZE); // lastAdaptiveReadSessionEnd = 100
    strategy.detectSequentialAccess(firstSessionEnd);
    boolean randomAccessAfterFirstSequentialRead = strategy.isRandomAccess();
    long secondSessionEnd =
        strategy.calculateAdaptiveReadSessionEnd(
            firstSessionEnd, 10, OBJECT_SIZE); // lastAdaptiveReadSessionEnd = 200
    strategy.detectSequentialAccess(secondSessionEnd);
    boolean randomAccessAfterSecondSequentialRead = strategy.isRandomAccess();

    assertThat(initialRandomAccess).isFalse();
    assertThat(randomAccessAfterForceSet).isTrue();
    assertThat(randomAccessAfterFirstSequentialRead).isTrue();
    assertThat(randomAccessAfterSecondSequentialRead).isFalse();
  }

  @Test
  void detectSequentialAccess_ignoredIfRandomMode() {
    GcsReadOptions options =
        GcsReadOptions.builder()
            .setUserProjectId("test-project")
            .setFileAccessPattern(FileAccessPattern.RANDOM)
            .setMinRangeRequestSize(100)
            .setInplaceSeekLimit(10)
            .setSequentialReadSessionThreshold(0)
            .build();
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());
    long sessionEnd = strategy.calculateAdaptiveReadSessionEnd(0, 10, OBJECT_SIZE); // end = 100

    strategy.detectSequentialAccess(sessionEnd);

    // Should stay random because explicit RANDOM mode was requested
    assertThat(strategy.isRandomAccess()).isTrue();
  }

  @Test
  void calculateAdaptiveReadSessionEnd_sequentialAccess_returnsObjectSize() {
    GcsReadOptions options =
        GcsReadOptions.builder()
            .setUserProjectId("test-project")
            .setFileAccessPattern(FileAccessPattern.SEQUENTIAL)
            .setMinRangeRequestSize(MIN_RANGE_REQUEST_SIZE)
            .setInplaceSeekLimit(INPLACE_SEEK_LIMIT)
            .build();
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());

    long end = strategy.calculateAdaptiveReadSessionEnd(0, 10, OBJECT_SIZE);

    assertThat(end).isEqualTo(OBJECT_SIZE);
  }

  @Test
  void calculateAdaptiveReadSessionEnd_randomAccess_smallRead_expandsToMinRange() {
    GcsReadOptions options =
        GcsReadOptions.builder()
            .setUserProjectId("test-project")
            .setFileAccessPattern(FileAccessPattern.RANDOM)
            .setMinRangeRequestSize(MIN_RANGE_REQUEST_SIZE)
            .setInplaceSeekLimit(INPLACE_SEEK_LIMIT)
            .build();
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());

    long end = strategy.calculateAdaptiveReadSessionEnd(50, 10, OBJECT_SIZE);

    assertThat(end).isEqualTo(50 + MIN_RANGE_REQUEST_SIZE);
  }

  @Test
  void calculateAdaptiveReadSessionEnd_randomAccess_largeRead_respectsBytesToRead() {
    GcsReadOptions options =
        GcsReadOptions.builder()
            .setUserProjectId("test-project")
            .setFileAccessPattern(FileAccessPattern.RANDOM)
            .setMinRangeRequestSize(MIN_RANGE_REQUEST_SIZE)
            .setInplaceSeekLimit(INPLACE_SEEK_LIMIT)
            .build();
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());

    long end = strategy.calculateAdaptiveReadSessionEnd(50, 200, OBJECT_SIZE);

    assertThat(end).isEqualTo(50 + 200);
  }

  @Test
  void calculateAdaptiveReadSessionEnd_randomAccess_nearEndOfFile_clampsToObjectSize() {
    GcsReadOptions options =
        GcsReadOptions.builder()
            .setUserProjectId("test-project")
            .setFileAccessPattern(FileAccessPattern.RANDOM)
            .setMinRangeRequestSize(MIN_RANGE_REQUEST_SIZE)
            .setInplaceSeekLimit(INPLACE_SEEK_LIMIT)
            .build();
    AdaptiveRangeReadStrategy strategy = new AdaptiveRangeReadStrategy(options, createItemInfo());

    long pos = OBJECT_SIZE - 10;
    long end = strategy.calculateAdaptiveReadSessionEnd(pos, 100, OBJECT_SIZE);

    assertThat(end).isEqualTo(OBJECT_SIZE);
  }

  private GcsItemInfo createItemInfo() {
    return GcsItemInfo.builder()
        .setItemId(GcsItemId.builder().setBucketName("b").setObjectName("o").build())
        .setSize(OBJECT_SIZE)
        .setContentGeneration(1L)
        .build();
  }
}
