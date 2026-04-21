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

import org.junit.jupiter.api.Test;

class AdaptiveReadStrategyTest {

  @Test
  void calculateAdaptiveReadChannelLimit_randomAccess() {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFileAccessPattern(FileAccessPattern.RANDOM).build();
    AdaptiveReadStrategy strategy = new AdaptiveReadStrategy(readOptions);

    long limit = strategy.calculateAdaptiveReadChannelLimit(10, 20, 100);

    assertThat(limit).isEqualTo(30);
  }

  @Test
  void calculateAdaptiveReadChannelLimit_randomAccess_capsAtObjectSize() {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFileAccessPattern(FileAccessPattern.RANDOM).build();
    AdaptiveReadStrategy strategy = new AdaptiveReadStrategy(readOptions);

    long limit = strategy.calculateAdaptiveReadChannelLimit(90, 20, 100);

    assertThat(limit).isEqualTo(100);
  }

  @Test
  void calculateAdaptiveReadChannelLimit_sequentialAccess() {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFileAccessPattern(FileAccessPattern.SEQUENTIAL).build();
    AdaptiveReadStrategy strategy = new AdaptiveReadStrategy(readOptions);

    long limit = strategy.calculateAdaptiveReadChannelLimit(10, 20, 100);

    assertThat(limit).isEqualTo(100);
  }

  @Test
  void isRandomAccess_returnsTrueForRandom() {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFileAccessPattern(FileAccessPattern.RANDOM).build();
    AdaptiveReadStrategy strategy = new AdaptiveReadStrategy(readOptions);

    boolean isRandom = strategy.isRandomAccess();

    assertThat(isRandom).isTrue();
  }

  @Test
  void isRandomAccess_returnsFalseForSequential() {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setFileAccessPattern(FileAccessPattern.SEQUENTIAL).build();
    AdaptiveReadStrategy strategy = new AdaptiveReadStrategy(readOptions);

    boolean isRandom = strategy.isRandomAccess();

    assertThat(isRandom).isFalse();
  }
}
