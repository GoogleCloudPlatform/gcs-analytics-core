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

package com.google.cloud.gcs.analyticscore.benchmarks;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class ReaderOpenInvariantsTest {
  private static final int KB = 1024;
  private static final int MB = 1024 * KB;

  @Test
  void footerPrefetch_parquetTailReads_matchesMeasuredReaderOpensAndBytes() throws IOException {
    try (var bench = new FooterPrefetchBenchmark()) {
      bench.setUp();
      bench.readFooterUnknownSize(new IoCounters());
      // GcsFooterOptimizer.readWithUnknownFileSize serves trailer+footer in 1 speculative GET.
      assertThat(bench.storage.readerOpens()).isEqualTo(1);
      assertThat(bench.storage.bytesServed()).isEqualTo(128 * KB + 8);
      bench.storage.resetCounters();
      bench.readTail(new IoCounters());
      assertThat(bench.storage.readerOpens()).isEqualTo(1);
      assertThat(bench.storage.bytesServed()).isEqualTo(128 * KB);
      bench.prefetchEnabled = false;
      bench.setUp();
      bench.readFooterUnknownSize(new IoCounters());
      assertThat(bench.storage.readerOpens()).isEqualTo(2);
      bench.storage.resetCounters();
      bench.readTail(new IoCounters());
      assertThat(bench.storage.readerOpens()).isEqualTo(1);
      assertThat(bench.storage.bytesServed()).isEqualTo(64 * KB);
    }
  }

  @Test
  void smallObjectCache_warmRead_servesZeroBytesFromStorage() throws IOException {
    try (var bench = new SmallObjectCacheBenchmark()) {
      bench.setUp();
      assertThat(bench.storage.bytesServed()).isEqualTo(256 * KB);
      bench.storage.resetCounters();
      bench.warmRead(new IoCounters());
      // SmallObjectOptimizer.serveFromCache satisfies warm read from memory with 0 GETs.
      assertThat(bench.storage.bytesServed()).isEqualTo(0);
      assertThat(bench.storage.readerOpens()).isEqualTo(0);
      bench.coldRead(new IoCounters());
      assertThat(bench.storage.readerOpens()).isEqualTo(1);
      assertThat(bench.storage.bytesServed()).isEqualTo(256 * KB);
    }
  }

  @Test
  void readStrategy_forwardGap_skipsInPlaceWithinLimitAndReopensBeyond() throws IOException {
    try (var bench = new ReadStrategyBenchmark()) {
      for (String strategy : new String[] {"ADAPTIVE", "RANDOM", "SEQUENTIAL"}) {
        bench.strategy = strategy;
        bench.forwardGapBytes = 64 * KB;
        bench.setUp();
        bench.seekAndRead(new IoCounters());
        assertWithMessage(strategy).that(bench.storage.readerOpens()).isEqualTo(1);
        assertWithMessage(strategy).that(bench.storage.bytesServed()).isEqualTo(72 * KB);
        bench.forwardGapBytes = MB;
        bench.setUp();
        bench.seekAndRead(new IoCounters());
        assertWithMessage(strategy).that(bench.storage.readerOpens()).isEqualTo(2);
        assertWithMessage(strategy).that(bench.storage.bytesServed()).isEqualTo(8 * KB);
      }
    }
  }

  @Test
  void mergeGcsObjectRanges_withinVsBeyondMaxMergeGap_coalescesOnlyWithinLimit() {
    var bench = new VectoredRangeCoalescingBenchmark();
    bench.setUp();
    // VectoredIoUtil.canMerge requires gap < maxMergeGap (4 KiB default in GcsVectoredReadOptions).
    assertThat(bench.mergeRanges()).hasSize(1);
    bench.gapBytes = MB;
    bench.setUp();
    assertThat(bench.mergeRanges()).hasSize(16);
  }

  @Test
  void channelOpen_withFileInfoVsItemId_opensOneReaderAndServesOneByteEach() throws IOException {
    try (var bench = new GcsReadChannelOpenBenchmark()) {
      bench.setUp();
      assertThat(bench.openAndReadOneByte_withFileInfo(new IoCounters())).isEqualTo(0);
      assertThat(bench.storage.readerOpens()).isEqualTo(1);
      assertThat(bench.storage.bytesServed()).isEqualTo(1);
      bench.storage.resetCounters();
      assertThat(bench.openAndReadOneByte_withItemId(new IoCounters())).isEqualTo(0);
      assertThat(bench.storage.readerOpens()).isEqualTo(1);
      assertThat(bench.storage.bytesServed()).isEqualTo(1);
      bench.storage.resetCounters();
      try (var channel =
          bench.fileSystem.open(
              bench.itemId,
              bench.fileSystem.getFileSystemOptions().getGcsClientOptions().getGcsReadOptions())) {
        assertThat(channel.getItemInfo()).isNull();
        assertThat(channel.read(ByteBuffer.allocate(1))).isEqualTo(1);
        assertThat(channel.getItemInfo()).isNotNull();
        assertThat(channel.getItemInfo().getSize()).isEqualTo(64 * KB);
        assertThat(channel.getItemInfo().getContentGeneration()).hasValue(1L);
        assertThat(bench.storage.readerOpens()).isEqualTo(1);
      }
    }
  }
}
