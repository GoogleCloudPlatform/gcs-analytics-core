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

import static com.google.cloud.gcs.analyticscore.core.prefetch.ParquetLayoutFixtures.columnChunk;
import static com.google.cloud.gcs.analyticscore.core.prefetch.ParquetLayoutFixtures.dictionaryEncodedColumnChunk;
import static com.google.cloud.gcs.analyticscore.core.prefetch.ParquetLayoutFixtures.layout;
import static com.google.cloud.gcs.analyticscore.core.prefetch.ParquetLayoutFixtures.rowGroup;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.SchemaAccessHistory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RowGroupPrefetchTrackerTest {

  private static final int SCHEMA_FINGERPRINT = 7;

  private ParquetFileLayout fileLayout;
  private SchemaAccessHistory accessHistory;
  private RowGroupPrefetchTracker tracker;

  @BeforeEach
  void createTracker() {
    fileLayout =
        layout(
            rowGroup(
                0,
                columnChunk("id", 100, 50),
                dictionaryEncodedColumnChunk("category", 150, 50, 170),
                columnChunk("value", 250, 50)),
            rowGroup(
                1,
                columnChunk("id", 400, 50),
                dictionaryEncodedColumnChunk("category", 450, 50, 470)));
    accessHistory = new SchemaAccessHistory();
    tracker = new RowGroupPrefetchTracker(fileLayout, accessHistory);
  }

  @Test
  void onSingleRead_dataPageRead_recordsColumnInHistory() {
    tracker.onSingleRead(100, 16, ranges -> true);

    assertThat(accessHistory.getDataColumns(SCHEMA_FINGERPRINT)).containsExactly("id");
  }

  @Test
  void onSingleRead_secondSliceInsideSameDataPage_skipsReobserving() {
    tracker.onSingleRead(100, 16, ranges -> true);
    accessHistory.invalidateAll();

    tracker.onSingleRead(116, 16, ranges -> true);

    assertThat(accessHistory.getDataColumns(SCHEMA_FINGERPRINT)).isEmpty();
  }

  @Test
  void onSingleRead_dictionaryPageReadWithLearnedColumns_schedulesCoalescedRanges() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "category");
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    tracker.onSingleRead(150, 8, scheduledRanges::addAll);

    assertThat(scheduledRanges).containsExactly(Range.closedOpen(100L, 200L));
  }

  @Test
  void onSingleRead_dictionaryPageReadTwiceAfterSuccess_schedulesOnlyOnce() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    List<Range<Long>> scheduledRanges = new ArrayList<>();
    tracker.onSingleRead(150, 8, scheduledRanges::addAll);

    tracker.onSingleRead(158, 8, scheduledRanges::addAll);

    assertThat(scheduledRanges).hasSize(1);
  }

  @Test
  void onSingleRead_dictionaryPageReadWhenScheduleFails_retriesOnNextDictionaryRead() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    tracker.onSingleRead(150, 8, ranges -> false);
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    tracker.onSingleRead(158, 8, scheduledRanges::addAll);

    assertThat(scheduledRanges).containsExactly(Range.closedOpen(100L, 150L));
  }

  @Test
  void onSingleRead_dictionaryPageReadAfterDataPageInSameRowGroup_doesNotSchedule() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    tracker.onSingleRead(100, 16, ranges -> true);
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    tracker.onSingleRead(150, 8, scheduledRanges::addAll);

    assertThat(scheduledRanges).isEmpty();
  }

  @Test
  void onSingleRead_positionOutsideAllRowGroups_schedulesNothing() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    tracker.onSingleRead(900, 16, scheduledRanges::addAll);

    assertThat(scheduledRanges).isEmpty();
  }

  @Test
  void onVectoredRead_rangeInsideRowGroup_recordsDataColumn() {
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(100)
            .setLength(50)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    tracker.onVectoredRead(ImmutableList.of(range));

    assertThat(accessHistory.getDataColumns(SCHEMA_FINGERPRINT)).containsExactly("id");
  }

  @Test
  void onSingleRead_readBeforeLastObservedDataPage_observesNewPosition() {
    tracker.onSingleRead(170, 16, ranges -> true);

    tracker.onSingleRead(100, 16, ranges -> true);

    assertThat(accessHistory.getDataColumns(SCHEMA_FINGERPRINT)).containsExactly("category", "id");
  }

  @Test
  void onSingleRead_readInGapBetweenColumns_schedulesNothing() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    tracker.onSingleRead(210, 16, scheduledRanges::addAll);

    assertThat(scheduledRanges).isEmpty();
  }

  @Test
  void onVectoredRead_rangeOutsideAllRowGroups_recordsNothing() {
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(900)
            .setLength(16)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    tracker.onVectoredRead(ImmutableList.of(range));

    assertThat(accessHistory.getDataColumns(SCHEMA_FINGERPRINT)).isEmpty();
  }
}
