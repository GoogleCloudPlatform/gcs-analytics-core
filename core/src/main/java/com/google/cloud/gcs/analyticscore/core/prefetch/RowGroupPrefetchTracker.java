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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.SchemaAccessHistory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * Tracks per-stream read progress across a {@link ParquetFileLayout}, records learned data columns
 * into {@link SchemaAccessHistory}, and triggers row-group prefetching when a dictionary page is
 * read.
 */
final class RowGroupPrefetchTracker {

  private final ParquetFileLayout layout;
  private final SchemaAccessHistory accessHistory;
  private final Set<Integer> prefetchedRowGroups = new HashSet<>();
  private int lastDataReadIndex = -1;
  @Nullable private Range<Long> lastObservedDataPageRange;

  RowGroupPrefetchTracker(ParquetFileLayout layout, SchemaAccessHistory accessHistory) {
    this.layout = checkNotNull(layout, "layout cannot be null");
    this.accessHistory = checkNotNull(accessHistory, "accessHistory cannot be null");
  }

  /**
   * Observes a single-buffer read at {@code [position, position + length)} and invokes {@code
   * scheduleRanges} when a dictionary read should prefetch the row group's learned data columns.
   */
  void onSingleRead(
      long position, int length, Predicate<ImmutableList<Range<Long>>> scheduleRanges) {
    if (isWithinLastDataPageRange(position, length)) {
      return;
    }
    Optional<ParquetRowGroup> maybeRowGroup = layout.findRowGroupAt(position);
    if (!maybeRowGroup.isPresent()) {
      return;
    }
    ParquetRowGroup rowGroup = maybeRowGroup.get();
    long endOffset = position + length;
    boolean touchedDataPage = recordDataColumnsRead(rowGroup, position, endOffset);
    if (!touchedDataPage
        && rowGroup.getIndex() > lastDataReadIndex
        && rowGroup.touchesDictionaryPage(position, endOffset)) {
      prefetchRowGroupOnDictionaryRead(rowGroup, scheduleRanges);
    }
    lastObservedDataPageRange = rowGroup.findDataPageRangeAt(position).orElse(null);
  }

  /** Records the data columns touched by a vectored read. */
  void onVectoredRead(List<GcsObjectRange> ranges) {
    for (GcsObjectRange range : ranges) {
      layout
          .findRowGroupAt(range.getOffset())
          .ifPresent(
              rowGroup ->
                  recordDataColumnsRead(
                      rowGroup, range.getOffset(), range.getOffset() + range.getLength()));
    }
  }

  private boolean recordDataColumnsRead(
      ParquetRowGroup rowGroup, long startOffset, long endOffset) {
    ImmutableSet<String> touchedColumns = rowGroup.findDataColumnsInRange(startOffset, endOffset);
    if (touchedColumns.isEmpty()) {
      return false;
    }
    int fingerprint = layout.getSchemaFingerprint();
    for (String columnPath : touchedColumns) {
      accessHistory.recordDataAccess(fingerprint, columnPath);
    }
    lastDataReadIndex = Math.max(lastDataReadIndex, rowGroup.getIndex());
    return true;
  }

  private void prefetchRowGroupOnDictionaryRead(
      ParquetRowGroup rowGroup, Predicate<ImmutableList<Range<Long>>> scheduleRanges) {
    int rowGroupIndex = rowGroup.getIndex();
    if (prefetchedRowGroups.contains(rowGroupIndex)) {
      return;
    }
    ImmutableList<Range<Long>> coalescedRanges =
        rowGroup.getCoalescedColumnRanges(
            accessHistory.getDataColumns(layout.getSchemaFingerprint()));
    if (!coalescedRanges.isEmpty() && scheduleRanges.test(coalescedRanges)) {
      prefetchedRowGroups.add(rowGroupIndex);
    }
  }

  private boolean isWithinLastDataPageRange(long position, int length) {
    return lastObservedDataPageRange != null
        && position >= lastObservedDataPageRange.lowerEndpoint()
        && position + length <= lastObservedDataPageRange.upperEndpoint();
  }
}
