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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.Optional;

/**
 * The decoded byte layout of a Parquet object.
 *
 * <p>Describes the row groups and column chunks recorded in the file footer. Row groups are kept in
 * byte order, and no two row groups share a byte.
 */
@AutoValue
abstract class ParquetFileLayout {

  /**
   * Returns a 64-bit fingerprint of the columns the schema defines, equal across files whose
   * schemas define the same columns.
   *
   * @see ParquetSchemaFingerprint
   */
  abstract long getSchemaFingerprint();

  /**
   * Returns the row groups of this file in byte order, where the row group at position {@code i}
   * has index {@code i}.
   */
  abstract ImmutableList<ParquetRowGroup> getRowGroups();

  /**
   * Creates a layout from the row groups described by the file footer.
   *
   * @throws IllegalArgumentException if the index of a row group differs from its position in
   *     {@code rowGroups}, or a row group starts before the previous one ends
   */
  static ParquetFileLayout create(
      long schemaFingerprint, ImmutableList<ParquetRowGroup> rowGroups) {
    long endOffset = 0;
    for (int position = 0; position < rowGroups.size(); position++) {
      ParquetRowGroup rowGroup = rowGroups.get(position);
      checkArgument(
          rowGroup.getIndex() == position,
          "Row group at position %s has index %s",
          position,
          rowGroup.getIndex());
      checkArgument(
          rowGroup.getStartOffset() >= endOffset,
          "Row group %s starts before the previous row group ends at %s",
          position,
          endOffset);
      endOffset = rowGroup.getEndOffset();
    }
    return new AutoValue_ParquetFileLayout(schemaFingerprint, rowGroups);
  }

  /** Returns the row group at the given index, if it exists. */
  final Optional<ParquetRowGroup> getRowGroup(int index) {
    if (index < 0 || index >= getRowGroups().size()) {
      return Optional.empty();
    }
    return Optional.of(getRowGroups().get(index));
  }

  /** Returns the row group containing {@code position}, if any. */
  final Optional<ParquetRowGroup> findRowGroupAt(long position) {
    for (ParquetRowGroup rowGroup : getRowGroups()) {
      if (rowGroup.containsPosition(position)) {
        return Optional.of(rowGroup);
      }
      if (rowGroup.getStartOffset() > position) {
        break; // Row groups are in byte order, so no later row group contains the position.
      }
    }
    return Optional.empty();
  }
}
