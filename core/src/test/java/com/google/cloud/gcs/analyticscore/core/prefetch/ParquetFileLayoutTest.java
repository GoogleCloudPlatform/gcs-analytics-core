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
import static com.google.cloud.gcs.analyticscore.core.prefetch.ParquetLayoutFixtures.layout;
import static com.google.cloud.gcs.analyticscore.core.prefetch.ParquetLayoutFixtures.rowGroup;
import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.Test;

public class ParquetFileLayoutTest {

  @Test
  void getRowGroup_knownIndex_returnsThatRowGroup() {
    ParquetRowGroup second = rowGroup(1, columnChunk("price", 200, 50));
    ParquetFileLayout fileLayout = layout(rowGroup(0, columnChunk("price", 100, 50)), second);

    assertThat(fileLayout.getRowGroup(1)).hasValue(second);
  }

  @Test
  void getRowGroup_negativeIndex_returnsEmpty() {
    ParquetFileLayout fileLayout = layout(rowGroup(0, columnChunk("price", 100, 50)));

    assertThat(fileLayout.getRowGroup(-1)).isEmpty();
  }

  @Test
  void getRowGroup_indexPastLastRowGroup_returnsEmpty() {
    ParquetFileLayout fileLayout = layout(rowGroup(0, columnChunk("price", 100, 50)));

    assertThat(fileLayout.getRowGroup(1)).isEmpty();
  }

  @Test
  void findRowGroupAt_positionInsideRowGroup_returnsThatRowGroup() {
    ParquetRowGroup second = rowGroup(1, columnChunk("price", 200, 50));
    ParquetFileLayout fileLayout = layout(rowGroup(0, columnChunk("price", 100, 50)), second);

    assertThat(fileLayout.findRowGroupAt(225)).hasValue(second);
  }

  @Test
  void findRowGroupAt_positionOutsideAllRowGroups_returnsEmpty() {
    ParquetFileLayout fileLayout = layout(rowGroup(0, columnChunk("price", 100, 50)));

    assertThat(fileLayout.findRowGroupAt(500)).isEmpty();
  }

  @Test
  void equals_layoutsWithEqualRowGroups_areEqual() {
    ParquetFileLayout first = layout(rowGroup(0, columnChunk("price", 100, 50)));
    ParquetFileLayout second = layout(rowGroup(0, columnChunk("price", 100, 50)));

    assertThat(first).isEqualTo(second);
  }
}
