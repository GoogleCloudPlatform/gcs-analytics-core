/*
 * Copyright 2026 Google LLC
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

package com.google.cloud.gcs.analyticscore.core.optimizer;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class FormatOptimizerTest {

  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName("b").setObjectName("o").build();
  private static final GcsItemInfo ITEM_INFO = GcsItemInfo.builder().setItemId(ITEM_ID).build();
  private static final GcsFileInfo FILE_INFO =
      GcsFileInfo.builder()
          .setItemInfo(ITEM_INFO)
          .setUri(URI.create("gs://b/o"))
          .setAttributes(ImmutableMap.of())
          .build();

  @Test
  void isApplicable_fileInfo_delegatesToItemId() {
    FormatOptimizer optimizer =
        new FormatOptimizer() {
          @Override
          public boolean isApplicable(GcsItemId itemId) {
            return itemId.equals(ITEM_ID);
          }

          @Override
          public void onOpen(GcsItemId itemId, AnalyticsCacheManager cacheManager)
              throws IOException {}

          @Override
          public int read(long position, ByteBuffer dst, VectoredSeekableByteChannel delegate) {
            return 0;
          }
        };

    assertThat(optimizer.isApplicable(FILE_INFO)).isTrue();
  }

  @Test
  void onOpen_fileInfo_delegatesToItemId() throws IOException {
    AnalyticsCacheManager mockCacheManager = mock(AnalyticsCacheManager.class);
    final GcsItemId[] capturedId = new GcsItemId[1];
    FormatOptimizer optimizer =
        new FormatOptimizer() {
          @Override
          public boolean isApplicable(GcsItemId itemId) {
            return true;
          }

          @Override
          public void onOpen(GcsItemId itemId, AnalyticsCacheManager cacheManager)
              throws IOException {
            capturedId[0] = itemId;
          }

          @Override
          public int read(long position, ByteBuffer dst, VectoredSeekableByteChannel delegate) {
            return 0;
          }
        };

    optimizer.onOpen(FILE_INFO, mockCacheManager);

    assertThat(capturedId[0]).isEqualTo(ITEM_ID);
  }

  @Test
  void onClose_defaultIsNoOp() throws IOException {
    FormatOptimizer optimizer =
        new FormatOptimizer() {
          @Override
          public boolean isApplicable(GcsItemId itemId) {
            return true;
          }

          @Override
          public void onOpen(GcsItemId itemId, AnalyticsCacheManager cacheManager)
              throws IOException {}

          @Override
          public int read(long position, ByteBuffer dst, VectoredSeekableByteChannel delegate) {
            return 0;
          }
        };

    // Should not throw
    optimizer.onClose();
  }
}
