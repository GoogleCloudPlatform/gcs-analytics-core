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
package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.Test;

class GcsItemInfoTest {

  @Test
  void isInferredDirectory() {
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(GcsItemId.builder().setBucketName("bucket").setObjectName("dir/").build())
            .setItemType(GcsItemInfo.ItemType.INFERRED_DIRECTORY)
            .build();

    assertThat(itemInfo.isInferredDirectory()).isTrue();
    assertThat(itemInfo.isExplicitDirectory()).isFalse();
  }

  @Test
  void isExplicitDirectory() {
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(GcsItemId.builder().setBucketName("bucket").setObjectName("folder/").build())
            .setItemType(GcsItemInfo.ItemType.EXPLICIT_DIRECTORY)
            .build();

    assertThat(itemInfo.isInferredDirectory()).isFalse();
    assertThat(itemInfo.isExplicitDirectory()).isTrue();
  }

  @Test
  void isObject() {
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(GcsItemId.builder().setBucketName("bucket").setObjectName("obj").build())
            .setItemType(GcsItemInfo.ItemType.OBJECT)
            .build();

    assertThat(itemInfo.isInferredDirectory()).isFalse();
    assertThat(itemInfo.isExplicitDirectory()).isFalse();
  }

  @Test
  void rootInfo_hasRootItemTypeAndZeroSize() {
    GcsItemInfo rootInfo = GcsItemInfo.ROOT_INFO;

    assertThat(rootInfo.getItemId()).isEqualTo(GcsItemId.ROOT);
    assertThat(rootInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.ROOT);
    assertThat(rootInfo.getSize()).isEqualTo(0L);
  }
}
