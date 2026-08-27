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

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GcsItemInfoTest {

  private static final String TEST_BUCKET = "bucket";
  private static final String TEST_DIR = "dir/";
  private static final String TEST_FOLDER = "folder/";

  @Test
  void createInferredDirectory_returnsItemInfoWithInferredDirectoryType() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_DIR).build();

    GcsItemInfo itemInfo = GcsItemInfo.createInferredDirectory(itemId);

    assertThat(itemInfo.getItemId()).isEqualTo(itemId);
    assertThat(itemInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.INFERRED_DIRECTORY);
    assertThat(itemInfo.getSize()).isEqualTo(0L);
  }

  @Test
  void createPlaceholderDirectory_returnsItemInfoWithPlaceholderDirectoryType() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_DIR).build();

    GcsItemInfo itemInfo = GcsItemInfo.createPlaceholderDirectory(itemId);

    assertThat(itemInfo.getItemId()).isEqualTo(itemId);
    assertThat(itemInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY);
    assertThat(itemInfo.getSize()).isEqualTo(0L);
  }

  @Test
  void createFolder_returnsItemInfoWithNativeFolderTypeAndTimestamps() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_FOLDER).build();

    GcsItemInfo itemInfo = GcsItemInfo.createFolder(itemId, 1000L, 2000L, 1L);

    assertThat(itemInfo.getItemId()).isEqualTo(itemId);
    assertThat(itemInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.NATIVE_FOLDER);
    assertThat(itemInfo.getSize()).isEqualTo(0L);
    assertThat(itemInfo.getContentGeneration()).isEmpty();
    assertThat(itemInfo.getMetaGeneration()).isEqualTo(1L);
    assertThat(itemInfo.getCreationTime()).isEqualTo(1000L);
    assertThat(itemInfo.getModificationTime()).isEqualTo(2000L);
    assertThat(itemInfo.getExtendedAttributes()).isEmpty();
  }

  @Test
  void createBucket_returnsItemInfoWithBucketType() {
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();

    GcsItemInfo bucketInfo = GcsItemInfo.createBucket(bucketId);

    assertThat(bucketInfo.getItemId()).isEqualTo(bucketId);
    assertThat(bucketInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.BUCKET);
    assertThat(bucketInfo.getSize()).isEqualTo(0L);
  }

  @Test
  void rootInfo_hasRootItemTypeAndZeroSize() {
    GcsItemInfo rootInfo = GcsItemInfo.ROOT_INFO;

    assertThat(rootInfo.getItemId()).isEqualTo(GcsItemId.ROOT);
    assertThat(rootInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.ROOT);
    assertThat(rootInfo.getSize()).isEqualTo(0L);
    assertThat(rootInfo.exists()).isTrue();
  }

  @Test
  void createNotFound_returnsItemInfoWithNegativeSizeAndExistsFalse() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("non-existent.txt").build();

    GcsItemInfo itemInfo = GcsItemInfo.createNotFound(itemId);

    assertThat(itemInfo.getItemId()).isEqualTo(itemId);
    assertThat(itemInfo.getSize()).isEqualTo(-1L);
    assertThat(itemInfo.exists()).isFalse();
  }

  @Test
  void exists_withNonNegativeSize_returnsTrue() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("file.txt").build();

    GcsItemInfo existingInfo = GcsItemInfo.builder().setItemId(itemId).setSize(100L).build();
    GcsItemInfo zeroSizeInfo = GcsItemInfo.builder().setItemId(itemId).setSize(0L).build();

    assertThat(existingInfo.exists()).isTrue();
    assertThat(zeroSizeInfo.exists()).isTrue();
  }

  @Test
  void encodeAndDecodeMetadata_roundTripsCorrectly() {
    ImmutableMap<String, byte[]> original =
        ImmutableMap.of(
            "key1", new byte[] {1, 2, 3},
            "key2", new byte[] {0, -1, 127});

    ImmutableMap<String, String> encoded = GcsItemInfo.encodeMetadata(original);
    ImmutableMap<String, byte[]> decoded = GcsItemInfo.decodeMetadata(encoded);

    assertThat(decoded.keySet()).isEqualTo(original.keySet());
    decoded.forEach((k, v) -> assertThat(v).isEqualTo(original.get(k)));
  }

  @Test
  void decodeMetadata_nullOrEmpty_returnsEmptyMap() {
    assertThat(GcsItemInfo.decodeMetadata(null)).isEmpty();
    assertThat(GcsItemInfo.decodeMetadata(ImmutableMap.of())).isEmpty();
  }

  @Test
  void decodeMetadata_invalidBase64Value_ignoresInvalidAttributeAndDecodesValidAttributes() {
    Map<String, String> metadata =
        ImmutableMap.of(
            "validKey",
            GcsItemInfo.encodeMetadata(ImmutableMap.of("validKey", new byte[] {1, 2}))
                .get("validKey"),
            "invalidKey",
            "not-base-64!!!");

    ImmutableMap<String, byte[]> decoded = GcsItemInfo.decodeMetadata(metadata);

    assertThat(decoded.keySet()).containsExactly("validKey");
    assertThat(decoded.get("validKey")).isEqualTo(new byte[] {1, 2});
  }

  @Test
  void decodeMetadata_nullKeyIsIgnored_nullValueMapsToEmptyByteArray() {
    assertThat(GcsItemInfo.decodeMetadata(Collections.singletonMap(null, "dmFs"))).isEmpty();
    ImmutableMap<String, byte[]> decodedNullVal =
        GcsItemInfo.decodeMetadata(Collections.singletonMap("key", null));
    assertThat(decodedNullVal.keySet()).containsExactly("key");
    assertThat(decodedNullVal.get("key")).isEmpty();
  }
}
