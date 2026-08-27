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

import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class GcsFileInfoTest {

  private static final String TEST_BUCKET = "bucket";

  @Test
  void rootInfo_hasRootUriAndRootItemInfo() {
    GcsFileInfo rootInfo = GcsFileInfo.ROOT_INFO;

    assertThat(rootInfo.getUri()).isEqualTo(GcsFileInfo.GCS_ROOT_URI);
    assertThat(rootInfo.getItemInfo()).isEqualTo(GcsItemInfo.ROOT_INFO);
    assertThat(rootInfo.getAttributes()).isEmpty();
    assertThat(rootInfo.exists()).isTrue();
  }

  @Test
  void createNotFound_withUri_returnsNotFoundFileInfo() {
    URI uri = URI.create("gs://" + TEST_BUCKET + "/non-existent.txt");

    GcsFileInfo fileInfo = GcsFileInfo.createNotFound(uri);

    assertNotFound(fileInfo, uri);
  }

  @ParameterizedTest
  @MethodSource("provideItemIdsAndExpectedUris")
  void createNotFound_withItemId_returnsNotFoundFileInfo(GcsItemId itemId, URI expectedUri) {
    GcsFileInfo fileInfo = GcsFileInfo.createNotFound(itemId);

    assertNotFound(fileInfo, expectedUri);
    assertThat(fileInfo.getItemInfo().getItemId()).isEqualTo(itemId);
  }

  private static Stream<Arguments> provideItemIdsAndExpectedUris() {
    return Stream.of(
        Arguments.of(
            GcsItemId.builder()
                .setBucketName(TEST_BUCKET)
                .setObjectName("non-existent.txt")
                .build(),
            URI.create("gs://" + TEST_BUCKET + "/non-existent.txt")),
        Arguments.of(
            GcsItemId.builder().setBucketName(TEST_BUCKET).build(),
            URI.create("gs://" + TEST_BUCKET)),
        Arguments.of(GcsItemId.ROOT, GcsFileInfo.GCS_ROOT_URI));
  }

  @Test
  void exists_withExistingItemInfo_returnsTrue() {
    GcsItemInfo existingInfo =
        GcsItemInfo.builder()
            .setItemId(
                GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("file.txt").build())
            .setSize(100L)
            .build();
    GcsFileInfo fileInfo =
        GcsFileInfo.builder()
            .setItemInfo(existingInfo)
            .setUri(URI.create("gs://" + TEST_BUCKET + "/file.txt"))
            .setAttributes(ImmutableMap.of())
            .build();

    assertThat(fileInfo.exists()).isTrue();
  }

  private static void assertNotFound(GcsFileInfo fileInfo, URI expectedUri) {
    assertThat(fileInfo.getUri()).isEqualTo(expectedUri);
    assertThat(fileInfo.exists()).isFalse();
    assertThat(fileInfo.getItemInfo().exists()).isFalse();
    assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(-1L);
  }
}
