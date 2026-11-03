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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class GcsListPageTest {

  private static GcsItemInfo createObjectInfo(String bucket, String name, long size) {
    GcsItemId id = GcsItemId.builder().setBucketName(bucket).setObjectName(name).build();
    return GcsItemInfo.builder().setItemId(id).setSize(size).build();
  }

  private static GcsItemId createPrefixId(String bucket, String prefix) {
    return GcsItemId.builder().setBucketName(bucket).setObjectName(prefix).build();
  }

  @Test
  void builder_defaults_returnsEmptyPageWithoutContinuation() {
    GcsListPage<GcsItemInfo> page = GcsListPage.<GcsItemInfo>builder().build();

    assertThat(page.getItems()).isEmpty();
    assertThat(page.getPrefixes()).isEmpty();
    assertThat(page.getPrefixObject()).isEmpty();
    assertThat(page.getNextPageToken()).isEmpty();
    assertThat(page.hasNextPage()).isFalse();
  }

  @Test
  void builder_itemsPrefixesPrefixObjectAndToken_roundTripsThroughToBuilder() {
    GcsItemInfo marker =
        GcsItemInfo.createPlaceholderDirectory(createPrefixId("test-bucket", "dir/"));
    GcsItemInfo item1 = createObjectInfo("test-bucket", "dir/a.parquet", 10L);
    GcsItemInfo item2 = createObjectInfo("test-bucket", "dir/b.parquet", 20L);
    GcsItemId prefix1 = createPrefixId("test-bucket", "dir/sub1/");
    GcsItemId prefix2 = createPrefixId("test-bucket", "dir/sub2/");

    GcsListPage<GcsItemInfo> firstPage =
        GcsListPage.<GcsItemInfo>builder()
            .setItems(List.of(item1, item2))
            .setPrefixes(List.of(prefix1, prefix2))
            .setPrefixObject(marker)
            .setNextPageToken("token-1")
            .build();
    GcsListPage<GcsItemInfo> clearedPage =
        firstPage.toBuilder()
            .setPrefixObject(Optional.empty())
            .setNextPageToken(Optional.empty())
            .build();

    assertThat(firstPage.getItems()).containsExactly(item1, item2).inOrder();
    assertThat(firstPage.getPrefixes()).containsExactly(prefix1, prefix2).inOrder();
    assertThat(firstPage.getPrefixObject()).hasValue(marker);
    assertThat(firstPage.getNextPageToken()).hasValue("token-1");
    assertThat(firstPage.hasNextPage()).isTrue();
    assertThat(firstPage.toBuilder().build()).isEqualTo(firstPage);
    assertThat(clearedPage.getPrefixObject()).isEmpty();
    assertThat(clearedPage.getNextPageToken()).isEmpty();
    assertThat(clearedPage.hasNextPage()).isFalse();
  }

  @ParameterizedTest
  @MethodSource("emptyNextPageTokenSetters")
  void build_emptyNextPageToken_normalizesToAbsentAndHasNextPageFalse(
      Consumer<GcsListPage.Builder<GcsItemInfo>> tokenSetter) {
    GcsListPage.Builder<GcsItemInfo> builder = GcsListPage.builder();
    tokenSetter.accept(builder);

    GcsListPage<GcsItemInfo> page = builder.build();

    assertThat(page.getNextPageToken()).isEmpty();
    assertThat(page.hasNextPage()).isFalse();
    assertThat(page.hasNextPage()).isEqualTo(page.getNextPageToken().isPresent());
  }

  private static Stream<Consumer<GcsListPage.Builder<GcsItemInfo>>> emptyNextPageTokenSetters() {
    return Stream.of(b -> b.setNextPageToken(""), b -> b.setNextPageToken(Optional.of("")));
  }

  @Test
  void setItemsAndPrefixes_iterableOverloads_defensivelyCopyAndReturnUnmodifiableLists() {
    GcsItemId prefix1 = createPrefixId("test-bucket", "dir/a/");
    GcsItemId prefix2 = createPrefixId("test-bucket", "dir/b/");
    Deque<String> iterableItems = new ArrayDeque<>(List.of("second", "first"));
    Deque<GcsItemId> iterablePrefixes = new ArrayDeque<>(List.of(prefix1, prefix2));
    GcsListPage.Builder<CharSequence> builder =
        GcsListPage.<CharSequence>builder()
            .setItems((Iterable<String>) iterableItems)
            .setPrefixes((Iterable<GcsItemId>) iterablePrefixes);
    iterableItems.clear();
    iterablePrefixes.clear();

    GcsListPage<CharSequence> page = builder.build();

    assertThat(page.getItems()).containsExactly("second", "first").inOrder();
    assertThat(page.getPrefixes()).containsExactly(prefix1, prefix2).inOrder();
    assertThrows(UnsupportedOperationException.class, () -> page.getItems().clear());
    assertThrows(UnsupportedOperationException.class, () -> page.getPrefixes().clear());
  }

  @Test
  void setItemsAndPrefixes_packagePrivateListOverloads_defensivelyCopyOnBuild() {
    GcsItemId prefix = createPrefixId("test-bucket", "dir/sub/");
    List<String> mutableItems = new ArrayList<>(List.of("x", "y"));
    List<GcsItemId> mutablePrefixes = new ArrayList<>(List.of(prefix));

    GcsListPage<String> page =
        GcsListPage.<String>builder().setItems(mutableItems).setPrefixes(mutablePrefixes).build();
    mutableItems.clear();
    mutablePrefixes.clear();

    assertThat(page.getItems()).containsExactly("x", "y").inOrder();
    assertThat(page.getPrefixes()).containsExactly(prefix);
    assertThrows(UnsupportedOperationException.class, () -> page.getItems().clear());
    assertThrows(UnsupportedOperationException.class, () -> page.getPrefixes().clear());
  }

  @ParameterizedTest
  @MethodSource("nullBuilderActions")
  void builder_nullArgumentsOrElements_throwsNullPointerException(
      Consumer<GcsListPage.Builder<GcsItemInfo>> action) {
    GcsListPage.Builder<GcsItemInfo> builder = GcsListPage.builder();

    assertThrows(NullPointerException.class, () -> action.accept(builder));
  }

  private static Stream<Consumer<GcsListPage.Builder<GcsItemInfo>>> nullBuilderActions() {
    List<GcsItemInfo> itemWithNull = Collections.singletonList(null);
    List<GcsItemId> prefixWithNull = Collections.singletonList(null);
    return Stream.of(
        b -> b.setItems((Iterable<GcsItemInfo>) null),
        b -> b.setItems((List<GcsItemInfo>) null),
        b -> b.setPrefixes((Iterable<GcsItemId>) null),
        b -> b.setPrefixes((List<GcsItemId>) null),
        b -> b.setItems((Iterable<GcsItemInfo>) itemWithNull),
        b -> b.setPrefixes((Iterable<GcsItemId>) prefixWithNull),
        b -> b.setItems(itemWithNull).build(),
        b -> b.setPrefixes(prefixWithNull).build(),
        b -> b.setPrefixObject((GcsItemInfo) null),
        b -> b.setPrefixObject((Optional<GcsItemInfo>) null),
        b -> b.setNextPageToken((String) null),
        b -> b.setNextPageToken((Optional<String>) null));
  }

  @Test
  void signatures_doNotExposeGuavaTypes() {
    GcsListPage.Builder<GcsItemInfo> builder = GcsListPage.builder();
    GcsListPage<GcsItemInfo> page = builder.build();

    ApiSignatureTestUtils.assertNoGuavaTypesInSignatures(
        GcsListPage.class, GcsListPage.Builder.class, page.getClass(), builder.getClass());
  }
}
