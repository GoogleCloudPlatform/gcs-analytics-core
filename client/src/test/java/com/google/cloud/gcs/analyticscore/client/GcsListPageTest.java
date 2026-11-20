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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

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
  void builder_withAllProperties_populatesPage() {
    GcsItemInfo marker =
        GcsItemInfo.createPlaceholderDirectory(createPrefixId("test-bucket", "dir/"));
    GcsItemInfo item1 = createObjectInfo("test-bucket", "dir/a.parquet", 10L);
    GcsItemInfo item2 = createObjectInfo("test-bucket", "dir/b.parquet", 20L);
    GcsItemId prefix1 = createPrefixId("test-bucket", "dir/sub1/");
    GcsItemId prefix2 = createPrefixId("test-bucket", "dir/sub2/");

    GcsListPage<GcsItemInfo> page =
        GcsListPage.<GcsItemInfo>builder()
            .setItems((Iterable<GcsItemInfo>) List.of(item1, item2))
            .setPrefixes((Iterable<GcsItemId>) List.of(prefix1, prefix2))
            .setPrefixObject(marker)
            .setNextPageToken("token-1")
            .build();

    assertThat(page.getItems()).containsExactly(item1, item2).inOrder();
    assertThat(page.getPrefixes()).containsExactly(prefix1, prefix2).inOrder();
    assertThat(page.getPrefixObject()).hasValue(marker);
    assertThat(page.getNextPageToken()).hasValue("token-1");
    assertThat(page.hasNextPage()).isTrue();
  }

  @Test
  void toBuilder_allFieldsSet_preservesEquality() {
    GcsItemInfo marker =
        GcsItemInfo.createPlaceholderDirectory(createPrefixId("test-bucket", "dir/"));
    GcsItemInfo item = createObjectInfo("test-bucket", "dir/a.parquet", 10L);
    GcsItemId prefix = createPrefixId("test-bucket", "dir/sub1/");
    GcsListPage<GcsItemInfo> original =
        GcsListPage.<GcsItemInfo>builder()
            .setItems((Iterable<GcsItemInfo>) List.of(item))
            .setPrefixes((Iterable<GcsItemId>) List.of(prefix))
            .setPrefixObject(marker)
            .setNextPageToken("token-1")
            .build();

    GcsListPage<GcsItemInfo> copy = original.toBuilder().build();

    assertThat(copy).isEqualTo(original);
  }

  @Test
  void toBuilder_emptyNextPageToken_clearsContinuationAndSetsHasNextPageFalse() {
    GcsListPage<GcsItemInfo> firstPage =
        GcsListPage.<GcsItemInfo>builder().setNextPageToken("token-1").build();

    GcsListPage<GcsItemInfo> lastPage = firstPage.toBuilder().setNextPageToken("").build();

    assertThat(lastPage.getNextPageToken()).isEmpty();
    assertThat(lastPage.hasNextPage()).isFalse();
  }

  @Test
  void build_emptyNextPageToken_normalizesToAbsentAndHasNextPageFalse() {
    GcsListPage<GcsItemInfo> page = GcsListPage.<GcsItemInfo>builder().setNextPageToken("").build();

    assertThat(page.getNextPageToken()).isEmpty();
    assertThat(page.hasNextPage()).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = {"   ", "\t\n", "\u2003"})
  void build_withWhitespaceOnlyNextPageToken_normalizesToEmpty(String token) {
    GcsListPage<GcsItemInfo> page =
        GcsListPage.<GcsItemInfo>builder().setNextPageToken(token).build();

    assertThat(page.getNextPageToken()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"   ", "\t\n", "\u2003"})
  void build_withWhitespaceOnlyOptionalNextPageToken_normalizesToEmpty(String token) {
    GcsListPage<GcsItemInfo> page =
        GcsListPage.<GcsItemInfo>builder().setNextPageToken(Optional.of(token)).build();

    assertThat(page.getNextPageToken()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"   ", "\t\n", "\u2003"})
  void hasNextPage_withWhitespaceOnlyNextPageToken_returnsFalse(String token) {
    GcsListPage<GcsItemInfo> page =
        GcsListPage.<GcsItemInfo>builder().setNextPageToken(token).build();

    assertThat(page.hasNextPage()).isFalse();
  }

  @Test
  void build_withPaddedNextPageToken_preservesToken() {
    GcsListPage<GcsItemInfo> page =
        GcsListPage.<GcsItemInfo>builder().setNextPageToken(" token-1 ").build();

    assertThat(page.getNextPageToken()).hasValue(" token-1 ");
  }

  @Test
  void setItemsAndPrefixes_iterableOverloads_defensivelyCopyInputs() {
    GcsItemId prefix1 = createPrefixId("test-bucket", "dir/a/");
    GcsItemId prefix2 = createPrefixId("test-bucket", "dir/b/");
    Deque<String> mutableItems = new ArrayDeque<>(List.of("second", "first"));
    Deque<GcsItemId> mutablePrefixes = new ArrayDeque<>(List.of(prefix1, prefix2));
    Iterable<? extends CharSequence> iterableItems = mutableItems;
    Iterable<? extends GcsItemId> iterablePrefixes = mutablePrefixes;
    GcsListPage.Builder<CharSequence> builder =
        GcsListPage.<CharSequence>builder().setItems(iterableItems).setPrefixes(iterablePrefixes);
    mutableItems.clear();
    mutablePrefixes.clear();

    GcsListPage<CharSequence> page = builder.build();

    assertThat(page.getItems()).containsExactly("second", "first").inOrder();
    assertThat(page.getPrefixes()).containsExactly(prefix1, prefix2).inOrder();
  }

  @Test
  void getItemsAndPrefixes_afterToBuilderRoundTrip_returnUnmodifiableLists() {
    GcsItemId prefix = createPrefixId("test-bucket", "dir/sub/");
    GcsListPage<String> initial =
        GcsListPage.<String>builder()
            .setItems((Iterable<String>) List.of("x", "y"))
            .setPrefixes((Iterable<GcsItemId>) List.of(prefix))
            .build();

    GcsListPage<String> copy = initial.toBuilder().build();

    assertThrows(UnsupportedOperationException.class, () -> copy.getItems().clear());
    assertThrows(UnsupportedOperationException.class, () -> copy.getPrefixes().clear());
  }

  @ParameterizedTest
  @MethodSource("nullBuilderActions")
  void builder_nullArgumentsOrElements_throwsNullPointerException(
      Consumer<GcsListPage.Builder<GcsItemInfo>> action) {
    GcsListPage.Builder<GcsItemInfo> builder = GcsListPage.builder();

    assertThrows(NullPointerException.class, () -> action.accept(builder));
  }

  private static Stream<Consumer<GcsListPage.Builder<GcsItemInfo>>> nullBuilderActions() {
    Set<GcsItemInfo> itemWithNull = Collections.singleton(null);
    Set<GcsItemId> prefixWithNull = Collections.singleton(null);
    return Stream.of(
        b -> b.setItems((Iterable<GcsItemInfo>) null),
        b -> b.setPrefixes((Iterable<GcsItemId>) null),
        b -> b.setItems(itemWithNull),
        b -> b.setPrefixes(prefixWithNull),
        b -> b.setPrefixObject(null),
        b -> b.setNextPageToken((String) null));
  }

  @Test
  void builder_doesNotExposePublicOptionalSetterOverloads() {
    for (Method method : GcsListPage.Builder.class.getDeclaredMethods()) {
      if (Modifier.isPublic(method.getModifiers())) {
        for (Class<?> paramType : method.getParameterTypes()) {
          assertThat(paramType).isNotEqualTo(Optional.class);
        }
      }
    }
  }

  @Test
  void signatures_doNotExposeGuavaTypes() {
    ApiSignatureTestUtils.assertNoGuavaTypesInSignatures(
        GcsListPage.class, GcsListPage.Builder.class);
  }
}
