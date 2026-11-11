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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class GcsListOptionsTest {

  @Test
  void defaultMaxListItemsPerCall_is5000() {
    assertThat(GcsListOptions.DEFAULT_MAX_LIST_ITEMS_PER_CALL).isEqualTo(5000);
  }

  @Test
  void builder_defaults_matchesListDirectoryPreset() {
    GcsListOptions options = GcsListOptions.builder().build();

    assertThat(options).isEqualTo(GcsListOptions.LIST_DIRECTORY);
    assertThat(options.getDelimiter()).hasValue("/");
    assertThat(options.isIncludeFoldersAsPrefixes()).isFalse();
    assertThat(options.isIncludePrefixObject()).isFalse();
    assertThat(options.getPageSize()).isEmpty();
    assertThat(options.getPageToken()).isEmpty();
    assertThat(options.getStartOffset()).isEmpty();
    assertThat(options.getEndOffset()).isEmpty();
    assertThat(options.getMatchGlob()).isEmpty();
    assertThat(options.getFields())
        .containsExactly(
            GcsListOptions.ListField.NAME,
            GcsListOptions.ListField.BUCKET,
            GcsListOptions.ListField.GENERATION,
            GcsListOptions.ListField.METAGENERATION,
            GcsListOptions.ListField.SIZE,
            GcsListOptions.ListField.CONTENT_TYPE,
            GcsListOptions.ListField.CONTENT_ENCODING,
            GcsListOptions.ListField.TIME_CREATED,
            GcsListOptions.ListField.UPDATED,
            GcsListOptions.ListField.CRC32C,
            GcsListOptions.ListField.MD5HASH,
            GcsListOptions.ListField.METADATA)
        .inOrder();
  }

  @Test
  void listField_values_containExpectedMetadataFieldsInOrder() {
    GcsListOptions.ListField[] fields = GcsListOptions.ListField.values();

    assertThat(fields)
        .asList()
        .containsExactly(
            GcsListOptions.ListField.NAME,
            GcsListOptions.ListField.BUCKET,
            GcsListOptions.ListField.GENERATION,
            GcsListOptions.ListField.METAGENERATION,
            GcsListOptions.ListField.SIZE,
            GcsListOptions.ListField.CONTENT_TYPE,
            GcsListOptions.ListField.CONTENT_ENCODING,
            GcsListOptions.ListField.TIME_CREATED,
            GcsListOptions.ListField.UPDATED,
            GcsListOptions.ListField.STORAGE_CLASS,
            GcsListOptions.ListField.CRC32C,
            GcsListOptions.ListField.MD5HASH,
            GcsListOptions.ListField.METADATA)
        .inOrder();
  }

  @Test
  void listDirectoryHnsPreset_includesFoldersAsPrefixes() {
    GcsListOptions options = GcsListOptions.LIST_DIRECTORY_HNS;

    assertThat(options.getDelimiter()).hasValue("/");
    assertThat(options.isIncludeFoldersAsPrefixes()).isTrue();
    assertThat(options.isIncludePrefixObject()).isFalse();
  }

  @Test
  void listRecursivePreset_clearsDelimiterAndExcludesPrefixObject() {
    GcsListOptions options = GcsListOptions.LIST_RECURSIVE;

    assertThat(options.getDelimiter()).isEmpty();
    assertThat(options.isIncludeFoldersAsPrefixes()).isFalse();
    assertThat(options.isIncludePrefixObject()).isFalse();
  }

  @Test
  void emptinessCheckFlatPreset_requestsPageSizeTwoAndMinimalFields() {
    GcsListOptions options = GcsListOptions.EMPTINESS_CHECK_FLAT;

    assertThat(options.getDelimiter()).hasValue("/");
    assertThat(options.getPageSize()).hasValue(2);
    assertThat(options.getFields())
        .containsExactly(GcsListOptions.ListField.NAME, GcsListOptions.ListField.BUCKET);
    assertThat(options.isIncludeFoldersAsPrefixes()).isFalse();
    assertThat(options.isIncludePrefixObject()).isFalse();
  }

  @Test
  void emptinessCheckHnsPreset_requestsPageSizeTwoFoldersAndMinimalFields() {
    GcsListOptions options = GcsListOptions.EMPTINESS_CHECK_HNS;

    assertThat(options.getDelimiter()).hasValue("/");
    assertThat(options.getPageSize()).hasValue(2);
    assertThat(options.getFields())
        .containsExactly(GcsListOptions.ListField.NAME, GcsListOptions.ListField.BUCKET);
    assertThat(options.isIncludeFoldersAsPrefixes()).isTrue();
    assertThat(options.isIncludePrefixObject()).isFalse();
  }

  @Test
  void emptinessCheckPresets_arePackagePrivate() throws NoSuchFieldException {
    int flatModifiers =
        GcsListOptions.class.getDeclaredField("EMPTINESS_CHECK_FLAT").getModifiers();
    int hnsModifiers = GcsListOptions.class.getDeclaredField("EMPTINESS_CHECK_HNS").getModifiers();

    assertThat(Modifier.isPublic(flatModifiers) || Modifier.isProtected(flatModifiers)).isFalse();
    assertThat(Modifier.isPublic(hnsModifiers) || Modifier.isProtected(hnsModifiers)).isFalse();
  }

  @Test
  void builder_clearDelimiter_equalsListRecursiveAndRoundTripsToListDirectory() {
    GcsListOptions options = GcsListOptions.builder().clearDelimiter().build();

    assertThat(options).isEqualTo(GcsListOptions.LIST_RECURSIVE);
    assertThat(options.toBuilder().setDelimiter("/").build())
        .isEqualTo(GcsListOptions.LIST_DIRECTORY);
  }

  @Test
  void toBuilder_allFieldsSet_preservesEquality() {
    GcsListOptions full =
        GcsListOptions.builder()
            .setDelimiter("/")
            .setIncludeFoldersAsPrefixes(true)
            .setIncludePrefixObject(true)
            .setPageSize(250)
            .setPageToken("token-1")
            .setStartOffset("dir/a")
            .setEndOffset("dir/z")
            .setMatchGlob("dir/*.parquet")
            .setFields(GcsListOptions.ListField.NAME, GcsListOptions.ListField.SIZE)
            .build();

    GcsListOptions copy = full.toBuilder().build();

    assertThat(copy).isEqualTo(full);
  }

  @Test
  void toBuilder_clearedDelimiterAndEmptyPageToken_clearsValues() {
    GcsListOptions full =
        GcsListOptions.builder()
            .setDelimiter("/")
            .setIncludeFoldersAsPrefixes(true)
            .setPageToken("token-1")
            .setFields(GcsListOptions.ListField.NAME, GcsListOptions.ListField.SIZE)
            .build();

    GcsListOptions updated =
        full.toBuilder()
            .setIncludeFoldersAsPrefixes(false)
            .clearDelimiter()
            .setPageToken("")
            .setFields()
            .build();

    assertThat(updated.getDelimiter()).isEmpty();
    assertThat(updated.getPageToken()).isEmpty();
    assertThat(updated.getFields())
        .containsExactly(GcsListOptions.ListField.NAME, GcsListOptions.ListField.BUCKET);
  }

  @Test
  void build_emptyFields_selectsMinimalNameAndBucketProjection() {
    GcsListOptions options =
        GcsListOptions.EMPTINESS_CHECK_FLAT.toBuilder().setFields(List.of()).build();

    assertThat(options.getFields())
        .containsExactly(GcsListOptions.ListField.NAME, GcsListOptions.ListField.BUCKET);
  }

  @Test
  void build_fieldsWithoutRequired_autoIncludesNameAndBucket() {
    GcsListOptions options =
        GcsListOptions.builder()
            .setFields(GcsListOptions.ListField.SIZE, GcsListOptions.ListField.STORAGE_CLASS)
            .build();

    assertThat(options.getFields())
        .containsExactly(
            GcsListOptions.ListField.NAME,
            GcsListOptions.ListField.BUCKET,
            GcsListOptions.ListField.SIZE,
            GcsListOptions.ListField.STORAGE_CLASS)
        .inOrder();
  }

  @Test
  void build_emptyPageToken_normalizesToAbsent() {
    GcsListOptions options = GcsListOptions.builder().setPageToken("").build();

    assertThat(options.getPageToken()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"   ", "\t\n", "\u2003"})
  void build_withWhitespaceOnlyPageToken_normalizesToEmpty(String token) {
    GcsListOptions options = GcsListOptions.builder().setPageToken(token).build();

    assertThat(options.getPageToken()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"   ", "\t\n", "\u2003"})
  void build_withWhitespaceOnlyOptionalPageToken_normalizesToEmpty(String token) {
    GcsListOptions options = GcsListOptions.builder().setPageToken(Optional.of(token)).build();

    assertThat(options.getPageToken()).isEmpty();
  }

  @Test
  void build_withPaddedPageToken_preservesToken() {
    GcsListOptions options = GcsListOptions.builder().setPageToken(" token-1 ").build();

    assertThat(options.getPageToken()).hasValue(" token-1 ");
  }

  @Test
  void setFields_iterableOverload_defensivelyCopiesInput() {
    List<GcsListOptions.ListField> mutableFields =
        new ArrayList<>(List.of(GcsListOptions.ListField.GENERATION));
    Iterable<GcsListOptions.ListField> nonCollectionIterable = mutableFields::iterator;
    GcsListOptions.Builder builder = GcsListOptions.builder().setFields(nonCollectionIterable);
    mutableFields.add(GcsListOptions.ListField.SIZE);

    GcsListOptions options = builder.build();

    assertThat(options.getFields())
        .containsExactly(
            GcsListOptions.ListField.NAME,
            GcsListOptions.ListField.BUCKET,
            GcsListOptions.ListField.GENERATION);
  }

  @Test
  void setFields_varargsOverload_ordersInCanonicalEnumOrder() {
    GcsListOptions options =
        GcsListOptions.builder()
            .setFields(GcsListOptions.ListField.METADATA, GcsListOptions.ListField.GENERATION)
            .build();

    assertThat(options.getFields())
        .containsExactly(
            GcsListOptions.ListField.NAME,
            GcsListOptions.ListField.BUCKET,
            GcsListOptions.ListField.GENERATION,
            GcsListOptions.ListField.METADATA)
        .inOrder();
  }

  @Test
  void setFields_emptyVarargs_selectsMinimalNameAndBucketProjection() {
    GcsListOptions options = GcsListOptions.builder().setFields().build();

    assertThat(options.getFields())
        .containsExactly(GcsListOptions.ListField.NAME, GcsListOptions.ListField.BUCKET)
        .inOrder();
  }

  @Test
  void getFields_afterToBuilderRoundTrip_returnsUnmodifiableSet() {
    GcsListOptions initial =
        GcsListOptions.builder()
            .setFields(List.of(GcsListOptions.ListField.NAME, GcsListOptions.ListField.SIZE))
            .build();

    GcsListOptions copy = initial.toBuilder().build();

    assertThrows(
        UnsupportedOperationException.class,
        () -> copy.getFields().add(GcsListOptions.ListField.BUCKET));
  }

  @Test
  void build_emptyDelimiterString_throwsIllegalArgumentException() {
    GcsListOptions.Builder builder = GcsListOptions.builder().setDelimiter("");

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e).hasMessageThat().contains("delimiter must not be empty");
  }

  @ParameterizedTest
  @MethodSource("nonSlashDelimiterConfigurers")
  void build_includeFoldersAsPrefixesWithoutSlashDelimiter_throwsIllegalArgumentException(
      Consumer<GcsListOptions.Builder> delimiterConfigurer) {
    GcsListOptions.Builder builder = GcsListOptions.builder().setIncludeFoldersAsPrefixes(true);
    delimiterConfigurer.accept(builder);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e).hasMessageThat().contains("includeFoldersAsPrefixes requires delimiter \"/\"");
  }

  private static Stream<Consumer<GcsListOptions.Builder>> nonSlashDelimiterConfigurers() {
    return Stream.of(
        GcsListOptions.Builder::clearDelimiter, b -> b.setDelimiter(":"), b -> b.setDelimiter("|"));
  }

  @ParameterizedTest
  @ValueSource(strings = {":", "|"})
  void build_matchGlobWithUnsupportedDelimiter_throwsIllegalArgumentException(String delimiter) {
    GcsListOptions.Builder builder =
        GcsListOptions.builder().setDelimiter(delimiter).setMatchGlob("*.parquet");

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e).hasMessageThat().contains("matchGlob requires delimiter to be absent or \"/\"");
  }

  @Test
  void build_matchGlobWithAbsentDelimiter_succeeds() {
    GcsListOptions options =
        GcsListOptions.builder().clearDelimiter().setMatchGlob("**/*.parquet").build();

    assertThat(options.getDelimiter()).isEmpty();
    assertThat(options.getMatchGlob()).hasValue("**/*.parquet");
  }

  @Test
  void build_matchGlobWithSlashDelimiter_succeeds() {
    GcsListOptions options =
        GcsListOptions.builder().setDelimiter("/").setMatchGlob("**/*.parquet").build();

    assertThat(options.getDelimiter()).hasValue("/");
    assertThat(options.getMatchGlob()).hasValue("**/*.parquet");
  }

  @Test
  void build_emptyMatchGlob_throwsIllegalArgumentException() {
    GcsListOptions.Builder builder = GcsListOptions.builder().setMatchGlob("");

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e).hasMessageThat().contains("matchGlob must not be empty");
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
  void build_nonPositivePageSize_throwsIllegalArgumentException(int invalidPageSize) {
    GcsListOptions.Builder builder = GcsListOptions.builder().setPageSize(invalidPageSize);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e).hasMessageThat().contains("pageSize must be positive");
  }

  @Test
  void build_pageSizeAbove5000_preservesHintValue() {
    GcsListOptions options = GcsListOptions.builder().setPageSize(10_000).build();

    assertThat(options.getPageSize()).hasValue(10_000);
  }

  @Test
  void build_emptyStartOffset_throwsIllegalArgumentException() {
    GcsListOptions.Builder builder = GcsListOptions.builder().setStartOffset("");

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e).hasMessageThat().contains("startOffset must not be empty");
  }

  @Test
  void build_emptyEndOffset_throwsIllegalArgumentException() {
    GcsListOptions.Builder builder = GcsListOptions.builder().setEndOffset("");

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e).hasMessageThat().contains("endOffset must not be empty");
  }

  @Test
  void build_onlyStartOffset_succeeds() {
    GcsListOptions options = GcsListOptions.builder().setStartOffset("dir/a").build();

    assertThat(options.getStartOffset()).hasValue("dir/a");
    assertThat(options.getEndOffset()).isEmpty();
  }

  @Test
  void build_onlyEndOffset_succeeds() {
    GcsListOptions options = GcsListOptions.builder().setEndOffset("dir/z").build();

    assertThat(options.getStartOffset()).isEmpty();
    assertThat(options.getEndOffset()).hasValue("dir/z");
  }

  @ParameterizedTest
  @MethodSource("validUtf8OffsetPairs")
  void build_validUtf8OffsetOrdering_succeeds(String startOffset, String endOffset) {
    GcsListOptions.Builder builder =
        GcsListOptions.builder().setStartOffset(startOffset).setEndOffset(endOffset);

    GcsListOptions options = builder.build();

    assertThat(options.getStartOffset()).hasValue(startOffset);
    assertThat(options.getEndOffset()).hasValue(endOffset);
  }

  private static Stream<Arguments> validUtf8OffsetPairs() {
    return Stream.of(Arguments.of("dir/a", "dir/b"), Arguments.of("\uFFFF", "\uD83D\uDE00"));
  }

  @Test
  void build_equalStartAndEndOffset_throwsIllegalArgumentException() {
    GcsListOptions.Builder builder =
        GcsListOptions.builder().setStartOffset("dir/a").setEndOffset("dir/a");

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e).hasMessageThat().contains("must be < endOffset");
  }

  @ParameterizedTest
  @MethodSource("invalidUtf8OffsetPairs")
  void build_invertedUtf8OffsetOrdering_throwsIllegalArgumentException(
      String startOffset, String endOffset) {
    GcsListOptions.Builder builder =
        GcsListOptions.builder().setStartOffset(startOffset).setEndOffset(endOffset);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e).hasMessageThat().contains("UTF-8 byte order");
  }

  private static Stream<Arguments> invalidUtf8OffsetPairs() {
    return Stream.of(Arguments.of("dir/b", "dir/a"), Arguments.of("\uD83D\uDE00", "\uFFFF"));
  }

  @ParameterizedTest
  @MethodSource("nullBuilderActions")
  void builder_nullArguments_throwsNullPointerException(Consumer<GcsListOptions.Builder> action) {
    GcsListOptions.Builder builder = GcsListOptions.builder();

    assertThrows(NullPointerException.class, () -> action.accept(builder));
  }

  private static Stream<Consumer<GcsListOptions.Builder>> nullBuilderActions() {
    List<GcsListOptions.ListField> listWithNull = Collections.singletonList(null);
    return Stream.of(
        b -> b.setDelimiter((String) null),
        b -> b.setPageToken((String) null),
        b -> b.setStartOffset(null),
        b -> b.setEndOffset(null),
        b -> b.setMatchGlob(null),
        b -> b.setFields((Iterable<GcsListOptions.ListField>) null),
        b -> b.setFields((GcsListOptions.ListField[]) null),
        b -> b.setFields(GcsListOptions.ListField.NAME, null),
        b -> b.setFields(listWithNull));
  }

  @Test
  void builder_doesNotExposePublicOptionalSetterOverloads() {
    for (Method method : GcsListOptions.Builder.class.getDeclaredMethods()) {
      if (Modifier.isPublic(method.getModifiers())) {
        for (Class<?> paramType : method.getParameterTypes()) {
          assertThat(paramType).isNotEqualTo(Optional.class);
          assertThat(paramType).isNotEqualTo(OptionalInt.class);
        }
      }
    }
  }

  @Test
  void signatures_doNotExposeGuavaTypes() {
    ApiSignatureTestUtils.assertNoGuavaTypesInSignatures(
        GcsListOptions.class, GcsListOptions.Builder.class, GcsListOptions.ListField.class);
  }
}
