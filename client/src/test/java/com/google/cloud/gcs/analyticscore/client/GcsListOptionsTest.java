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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class GcsListOptionsTest {

  @Test
  void builder_defaults_matchesListDirectoryPreset() {
    GcsListOptions options = GcsListOptions.builder().build();

    assertThat(GcsListOptions.DEFAULT_MAX_LIST_ITEMS_PER_CALL).isEqualTo(5000);
    assertThat(options).isEqualTo(GcsListOptions.LIST_DIRECTORY);
    assertThat(options.getDelimiter()).hasValue("/");
    assertThat(options.isIncludeFoldersAsPrefixes()).isFalse();
    assertThat(options.isIncludeTrailingDelimiter()).isTrue();
    assertThat(options.isIncludePrefixObject()).isFalse();
    assertThat(options.getPageSize()).isEmpty();
    assertThat(options.getPageToken()).isEmpty();
    assertThat(options.getStartOffset()).isEmpty();
    assertThat(options.getEndOffset()).isEmpty();
    assertThat(options.getMatchGlob()).isEmpty();
    assertThat(options.getFields()).isEmpty();
  }

  @Test
  void presets_haveExpectedConfiguration() {
    GcsListOptions hnsDir = GcsListOptions.LIST_DIRECTORY_HNS;
    GcsListOptions recursive = GcsListOptions.LIST_RECURSIVE;
    GcsListOptions emptyFlat = GcsListOptions.EMPTINESS_CHECK_FLAT;
    GcsListOptions emptyHns = GcsListOptions.EMPTINESS_CHECK_HNS;

    GcsListOptions recursiveIds =
        recursive.toBuilder()
            .setFields(List.of(GcsListOptions.ListField.NAME, GcsListOptions.ListField.GENERATION))
            .build();

    assertThat(hnsDir.getDelimiter()).hasValue("/");
    assertThat(hnsDir.isIncludeFoldersAsPrefixes()).isTrue();
    assertThat(hnsDir.isIncludeTrailingDelimiter()).isTrue();
    assertThat(hnsDir.isIncludePrefixObject()).isFalse();
    assertThat(recursive.getDelimiter()).isEmpty();
    assertThat(recursive.isIncludeFoldersAsPrefixes()).isFalse();
    assertThat(recursive.isIncludeTrailingDelimiter()).isFalse();
    assertThat(recursive.isIncludePrefixObject()).isTrue();
    assertThat(recursiveIds.getFields())
        .containsExactly(GcsListOptions.ListField.NAME, GcsListOptions.ListField.GENERATION);
    assertThat(emptyFlat.getPageSize()).hasValue(2);
    assertThat(emptyFlat.getFields()).containsExactly(GcsListOptions.ListField.NAME);
    assertThat(emptyFlat.isIncludeFoldersAsPrefixes()).isFalse();
    assertThat(emptyFlat.isIncludeTrailingDelimiter()).isTrue();
    assertThat(emptyFlat.isIncludePrefixObject()).isFalse();
    assertThat(emptyHns.getPageSize()).hasValue(2);
    assertThat(emptyHns.getFields()).containsExactly(GcsListOptions.ListField.NAME);
    assertThat(emptyHns.isIncludeFoldersAsPrefixes()).isTrue();
    assertThat(emptyHns.isIncludeTrailingDelimiter()).isTrue();
    assertThat(emptyHns.isIncludePrefixObject()).isFalse();
  }

  @Test
  void builder_emptyDelimiterWithDefaultIncludeTrailingDelimiter_succeeds() {
    GcsListOptions.Builder builder = GcsListOptions.builder().setDelimiter(Optional.empty());

    GcsListOptions options = builder.build();

    assertThat(options.getDelimiter()).isEmpty();
    assertThat(options.isIncludeTrailingDelimiter()).isTrue();
    assertThat(options.isIncludeFoldersAsPrefixes()).isFalse();
  }

  @Test
  void toBuilder_allFieldsSetAndCleared_roundTripsCleanly() {
    GcsListOptions full =
        GcsListOptions.builder()
            .setDelimiter("/")
            .setIncludeFoldersAsPrefixes(true)
            .setIncludeTrailingDelimiter(false)
            .setIncludePrefixObject(true)
            .setPageSize(250)
            .setPageToken("token-1")
            .setStartOffset("dir/a")
            .setEndOffset("dir/z")
            .setMatchGlob("dir/*.parquet")
            .setFields(List.of(GcsListOptions.ListField.NAME, GcsListOptions.ListField.SIZE))
            .build();

    GcsListOptions cleared =
        full.toBuilder()
            .setIncludeFoldersAsPrefixes(false)
            .setDelimiter(Optional.empty())
            .setPageSize(OptionalInt.empty())
            .setPageToken(Optional.empty())
            .setStartOffset(Optional.empty())
            .setEndOffset(Optional.empty())
            .setMatchGlob(Optional.empty())
            .setFields(List.of())
            .build();

    assertThat(full.toBuilder().build()).isEqualTo(full);
    assertThat(cleared.getDelimiter()).isEmpty();
    assertThat(cleared.getPageSize()).isEmpty();
    assertThat(cleared.getPageToken()).isEmpty();
    assertThat(cleared.getStartOffset()).isEmpty();
    assertThat(cleared.getEndOffset()).isEmpty();
    assertThat(cleared.getMatchGlob()).isEmpty();
    assertThat(cleared.getFields()).isEmpty();
  }

  @ParameterizedTest
  @MethodSource("emptyPageTokenSetters")
  void build_emptyPageToken_normalizesToAbsent(Consumer<GcsListOptions.Builder> tokenSetter) {
    GcsListOptions.Builder builder = GcsListOptions.builder();
    tokenSetter.accept(builder);

    GcsListOptions options = builder.build();

    assertThat(options.getPageToken()).isEmpty();
  }

  private static Stream<Consumer<GcsListOptions.Builder>> emptyPageTokenSetters() {
    return Stream.of(b -> b.setPageToken(""), b -> b.setPageToken(Optional.of("")));
  }

  @Test
  void setFields_iterableOverload_defensivelyCopiesAndReturnsUnmodifiableSet() {
    Set<GcsListOptions.ListField> mutableSet = new LinkedHashSet<>();
    mutableSet.add(GcsListOptions.ListField.NAME);
    Iterable<GcsListOptions.ListField> nonCollectionIterable = mutableSet::iterator;
    GcsListOptions.Builder builder = GcsListOptions.builder().setFields(nonCollectionIterable);
    mutableSet.add(GcsListOptions.ListField.SIZE);

    GcsListOptions options = builder.build();

    assertThat(options.getFields()).containsExactly(GcsListOptions.ListField.NAME);
    assertThrows(
        UnsupportedOperationException.class,
        () -> options.getFields().add(GcsListOptions.ListField.BUCKET));
  }

  @Test
  void setFields_packagePrivateSetOverload_defensivelyCopiesOnBuild() {
    Set<GcsListOptions.ListField> mutableSet = new LinkedHashSet<>();
    mutableSet.add(GcsListOptions.ListField.NAME);
    mutableSet.add(GcsListOptions.ListField.SIZE);

    GcsListOptions options = GcsListOptions.builder().setFields(mutableSet).build();
    mutableSet.clear();

    assertThat(options.getFields())
        .containsExactly(GcsListOptions.ListField.NAME, GcsListOptions.ListField.SIZE)
        .inOrder();
    assertThrows(
        UnsupportedOperationException.class,
        () -> options.getFields().add(GcsListOptions.ListField.BUCKET));
  }

  @ParameterizedTest
  @MethodSource("emptyDelimiterSetters")
  void build_emptyDelimiterString_throwsIllegalArgumentException(
      Consumer<GcsListOptions.Builder> delimiterSetter) {
    GcsListOptions.Builder builder = GcsListOptions.builder();
    delimiterSetter.accept(builder);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e).hasMessageThat().contains("delimiter must not be empty");
  }

  private static Stream<Consumer<GcsListOptions.Builder>> emptyDelimiterSetters() {
    return Stream.of(b -> b.setDelimiter(""), b -> b.setDelimiter(Optional.of("")));
  }

  @ParameterizedTest
  @MethodSource("nonSlashDelimiters")
  void build_includeFoldersAsPrefixesWithoutSlashDelimiter_throwsIllegalArgumentException(
      Optional<String> delimiter) {
    GcsListOptions.Builder builder =
        GcsListOptions.builder().setDelimiter(delimiter).setIncludeFoldersAsPrefixes(true);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e).hasMessageThat().contains("includeFoldersAsPrefixes requires delimiter \"/\"");
  }

  private static Stream<Optional<String>> nonSlashDelimiters() {
    return Stream.of(Optional.empty(), Optional.of(":"), Optional.of("|"));
  }

  @ParameterizedTest
  @ValueSource(strings = {":", "|"})
  void build_matchGlobWithUnsupportedDelimiter_throwsIllegalArgumentException(String delimiter) {
    GcsListOptions.Builder builder =
        GcsListOptions.builder().setDelimiter(delimiter).setMatchGlob("*.parquet");

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e).hasMessageThat().contains("matchGlob requires delimiter to be absent or \"/\"");
  }

  @ParameterizedTest
  @MethodSource("validMatchGlobDelimiters")
  void build_matchGlobWithAbsentOrSlashDelimiter_succeeds(Optional<String> delimiter) {
    GcsListOptions.Builder builder =
        GcsListOptions.builder().setDelimiter(delimiter).setMatchGlob("**/*.parquet");

    GcsListOptions options = builder.build();

    assertThat(options.getDelimiter()).isEqualTo(delimiter);
    assertThat(options.getMatchGlob()).hasValue("**/*.parquet");
  }

  private static Stream<Optional<String>> validMatchGlobDelimiters() {
    return Stream.of(Optional.empty(), Optional.of("/"));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
  void build_nonPositivePageSize_throwsIllegalArgumentException(int invalidPageSize) {
    GcsListOptions.Builder builder = GcsListOptions.builder().setPageSize(invalidPageSize);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e).hasMessageThat().contains("pageSize must be positive");
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
    return Stream.of(
        Arguments.of("dir/a", "dir/a"),
        Arguments.of("dir/a", "dir/b"),
        Arguments.of("\uFFFF", "\uD83D\uDE00"));
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
    Set<GcsListOptions.ListField> setWithNull = Collections.singleton(null);
    return Stream.of(
        b -> b.setDelimiter((String) null),
        b -> b.setDelimiter((Optional<String>) null),
        b -> b.setPageSize(null),
        b -> b.setPageToken((String) null),
        b -> b.setPageToken((Optional<String>) null),
        b -> b.setStartOffset((String) null),
        b -> b.setStartOffset((Optional<String>) null),
        b -> b.setEndOffset((String) null),
        b -> b.setEndOffset((Optional<String>) null),
        b -> b.setMatchGlob((String) null),
        b -> b.setMatchGlob((Optional<String>) null),
        b -> b.setFields((Iterable<GcsListOptions.ListField>) null),
        b -> b.setFields((Set<GcsListOptions.ListField>) null),
        b -> b.setFields((Iterable<GcsListOptions.ListField>) setWithNull),
        b -> b.setFields(setWithNull).build());
  }

  @Test
  void signatures_doNotExposeGuavaTypes() {
    GcsListOptions.Builder builder = GcsListOptions.builder();

    ApiSignatureTestUtils.assertNoGuavaTypesInSignatures(
        GcsListOptions.class,
        GcsListOptions.Builder.class,
        GcsListOptions.LIST_DIRECTORY.getClass(),
        builder.getClass());
  }
}
