/*
 * Copyright 2025 Google LLC
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.StorageClass;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class FakeGcsClientImplTest {

  private FakeGcsClientImpl fakeGcsClient;

  @BeforeEach
  void setUp() {
    GcsFileSystemOptions options = GcsFileSystemOptions.builder().build();
    fakeGcsClient =
        new FakeGcsClientImpl(
            options.getGcsClientOptions(),
            Suppliers.ofInstance(Executors.newSingleThreadExecutor()),
            new Telemetry(ImmutableList.of()),
            new AnalyticsCacheManager(options.getGcsCacheOptions()).getBucketPropertiesCache());
    FakeGcsClientImpl.resetCounts();
  }

  @Test
  void openReadChannel_incrementsOpenReadChannelCount() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(100L).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    TestDataGenerator.createGcsData(itemId, 100);

    VectoredSeekableByteChannel channel = fakeGcsClient.openReadChannel(itemInfo, readOptions);

    assertNotNull(channel);
    assertEquals(1, FakeGcsClientImpl.getOpenReadChannelCount());
  }

  @Test
  void getGcsItemInfo_objectExists_pass() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    TestDataGenerator.createGcsData(itemId, 100);

    GcsItemInfo itemInfo = fakeGcsClient.getGcsItemInfo(itemId);

    assertNotNull(itemInfo);
    GcsItemId expectedItemId =
        GcsItemId.builder()
            .setBucketName("test-bucket")
            .setObjectName("test-object")
            .setContentGeneration(itemInfo.getContentGeneration().get())
            .build();
    assertThat(itemInfo.getItemId()).isEqualTo(expectedItemId);
    assertEquals(100L, itemInfo.getSize());
  }

  @Test
  void getGcsItemInfo_objectDoesNotExists_throwsFileNotFoundException() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("not-exists").build();

    assertThrows(FileNotFoundException.class, () -> fakeGcsClient.getGcsItemInfo(itemId));
  }

  @Test
  void getGcsItemInfo_itemIsNotObject_throws() {
    GcsItemId itemId = GcsItemId.builder().setBucketName("test-bucket").build(); // No object name

    assertThrows(UnsupportedOperationException.class, () -> fakeGcsClient.getGcsItemInfo(itemId));
  }

  @Test
  void close_incrementsCloseCount() {
    fakeGcsClient.close();
    assertEquals(1, FakeGcsClientImpl.getCloseCount());
  }

  @Test
  void lazyGetGrpcStorage_returnsLocalStorageInstance() throws IOException {
    assertThat(fakeGcsClient.lazyGetGrpcStorage()).isSameInstanceAs(FakeGcsClientImpl.storage);
  }

  @Test
  void listObjectsPage_listDirectory_separatesSelfMarkerAndDeduplicatesChildPrefix()
      throws IOException {
    String bucket = "list-dir-bucket";
    putBlob(bucket, "dir/", "");
    putBlob(bucket, "dir/file.txt", "abc");
    putBlob(bucket, "dir/sub/", "");
    putBlob(bucket, "dir/sub/x", "nested");
    putBlob(bucket, "dir/other/y", "inferred");

    GcsListPage<GcsItemInfo> page =
        fakeGcsClient.listObjectsPage(id(bucket, "dir/"), GcsListOptions.LIST_DIRECTORY);

    assertThat(page.getPrefixObject()).isPresent();
    assertThat(page.getPrefixObject().get().getItemId().getObjectName()).hasValue("dir/");
    assertThat(page.getPrefixObject().get().getItemType())
        .isEqualTo(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY);
    assertThat(itemNames(page)).containsExactly("dir/file.txt", "dir/sub/").inOrder();
    assertThat(page.getPrefixes()).containsExactly(id(bucket, "dir/other/"));
    assertThat(page.hasNextPage()).isFalse();
  }

  @Test
  void listObjectsPage_emptinessCheckFlatOnEmptyMarker_returnsPrefixObjectOnly()
      throws IOException {
    String bucket = "emptiness-empty-bucket";
    putBlob(bucket, "empty/", "");

    GcsListPage<GcsItemInfo> page =
        fakeGcsClient.listObjectsPage(id(bucket, "empty/"), GcsListOptions.EMPTINESS_CHECK_FLAT);

    assertThat(page.getPrefixObject()).isPresent();
    assertThat(page.getItems()).isEmpty();
    assertThat(page.getPrefixes()).isEmpty();
    assertThat(page.hasNextPage()).isFalse();
  }

  @Test
  void listObjectsPage_emptinessCheckFlatOnNonEmptyMarker_returnsPrefixObjectAndChild()
      throws IOException {
    String bucket = "emptiness-nonempty-bucket";
    putBlob(bucket, "nonempty/", "");
    putBlob(bucket, "nonempty/child1", "1");
    putBlob(bucket, "nonempty/child2", "2");

    GcsListPage<GcsItemInfo> page =
        fakeGcsClient.listObjectsPage(id(bucket, "nonempty/"), GcsListOptions.EMPTINESS_CHECK_FLAT);

    assertThat(page.getPrefixObject()).isPresent();
    assertThat(itemNames(page)).containsExactly("nonempty/child1");
    assertThat(page.hasNextPage()).isTrue();
  }

  @TestFactory
  Stream<DynamicTest> listObjectsPage_recursiveWithOffsetsAndMatchGlob_sortsAndFiltersUtf8() {
    GcsListOptions rec = GcsListOptions.LIST_RECURSIVE;
    GcsListOptions globRange =
        rec.toBuilder()
            .setStartOffset("dir/\uFFFF")
            .setEndOffset("dir/\uD83D\uDE01")
            .setMatchGlob("dir/**/*.parquet")
            .build();
    String[] all = {
      "dir/a/b/nested.parquet", "dir/\uFFFF.parquet", "dir/\uD83D\uDE00.parquet", "top.parquet"
    };
    String[] unfiltered = {all[0], "dir/skip.csv", all[1], all[2], all[3]};
    return Stream.of(
        recursiveCase("utf8-unfiltered", rec, unfiltered),
        recursiveCase("utf8-all", rec.toBuilder().setMatchGlob("**.parquet").build(), all),
        recursiveCase("utf8-root", rec.toBuilder().setMatchGlob("**/*.parquet").build(), all),
        recursiveCase(
            "utf8-glob-range", globRange, "dir/\uFFFF.parquet", "dir/\uD83D\uDE00.parquet"));
  }

  private DynamicTest recursiveCase(String bucket, GcsListOptions opts, String... expected) {
    return dynamicTest(
        bucket,
        () -> {
          putBlob(bucket, "top.parquet", "top");
          putBlob(bucket, "dir/\uFFFF.parquet", "bmp");
          putBlob(bucket, "dir/\uD83D\uDE00.parquet", "emoji");
          putBlob(bucket, "dir/a/b/nested.parquet", "nested");
          putBlob(bucket, "dir/skip.csv", "csv");

          GcsListPage<GcsItemInfo> page = fakeGcsClient.listObjectsPage(id(bucket, ""), opts);

          assertThat(itemNames(page)).containsExactly((Object[]) expected).inOrder();
        });
  }

  @Test
  void listObjectsPage_directoryTrailingSlashGlob_matchesDirsAndExcludesPlainObject()
      throws IOException {
    String bucket = "dir-glob-bucket";
    putBlob(bucket, "foo", "plain");
    putBlob(bucket, "foo/", "");
    putBlob(bucket, "foobar/child", "x");
    putBlob(bucket, "other/child", "y");
    GcsListOptions opts = GcsListOptions.LIST_DIRECTORY.toBuilder().setMatchGlob("foo**/").build();

    GcsListPage<GcsItemInfo> page = fakeGcsClient.listObjectsPage(id(bucket, ""), opts);

    assertThat(itemNames(page)).containsExactly("foo/");
    assertThat(page.getPrefixes()).containsExactly(id(bucket, "foobar/"));
  }

  @Test
  void listObjectsPage_withPageSizeAndPageToken_paginatesAcrossItemsAndPrefixes()
      throws IOException {
    String bucket = "pagination-bucket";
    putBlob(bucket, "p/a", "1");
    putBlob(bucket, "p/b/x", "2");
    putBlob(bucket, "p/c", "3");
    GcsItemId prefixId = id(bucket, "p/");
    GcsListOptions opts = GcsListOptions.LIST_DIRECTORY.toBuilder().setPageSize(2).build();

    GcsListPage<GcsItemInfo> first = fakeGcsClient.listObjectsPage(prefixId, opts);
    GcsListPage<GcsItemInfo> second =
        fakeGcsClient.listObjectsPage(
            prefixId,
            opts.toBuilder().setPageToken(first.getNextPageToken().orElseThrow()).build());

    assertThat(itemNames(first)).containsExactly("p/a");
    assertThat(first.getPrefixes()).containsExactly(id(bucket, "p/b/"));
    assertThat(first.hasNextPage()).isTrue();
    assertThat(itemNames(second)).containsExactly("p/c");
    assertThat(second.getPrefixes()).isEmpty();
    assertThat(second.hasNextPage()).isFalse();
  }

  @Test
  void listObjectsPage_absentPageSize_capsFirstPageAtDefaultMaxListItemsPerCall()
      throws IOException {
    String bucket = "default-5000-cap-bucket";
    createNumberedBlobs(bucket, "cap/", GcsListOptions.DEFAULT_MAX_LIST_ITEMS_PER_CALL + 2);
    GcsItemId prefixId = id(bucket, "cap/");

    GcsListPage<GcsItemInfo> first =
        fakeGcsClient.listObjectsPage(prefixId, GcsListOptions.LIST_DIRECTORY);

    assertThat(first.getItems()).hasSize(GcsListOptions.DEFAULT_MAX_LIST_ITEMS_PER_CALL);
    assertThat(first.hasNextPage()).isTrue();
  }

  @Test
  void listObjectsPage_oversizedPageSize_capsPageAtDefaultMaxListItemsPerCall() throws IOException {
    String bucket = "oversized-5000-cap-bucket";
    createNumberedBlobs(bucket, "cap/", GcsListOptions.DEFAULT_MAX_LIST_ITEMS_PER_CALL + 2);
    GcsListOptions oversizedOptions =
        GcsListOptions.LIST_DIRECTORY.toBuilder().setPageSize(10_000).build();

    GcsListPage<GcsItemInfo> page =
        fakeGcsClient.listObjectsPage(id(bucket, "cap/"), oversizedOptions);

    assertThat(page.getItems()).hasSize(GcsListOptions.DEFAULT_MAX_LIST_ITEMS_PER_CALL);
    assertThat(page.hasNextPage()).isTrue();
  }

  @Test
  void listObjectsPage_secondPageAfterDefaultCap_returnsRemainingItems() throws IOException {
    String bucket = "second-page-5000-cap-bucket";
    createNumberedBlobs(bucket, "cap/", GcsListOptions.DEFAULT_MAX_LIST_ITEMS_PER_CALL + 2);
    GcsItemId prefixId = id(bucket, "cap/");
    GcsListPage<GcsItemInfo> first =
        fakeGcsClient.listObjectsPage(prefixId, GcsListOptions.LIST_DIRECTORY);

    GcsListPage<GcsItemInfo> second =
        fakeGcsClient.listObjectsPage(
            prefixId,
            GcsListOptions.LIST_DIRECTORY.toBuilder()
                .setPageToken(first.getNextPageToken().orElseThrow())
                .build());

    assertThat(itemNames(second)).containsExactly("cap/obj-5000", "cap/obj-5001").inOrder();
    assertThat(second.hasNextPage()).isFalse();
  }

  @Test
  void listObjectsPage_deleteFirstPageAndContinue_doesNotSkipRemainingObjects() throws IOException {
    String bucket = "delete-between-pages-bucket";
    for (String name : new String[] {"p/a", "p/b", "p/c", "p/d", "p/e"}) {
      putBlob(bucket, name, "x");
    }
    GcsItemId dirId = id(bucket, "p/");
    GcsListOptions opts = GcsListOptions.LIST_DIRECTORY.toBuilder().setPageSize(2).build();
    GcsListPage<GcsItemInfo> first = fakeGcsClient.listObjectsPage(dirId, opts);
    for (GcsItemInfo item : first.getItems()) {
      FakeGcsClientImpl.storage.delete(
          BlobId.of(bucket, item.getItemId().getObjectName().orElseThrow()));
    }

    GcsListPage<GcsItemInfo> second =
        fakeGcsClient.listObjectsPage(
            dirId, opts.toBuilder().setPageToken(first.getNextPageToken().orElseThrow()).build());
    GcsListPage<GcsItemInfo> third =
        fakeGcsClient.listObjectsPage(
            dirId, opts.toBuilder().setPageToken(second.getNextPageToken().orElseThrow()).build());

    assertThat(itemNames(first)).containsExactly("p/a", "p/b").inOrder();
    assertThat(itemNames(second)).containsExactly("p/c", "p/d").inOrder();
    assertThat(itemNames(third)).containsExactly("p/e");
    assertThat(third.hasNextPage()).isFalse();
  }

  @Test
  void listObjectsPage_minimalFieldProjection_masksUnprojectedMetadata() throws IOException {
    String bucket = "fake-minimal-fields-bucket";
    BlobInfo blobInfo =
        BlobInfo.newBuilder(BlobId.of(bucket, "f/data.bin", 99L))
            .setContentType("application/octet-stream")
            .setContentEncoding("gzip")
            .setStorageClass(StorageClass.NEARLINE)
            .setMetadata(ImmutableMap.of("k", "dmFs"))
            .build();
    FakeGcsClientImpl.storage.create(blobInfo, "1234567".getBytes(StandardCharsets.UTF_8));

    GcsListPage<GcsItemInfo> page =
        fakeGcsClient.listObjectsPage(id(bucket, "f/"), GcsListOptions.EMPTINESS_CHECK_FLAT);

    assertThat(page.getItems()).hasSize(1);
    GcsItemInfo item = page.getItems().get(0);
    assertThat(item.getItemId().getObjectName()).hasValue("f/data.bin");
    assertThat(item.getItemId().getContentGeneration()).isEmpty();
    assertThat(item.getContentGeneration()).isEmpty();
    assertThat(item.getSize()).isEqualTo(0L);
    assertThat(item.getCreationTime()).isEqualTo(0L);
    assertThat(item.getModificationTime()).isEqualTo(0L);
    assertThat(item.getMetaGeneration()).isEqualTo(0L);
    assertThat(item.getContentType()).isEmpty();
    assertThat(item.getContentEncoding()).isEmpty();
    assertThat(item.getStorageClass()).isEmpty();
    assertThat(item.getVerificationAttributes()).isEmpty();
    assertThat(item.getExtendedAttributes()).isEmpty();
  }

  @Test
  void listObjectsPage_allFieldsProjected_preservesObjectMetadata() throws IOException {
    String bucket = "fake-all-fields-bucket";
    BlobInfo blobInfo =
        BlobInfo.newBuilder(BlobId.of(bucket, "f/data.bin", 99L))
            .setContentType("application/parquet")
            .setContentEncoding("zstd")
            .setStorageClass(StorageClass.STANDARD)
            .setMetadata(ImmutableMap.of("k", "dmFs"))
            .build();
    FakeGcsClientImpl.storage.create(blobInfo, "1234567".getBytes(StandardCharsets.UTF_8));
    GcsListOptions allFields =
        GcsListOptions.LIST_DIRECTORY.toBuilder()
            .setFields(GcsListOptions.ListField.values())
            .build();

    GcsListPage<GcsItemInfo> page = fakeGcsClient.listObjectsPage(id(bucket, "f/"), allFields);

    assertThat(page.getItems()).hasSize(1);
    GcsItemInfo item = page.getItems().get(0);
    assertThat(item.getSize()).isEqualTo(7L);
    assertThat(item.getContentType()).hasValue("application/parquet");
    assertThat(item.getContentEncoding()).hasValue("zstd");
    assertThat(item.getStorageClass()).hasValue("STANDARD");
    assertThat(item.getVerificationAttributes()).isPresent();
    assertThat(item.getVerificationAttributes().get().getCrc32c()).isNotNull();
    assertThat(item.getExtendedAttributes()).containsKey("k");
  }

  @Test
  void listObjectsPage_leadingSlashPrefix_returnsMatchingObjects() throws IOException {
    String bucket = "leading-slash-bucket";
    putBlob(bucket, "/lead/x", "x");
    putBlob(bucket, "lead/y", "y");

    GcsListPage<GcsItemInfo> page =
        fakeGcsClient.listObjectsPage(id(bucket, "/lead/"), GcsListOptions.LIST_DIRECTORY);

    assertThat(itemNames(page)).containsExactly("/lead/x");
  }

  @TestFactory
  Stream<DynamicTest> listObjectsPage_advancedGlobSyntax_matchesPerGcsRules() {
    String bucket = "adv-glob-bucket";
    return Stream.of(
        globCase(
            bucket + "-charclass",
            "g/part-[0-9].parquet",
            new String[] {"g/part-1.parquet", "g/part-x.parquet", "g/part-/.parquet"},
            "g/part-1.parquet"),
        globCase(
            bucket + "-negclass",
            "g/part-[!0-9].parquet",
            new String[] {"g/part-1.parquet", "g/part-x.parquet", "g/part-/.parquet"},
            "g/part-x.parquet"),
        globCase(
            bucket + "-range-excludes-slash",
            "g/[.-0]",
            new String[] {"g/.", "g//", "g/0"},
            "g/.",
            "g/0"),
        globCase(
            bucket + "-braces",
            "g/*.{parquet,orc}",
            new String[] {"g/part-1.parquet", "g/part-2.orc", "g/part-3.csv"},
            "g/part-1.parquet",
            "g/part-2.orc"),
        globCase(
            bucket + "-escaped-comma-in-braces",
            "g/{a\\,b,c\\*}.parquet",
            new String[] {"g/a,b.parquet", "g/a.parquet", "g/c*.parquet", "g/cx.parquet"},
            "g/a,b.parquet",
            "g/c*.parquet"),
        globCase(
            bucket + "-escaped-literals",
            "g/file\\?\\[1\\]\\*\\{a\\,b\\}.parquet",
            new String[] {"g/file?[1]*{a,b}.parquet", "g/fileA[1]B{a,b}.parquet"},
            "g/file?[1]*{a,b}.parquet"),
        globCase(
            bucket + "-escaped-double-star",
            "g/\\**/file.txt",
            new String[] {"g/*/file.txt", "g/**/file.txt", "g/a/b/file.txt"},
            "g/**/file.txt",
            "g/*/file.txt"));
  }

  private DynamicTest globCase(
      String bucket, String globPattern, String[] objects, String... expected) {
    return dynamicTest(
        globPattern,
        () -> {
          for (String obj : objects) {
            putBlob(bucket, obj, "x");
          }
          GcsListOptions opts =
              GcsListOptions.LIST_RECURSIVE.toBuilder().setMatchGlob(globPattern).build();

          GcsListPage<GcsItemInfo> page = fakeGcsClient.listObjectsPage(id(bucket, ""), opts);

          assertThat(itemNames(page)).containsExactly((Object[]) expected).inOrder();
        });
  }

  @TestFactory
  Stream<DynamicTest> listObjectsPage_invalidGlobSyntax_throwsIOException() {
    String bucket = "invalid-glob-bucket";
    putBlob(bucket, "a", "1");
    return Stream.of(
            "g/[]",
            "g/[!]",
            "g/[unclosed",
            "g/[a\\",
            "g/[a[b]",
            "g/unmatched]",
            "g/{a,b",
            "g/a}",
            "g/{a/b,c}",
            "g/{a\\/b,c}",
            "g/{a/**/b,c}",
            "g/{a**b,c}",
            "g/[z-a]")
        .map(
            badGlob ->
                dynamicTest(
                    badGlob,
                    () ->
                        assertThrows(
                            IOException.class,
                            () ->
                                fakeGcsClient.listObjectsPage(
                                    id(bucket, ""),
                                    GcsListOptions.LIST_RECURSIVE.toBuilder()
                                        .setMatchGlob(badGlob)
                                        .build()))));
  }

  @Test
  void listObjectsPage_placeholderDirectoryItemIdWithGeneration_ignoresGenerationAndListsChildren()
      throws IOException {
    String bucket = "fake-versioned-dir-bucket";
    StorageTestUtils.createBlobInStorage(
        FakeGcsClientImpl.storage, BlobId.of(bucket, "dir/", 42L), "");
    putBlob(bucket, "dir/child.txt", "abc");
    GcsItemInfo dirInfo = fakeGcsClient.getGcsItemInfo(id(bucket, "dir/"));
    assertThat(dirInfo.getItemId().getContentGeneration()).hasValue(42L);

    GcsListPage<GcsItemInfo> page =
        fakeGcsClient.listObjectsPage(dirInfo.getItemId(), GcsListOptions.LIST_DIRECTORY);

    assertThat(page.getPrefixObject()).isPresent();
    assertThat(itemNames(page)).containsExactly("dir/child.txt");
  }

  @Test
  void listFirstObjectWithPrefix_nestedObjectWithoutPlaceholder_returnsFirstNestedObject()
      throws IOException {
    String bucket = "fake-first-nested-bucket";
    putBlob(bucket, "b/c/obj", "nested");
    putBlob(bucket, "b/d/other", "other");
    GcsItemId prefixId = id(bucket, "b/");

    assertThat(
            fakeGcsClient
                .listFirstObjectWithPrefix(prefixId)
                .flatMap(i -> i.getItemId().getObjectName()))
        .hasValue("b/c/obj");
  }

  @Test
  void listFirstObjectWithPrefix_placeholderAndNestedObjectExist_returnsPlaceholderObject()
      throws IOException {
    String bucket = "fake-first-placeholder-bucket";
    putBlob(bucket, "b/c/obj", "nested");
    putBlob(bucket, "b/", "");
    GcsItemId prefixId = id(bucket, "b/");

    assertThat(
            fakeGcsClient
                .listFirstObjectWithPrefix(prefixId)
                .flatMap(i -> i.getItemId().getObjectName()))
        .hasValue("b/");
  }

  @TestFactory
  Stream<DynamicTest> listObjectsPage_invalidArguments_throwsExpectedException() {
    GcsItemId valid = id("token-bucket", "p/");
    GcsListOptions dir = GcsListOptions.LIST_DIRECTORY;
    GcsListOptions badPrefixTok = dir.toBuilder().setPageToken("not-a-cursor").build();
    GcsListOptions badBase64Tok = dir.toBuilder().setPageToken("name:!!!").build();
    GcsListOptions emptyCursorTok = dir.toBuilder().setPageToken("name:").build();
    GcsListOptions badUtf8Tok = dir.toBuilder().setPageToken("name:_w").build();
    return Stream.of(
        expectThrows("nullId", NullPointerException.class, null, dir),
        expectThrows("nullOpts", NullPointerException.class, valid, null),
        expectThrows("root", IllegalArgumentException.class, GcsItemId.ROOT, dir),
        expectThrows("emptyBucket", IllegalArgumentException.class, id("", "p/"), dir),
        expectThrows("badPrefixToken", IOException.class, valid, badPrefixTok),
        expectThrows("badBase64Token", IOException.class, valid, badBase64Tok),
        expectThrows("emptyCursorToken", IOException.class, valid, emptyCursorTok),
        expectThrows("badUtf8Token", IOException.class, valid, badUtf8Tok));
  }

  private static void createNumberedBlobs(String bucket, String prefix, int count) {
    for (int i = 0; i < count; i++) {
      putBlob(bucket, String.format("%sobj-%04d", prefix, i), "");
    }
  }

  private DynamicTest expectThrows(
      String name, Class<? extends Throwable> type, GcsItemId prefixId, GcsListOptions opts) {
    return dynamicTest(
        name, () -> assertThrows(type, () -> fakeGcsClient.listObjectsPage(prefixId, opts)));
  }

  private static Stream<String> itemNames(GcsListPage<GcsItemInfo> page) {
    return page.getItems().stream().map(i -> i.getItemId().getObjectName().orElse(""));
  }

  private static GcsItemId id(String bucket, String object) {
    return GcsItemId.builder().setBucketName(bucket).setObjectName(object).build();
  }

  private static void putBlob(String bucket, String name, String content) {
    StorageTestUtils.createBlobInStorage(
        FakeGcsClientImpl.storage, BlobId.of(bucket, name), content);
  }
}
