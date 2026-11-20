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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.FailedPreconditionException;
import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.UnimplementedException;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.StorageClass;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.storage.control.v2.CreateFolderRequest;
import com.google.storage.control.v2.Folder;
import com.google.storage.control.v2.GetFolderRequest;
import com.google.storage.control.v2.ListFoldersRequest;
import com.google.storage.control.v2.RenameFolderRequest;
import com.google.storage.control.v2.StorageControlClient;
import com.google.storage.control.v2.StorageControlClient.ListFoldersPagedResponse;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
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

  @Test
  void createGetAndDeleteFolder_roundTripsHermeticallyInMemory() throws IOException {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName("hns-fake-bucket").setObjectName("a/b/c").build();

    fakeGcsClient.createFolder(folderId, true);

    GcsItemInfo folderInfo = fakeGcsClient.getFolderInfo(folderId);
    assertThat(folderInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.NATIVE_FOLDER);
    assertThat(folderInfo.getItemId())
        .isEqualTo(
            GcsItemId.builder().setBucketName("hns-fake-bucket").setObjectName("a/b/c/").build());
    assertThat(folderInfo.getMetaGeneration()).isEqualTo(1L);

    fakeGcsClient.deleteFolder(folderId);

    assertThrows(FileNotFoundException.class, () -> fakeGcsClient.getFolderInfo(folderId));
  }

  @Test
  void createFolder_duplicateFolder_throwsFileAlreadyExistsException() throws IOException {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName("hns-fake-bucket").setObjectName("dup/").build();
    fakeGcsClient.createFolder(folderId, false);

    assertThrows(
        FileAlreadyExistsException.class, () -> fakeGcsClient.createFolder(folderId, false));
  }

  @Test
  void createFolder_nonRecursiveWithoutParent_throwsIOExceptionWithFailedPrecondition() {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName("hns-fake-bucket").setObjectName("missing/child").build();

    IOException e =
        assertThrows(IOException.class, () -> fakeGcsClient.createFolder(folderId, false));

    assertThat(e).hasCauseThat().isInstanceOf(FailedPreconditionException.class);
  }

  @Test
  void deleteFolder_missingFolder_returnsNormally() {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName("hns-fake-bucket").setObjectName("missing/").build();

    assertDoesNotThrow(() -> fakeGcsClient.deleteFolder(folderId));
  }

  @Test
  void deleteFolder_nonEmptyChildFolder_throwsDirectoryNotEmptyExceptionWithFailedPrecondition()
      throws IOException {
    GcsItemId parentId =
        GcsItemId.builder().setBucketName("hns-fake-bucket").setObjectName("parent/").build();
    GcsItemId childId =
        GcsItemId.builder().setBucketName("hns-fake-bucket").setObjectName("parent/child/").build();
    fakeGcsClient.createFolder(childId, true);

    DirectoryNotEmptyException e =
        assertThrows(DirectoryNotEmptyException.class, () -> fakeGcsClient.deleteFolder(parentId));

    assertThat(e.getFile()).isEqualTo("gs://hns-fake-bucket/parent/");
    assertThat(e).hasCauseThat().isInstanceOf(FailedPreconditionException.class);
  }

  @Test
  void deleteFolder_nonEmptyChildObject_throwsDirectoryNotEmptyExceptionWithFailedPrecondition()
      throws IOException {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName("hns-object-bucket").setObjectName("dir/").build();
    GcsItemId objectId =
        GcsItemId.builder()
            .setBucketName("hns-object-bucket")
            .setObjectName("dir/child.parquet")
            .build();
    fakeGcsClient.createFolder(folderId, false);
    TestDataGenerator.createGcsData(objectId, 16);

    try {
      DirectoryNotEmptyException e =
          assertThrows(
              DirectoryNotEmptyException.class, () -> fakeGcsClient.deleteFolder(folderId));

      assertThat(e.getFile()).isEqualTo("gs://hns-object-bucket/dir/");
      assertThat(e).hasCauseThat().isInstanceOf(FailedPreconditionException.class);
    } finally {
      // FakeGcsClientImpl.storage is shared across the JVM; don't leak the object into other tests.
      FakeGcsClientImpl.storage.delete(
          BlobId.of(objectId.getBucketName(), objectId.getObjectName().get()));
    }
  }

  @Test
  void deleteFolder_leadingSpaceFolder_deletesOnlyThatFolder() throws IOException {
    GcsItemId spaceFolderId =
        GcsItemId.builder().setBucketName("hns-space-bucket").setObjectName(" dir/").build();
    GcsItemId plainFolderId =
        GcsItemId.builder().setBucketName("hns-space-bucket").setObjectName("dir/").build();
    fakeGcsClient.createFolder(spaceFolderId, false);
    fakeGcsClient.createFolder(plainFolderId, false);

    fakeGcsClient.deleteFolder(spaceFolderId);

    assertThat(folderExists("hns-space-bucket", " dir/")).isFalse();
    assertThat(folderExists("hns-space-bucket", "dir/")).isTrue();
  }

  @Test
  void deleteFolder_childUnderLeadingSpaceFolder_blocksOnlyThatFolder() throws IOException {
    GcsItemId spaceFolderId =
        GcsItemId.builder().setBucketName("hns-space-object-bucket").setObjectName(" dir/").build();
    GcsItemId plainFolderId =
        GcsItemId.builder().setBucketName("hns-space-object-bucket").setObjectName("dir/").build();
    GcsItemId childObjectId =
        GcsItemId.builder()
            .setBucketName("hns-space-object-bucket")
            .setObjectName(" dir/child.parquet")
            .build();
    fakeGcsClient.createFolder(spaceFolderId, false);
    fakeGcsClient.createFolder(plainFolderId, false);
    TestDataGenerator.createGcsData(childObjectId, 16);

    try {
      assertThrows(
          DirectoryNotEmptyException.class, () -> fakeGcsClient.deleteFolder(spaceFolderId));
      assertDoesNotThrow(() -> fakeGcsClient.deleteFolder(plainFolderId));
    } finally {
      // FakeGcsClientImpl.storage is shared across the JVM; don't leak the object into other tests.
      FakeGcsClientImpl.storage.delete(
          BlobId.of(childObjectId.getBucketName(), childObjectId.getObjectName().get()));
    }
  }

  @Test
  void deleteFolder_childObjectUnderLeadingSlashFolder_throwsDirectoryNotEmptyException()
      throws IOException {
    String bucketName = "hns-leading-slash-bucket";
    GcsItemId folderId =
        GcsItemId.builder().setBucketName(bucketName).setObjectName("/dir/").build();
    fakeGcsClient.createFolder(folderId, true);
    putBlob(bucketName, "/dir/child.parquet", "data");

    try {
      assertThrows(DirectoryNotEmptyException.class, () -> fakeGcsClient.deleteFolder(folderId));

      assertThat(folderExists(bucketName, "/dir/")).isTrue();
    } finally {
      // FakeGcsClientImpl.storage is shared across the JVM; don't leak the object into other tests.
      FakeGcsClientImpl.storage.delete(BlobId.of(bucketName, "/dir/child.parquet"));
    }
  }

  @Test
  void deleteFolder_emptySegmentFolder_deletesThatFolderNotParent() throws IOException {
    String bucketName = "hns-empty-segment-bucket";
    fakeGcsClient
        .lazyGetStorageControlClient()
        .createFolder(
            CreateFolderRequest.newBuilder()
                .setParent("projects/_/buckets/" + bucketName)
                .setFolderId("x//")
                .setRecursive(true)
                .build());
    GcsItemId emptySegmentFolderId =
        GcsItemId.builder().setBucketName(bucketName).setObjectName("x//").build();

    fakeGcsClient.deleteFolder(emptySegmentFolderId);

    assertThat(folderExists(bucketName, "x//")).isFalse();
    assertThat(folderExists(bucketName, "x/")).isTrue();
  }

  @Test
  void getFolder_emptyFolderName_throwsInvalidArgumentException() throws IOException {
    StorageControlClient controlClient = fakeGcsClient.lazyGetStorageControlClient();
    GetFolderRequest request =
        GetFolderRequest.newBuilder()
            .setName("projects/_/buckets/hns-fake-bucket/folders/")
            .build();

    assertThrows(InvalidArgumentException.class, () -> controlClient.getFolder(request));
  }

  @Test
  void deleteFolder_afterClose_throwsStorageControlClientClosedException() {
    fakeGcsClient.close();
    GcsItemId folderId =
        GcsItemId.builder().setBucketName("hns-fake-bucket").setObjectName("dir/").build();

    IOException e = assertThrows(IOException.class, () -> fakeGcsClient.deleteFolder(folderId));

    assertThat(e).hasMessageThat().isEqualTo("StorageControlClient is closed");
  }

  @Test
  void listFolders_prefix_returnsPrefixFolderAndDescendantsInUtf8Order() throws IOException {
    String bucketName = "hns-list-bucket";
    // U+FB01 sorts before U+1F600 by UTF-8 bytes, but after it by UTF-16 code units.
    String[] createdOutOfOrder = {
      "dir/b/", "dir/\uD83D\uDE00/", "dir/a/x/", "dir/B/", "dir/\uFB01/", "dir/a b/", "dir2/"
    };
    for (String folder : createdOutOfOrder) {
      fakeGcsClient.createFolder(id(bucketName, folder), true);
    }

    List<GcsItemInfo> folders = fakeGcsClient.listFolders(id(bucketName, "dir"));

    assertThat(folderNames(folders))
        .containsExactly(
            "dir/",
            "dir/B/",
            "dir/a b/",
            "dir/a/",
            "dir/a/x/",
            "dir/b/",
            "dir/\uFB01/",
            "dir/\uD83D\uDE00/")
        .inOrder();
  }

  @Test
  void listFolders_bucket_returnsOnlyFoldersOfThatBucket() throws IOException {
    fakeGcsClient.createFolder(id("hns-list-all-bucket", "b/"), false);
    fakeGcsClient.createFolder(id("hns-list-all-bucket", "a/"), false);
    fakeGcsClient.createFolder(id("hns-list-all-bucket2", "c/"), false);
    GcsItemId bucketId = GcsItemId.builder().setBucketName("hns-list-all-bucket").build();

    List<GcsItemInfo> folders = fakeGcsClient.listFolders(bucketId);

    assertThat(folderNames(folders)).containsExactly("a/", "b/").inOrder();
  }

  @Test
  void listFolders_noMatchingFolder_returnsEmptyList() throws IOException {
    fakeGcsClient.createFolder(id("hns-list-empty-bucket", "other/"), false);

    List<GcsItemInfo> folders = fakeGcsClient.listFolders(id("hns-list-empty-bucket", "missing/"));

    assertThat(folders).isEmpty();
  }

  @Test
  void listFolders_moreFoldersThanOnePage_returnsAllFolders() throws IOException {
    String bucketName = "hns-list-two-pages-bucket";
    createNumberedFolders(bucketName, "big/", 1000);

    List<GcsItemInfo> folders = fakeGcsClient.listFolders(id(bucketName, "big/"));

    assertThat(folders).hasSize(1001);
  }

  @Test
  void listFolders_pageSizeAboveLimit_returnsOneThousandFoldersPerPage() throws IOException {
    String bucketName = "hns-list-limit-bucket";
    createNumberedFolders(bucketName, "big/", 1000);
    ListFoldersRequest request =
        ListFoldersRequest.newBuilder()
            .setParent("projects/_/buckets/" + bucketName)
            .setPrefix("big/")
            .setPageSize(5000)
            .build();

    ListFoldersPagedResponse response =
        fakeGcsClient.lazyGetStorageControlClient().listFolders(request);

    assertThat(response.getPage().getValues()).hasSize(1000);
    assertThat(response.getNextPageToken()).isNotEmpty();
  }

  @Test
  void listFolders_pageSizeSmallerThanResult_pagesThroughAllFoldersInOrder() throws IOException {
    String bucketName = "hns-list-paging-bucket";
    for (String folder : new String[] {"p/c/", "p/a/", "p/d/", "p/b/"}) {
      fakeGcsClient.createFolder(id(bucketName, folder), true);
    }
    ListFoldersRequest request =
        ListFoldersRequest.newBuilder()
            .setParent("projects/_/buckets/" + bucketName)
            .setPrefix("p/")
            .setPageSize(2)
            .build();

    ListFoldersPagedResponse response =
        fakeGcsClient.lazyGetStorageControlClient().listFolders(request);

    assertThat(folderPaths(response.getPage().getValues())).containsExactly("p/", "p/a/").inOrder();
    assertThat(folderPaths(response.iterateAll()))
        .containsExactly("p/", "p/a/", "p/b/", "p/c/", "p/d/")
        .inOrder();
  }

  @TestFactory
  Stream<DynamicTest> listFolders_invalidRequest_throwsExpectedException() {
    ListFoldersRequest valid =
        ListFoldersRequest.newBuilder()
            .setParent("projects/_/buckets/hns-list-invalid-bucket")
            .build();
    return Stream.of(
        listFoldersThrows(
            "parentWithoutBucketPrefix",
            InvalidArgumentException.class,
            valid.toBuilder().setParent("hns-list-invalid-bucket").build()),
        listFoldersThrows(
            "prefixWithoutTrailingSlash",
            InvalidArgumentException.class,
            valid.toBuilder().setPrefix("dir").build()),
        listFoldersThrows(
            "negativePageSize",
            InvalidArgumentException.class,
            valid.toBuilder().setPageSize(-1).build()),
        listFoldersThrows(
            "invalidPageToken",
            InvalidArgumentException.class,
            valid.toBuilder().setPageToken("garbage").build()),
        listFoldersThrows(
            "delimiter", UnimplementedException.class, valid.toBuilder().setDelimiter("/").build()),
        listFoldersThrows(
            "lexicographicStart",
            UnimplementedException.class,
            valid.toBuilder().setLexicographicStart("a/").build()),
        listFoldersThrows(
            "lexicographicEnd",
            UnimplementedException.class,
            valid.toBuilder().setLexicographicEnd("b/").build()));
  }

  private DynamicTest listFoldersThrows(
      String name, Class<? extends Throwable> type, ListFoldersRequest request) {
    return dynamicTest(
        name,
        () -> {
          StorageControlClient controlClient = fakeGcsClient.lazyGetStorageControlClient();

          assertThrows(type, () -> controlClient.listFolders(request));
        });
  }

  /** Creates {@code parent} and {@code count} folders directly below it. */
  private void createNumberedFolders(String bucketName, String parent, int count)
      throws IOException {
    fakeGcsClient.createFolder(id(bucketName, parent), false);
    for (int i = 0; i < count; i++) {
      fakeGcsClient.createFolder(id(bucketName, String.format("%sf%04d/", parent, i)), false);
    }
  }

  private static List<String> folderNames(List<GcsItemInfo> folders) {
    List<String> names = new ArrayList<>();
    for (GcsItemInfo folder : folders) {
      names.add(folder.getItemId().getObjectName().orElse(""));
    }
    return names;
  }

  /** Returns the folder paths, the part of each resource name after {@code /folders/}. */
  private static List<String> folderPaths(Iterable<Folder> folders) {
    List<String> paths = new ArrayList<>();
    for (Folder folder : folders) {
      String name = folder.getName();
      paths.add(name.substring(name.indexOf("/folders/") + "/folders/".length()));
    }
    return paths;
  }

  /** Looks a folder up by its exact name, bypassing GcsClient name handling. */
  private boolean folderExists(String bucketName, String folderPath) throws IOException {
    try {
      fakeGcsClient
          .lazyGetStorageControlClient()
          .getFolder(
              GetFolderRequest.newBuilder()
                  .setName("projects/_/buckets/" + bucketName + "/folders/" + folderPath)
                  .build());
      return true;
    } catch (NotFoundException e) {
      return false;
    }
  }

  @Test
  void renameFolder_folderWithSubFoldersAndObjects_movesEverythingBelowIt() throws IOException {
    String bucketName = "hns-rename-bucket";
    fakeGcsClient.createFolder(id(bucketName, "src/sub/deep/"), true);
    putBlob(bucketName, "src/a.txt", "alpha");
    putBlob(bucketName, "src/sub/b.txt", "beta");

    try {
      fakeGcsClient.renameFolder(id(bucketName, "src"), id(bucketName, "dst"));

      GcsItemId bucketId = GcsItemId.builder().setBucketName(bucketName).build();
      assertThat(folderNames(fakeGcsClient.listFolders(bucketId)))
          .containsExactly("dst/", "dst/sub/", "dst/sub/deep/")
          .inOrder();
      assertThat(objectContents(bucketName))
          .containsExactly("dst/a.txt", "alpha", "dst/sub/b.txt", "beta");
    } finally {
      // FakeGcsClientImpl.storage is shared across the JVM; don't leak the objects into other
      // tests.
      deleteObjects(bucketName);
    }
  }

  @Test
  void renameFolder_leadingSpaceFolder_renamesOnlyThatFolder() throws IOException {
    String bucketName = "hns-rename-space-bucket";
    fakeGcsClient.createFolder(id(bucketName, " dir/"), false);
    fakeGcsClient.createFolder(id(bucketName, "dir/"), false);

    fakeGcsClient.renameFolder(id(bucketName, " dir/"), id(bucketName, "moved/"));

    GcsItemId bucketId = GcsItemId.builder().setBucketName(bucketName).build();
    assertThat(folderNames(fakeGcsClient.listFolders(bucketId))).containsExactly("dir/", "moved/");
  }

  @Test
  void renameFolder_renamedFolder_keepsCreationTimeAndMetageneration() throws IOException {
    String bucketName = "hns-rename-metadata-bucket";
    fakeGcsClient.createFolder(id(bucketName, "src/"), false);
    GcsItemInfo before = fakeGcsClient.getFolderInfo(id(bucketName, "src/"));

    fakeGcsClient.renameFolder(id(bucketName, "src/"), id(bucketName, "dst/"));

    GcsItemInfo after = fakeGcsClient.getFolderInfo(id(bucketName, "dst/"));
    assertThat(after.getCreationTime()).isEqualTo(before.getCreationTime());
    assertThat(after.getMetaGeneration()).isEqualTo(before.getMetaGeneration());
    assertThat(after.getModificationTime()).isAtLeast(before.getModificationTime());
  }

  @Test
  void renameFolder_objectNamedLikeDestination_renamesFolderAndKeepsObject() throws IOException {
    String bucketName = "hns-rename-object-name-bucket";
    fakeGcsClient.createFolder(id(bucketName, "src/"), false);
    putBlob(bucketName, "dst", "data");

    try {
      fakeGcsClient.renameFolder(id(bucketName, "src/"), id(bucketName, "dst/"));

      assertThat(folderExists(bucketName, "dst/")).isTrue();
      assertThat(objectContents(bucketName)).containsExactly("dst", "data");
    } finally {
      // FakeGcsClientImpl.storage is shared across the JVM; don't leak the object into other tests.
      deleteObjects(bucketName);
    }
  }

  @Test
  void renameFolder_missingSource_throwsFileNotFoundException() {
    String bucketName = "hns-rename-missing-bucket";

    FileNotFoundException e =
        assertThrows(
            FileNotFoundException.class,
            () -> fakeGcsClient.renameFolder(id(bucketName, "missing/"), id(bucketName, "dst/")));

    assertThat(e).hasMessageThat().contains("gs://hns-rename-missing-bucket/missing/");
  }

  @Test
  void renameFolder_existingDestination_throwsFileAlreadyExistsExceptionAndKeepsBothFolders()
      throws IOException {
    String bucketName = "hns-rename-exists-bucket";
    fakeGcsClient.createFolder(id(bucketName, "src/"), false);
    fakeGcsClient.createFolder(id(bucketName, "dst/"), false);

    FileAlreadyExistsException e =
        assertThrows(
            FileAlreadyExistsException.class,
            () -> fakeGcsClient.renameFolder(id(bucketName, "src/"), id(bucketName, "dst/")));

    assertThat(e.getFile()).isEqualTo("gs://hns-rename-exists-bucket/dst/");
    assertThat(folderExists(bucketName, "src/")).isTrue();
    assertThat(folderExists(bucketName, "dst/")).isTrue();
  }

  @Test
  void renameFolder_missingDestinationParent_throwsIOExceptionAndKeepsSource() throws IOException {
    String bucketName = "hns-rename-no-parent-bucket";
    fakeGcsClient.createFolder(id(bucketName, "src/"), false);

    IOException e =
        assertThrows(
            IOException.class,
            () ->
                fakeGcsClient.renameFolder(id(bucketName, "src/"), id(bucketName, "missing/dst/")));

    assertThat(e).hasCauseThat().isInstanceOf(FailedPreconditionException.class);
    assertThat(folderExists(bucketName, "src/")).isTrue();
  }

  @TestFactory
  Stream<DynamicTest> renameFolder_invalidRequest_failsWithExpectedException() throws IOException {
    String bucketName = "hns-rename-invalid-bucket";
    fakeGcsClient.createFolder(id(bucketName, "existing/"), false);
    fakeGcsClient.createFolder(id(bucketName, "other/"), false);
    return Stream.of(
        renameFolderFails(
            "sameSourceAndDestination",
            InvalidArgumentException.class,
            renameFolderRequest(bucketName, "missing/", "missing/")),
        renameFolderFails(
            "destinationInsideMissingSource",
            InvalidArgumentException.class,
            renameFolderRequest(bucketName, "missing/", "missing/sub/")),
        renameFolderFails(
            "emptyDestination",
            InvalidArgumentException.class,
            renameFolderRequest(bucketName, "existing/", "")),
        renameFolderFails(
            "missingSourceAndExistingDestination",
            NotFoundException.class,
            renameFolderRequest(bucketName, "missing/", "existing/")),
        renameFolderFails(
            "missingSourceAndMissingDestinationParent",
            NotFoundException.class,
            renameFolderRequest(bucketName, "missing/", "absent/dst/")),
        renameFolderFails(
            "existingDestination",
            AlreadyExistsException.class,
            renameFolderRequest(bucketName, "existing/", "other/")),
        renameFolderFails(
            "missingDestinationParent",
            FailedPreconditionException.class,
            renameFolderRequest(bucketName, "existing/", "absent/dst/")));
  }

  /** Sends {@code request} to the fake Storage Control service, bypassing GcsClient checks. */
  private DynamicTest renameFolderFails(
      String name, Class<? extends Throwable> type, RenameFolderRequest request) {
    return dynamicTest(
        name,
        () -> {
          StorageControlClient controlClient = fakeGcsClient.lazyGetStorageControlClient();

          ExecutionException e =
              assertThrows(
                  ExecutionException.class, () -> controlClient.renameFolderAsync(request).get());

          assertThat(e).hasCauseThat().isInstanceOf(type);
        });
  }

  private static RenameFolderRequest renameFolderRequest(
      String bucketName, String srcFolderPath, String dstFolderId) {
    return RenameFolderRequest.newBuilder()
        .setName("projects/_/buckets/" + bucketName + "/folders/" + srcFolderPath)
        .setDestinationFolderId(dstFolderId)
        .build();
  }

  /** Returns the content of every object in the bucket, by object name. */
  private static Map<String, String> objectContents(String bucketName) {
    Map<String, String> contents = new HashMap<>();
    for (Blob blob : FakeGcsClientImpl.storage.list(bucketName).iterateAll()) {
      byte[] content =
          FakeGcsClientImpl.storage.readAllBytes(BlobId.of(bucketName, blob.getName()));
      contents.put(blob.getName(), new String(content, StandardCharsets.UTF_8));
    }
    return contents;
  }

  private static void deleteObjects(String bucketName) {
    for (Blob blob : FakeGcsClientImpl.storage.list(bucketName).iterateAll()) {
      FakeGcsClientImpl.storage.delete(BlobId.of(bucketName, blob.getName()));
    }
  }

  @Test
  void moveObject_existingSource_movesObjectToDestination() throws IOException {
    GcsItemId srcId = moveObjectId("move-src");
    GcsItemId dstId = moveObjectId("move-dst");
    createMoveObjectBlob("move-src");

    fakeGcsClient.moveObject(srcId, dstId);

    assertThrows(FileNotFoundException.class, () -> fakeGcsClient.getGcsItemInfo(srcId));
    GcsItemInfo dstInfo = fakeGcsClient.getGcsItemInfo(dstId);
    assertThat(dstInfo.getItemId().getObjectName()).hasValue("move-dst");
    assertThat(dstInfo.getSize()).isEqualTo(10L);
  }

  @Test
  void moveObject_missingSource_throwsFileNotFoundException() {
    FileNotFoundException e =
        assertThrows(
            FileNotFoundException.class,
            () -> fakeGcsClient.moveObject(moveObjectId("missing-src"), moveObjectId("move-dst")));

    assertThat(e).hasMessageThat().contains("gs://move-bucket/missing-src");
  }

  @Test
  void moveObject_staleSourceGeneration_throwsIOExceptionAndKeepsSource() throws IOException {
    GcsItemId srcId = moveObjectId("stale-src");
    createMoveObjectBlob("stale-src");
    long generation = fakeGcsClient.getGcsItemInfo(srcId).getContentGeneration().get();
    GcsItemId staleSrcId =
        GcsItemId.builder()
            .setBucketName("move-bucket")
            .setObjectName("stale-src")
            .setContentGeneration(generation + 1)
            .build();

    IOException e =
        assertThrows(
            IOException.class,
            () -> fakeGcsClient.moveObject(staleSrcId, moveObjectId("stale-dst")));

    assertThat(e.getClass()).isEqualTo(IOException.class);
    assertThat(fakeGcsClient.getGcsItemInfo(srcId).getSize()).isEqualTo(10L);
  }

  @Test
  void moveObject_existingDestinationWithZeroGeneration_throwsFileAlreadyExistsAndKeepsBoth()
      throws IOException {
    GcsItemId srcId = moveObjectId("taken-src");
    GcsItemId dstId = moveObjectId("taken-dst");
    createMoveObjectBlob("taken-src");
    StorageTestUtils.createBlobInStorage(
        FakeGcsClientImpl.storage, BlobId.of("move-bucket", "taken-dst", 1L), "dst");
    GcsItemId mustNotExistDstId =
        GcsItemId.builder()
            .setBucketName("move-bucket")
            .setObjectName("taken-dst")
            .setContentGeneration(0L)
            .build();

    FileAlreadyExistsException e =
        assertThrows(
            FileAlreadyExistsException.class,
            () -> fakeGcsClient.moveObject(srcId, mustNotExistDstId));

    assertThat(e.getFile()).isEqualTo("gs://move-bucket/taken-dst");
    assertThat(fakeGcsClient.getGcsItemInfo(srcId).getSize()).isEqualTo(10L);
    assertThat(fakeGcsClient.getGcsItemInfo(dstId).getSize()).isEqualTo(3L);
  }

  @Test
  void moveObject_zeroDestinationGenerationAndMissingDestination_movesObject() throws IOException {
    GcsItemId srcId = moveObjectId("zero-dst-gen-src");
    GcsItemId dstId = moveObjectId("zero-dst-gen-dst");
    createMoveObjectBlob("zero-dst-gen-src");

    fakeGcsClient.moveObject(srcId, moveObjectId("zero-dst-gen-dst", 0L));

    assertThrows(FileNotFoundException.class, () -> fakeGcsClient.getGcsItemInfo(srcId));
    assertThat(fakeGcsClient.getGcsItemInfo(dstId).getSize()).isEqualTo(10L);
  }

  @Test
  void
      moveObject_positiveDestinationGenerationAndMissingDestination_throwsIOExceptionAndKeepsSource()
          throws IOException {
    GcsItemId srcId = moveObjectId("missing-dst-gen-src");
    GcsItemId dstId = moveObjectId("missing-dst-gen-dst");
    createMoveObjectBlob("missing-dst-gen-src");

    IOException e =
        assertThrows(
            IOException.class,
            () -> fakeGcsClient.moveObject(srcId, moveObjectId("missing-dst-gen-dst", 5L)));

    assertThat(e.getClass()).isEqualTo(IOException.class);
    assertThat(fakeGcsClient.getGcsItemInfo(srcId).getSize()).isEqualTo(10L);
    assertThrows(FileNotFoundException.class, () -> fakeGcsClient.getGcsItemInfo(dstId));
  }

  @Test
  void moveObject_staleDestinationGeneration_throwsIOExceptionAndKeepsBoth() throws IOException {
    GcsItemId srcId = moveObjectId("stale-dst-gen-src");
    GcsItemId dstId = moveObjectId("stale-dst-gen-dst");
    createMoveObjectBlob("stale-dst-gen-src");
    createMoveObjectBlob("stale-dst-gen-dst", "dst");
    long generation = fakeGcsClient.getGcsItemInfo(dstId).getContentGeneration().get();

    IOException e =
        assertThrows(
            IOException.class,
            () ->
                fakeGcsClient.moveObject(srcId, moveObjectId("stale-dst-gen-dst", generation + 1)));

    assertThat(e.getClass()).isEqualTo(IOException.class);
    assertThat(fakeGcsClient.getGcsItemInfo(srcId).getSize()).isEqualTo(10L);
    assertThat(fakeGcsClient.getGcsItemInfo(dstId).getSize()).isEqualTo(3L);
  }

  @Test
  void moveObject_matchingDestinationGeneration_replacesDestination() throws IOException {
    GcsItemId srcId = moveObjectId("matching-dst-gen-src");
    GcsItemId dstId = moveObjectId("matching-dst-gen-dst");
    createMoveObjectBlob("matching-dst-gen-src");
    createMoveObjectBlob("matching-dst-gen-dst", "dst");
    long generation = fakeGcsClient.getGcsItemInfo(dstId).getContentGeneration().get();

    fakeGcsClient.moveObject(srcId, moveObjectId("matching-dst-gen-dst", generation));

    assertThrows(FileNotFoundException.class, () -> fakeGcsClient.getGcsItemInfo(srcId));
    assertThat(fakeGcsClient.getGcsItemInfo(dstId).getSize()).isEqualTo(10L);
  }

  @Test
  void moveObject_matchingSourceGeneration_movesObject() throws IOException {
    GcsItemId srcId = moveObjectId("matching-src-gen-src");
    GcsItemId dstId = moveObjectId("matching-src-gen-dst");
    createMoveObjectBlob("matching-src-gen-src");
    long generation = fakeGcsClient.getGcsItemInfo(srcId).getContentGeneration().get();

    fakeGcsClient.moveObject(moveObjectId("matching-src-gen-src", generation), dstId);

    assertThrows(FileNotFoundException.class, () -> fakeGcsClient.getGcsItemInfo(srcId));
    assertThat(fakeGcsClient.getGcsItemInfo(dstId).getSize()).isEqualTo(10L);
  }

  @Test
  void moveObject_noDestinationGenerationAndExistingDestination_replacesDestination()
      throws IOException {
    GcsItemId srcId = moveObjectId("overwrite-src");
    GcsItemId dstId = moveObjectId("overwrite-dst");
    createMoveObjectBlob("overwrite-src");
    createMoveObjectBlob("overwrite-dst", "dst");

    fakeGcsClient.moveObject(srcId, dstId);

    assertThrows(FileNotFoundException.class, () -> fakeGcsClient.getGcsItemInfo(srcId));
    assertThat(fakeGcsClient.getGcsItemInfo(dstId).getSize()).isEqualTo(10L);
  }

  private static GcsItemId moveObjectId(String objectName) {
    return GcsItemId.builder().setBucketName("move-bucket").setObjectName(objectName).build();
  }

  private static GcsItemId moveObjectId(String objectName, long generation) {
    return GcsItemId.builder()
        .setBucketName("move-bucket")
        .setObjectName(objectName)
        .setContentGeneration(generation)
        .build();
  }

  private static void createMoveObjectBlob(String objectName) {
    createMoveObjectBlob(objectName, "0123456789");
  }

  private static void createMoveObjectBlob(String objectName, String content) {
    StorageTestUtils.createBlobInStorage(
        FakeGcsClientImpl.storage, BlobId.of("move-bucket", objectName, 1L), content);
  }

  @Test
  void deleteObject_existingObject_deletesObject() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("delete-bucket").setObjectName("delete-me").build();
    StorageTestUtils.createBlobInStorage(FakeGcsClientImpl.storage, itemId, 1L, "0123456789");

    fakeGcsClient.deleteObject(itemId);

    assertThrows(FileNotFoundException.class, () -> fakeGcsClient.getGcsItemInfo(itemId));
  }

  @Test
  void deleteObject_missingObject_returnsNormally() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("delete-bucket").setObjectName("missing").build();

    assertDoesNotThrow(() -> fakeGcsClient.deleteObject(itemId));
  }

  @Test
  void deleteObject_missingObjectWithGeneration_returnsNormally() {
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName("delete-bucket")
            .setObjectName("missing-with-generation")
            .setContentGeneration(1L)
            .build();

    assertDoesNotThrow(() -> fakeGcsClient.deleteObject(itemId));
  }

  @Test
  void deleteObject_matchingGeneration_deletesObject() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("delete-bucket").setObjectName("versioned").build();
    StorageTestUtils.createBlobInStorage(FakeGcsClientImpl.storage, itemId, 1L, "0123456789");
    long generation = fakeGcsClient.getGcsItemInfo(itemId).getContentGeneration().get();
    GcsItemId versionedId =
        GcsItemId.builder()
            .setBucketName("delete-bucket")
            .setObjectName("versioned")
            .setContentGeneration(generation)
            .build();

    fakeGcsClient.deleteObject(versionedId);

    assertThrows(FileNotFoundException.class, () -> fakeGcsClient.getGcsItemInfo(itemId));
  }

  @Test
  void deleteObject_staleGeneration_throwsIOExceptionAndKeepsObject() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("delete-bucket").setObjectName("stale").build();
    StorageTestUtils.createBlobInStorage(FakeGcsClientImpl.storage, itemId, 1L, "0123456789");
    long generation = fakeGcsClient.getGcsItemInfo(itemId).getContentGeneration().get();
    GcsItemId staleId =
        GcsItemId.builder()
            .setBucketName("delete-bucket")
            .setObjectName("stale")
            .setContentGeneration(generation + 1)
            .build();

    IOException e = assertThrows(IOException.class, () -> fakeGcsClient.deleteObject(staleId));

    assertThat(e).hasMessageThat().contains("Generation mismatch");
    assertThat(fakeGcsClient.getGcsItemInfo(itemId).getSize()).isEqualTo(10L);
  }

  @Test
  void deleteBucket_emptyBucket_succeeds() throws IOException {
    GcsItemId bucketId = GcsItemId.builder().setBucketName("empty-bucket-to-delete").build();

    fakeGcsClient.deleteBucket(bucketId);
  }

  @Test
  void deleteBucket_nonEmptyBucket_throwsDirectoryNotEmptyException() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("non-empty-bucket").setObjectName("object").build();
    StorageTestUtils.createBlobInStorage(FakeGcsClientImpl.storage, itemId, 1L, "0123456789");
    GcsItemId bucketId = GcsItemId.builder().setBucketName("non-empty-bucket").build();

    DirectoryNotEmptyException e =
        assertThrows(DirectoryNotEmptyException.class, () -> fakeGcsClient.deleteBucket(bucketId));

    assertThat(e.getFile()).isEqualTo("gs://non-empty-bucket");
  }
}
