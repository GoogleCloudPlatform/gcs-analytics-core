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
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
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
    GcsListOptions empty =
        rec.toBuilder().setStartOffset("").setEndOffset("").setMatchGlob("").build();
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
        recursiveCase("utf8-empty-filters", empty, unfiltered),
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
            prefixId, opts.toBuilder().setPageToken(first.getNextPageToken()).build());

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
    int totalObjects = GcsListOptions.DEFAULT_MAX_LIST_ITEMS_PER_CALL + 2;
    for (int i = 0; i < totalObjects; i++) {
      putBlob(bucket, String.format("cap/obj-%04d", i), "");
    }
    GcsItemId prefixId = id(bucket, "cap/");

    GcsListPage<GcsItemInfo> first =
        fakeGcsClient.listObjectsPage(prefixId, GcsListOptions.LIST_DIRECTORY);
    GcsListPage<GcsItemInfo> second =
        fakeGcsClient.listObjectsPage(
            prefixId,
            GcsListOptions.LIST_DIRECTORY.toBuilder()
                .setPageToken(first.getNextPageToken())
                .build());

    assertThat(first.getItems()).hasSize(GcsListOptions.DEFAULT_MAX_LIST_ITEMS_PER_CALL);
    assertThat(first.getNextPageToken()).hasValue("5000");
    assertThat(second.getItems()).hasSize(2);
    assertThat(second.hasNextPage()).isFalse();
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
  void listFirstObjectWithPrefix_nestedVsPlaceholder_returnsExpectedFirstObject()
      throws IOException {
    String bucket = "fake-first-object-bucket";
    putBlob(bucket, "b/c/obj", "nested");
    GcsItemId prefixId = id(bucket, "b/");

    assertThat(
            fakeGcsClient
                .listFirstObjectWithPrefix(prefixId)
                .flatMap(i -> i.getItemId().getObjectName()))
        .hasValue("b/c/obj");

    putBlob(bucket, "b/", "");
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
    GcsListOptions badTok = dir.toBuilder().setPageToken("not-an-int").build();
    GcsListOptions negTok = dir.toBuilder().setPageToken("-1").build();
    return Stream.of(
        expectThrows("nullId", NullPointerException.class, null, dir),
        expectThrows("nullOpts", NullPointerException.class, valid, null),
        expectThrows("root", IllegalArgumentException.class, GcsItemId.ROOT, dir),
        expectThrows("emptyBucket", IllegalArgumentException.class, id("", "p/"), dir),
        expectThrows("badToken", IOException.class, valid, badTok),
        expectThrows("negToken", IOException.class, valid, negTok));
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
