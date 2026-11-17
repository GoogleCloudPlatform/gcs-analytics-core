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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DirectoryListingTest {

  private static final String TEST_BUCKET = "test-bucket";
  private static final GcsItemId DIR_ID = itemId("dir/");
  private static final GcsListOptions OPTIONS = GcsListOptions.LIST_DIRECTORY;

  private GcsClient mockClient;

  @BeforeEach
  void setUp() {
    mockClient = mock(GcsClient.class);
  }

  @Test
  void list_objectsAndPrefixes_returnsObjectsThenInferredDirectories() throws IOException {
    GcsItemInfo file = objectInfo("dir/file.txt");
    GcsItemInfo childPlaceholder = placeholderInfo("dir/sub/");
    GcsItemId implicitDir = itemId("dir/implicit/");
    when(mockClient.listObjectsPage(DIR_ID, OPTIONS))
        .thenReturn(page(List.of(file, childPlaceholder), List.of(implicitDir), null));

    DirectoryListing listing = DirectoryListing.list(mockClient, DIR_ID, OPTIONS);

    assertThat(listing.getChildren())
        .containsExactly(file, childPlaceholder, GcsItemInfo.createInferredDirectory(implicitDir))
        .inOrder();
  }

  @Test
  void list_idWithoutTrailingSlash_listsDirectoryPrefix() throws IOException {
    GcsItemInfo file = objectInfo("dir/file.txt");
    when(mockClient.listObjectsPage(DIR_ID, OPTIONS)).thenReturn(page(List.of(file), null));

    DirectoryListing listing = DirectoryListing.list(mockClient, itemId("dir"), OPTIONS);

    assertThat(listing.getChildren()).containsExactly(file);
  }

  @Test
  void list_bucketId_listsBucketTopLevel() throws IOException {
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    GcsItemInfo file = objectInfo("file.txt");
    when(mockClient.listObjectsPage(bucketId, OPTIONS)).thenReturn(page(List.of(file), null));

    DirectoryListing listing = DirectoryListing.list(mockClient, bucketId, OPTIONS);

    assertThat(listing.getChildren()).containsExactly(file);
  }

  @Test
  void list_multiplePages_returnsChildrenOfEveryPage() throws IOException {
    GcsItemInfo first = objectInfo("dir/a");
    GcsItemInfo second = objectInfo("dir/b");
    GcsItemInfo third = objectInfo("dir/c");
    when(mockClient.listObjectsPage(DIR_ID, OPTIONS)).thenReturn(page(List.of(first), "token-1"));
    when(mockClient.listObjectsPage(DIR_ID, withPageToken("token-1")))
        .thenReturn(page(List.of(second), "token-2"));
    when(mockClient.listObjectsPage(DIR_ID, withPageToken("token-2")))
        .thenReturn(page(List.of(third), null));

    DirectoryListing listing = DirectoryListing.list(mockClient, DIR_ID, OPTIONS);

    assertThat(listing.getChildren()).containsExactly(first, second, third).inOrder();
  }

  @Test
  void list_multiplePages_requestsEachPageOnce() throws IOException {
    when(mockClient.listObjectsPage(DIR_ID, OPTIONS))
        .thenReturn(page(List.of(objectInfo("dir/a")), "token-1"));
    when(mockClient.listObjectsPage(DIR_ID, withPageToken("token-1")))
        .thenReturn(page(List.of(objectInfo("dir/b")), "token-2"));
    when(mockClient.listObjectsPage(DIR_ID, withPageToken("token-2")))
        .thenReturn(page(List.of(objectInfo("dir/c")), null));

    DirectoryListing.list(mockClient, DIR_ID, OPTIONS);

    verify(mockClient).listObjectsPage(DIR_ID, OPTIONS);
    verify(mockClient).listObjectsPage(DIR_ID, withPageToken("token-1"));
    verify(mockClient).listObjectsPage(DIR_ID, withPageToken("token-2"));
    verify(mockClient, times(3)).listObjectsPage(any(), any());
  }

  @Test
  void list_multiplePagesWithHnsOptions_keepsOptionsOnLaterRequests() throws IOException {
    GcsListOptions hnsOptions = GcsListOptions.LIST_DIRECTORY_HNS;
    GcsListOptions hnsOptionsWithToken = hnsOptions.toBuilder().setPageToken("token-1").build();
    GcsItemInfo second = objectInfo("dir/b");
    when(mockClient.listObjectsPage(DIR_ID, hnsOptions))
        .thenReturn(page(List.of(objectInfo("dir/a")), "token-1"));
    when(mockClient.listObjectsPage(DIR_ID, hnsOptionsWithToken))
        .thenReturn(page(List.of(second), null));

    DirectoryListing listing = DirectoryListing.list(mockClient, DIR_ID, hnsOptions);

    assertThat(listing.getChildren()).contains(second);
  }

  @Test
  void list_emptyPageWithNextPageToken_keepsListing() throws IOException {
    GcsItemInfo file = objectInfo("dir/file.txt");
    when(mockClient.listObjectsPage(DIR_ID, OPTIONS)).thenReturn(page(List.of(), "token-1"));
    when(mockClient.listObjectsPage(DIR_ID, withPageToken("token-1")))
        .thenReturn(page(List.of(file), null));

    DirectoryListing listing = DirectoryListing.list(mockClient, DIR_ID, OPTIONS);

    assertThat(listing.getChildren()).containsExactly(file);
  }

  @Test
  void list_prefixListedBeforeItsPlaceholderObject_keepsOnlyTheObject() throws IOException {
    GcsItemInfo childPlaceholder = placeholderInfo("dir/sub/");
    when(mockClient.listObjectsPage(DIR_ID, OPTIONS))
        .thenReturn(page(List.of(), List.of(itemId("dir/sub/")), "token-1"));
    when(mockClient.listObjectsPage(DIR_ID, withPageToken("token-1")))
        .thenReturn(page(List.of(childPlaceholder), null));

    DirectoryListing listing = DirectoryListing.list(mockClient, DIR_ID, OPTIONS);

    assertThat(listing.getChildren()).containsExactly(childPlaceholder);
  }

  @Test
  void list_prefixListedAfterItsPlaceholderObject_keepsOnlyTheObject() throws IOException {
    GcsItemInfo childPlaceholder = placeholderInfo("dir/sub/");
    when(mockClient.listObjectsPage(DIR_ID, OPTIONS))
        .thenReturn(page(List.of(childPlaceholder), "token-1"));
    when(mockClient.listObjectsPage(DIR_ID, withPageToken("token-1")))
        .thenReturn(page(List.of(), List.of(itemId("dir/sub/")), null));

    DirectoryListing listing = DirectoryListing.list(mockClient, DIR_ID, OPTIONS);

    assertThat(listing.getChildren()).containsExactly(childPlaceholder);
  }

  @Test
  void list_samePrefixOnTwoPages_returnsOneInferredDirectory() throws IOException {
    GcsItemId implicitDir = itemId("dir/implicit/");
    when(mockClient.listObjectsPage(DIR_ID, OPTIONS))
        .thenReturn(page(List.of(), List.of(implicitDir), "token-1"));
    when(mockClient.listObjectsPage(DIR_ID, withPageToken("token-1")))
        .thenReturn(page(List.of(), List.of(implicitDir), null));

    DirectoryListing listing = DirectoryListing.list(mockClient, DIR_ID, OPTIONS);

    assertThat(listing.getChildren())
        .containsExactly(GcsItemInfo.createInferredDirectory(implicitDir));
  }

  @Test
  void list_childListed_reportsDirectoryFound() throws IOException {
    when(mockClient.listObjectsPage(DIR_ID, OPTIONS))
        .thenReturn(page(List.of(objectInfo("dir/file.txt")), null));

    DirectoryListing listing = DirectoryListing.list(mockClient, DIR_ID, OPTIONS);

    assertThat(listing.isDirectoryFound()).isTrue();
  }

  @Test
  void list_onlyOwnPlaceholderListed_reportsDirectoryFound() throws IOException {
    when(mockClient.listObjectsPage(DIR_ID, OPTIONS))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder().setPrefixObject(placeholderInfo("dir/")).build());

    DirectoryListing listing = DirectoryListing.list(mockClient, DIR_ID, OPTIONS);

    assertThat(listing.isDirectoryFound()).isTrue();
  }

  @Test
  void list_onlyOwnPlaceholderListed_returnsNoChildren() throws IOException {
    when(mockClient.listObjectsPage(DIR_ID, OPTIONS))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder().setPrefixObject(placeholderInfo("dir/")).build());

    DirectoryListing listing = DirectoryListing.list(mockClient, DIR_ID, OPTIONS);

    assertThat(listing.getChildren()).isEmpty();
  }

  @Test
  void list_ownPlaceholderOnEarlierPageOnly_reportsDirectoryFound() throws IOException {
    when(mockClient.listObjectsPage(DIR_ID, OPTIONS))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixObject(placeholderInfo("dir/"))
                .setNextPageToken("token-1")
                .build());
    when(mockClient.listObjectsPage(DIR_ID, withPageToken("token-1")))
        .thenReturn(page(List.of(), null));

    DirectoryListing listing = DirectoryListing.list(mockClient, DIR_ID, OPTIONS);

    assertThat(listing.isDirectoryFound()).isTrue();
  }

  @Test
  void list_nothingListed_reportsDirectoryNotFound() throws IOException {
    when(mockClient.listObjectsPage(DIR_ID, OPTIONS)).thenReturn(page(List.of(), null));

    DirectoryListing listing = DirectoryListing.list(mockClient, DIR_ID, OPTIONS);

    assertThat(listing.isDirectoryFound()).isFalse();
  }

  @Test
  void list_children_areUnmodifiable() throws IOException {
    when(mockClient.listObjectsPage(DIR_ID, OPTIONS))
        .thenReturn(page(List.of(objectInfo("dir/file.txt")), null));

    DirectoryListing listing = DirectoryListing.list(mockClient, DIR_ID, OPTIONS);

    assertThrows(UnsupportedOperationException.class, () -> listing.getChildren().clear());
  }

  @Test
  void list_laterPageFails_propagatesException() throws IOException {
    AccessDeniedException denied = new AccessDeniedException("gs://test-bucket/dir/");
    when(mockClient.listObjectsPage(DIR_ID, OPTIONS))
        .thenReturn(page(List.of(objectInfo("dir/a")), "token-1"));
    when(mockClient.listObjectsPage(DIR_ID, withPageToken("token-1"))).thenThrow(denied);

    AccessDeniedException thrown =
        assertThrows(
            AccessDeniedException.class, () -> DirectoryListing.list(mockClient, DIR_ID, OPTIONS));

    assertThat(thrown).isSameInstanceAs(denied);
  }

  @Test
  void list_interruptedBeforeFirstPage_throwsWithoutListing() throws IOException {
    Thread.currentThread().interrupt();
    try {
      assertThrows(
          InterruptedIOException.class, () -> DirectoryListing.list(mockClient, DIR_ID, OPTIONS));
    } finally {
      // Clear the flag so that it does not leak into other tests.
      Thread.interrupted();
    }

    verify(mockClient, never()).listObjectsPage(any(), any());
  }

  @Test
  void list_interruptedBetweenPages_stopsListing() throws IOException {
    when(mockClient.listObjectsPage(DIR_ID, OPTIONS))
        .thenAnswer(
            invocation -> {
              Thread.currentThread().interrupt();
              return page(List.of(objectInfo("dir/a")), "token-1");
            });

    try {
      assertThrows(
          InterruptedIOException.class, () -> DirectoryListing.list(mockClient, DIR_ID, OPTIONS));
    } finally {
      // Clear the flag so that it does not leak into other tests.
      Thread.interrupted();
    }

    verify(mockClient, never()).listObjectsPage(DIR_ID, withPageToken("token-1"));
  }

  @Test
  void list_interrupted_keepsInterruptFlagSet() {
    Thread.currentThread().interrupt();
    boolean interruptedAfterList;
    try {
      assertThrows(
          InterruptedIOException.class, () -> DirectoryListing.list(mockClient, DIR_ID, OPTIONS));
    } finally {
      // Read and clear the flag so that it does not leak into other tests.
      interruptedAfterList = Thread.interrupted();
    }

    assertThat(interruptedAfterList).isTrue();
  }

  @Test
  void list_optionsIncludePrefixObject_throwsIllegalArgumentException() {
    GcsListOptions includePrefixObject = OPTIONS.toBuilder().setIncludePrefixObject(true).build();

    assertThrows(
        IllegalArgumentException.class,
        () -> DirectoryListing.list(mockClient, DIR_ID, includePrefixObject));
  }

  @Test
  void list_nullClient_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> DirectoryListing.list(null, DIR_ID, OPTIONS));
  }

  @Test
  void list_nullId_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class, () -> DirectoryListing.list(mockClient, null, OPTIONS));
  }

  @Test
  void list_nullOptions_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> DirectoryListing.list(mockClient, DIR_ID, null));
  }

  @Test
  void list_fakeClientWithSmallPages_returnsEachChildOnce() throws IOException {
    String bucket = "directory-listing-" + UUID.randomUUID();
    for (String name :
        List.of(
            "dir/",
            "dir/a.txt",
            "dir/b.txt",
            "dir/c.txt",
            "dir/implicit/nested.txt",
            "dir/sub/",
            "dir/sub/nested.txt",
            "dir/z.txt",
            "dir-sibling/file.txt",
            "other.txt")) {
      putBlob(bucket, name);
    }
    GcsListOptions twoPerPage = OPTIONS.toBuilder().setPageSize(2).build();
    GcsItemId dirId = GcsItemId.builder().setBucketName(bucket).setObjectName("dir/").build();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    GcsClient fakeClient = newFakeClient(executor);
    try {
      DirectoryListing listing = DirectoryListing.list(fakeClient, dirId, twoPerPage);

      assertThat(objectNames(listing.getChildren()))
          .containsExactly(
              "dir/a.txt", "dir/b.txt", "dir/c.txt", "dir/implicit/", "dir/sub/", "dir/z.txt");
    } finally {
      fakeClient.close();
      executor.shutdownNow();
    }
  }

  @Test
  void list_fakeClientWithSmallPages_marksOnlyPrefixesAsInferredDirectories() throws IOException {
    String bucket = "directory-listing-" + UUID.randomUUID();
    for (String name :
        List.of("dir/", "dir/a.txt", "dir/implicit/nested.txt", "dir/sub/", "dir/sub/nested.txt")) {
      putBlob(bucket, name);
    }
    GcsListOptions onePerPage = OPTIONS.toBuilder().setPageSize(1).build();
    GcsItemId dirId = GcsItemId.builder().setBucketName(bucket).setObjectName("dir/").build();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    GcsClient fakeClient = newFakeClient(executor);
    try {
      DirectoryListing listing = DirectoryListing.list(fakeClient, dirId, onePerPage);

      List<GcsItemInfo> inferred =
          listing.getChildren().stream()
              .filter(child -> child.getItemType() == GcsItemInfo.ItemType.INFERRED_DIRECTORY)
              .collect(Collectors.toList());
      assertThat(objectNames(inferred)).containsExactly("dir/implicit/");
    } finally {
      fakeClient.close();
      executor.shutdownNow();
    }
  }

  @Test
  void listFromFirstPage_nullFirstPage_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () -> DirectoryListing.listFromFirstPage(mockClient, itemId("dir/"), OPTIONS, null));
  }

  @Test
  void listFirstPage_thenListFromFirstPage_collectsAllPages() throws IOException {
    GcsItemId dirId = itemId("dir/");
    GcsItemInfo first = objectInfo("dir/a");
    GcsItemInfo second = objectInfo("dir/b");
    when(mockClient.listObjectsPage(dirId, OPTIONS)).thenReturn(page(List.of(first), "token-1"));
    when(mockClient.listObjectsPage(dirId, withPageToken("token-1")))
        .thenReturn(page(List.of(second), null));

    GcsListPage<GcsItemInfo> firstPage = DirectoryListing.listFirstPage(mockClient, dirId, OPTIONS);
    DirectoryListing listing =
        DirectoryListing.listFromFirstPage(mockClient, dirId, OPTIONS, firstPage);

    assertThat(listing.getChildren()).containsExactly(first, second).inOrder();
    assertThat(listing.isDirectoryFound()).isTrue();
  }

  private static GcsClient newFakeClient(ExecutorService executor) {
    GcsFileSystemOptions options = GcsFileSystemOptions.builder().build();
    return new FakeGcsClientImpl(
        options.getGcsClientOptions(),
        Suppliers.ofInstance(executor),
        new Telemetry(ImmutableList.of()),
        new AnalyticsCacheManager(options.getGcsCacheOptions()).getBucketPropertiesCache());
  }

  private static void putBlob(String bucket, String name) {
    FakeGcsClientImpl.storage.create(
        BlobInfo.newBuilder(BlobId.of(bucket, name)).build(),
        name.endsWith("/") ? new byte[0] : "data".getBytes(StandardCharsets.UTF_8));
  }

  private static List<String> objectNames(List<GcsItemInfo> itemInfos) {
    return itemInfos.stream()
        .map(itemInfo -> itemInfo.getItemId().getObjectName().orElse(""))
        .collect(Collectors.toList());
  }

  private static GcsListOptions withPageToken(String pageToken) {
    return OPTIONS.toBuilder().setPageToken(pageToken).build();
  }

  private static GcsListPage<GcsItemInfo> page(List<GcsItemInfo> items, String nextPageToken) {
    return page(items, List.of(), nextPageToken);
  }

  private static GcsListPage<GcsItemInfo> page(
      List<GcsItemInfo> items, List<GcsItemId> prefixes, String nextPageToken) {
    GcsListPage.Builder<GcsItemInfo> builder =
        GcsListPage.<GcsItemInfo>builder().setItems(items).setPrefixes(prefixes);
    if (nextPageToken != null) {
      builder.setNextPageToken(nextPageToken);
    }
    return builder.build();
  }

  private static GcsItemId itemId(String objectName) {
    return GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(objectName).build();
  }

  private static GcsItemInfo objectInfo(String objectName) {
    return GcsItemInfo.builder()
        .setItemId(itemId(objectName))
        .setSize(100L)
        .setContentGeneration(1L)
        .build();
  }

  private static GcsItemInfo placeholderInfo(String objectName) {
    return GcsItemInfo.builder()
        .setItemId(itemId(objectName))
        .setSize(0L)
        .setContentGeneration(1L)
        .setItemType(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY)
        .build();
  }
}
