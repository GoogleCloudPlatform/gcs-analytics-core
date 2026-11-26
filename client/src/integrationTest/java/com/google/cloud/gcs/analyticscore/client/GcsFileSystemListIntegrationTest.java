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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.CharMatcher;
import com.google.storage.control.v2.DeleteFolderRequest;
import com.google.storage.control.v2.StorageControlClient;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GcsFileSystemListIntegrationTest {

  private static final String GCS_INTEGRATION_TEST_BUCKET_PROPERTY = "gcs.integration.test.bucket";
  private static final String GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY =
      "gcs.integration.test.hns.bucket";
  private static final String GCS_INTEGRATION_TEST_PROJECT_PROPERTY =
      "gcs.integration.test.project-id";
  private static final byte[] TEST_FILE_CONTENT =
      "test file content".getBytes(StandardCharsets.UTF_8);

  private static final Logger LOG = LoggerFactory.getLogger(GcsFileSystemListIntegrationTest.class);

  private Storage storage;
  private List<BlobId> blobsToDelete;
  private List<String> bucketsToDelete;
  private List<String> foldersToDelete;
  private GcsFileSystemImpl gcsFileSystem;

  @BeforeEach
  void setUp() {
    storage = StorageOptions.getDefaultInstance().getService();
    blobsToDelete = new ArrayList<>();
    bucketsToDelete = new ArrayList<>();
    foldersToDelete = new ArrayList<>();
    gcsFileSystem = createFileSystem(GcsClientOptions.builder().build());
  }

  @AfterEach
  void tearDown() {
    try {
      if (gcsFileSystem != null) {
        try {
          gcsFileSystem.close();
        } catch (Exception e) {
          LOG.warn("Failed to close gcsFileSystem during cleanup", e);
        }
      }
      if (storage != null) {
        for (BlobId blobId : blobsToDelete) {
          try {
            storage.delete(blobId);
          } catch (Exception e) {
            LOG.warn("Failed to delete blob {} during cleanup", blobId, e);
          }
        }
      }
      if (!foldersToDelete.isEmpty()) {
        List<String> sortedFolders = new ArrayList<>(foldersToDelete);
        sortedFolders.sort(
            Comparator.comparingInt(
                    (String folder) ->
                        CharMatcher.is('/').countIn(UriUtil.removeTrailingSlash(folder)))
                .thenComparingInt(String::length)
                .reversed());
        try (StorageControlClient client = StorageControlClient.create()) {
          for (String folderResourceName : sortedFolders) {
            try {
              client.deleteFolder(
                  DeleteFolderRequest.newBuilder().setName(folderResourceName).build());
            } catch (Exception e) {
              LOG.warn("Failed to delete folder {} during cleanup", folderResourceName, e);
            }
          }
        } catch (Exception e) {
          LOG.warn("Failed to close StorageControlClient during cleanup", e);
        }
      }
      if (storage != null) {
        for (String bucketName : bucketsToDelete) {
          try {
            storage.delete(bucketName);
          } catch (Exception e) {
            LOG.warn("Failed to delete bucket {} during cleanup", bucketName, e);
          }
        }
      }
    } finally {
      blobsToDelete.clear();
      foldersToDelete.clear();
      bucketsToDelete.clear();
    }
  }

  @ParameterizedTest(name = "trailingSlash={0}")
  @ValueSource(booleans = {true, false})
  @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
  void listFileInfo_flatDirectory_returnsImmediateChildren(boolean trailingSlash)
      throws IOException {
    String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
    String dir = newListTestDirectory();
    createObject(bucketName, dir + "/");
    createObject(bucketName, dir + "/file.txt");
    createObject(bucketName, dir + "/implicit/nested.txt");
    createObject(bucketName, dir + "/placeholder/");
    createObject(bucketName, dir + "/placeholder/nested.txt");
    URI dirUri = URI.create("gs://" + bucketName + "/" + dir + "/");

    List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(toInputUri(dirUri, trailingSlash));

    assertThat(describeListed(listed))
        .containsExactly(
            dirUri + "file.txt OBJECT",
            dirUri + "implicit/ INFERRED_DIRECTORY",
            dirUri + "placeholder/ PLACEHOLDER_DIRECTORY")
        .inOrder();
  }

  @Test
  @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
  void listFileInfo_flatImplicitDirectory_returnsChildren() throws IOException {
    String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
    String dir = newListTestDirectory();
    createObject(bucketName, dir + "/b.txt");
    createObject(bucketName, dir + "/aa.txt");
    URI dirUri = URI.create("gs://" + bucketName + "/" + dir + "/");

    List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(dirUri);

    assertThat(describeListed(listed))
        .containsExactly(dirUri + "b.txt OBJECT", dirUri + "aa.txt OBJECT")
        .inOrder();
  }

  @Test
  @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
  void listFileInfo_flatChildrenNeedingPercentEncoding_returnUrisThatResolveToTheSameObjects()
      throws IOException {
    String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
    String dir = newListTestDirectory();
    createObject(bucketName, dir + "/a b.txt");
    createObject(bucketName, dir + "/a%20b.txt");
    createObject(bucketName, dir + "/a#b?c.txt");
    URI dirUri = URI.create("gs://" + bucketName + "/" + dir + "/");

    List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(dirUri);

    assertThat(describeListed(listed))
        .containsExactly(
            dirUri + "a%20b.txt OBJECT",
            dirUri + "a%2520b.txt OBJECT",
            dirUri + "a%23b%3Fc.txt OBJECT")
        .inOrder();
    for (GcsFileInfo child : listed) {
      GcsFileInfo resolved = gcsFileSystem.getFileInfo(child.getUri());
      assertThat(resolved.getItemInfo().getItemId()).isEqualTo(child.getItemInfo().getItemId());
    }
  }

  @Test
  @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
  void listFileInfo_flatListedObject_hasObjectMetadata() throws IOException {
    String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
    String dir = newListTestDirectory();
    createObject(bucketName, dir + "/file.txt");
    URI dirUri = URI.create("gs://" + bucketName + "/" + dir + "/");

    List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(dirUri);

    GcsItemInfo listedObject = listed.get(0).getItemInfo();
    assertThat(listedObject.getSize()).isEqualTo((long) TEST_FILE_CONTENT.length);
    assertThat(listedObject.getContentGeneration()).isPresent();
    assertThat(listedObject.getModificationTime()).isGreaterThan(0L);
  }

  @Test
  @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
  void listFileInfo_flatFile_returnsOnlyThatObject() throws IOException {
    String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
    String dir = newListTestDirectory();
    createObject(bucketName, dir + "/file.txt");
    createObject(bucketName, dir + "/other.txt");
    URI fileUri = URI.create("gs://" + bucketName + "/" + dir + "/file.txt");

    List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(fileUri);

    assertThat(describeListed(listed)).containsExactly(fileUri + " OBJECT");
  }

  @Test
  @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
  void listFileInfo_flatFileAndDirectoryWithSameName_returnsOnlyTheFile() throws IOException {
    String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
    String dir = newListTestDirectory();
    createObject(bucketName, dir + "/name");
    createObject(bucketName, dir + "/name/child.txt");
    URI fileUri = URI.create("gs://" + bucketName + "/" + dir + "/name");

    List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(fileUri);

    assertThat(describeListed(listed)).containsExactly(fileUri + " OBJECT");
  }

  @ParameterizedTest(name = "trailingSlash={0}")
  @ValueSource(booleans = {true, false})
  @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
  void listFileInfo_flatEmptyPlaceholderDirectory_returnsEmptyList(boolean trailingSlash)
      throws IOException {
    String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
    String dir = newListTestDirectory();
    createObject(bucketName, dir + "/");
    URI dirUri = URI.create("gs://" + bucketName + "/" + dir + "/");

    List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(toInputUri(dirUri, trailingSlash));

    assertThat(listed).isEmpty();
  }

  @ParameterizedTest(name = "trailingSlash={0}")
  @ValueSource(booleans = {true, false})
  @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
  void listFileInfo_flatNonExistentPath_throwsFileNotFoundException(boolean trailingSlash) {
    String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
    URI dirUri = URI.create("gs://" + bucketName + "/" + newListTestDirectory() + "/");
    URI inputUri = toInputUri(dirUri, trailingSlash);

    assertThrows(FileNotFoundException.class, () -> gcsFileSystem.listFileInfo(inputUri));
  }

  @Test
  @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_PROJECT_PROPERTY, matches = ".+")
  void listFileInfo_flatBucket_returnsTopLevelDirectory() throws IOException {
    String bucketName = "test-list-flat-bucket-" + UUID.randomUUID();
    bucketsToDelete.add(bucketName);
    gcsFileSystem.getGcsClient().createBucket(bucketName);
    String dir = newListTestDirectory();
    createObject(bucketName, dir + "/file.txt");
    URI bucketUri = URI.create("gs://" + bucketName);

    List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(bucketUri);

    assertThat(describeListed(listed))
        .containsExactly("gs://" + bucketName + "/" + dir + "/ INFERRED_DIRECTORY");
  }

  @Test
  void listFileInfo_nonExistentBucket_throwsFileNotFoundException() {
    URI nonExistentBucketUri = URI.create("gs://non-existent-bucket-" + UUID.randomUUID());

    assertThrows(
        FileNotFoundException.class, () -> gcsFileSystem.listFileInfo(nonExistentBucketUri));
  }

  @Test
  @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
  void list_flatDirectoryInSmallPages_returnsEachChildOnce() throws IOException {
    String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
    String dir = newListTestDirectory();
    createObject(bucketName, dir + "/");
    createObject(bucketName, dir + "/a.txt");
    createObject(bucketName, dir + "/b.txt");
    createObject(bucketName, dir + "/c.txt");
    createObject(bucketName, dir + "/implicit/nested.txt");
    createObject(bucketName, dir + "/placeholder/");
    createObject(bucketName, dir + "/placeholder/nested.txt");
    createObject(bucketName, dir + "/z.txt");
    GcsItemId dirId =
        GcsItemId.builder().setBucketName(bucketName).setObjectName(dir + "/").build();
    GcsListOptions twoPerPage = GcsListOptions.LIST_DIRECTORY.toBuilder().setPageSize(2).build();

    DirectoryListing listing =
        DirectoryListing.list(gcsFileSystem.getGcsClient(), dirId, twoPerPage);

    assertThat(listedNames(listing, dir))
        .containsExactly("a.txt", "b.txt", "c.txt", "implicit/", "placeholder/", "z.txt");
  }

  @ParameterizedTest(name = "trailingSlash={0}")
  @ValueSource(booleans = {true, false})
  @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
  void listFileInfo_hnsFolder_returnsObjectsAndChildFolders(boolean trailingSlash)
      throws IOException {
    String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
    String dir = newListTestDirectory();
    registerFoldersForCleanup(bucketName, dir, dir + "/sub", dir + "/empty-child");
    createObject(bucketName, dir + "/file.txt");
    createObject(bucketName, dir + "/sub/nested.txt");
    URI dirUri = URI.create("gs://" + bucketName + "/" + dir + "/");
    gcsFileSystem.mkdirs(URI.create(dirUri + "empty-child/"));

    List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(toInputUri(dirUri, trailingSlash));

    assertThat(describeListed(listed))
        .containsExactly(
            dirUri + "sub/ INFERRED_DIRECTORY",
            dirUri + "file.txt OBJECT",
            dirUri + "empty-child/ INFERRED_DIRECTORY")
        .inOrder();
  }

  @ParameterizedTest(name = "trailingSlash={0}")
  @ValueSource(booleans = {true, false})
  @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
  void listFileInfo_hnsEmptyFolder_returnsEmptyList(boolean trailingSlash) throws IOException {
    String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
    String dir = newListTestDirectory();
    registerFoldersForCleanup(bucketName, dir);
    URI dirUri = URI.create("gs://" + bucketName + "/" + dir + "/");
    gcsFileSystem.mkdirs(dirUri);

    List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(toInputUri(dirUri, trailingSlash));

    assertThat(listed).isEmpty();
  }

  @ParameterizedTest(name = "trailingSlash={0}")
  @ValueSource(booleans = {true, false})
  @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
  void listFileInfo_hnsNonExistentPath_throwsFileNotFoundException(boolean trailingSlash) {
    String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
    URI dirUri = URI.create("gs://" + bucketName + "/" + newListTestDirectory() + "/");
    URI inputUri = toInputUri(dirUri, trailingSlash);

    assertThrows(FileNotFoundException.class, () -> gcsFileSystem.listFileInfo(inputUri));
  }

  @Test
  @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
  void listFileInfo_hnsFile_returnsOnlyThatObject() throws IOException {
    String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
    String dir = newListTestDirectory();
    registerFoldersForCleanup(bucketName, dir);
    createObject(bucketName, dir + "/file.txt");
    createObject(bucketName, dir + "/other.txt");
    URI fileUri = URI.create("gs://" + bucketName + "/" + dir + "/file.txt");

    List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(fileUri);

    assertThat(describeListed(listed)).containsExactly(fileUri + " OBJECT");
  }

  @Test
  @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_PROJECT_PROPERTY, matches = ".+")
  void listFileInfo_hnsBucket_returnsTopLevelEmptyFolder() throws IOException {
    String bucketName = "test-list-hns-bucket-" + UUID.randomUUID();
    bucketsToDelete.add(bucketName);
    gcsFileSystem
        .getGcsClient()
        .createBucket(
            bucketName,
            GcsCreateBucketOptions.builder().setHierarchicalNamespaceEnabled(true).build());
    String dir = newListTestDirectory();
    registerFoldersForCleanup(bucketName, dir);
    gcsFileSystem.mkdirs(URI.create("gs://" + bucketName + "/" + dir + "/"));
    URI bucketUri = URI.create("gs://" + bucketName);

    List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(bucketUri);

    assertThat(describeListed(listed))
        .containsExactly("gs://" + bucketName + "/" + dir + "/ INFERRED_DIRECTORY");
  }

  @Test
  @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
  void listFileInfo_hnsBucketListedAsFlat_doesNotReturnEmptyChildFolder() throws IOException {
    String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
    String dir = newListTestDirectory();
    registerFoldersForCleanup(bucketName, dir, dir + "/empty-child");
    createObject(bucketName, dir + "/file.txt");
    URI dirUri = URI.create("gs://" + bucketName + "/" + dir + "/");
    gcsFileSystem.mkdirs(URI.create(dirUri + "empty-child/"));
    GcsFileSystemOptions flatOptions =
        GcsFileSystemOptions.builder().setHnsApiEnabled(false).build();

    try (GcsFileSystemImpl flatFileSystem = new GcsFileSystemImpl(flatOptions)) {
      List<GcsFileInfo> listed = flatFileSystem.listFileInfo(dirUri);

      assertThat(describeListed(listed)).containsExactly(dirUri + "file.txt OBJECT");
    }
  }

  @Test
  @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
  void listFileInfo_hnsBucketListedAsFlat_emptyFolderThrowsFileNotFoundException()
      throws IOException {
    String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
    String dir = newListTestDirectory();
    registerFoldersForCleanup(bucketName, dir);
    URI dirUri = URI.create("gs://" + bucketName + "/" + dir + "/");
    gcsFileSystem.mkdirs(dirUri);
    GcsFileSystemOptions flatOptions =
        GcsFileSystemOptions.builder().setHnsApiEnabled(false).build();

    try (GcsFileSystemImpl flatFileSystem = new GcsFileSystemImpl(flatOptions)) {
      assertThrows(FileNotFoundException.class, () -> flatFileSystem.listFileInfo(dirUri));
    }
  }

  @Test
  @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
  void list_hnsFolderInSmallPages_returnsEachChildOnce() throws IOException {
    String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
    String dir = newListTestDirectory();
    registerFoldersForCleanup(bucketName, dir, dir + "/sub", dir + "/empty-child");
    createObject(bucketName, dir + "/a.txt");
    createObject(bucketName, dir + "/b.txt");
    createObject(bucketName, dir + "/c.txt");
    createObject(bucketName, dir + "/sub/nested.txt");
    createObject(bucketName, dir + "/z.txt");
    gcsFileSystem.mkdirs(URI.create("gs://" + bucketName + "/" + dir + "/empty-child/"));
    GcsItemId dirId =
        GcsItemId.builder().setBucketName(bucketName).setObjectName(dir + "/").build();
    GcsListOptions twoPerPage =
        GcsListOptions.LIST_DIRECTORY_HNS.toBuilder().setPageSize(2).build();

    DirectoryListing listing =
        DirectoryListing.list(gcsFileSystem.getGcsClient(), dirId, twoPerPage);

    assertThat(listedNames(listing, dir))
        .containsExactly("a.txt", "b.txt", "c.txt", "empty-child/", "sub/", "z.txt");
  }

  /** Returns a unique top-level directory name for a listing test. */
  private static String newListTestDirectory() {
    return "list-file-info-it-" + UUID.randomUUID();
  }

  /** Creates an object and registers it for deletion; a name ending in "/" gets no content. */
  private void createObject(String bucketName, String objectName) {
    BlobId blobId = BlobId.of(bucketName, objectName);
    blobsToDelete.add(blobId);
    storage.create(
        BlobInfo.newBuilder(blobId).build(),
        objectName.endsWith("/") ? new byte[0] : TEST_FILE_CONTENT);
  }

  /** Registers HNS folders, which GCS creates along with their objects, for deletion. */
  private void registerFoldersForCleanup(String bucketName, String... folderNames) {
    for (String folderName : folderNames) {
      foldersToDelete.add("projects/_/buckets/" + bucketName + "/folders/" + folderName);
    }
  }

  /** Returns each listed item as its URI followed by its item type. */
  private static List<String> describeListed(List<GcsFileInfo> listed) {
    List<String> described = new ArrayList<>();
    for (GcsFileInfo fileInfo : listed) {
      described.add(fileInfo.getUri() + " " + fileInfo.getItemInfo().getItemType());
    }
    return described;
  }

  /** Returns the names of the listed children, relative to the listed directory. */
  private static List<String> listedNames(DirectoryListing listing, String dir) {
    List<String> names = new ArrayList<>();
    for (GcsItemInfo child : listing.getChildren()) {
      names.add(child.getItemId().getObjectName().orElse("").substring(dir.length() + 1));
    }
    return names;
  }

  /** Returns {@code dirUri} as-is, or with its trailing slash removed. */
  private static URI toInputUri(URI dirUri, boolean trailingSlash) {
    String uri = dirUri.toString();
    return trailingSlash ? dirUri : URI.create(uri.substring(0, uri.length() - 1));
  }

  private GcsFileSystemImpl createFileSystem(GcsClientOptions clientOptions) {
    GcsClientOptions.Builder builder = clientOptions.toBuilder();
    String projectId = System.getProperty(GCS_INTEGRATION_TEST_PROJECT_PROPERTY);
    if (projectId != null && !projectId.isEmpty()) {
      builder.setProjectId(projectId);
    }
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder().setGcsClientOptions(builder.build()).build();
    return new GcsFileSystemImpl(options);
  }
}
