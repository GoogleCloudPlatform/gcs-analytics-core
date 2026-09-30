/*
 * Copyright 2025 Google LLC
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

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableMap;
import com.google.storage.control.v2.DeleteFolderRequest;
import com.google.storage.control.v2.StorageControlClient;
import java.io.FileNotFoundException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
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

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

// TODO: Setup buckets and test data as part of setup on place of relying on existing bucket.
class GcsFileSystemImplIntegrationTest {

    private static final String GCS_INTEGRATION_TEST_BUCKET_PROPERTY = "gcs.integration.test.bucket";
    private static final String GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY =
            "gcs.integration.hns.test.bucket";
    private static final String PUBLIC_BUCKET_NAME = "cloud-samples-data";
    private static final String PUBLIC_PARQUET_OBJECT = "bigquery/us-states/us-states.parquet";
    private static final String PUBLIC_CSV_OBJECT = "bigquery/us-states/us-states.csv";
    private static final String PUBLIC_PARQUET_URI_STRING =
            "gs://" + PUBLIC_BUCKET_NAME + "/" + PUBLIC_PARQUET_OBJECT;
    private static final String PUBLIC_CSV_URI_STRING =
            "gs://" + PUBLIC_BUCKET_NAME + "/" + PUBLIC_CSV_OBJECT;
    private static final String PRIVATE_BUCKET_NAME =
            "gcs-connector-private-test-bucket-do-not-delete";
    private static final String PRIVATE_PARQUET_OBJECT = "tpch_customer_1.parquet";
    private static final String PRIVATE_PARQUET_URI_STRING =
            "gs://" + PRIVATE_BUCKET_NAME + "/" + PRIVATE_PARQUET_OBJECT;
    private static final byte[] TEST_FILE_CONTENT =
            "test file content".getBytes(StandardCharsets.UTF_8);

    private static final Logger LOG = LoggerFactory.getLogger(GcsFileSystemImplIntegrationTest.class);

    private Storage storage;
    private List<BlobId> blobsToDelete;
    private List<String> foldersToDelete;
    private GcsFileSystemImpl gcsFileSystem;

    @BeforeEach
    void setUp() {
        storage = StorageOptions.getDefaultInstance().getService();
        blobsToDelete = new ArrayList<>();
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
            // Ignore all cleanup errors
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
                try (StorageControlClient client = StorageControlClient.create()) {
                    for (String folderResourceName : foldersToDelete) {
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
        } finally {
            blobsToDelete.clear();
            foldersToDelete.clear();
        }
    }

    @Test
    void open_publicObject_canReadContent() throws IOException {
        String gcsObject = PUBLIC_CSV_URI_STRING;
        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(URI.create(gcsObject));
        GcsReadOptions readOptions = GcsReadOptions.builder().build();

        try (VectoredSeekableByteChannel channel = gcsFileSystem.open(fileInfo, readOptions)) {
            assertThat(channel.isOpen()).isTrue();
            assertThat(channel.size()).isGreaterThan(0L);

            ByteBuffer buffer = ByteBuffer.allocate(10);
            int bytesRead = channel.read(buffer);

            assertThat(bytesRead).isEqualTo(10);
            // The first line of us-states.csv is "name,post_abbr"
            assertThat(new String(buffer.array(), StandardCharsets.UTF_8)).isEqualTo("name,post_");
        }
    }

    @Test
    void getFileInfo_noCredentialProvided_urlPointsToPublicObject_success() throws IOException {
        String gcsObject = PUBLIC_PARQUET_URI_STRING;

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(URI.create(gcsObject));

        assertThat(fileInfo.getItemInfo().getItemId().isGcsObject()).isTrue();
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(PUBLIC_PARQUET_OBJECT);
        assertThat(fileInfo.getItemInfo().getItemId().getBucketName()).isEqualTo(PUBLIC_BUCKET_NAME);
        assertThat(fileInfo.getItemInfo().getSize()).isGreaterThan(0L);
        assertThat(fileInfo.getItemInfo().getContentGeneration().isPresent()).isTrue();
        assertThat(fileInfo.getItemInfo().getCreationTime()).isGreaterThan(0L);
        assertThat(fileInfo.getItemInfo().getModificationTime()).isGreaterThan(0L);
        assertThat(fileInfo.getItemInfo().getItemType()).isEqualTo(GcsItemInfo.ItemType.OBJECT);
    }

    @Test
    void getFileInfo_noCredentialProvided_urlPointsToPrivateObject_usesApplicationDefaultCredentials()
            throws IOException {
        String object = PRIVATE_PARQUET_URI_STRING;

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(URI.create(object));

        assertThat(fileInfo.getItemInfo().getItemId().isGcsObject()).isTrue();
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(PRIVATE_PARQUET_OBJECT);
        assertThat(fileInfo.getItemInfo().getItemId().getBucketName()).isEqualTo(PRIVATE_BUCKET_NAME);
    }

    @Test
    void getFileInfo_anonymousCredentialProvided_urlPointsToPublicObject_success()
            throws IOException {
        String gcsObject = PUBLIC_PARQUET_URI_STRING;
        GcsFileSystemOptions options =
                GcsFileSystemOptions.builder()
                        .setGcsClientOptions(GcsClientOptions.builder().build())
                        .build();

        try (GcsFileSystemImpl anonFileSystem =
                new GcsFileSystemImpl(NoCredentials.getInstance(), options)) {
            GcsFileInfo fileInfo = anonFileSystem.getFileInfo(URI.create(gcsObject));

        assertThat(fileInfo.getItemInfo().getItemId().isGcsObject()).isTrue();
            assertThat(fileInfo.getItemInfo().getItemId().getObjectName())
                    .hasValue(PUBLIC_PARQUET_OBJECT);
            assertThat(fileInfo.getItemInfo().getItemId().getBucketName()).isEqualTo(PUBLIC_BUCKET_NAME);
        }
    }

    @Test
    void getFileInfo_anonymousCredentialProvided_urlPointsToPrivateObject_throws() {
        String object = PRIVATE_PARQUET_URI_STRING;
        GcsFileSystemOptions options =
                GcsFileSystemOptions.builder()
                        .setGcsClientOptions(GcsClientOptions.builder().build())
                        .build();

        try (GcsFileSystemImpl anonFileSystem =
                new GcsFileSystemImpl(NoCredentials.getInstance(), options)) {
            AccessDeniedException exception =
                    assertThrows(
                            AccessDeniedException.class, () -> anonFileSystem.getFileInfo(URI.create(object)));

            assertThat(exception).hasMessageThat().contains("Access denied to object during metadata lookup");
        }
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_flatBucket_returnsBucketInfo() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        URI bucketUri = URI.create("gs://" + bucketName);

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(bucketUri);

        assertThat(fileInfo.getItemInfo().getItemType()).isEqualTo(GcsItemInfo.ItemType.BUCKET);
        assertThat(fileInfo.getItemInfo().getItemId().getBucketName()).isEqualTo(bucketName);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).isEmpty();
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);
        assertThat(fileInfo.getUri()).isEqualTo(bucketUri);
    }

    @Test
    void getFileInfo_nonExistentBucket_throwsFileNotFoundException() {
        URI nonExistentBucketUri = URI.create("gs://non-existent-bucket-" + UUID.randomUUID());
        assertThrows(
                FileNotFoundException.class, () -> gcsFileSystem.getFileInfo(nonExistentBucketUri));
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_flatFile_returnsObjectInfoWithAttributes() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        TestWriteContext ctx = new TestWriteContext(bucketName, blobsToDelete);
        byte[] ownerValue = "alice".getBytes(StandardCharsets.UTF_8);
        storage.create(
                BlobInfo.newBuilder(BlobId.of(bucketName, ctx.objectName))
                        .setMetadata(GcsItemInfo.encodeMetadata(ImmutableMap.of("owner", ownerValue)))
                        .build(),
                TEST_FILE_CONTENT);

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(ctx.uri);

        assertThat(fileInfo.getItemInfo().getItemType()).isEqualTo(GcsItemInfo.ItemType.OBJECT);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo((long) TEST_FILE_CONTENT.length);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(ctx.objectName);
        assertThat(fileInfo.getUri()).isEqualTo(ctx.uri);
        assertThat(fileInfo.getAttributes().get("owner")).isEqualTo(ownerValue);
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_flatNonExistentObject_throwsFileNotFoundException() {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        URI nonExistentUri =
                URI.create("gs://" + bucketName + "/non-existent-file-" + UUID.randomUUID() + ".parquet");
        assertThrows(FileNotFoundException.class, () -> gcsFileSystem.getFileInfo(nonExistentUri));
    }

    @ParameterizedTest(name = "trailingSlash={0}")
    @ValueSource(booleans = {true, false})
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_flatImplicitDirectory_returnsInferredDirectory(boolean trailingSlash)
            throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        TestWriteContext ctx = new TestWriteContext(bucketName, blobsToDelete);
        writeObject(ctx.itemId, TEST_FILE_CONTENT);
        URI dirUri = URI.create("gs://" + bucketName + "/" + ctx.folderName + "/");

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(toInputUri(dirUri, trailingSlash));

        assertThat(fileInfo.getItemInfo().getItemType())
                .isEqualTo(GcsItemInfo.ItemType.INFERRED_DIRECTORY);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(ctx.folderName + "/");
        assertThat(fileInfo.getItemInfo().getContentGeneration()).isEmpty();
        assertThat(fileInfo.getUri()).isEqualTo(dirUri);
    }

    @ParameterizedTest(name = "trailingSlash={0}")
    @ValueSource(booleans = {true, false})
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_flatPlaceholderDirectory_returnsPlaceholderDirectory(boolean trailingSlash)
            throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        String folderName = "test-folder-" + UUID.randomUUID();
        createPlaceholderDirectory(bucketName, folderName);
        URI dirUri = URI.create("gs://" + bucketName + "/" + folderName + "/");

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(toInputUri(dirUri, trailingSlash));

        assertThat(fileInfo.getItemInfo().getItemType())
                .isEqualTo(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(folderName + "/");
        assertThat(fileInfo.getItemInfo().getContentGeneration()).isPresent();
        assertThat(fileInfo.getUri()).isEqualTo(dirUri);
    }

    @ParameterizedTest(name = "trailingSlash={0}")
    @ValueSource(booleans = {true, false})
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_flatNonExistentDirectory_throwsFileNotFoundException(boolean trailingSlash) {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        URI dirUri =
                URI.create("gs://" + bucketName + "/non-existent-folder-" + UUID.randomUUID() + "/");
        URI inputUri = toInputUri(dirUri, trailingSlash);

        assertThrows(FileNotFoundException.class, () -> gcsFileSystem.getFileInfo(inputUri));
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_hnsBucket_returnsBucketInfo() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        URI bucketUri = URI.create("gs://" + bucketName);

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(bucketUri);

        assertThat(fileInfo.getItemInfo().getItemType()).isEqualTo(GcsItemInfo.ItemType.BUCKET);
        assertThat(fileInfo.getItemInfo().getItemId().getBucketName()).isEqualTo(bucketName);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).isEmpty();
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);
        assertThat(fileInfo.getUri()).isEqualTo(bucketUri);
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_hnsFile_returnsObjectInfo() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        TestWriteContext ctx = new TestWriteContext(bucketName, blobsToDelete, foldersToDelete);
        writeObject(ctx.itemId, TEST_FILE_CONTENT);

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(ctx.uri);

        assertThat(fileInfo.getItemInfo().getItemType()).isEqualTo(GcsItemInfo.ItemType.OBJECT);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo((long) TEST_FILE_CONTENT.length);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(ctx.objectName);
        assertThat(fileInfo.getUri()).isEqualTo(ctx.uri);
    }

    @ParameterizedTest(name = "trailingSlash={0}")
    @ValueSource(booleans = {true, false})
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_hnsFolder_returnsNativeFolder(boolean trailingSlash) throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        TestWriteContext ctx = new TestWriteContext(bucketName, blobsToDelete, foldersToDelete);
        writeObject(ctx.itemId, TEST_FILE_CONTENT);
        URI folderUri = URI.create("gs://" + bucketName + "/" + ctx.folderName + "/");

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(toInputUri(folderUri, trailingSlash));

        assertThat(fileInfo.getItemInfo().getItemType()).isEqualTo(GcsItemInfo.ItemType.NATIVE_FOLDER);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(ctx.folderName + "/");
        assertThat(fileInfo.getUri()).isEqualTo(folderUri);
    }

    @ParameterizedTest(name = "trailingSlash={0}")
    @ValueSource(booleans = {true, false})
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_hnsNonExistentDirectory_throwsFileNotFoundException(boolean trailingSlash) {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        URI dirUri =
                URI.create("gs://" + bucketName + "/non-existent-folder-" + UUID.randomUUID() + "/");
        URI inputUri = toInputUri(dirUri, trailingSlash);

        assertThrows(FileNotFoundException.class, () -> gcsFileSystem.getFileInfo(inputUri));
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void create_object_canWriteContent() throws IOException {
        TestWriteContext ctx =
                new TestWriteContext(
                        System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY), blobsToDelete);
        GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);

        try (WritableByteChannel channel = gcsFileSystem.create(ctx.itemId, writeOptions)) {
            ByteBuffer buffer = ByteBuffer.wrap(content);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(ctx.uri);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo((long) content.length);
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void create_overwriteDisabled_throwsFileAlreadyExistsException() throws IOException {
        TestWriteContext ctx =
                new TestWriteContext(
                        System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY), blobsToDelete);
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);
        // We do a preliminary setup write
        GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
        try (WritableByteChannel channel = gcsFileSystem.create(ctx.itemId, writeOptions)) {
            ByteBuffer buffer = ByteBuffer.wrap(content);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }

        GcsWriteOptions noOverwriteOptions = GcsWriteOptions.builder()
                .setOverwriteExisting(false)
                .build();

        assertThrows(FileAlreadyExistsException.class, () -> {
            try (WritableByteChannel channel = gcsFileSystem.create(ctx.itemId, noOverwriteOptions)) {
                ByteBuffer buffer = ByteBuffer.wrap(content);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
            }
        });
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void create_withParallelCompositeUpload_success() throws IOException {
        TestWriteContext ctx =
                new TestWriteContext(
                        System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY), blobsToDelete);
        GcsClientOptions clientOptions =
                GcsClientOptions.builder()
                .setUploadType(GcsClientOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD)
                .build();
        try (GcsFileSystemImpl pcuFileSystem = createFileSystem(clientOptions)) {
        GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);

            try (WritableByteChannel channel = pcuFileSystem.create(ctx.itemId, writeOptions)) {
            ByteBuffer buffer = ByteBuffer.wrap(content);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }

            GcsFileInfo fileInfo = pcuFileSystem.getFileInfo(ctx.uri);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo((long) content.length);
        }
    }

    @Test
    void create_nonExistentBucket_throwsFileNotFoundException() {
        TestWriteContext ctx =
                new TestWriteContext("non-existent-bucket-" + UUID.randomUUID(), blobsToDelete);
        GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);

        assertThrows(FileNotFoundException.class, () -> {
            try (WritableByteChannel channel = gcsFileSystem.create(ctx.itemId, writeOptions)) {
                ByteBuffer buffer = ByteBuffer.wrap(content);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
            }
        });
    }

    private static class TestWriteContext {
        final String folderName;
        final String objectName;
        final URI uri;
        final GcsItemId itemId;

        TestWriteContext(String bucketName, List<BlobId> blobsToDelete) {
            this(bucketName, blobsToDelete, null);
        }

        TestWriteContext(String bucketName, List<BlobId> blobsToDelete, List<String> foldersToDelete) {
            this.folderName = "test-folder-" + UUID.randomUUID();
            this.objectName = folderName + "/test-file-" + UUID.randomUUID() + ".txt";
            this.uri = URI.create("gs://" + bucketName + "/" + objectName);
            this.itemId = GcsItemId.builder()
                    .setBucketName(bucketName)
                    .setObjectName(objectName)
                    .build();
            blobsToDelete.add(BlobId.of(bucketName, objectName));
            if (foldersToDelete != null) {
                foldersToDelete.add("projects/_/buckets/" + bucketName + "/folders/" + folderName);
            }
        }
    }

    private void writeObject(GcsItemId itemId, byte[] content) throws IOException {
        try (WritableByteChannel channel =
                gcsFileSystem.create(itemId, GcsWriteOptions.builder().build())) {
            ByteBuffer buffer = ByteBuffer.wrap(content);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }
    }

    /** Returns {@code dirUri} as-is, or with its trailing slash removed. */
    private static URI toInputUri(URI dirUri, boolean trailingSlash) {
        String uri = dirUri.toString();
        return trailingSlash ? dirUri : URI.create(uri.substring(0, uri.length() - 1));
    }

    private void createPlaceholderDirectory(String bucketName, String folderName) {
        BlobId placeholderId = BlobId.of(bucketName, folderName + "/");
        storage.create(BlobInfo.newBuilder(placeholderId).build());
        blobsToDelete.add(placeholderId);
    }

    private GcsFileSystemImpl createFileSystem(GcsClientOptions clientOptions) {
        GcsFileSystemOptions options = GcsFileSystemOptions.builder()
                .setGcsClientOptions(clientOptions)
                .build();
        return new GcsFileSystemImpl(options);
    }
}
