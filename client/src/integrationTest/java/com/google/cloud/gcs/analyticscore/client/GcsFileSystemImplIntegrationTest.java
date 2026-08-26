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

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.storage.control.v2.DeleteFolderRequest;
import com.google.storage.control.v2.StorageControlClient;

import java.io.FileNotFoundException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

// TODO: Setup buckets and test data as part of setup on place of relying on existing bucket.
// TODO: Update both Flat and HNS getFileInfo tests to run using the bucket provided via
// GCS_INTEGRATION_TEST_BUCKET_PROPERTY and GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY
class GcsFileSystemImplIntegrationTest {

    private static final String GCS_INTEGRATION_TEST_BUCKET_PROPERTY =
            "gcs.integration.test.bucket";
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
    private static final String PRIVATE_IMPLICIT_FOLDER = "implicit-folder";
    private static final byte[] TEST_HNS_FILE_CONTENT =
            "test hns file content".getBytes(StandardCharsets.UTF_8);

    private static final Logger LOG =
            LoggerFactory.getLogger(GcsFileSystemImplIntegrationTest.class);

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
        assertThat(fileInfo.getItemInfo().getItemId().getBucketName())
                .isEqualTo(PRIVATE_BUCKET_NAME);
    }

    @Test
    void getFileInfo_anonymousCredentialProvided_urlPointsToPublicObject_success() throws IOException {
        String gcsObject = PUBLIC_PARQUET_URI_STRING;
        GcsFileSystemOptions options =
                GcsFileSystemOptions.builder()
                        .setGcsClientOptions(GcsClientOptions.builder().build())
                        .build();

        try (GcsFileSystemImpl anonFileSystem =
                new GcsFileSystemImpl(NoCredentials.getInstance(), options)) {
            GcsFileInfo fileInfo = anonFileSystem.getFileInfo(URI.create(gcsObject));

            assertThat(fileInfo.getItemInfo().getItemId().isGcsObject()).isTrue();
            assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(PUBLIC_PARQUET_OBJECT);
            assertThat(fileInfo.getItemInfo().getItemId().getBucketName()).isEqualTo(PUBLIC_BUCKET_NAME);
        }
    }

    @Test
    void getFileInfo_anonymousCredentialProvided_urlPointsToPrivateObject_throws() throws IOException {
        String object = PRIVATE_PARQUET_URI_STRING;
        GcsFileSystemOptions options =
                GcsFileSystemOptions.builder()
                        .setGcsClientOptions(GcsClientOptions.builder().build())
                        .build();

        try (GcsFileSystemImpl anonFileSystem =
                new GcsFileSystemImpl(NoCredentials.getInstance(), options)) {
            IOException exception =
                    assertThrows(IOException.class, () -> anonFileSystem.getFileInfo(URI.create(object)));

            assertThat(exception).hasMessageThat().contains("Unable to access blob");
        }
    }

    @Test
    void getFileInfo_uriWithoutBucket_throwsIllegalArgumentException() {
        URI uriWithoutBucket = URI.create("gs:///path");
        assertThrows(
                IllegalArgumentException.class, () -> gcsFileSystem.getFileInfo(uriWithoutBucket));
    }

    @Test
    void getFileInfo_rootItemId_returnsRootInfo() throws IOException {
        GcsItemId rootId = GcsItemId.ROOT;
        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(rootId);

        assertThat(fileInfo).isEqualTo(GcsFileInfo.ROOT_INFO);
        assertThat(fileInfo.getItemInfo().getItemType()).isEqualTo(GcsItemInfo.ItemType.ROOT);
        assertThat(fileInfo.getItemInfo().getItemId()).isEqualTo(GcsItemId.ROOT);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);
    }

    @Test
    void getFileInfo_bucketUri_returnsBucketInfo() throws IOException {
        URI bucketUri = URI.create("gs://" + PRIVATE_BUCKET_NAME);

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(bucketUri);

        assertThat(fileInfo.getItemInfo().getItemType()).isEqualTo(GcsItemInfo.ItemType.BUCKET);
        assertThat(fileInfo.getItemInfo().getItemId().getBucketName())
                .isEqualTo(PRIVATE_BUCKET_NAME);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName().isPresent()).isFalse();
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
    void getFileInfo_nonExistentObject_throwsFileNotFoundException() {
        URI nonExistentUri = URI.create("gs://" + PRIVATE_BUCKET_NAME + "/non-existent-file-" + UUID.randomUUID() + ".parquet");
        assertThrows(FileNotFoundException.class, () -> gcsFileSystem.getFileInfo(nonExistentUri));
    }

    @Test
    void getFileInfo_implicitDirectoryWithTrailingSlash_returnsInferredDirectory() throws IOException {
        URI dirUri = URI.create("gs://" + PRIVATE_BUCKET_NAME + "/" + PRIVATE_IMPLICIT_FOLDER + "/");

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(dirUri);

        assertThat(fileInfo.getItemInfo().getItemType())
                .isEqualTo(GcsItemInfo.ItemType.INFERRED_DIRECTORY);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName())
                .hasValue(PRIVATE_IMPLICIT_FOLDER + "/");
        assertThat(fileInfo.getUri()).isEqualTo(dirUri);
    }

    @Test
    void getFileInfo_implicitDirectoryWithoutTrailingSlash_returnsInferredDirectory() throws IOException {
        URI dirUri = URI.create("gs://" + PRIVATE_BUCKET_NAME + "/" + PRIVATE_IMPLICIT_FOLDER);

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(dirUri);

        assertThat(fileInfo.getItemInfo().getItemType())
                .isEqualTo(GcsItemInfo.ItemType.INFERRED_DIRECTORY);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName())
                .hasValue(PRIVATE_IMPLICIT_FOLDER);
        assertThat(fileInfo.getUri()).isEqualTo(dirUri);
    }

    @Test
    void getFileInfo_nonExistentDirectoryWithTrailingSlash_throwsFileNotFoundException() {
        URI nonExistentDirUri =
                URI.create("gs://" + PRIVATE_BUCKET_NAME + "/non-existent-folder-" + UUID.randomUUID() + "/");
        assertThrows(
                FileNotFoundException.class, () -> gcsFileSystem.getFileInfo(nonExistentDirUri));
    }

    @Test
    void getFileInfo_nonExistentDirectoryWithoutTrailingSlash_throwsFileNotFoundException() {
        URI nonExistentDirUri =
                URI.create("gs://" + PRIVATE_BUCKET_NAME + "/non-existent-folder-" + UUID.randomUUID());
        assertThrows(
                FileNotFoundException.class, () -> gcsFileSystem.getFileInfo(nonExistentDirUri));
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_hnsBucket_returnsBucketInfo() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        URI bucketUri = URI.create("gs://" + bucketName);

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(bucketUri);

        assertThat(fileInfo.getItemInfo().getItemType()).isEqualTo(GcsItemInfo.ItemType.BUCKET);
        assertThat(fileInfo.getItemInfo().getItemId().getBucketName()).isEqualTo(bucketName);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName().isPresent()).isFalse();
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);
        assertThat(fileInfo.getUri()).isEqualTo(bucketUri);
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_hnsFile_returnsObjectInfo() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        TestWriteContext ctx = new TestWriteContext(bucketName, blobsToDelete, foldersToDelete);
        
        GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
        try (WritableByteChannel channel = gcsFileSystem.create(ctx.itemId, writeOptions)) {
            ByteBuffer buffer = ByteBuffer.wrap(TEST_HNS_FILE_CONTENT);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(ctx.uri);

        assertThat(fileInfo.getItemInfo().getItemId().isGcsObject()).isTrue();
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(ctx.objectName);
        assertThat(fileInfo.getItemInfo().getItemId().getBucketName()).isEqualTo(bucketName);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo((long) TEST_HNS_FILE_CONTENT.length);
        assertThat(fileInfo.getItemInfo().getItemType()).isEqualTo(GcsItemInfo.ItemType.OBJECT);
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_hnsFolderWithTrailingSlash_returnsNativeFolder() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        TestWriteContext ctx = new TestWriteContext(bucketName, blobsToDelete, foldersToDelete);
        
        GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
        try (WritableByteChannel channel = gcsFileSystem.create(ctx.itemId, writeOptions)) {
            ByteBuffer buffer = ByteBuffer.wrap(TEST_HNS_FILE_CONTENT);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }
        URI folderWithSlashUri = URI.create("gs://" + bucketName + "/" + ctx.folderName + "/");

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(folderWithSlashUri);

        assertThat(fileInfo.getItemInfo().getItemType())
                .isEqualTo(GcsItemInfo.ItemType.NATIVE_FOLDER);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName())
                .hasValue(ctx.folderName + "/");
        assertThat(fileInfo.getUri()).isEqualTo(folderWithSlashUri);
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_hnsFolderWithoutTrailingSlash_returnsNativeFolder() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        TestWriteContext ctx = new TestWriteContext(bucketName, blobsToDelete, foldersToDelete);
        
        GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
        try (WritableByteChannel channel = gcsFileSystem.create(ctx.itemId, writeOptions)) {
            ByteBuffer buffer = ByteBuffer.wrap(TEST_HNS_FILE_CONTENT);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }
        URI folderWithoutSlashUri = URI.create("gs://" + bucketName + "/" + ctx.folderName);

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(folderWithoutSlashUri);

        assertThat(fileInfo.getItemInfo().getItemType())
                .isEqualTo(GcsItemInfo.ItemType.NATIVE_FOLDER);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(ctx.folderName);
        assertThat(fileInfo.getUri()).isEqualTo(folderWithoutSlashUri);
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_hnsNonExistentItem_throwsFileNotFoundException() {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        URI nonExistentUri =
                URI.create("gs://" + bucketName + "/non-existent-folder-" + UUID.randomUUID());
        assertThrows(FileNotFoundException.class, () -> gcsFileSystem.getFileInfo(nonExistentUri));
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
        TestWriteContext ctx = new TestWriteContext(System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY), blobsToDelete);
        GcsClientOptions clientOptions = GcsClientOptions.builder()
                .setUploadType(GcsClientOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD)
                .build();
        GcsFileSystemImpl gcsFileSystem = createFileSystem(clientOptions);
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
    void create_nonExistentBucket_throwsFileNotFoundException() throws IOException {
        TestWriteContext ctx = new TestWriteContext("non-existent-bucket-" + UUID.randomUUID(), blobsToDelete);
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

        TestWriteContext(
                String bucketName,
                List<BlobId> blobsToDelete,
                List<String> foldersToDelete) {
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

    private GcsFileSystemImpl createFileSystem(GcsClientOptions clientOptions) {
        GcsFileSystemOptions options = GcsFileSystemOptions.builder()
                .setGcsClientOptions(clientOptions)
                .build();
        return new GcsFileSystemImpl(options);
    }
}
