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
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

// TODO: Setup buckets and test data as part of setup on place of relying on existing bucket.
class GcsFileSystemImplIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(GcsFileSystemImplIntegrationTest.class);

    private Storage storage;
    private List<BlobId> blobsToDelete;
    private List<String> bucketsToDelete;
    private List<String> foldersToDelete;

    @BeforeEach
    void setUp() {
        storage = StorageOptions.getDefaultInstance().getService();
        blobsToDelete = new ArrayList<>();
        bucketsToDelete = new ArrayList<>();
        foldersToDelete = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        try {
            if (storage != null) {
                for (BlobId blobId : blobsToDelete) {
                    try {
                        storage.delete(blobId);
                    } catch (Exception e) {
                        LOG.warn("Failed to delete blob {} during cleanup", blobId, e);
                    }
                }
                for (String bucketName : bucketsToDelete) {
                    try {
                        storage.delete(bucketName);
                    } catch (Exception e) {
                        LOG.warn("Failed to delete bucket {} during cleanup", bucketName, e);
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
            bucketsToDelete.clear();
            foldersToDelete.clear();
        }
    }

    @Test
    void open_publicObject_canReadContent() throws IOException {
        String gcsObject = "gs://cloud-samples-data/bigquery/us-states/us-states.csv";
        GcsFileSystemOptions options = GcsFileSystemOptions.builder()
                .setGcsClientOptions(GcsClientOptions.builder().build())
                .build();
        GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(options);
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
        String gcsObject = "gs://cloud-samples-data/bigquery/us-states/us-states.parquet";
        GcsFileSystemOptions options = GcsFileSystemOptions.builder()
                .setGcsClientOptions(GcsClientOptions.builder().build())
                .build();
        GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(options);

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(URI.create(gcsObject));

        assertThat(fileInfo.getItemInfo().getItemId().isGcsObject()).isTrue();
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue("bigquery/us-states/us-states.parquet");
        assertThat(fileInfo.getItemInfo().getItemId().getBucketName()).isEqualTo("cloud-samples-data");
    }

    @Test
    void getFileInfo_noCredentialProvided_urlPointsToPrivateObject_usesApplicationDefaultCredentials()
            throws IOException {
        String object = "gs://gcs-connector-private-test-bucket-do-not-delete/tpch_customer_1.parquet";
        GcsFileSystemOptions options =
                GcsFileSystemOptions.builder()
                        .setGcsClientOptions(GcsClientOptions.builder().build())
                        .build();
        GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(options);

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(URI.create(object));

        assertThat(fileInfo.getItemInfo().getItemId().isGcsObject()).isTrue();
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue("tpch_customer_1.parquet");
        assertThat(fileInfo.getItemInfo().getItemId().getBucketName())
                .isEqualTo("gcs-connector-private-test-bucket-do-not-delete");
    }

    @Test
    void getFileInfo_anonymousCredentialProvided_urlPointsToPublicObject_success() throws IOException {
        String gcsObject = "gs://cloud-samples-data/bigquery/us-states/us-states.parquet";
        GcsFileSystemOptions options =
                GcsFileSystemOptions.builder()
                        .setGcsClientOptions(GcsClientOptions.builder().build())
                        .build();
        GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(NoCredentials.getInstance(), options);

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(URI.create(gcsObject));

        assertThat(fileInfo.getItemInfo().getItemId().isGcsObject()).isTrue();
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue("bigquery/us-states/us-states.parquet");
        assertThat(fileInfo.getItemInfo().getItemId().getBucketName()).isEqualTo("cloud-samples-data");
    }

    @Test
    void getFileInfo_anonymousCredentialProvided_urlPointsToPrivateObject_throws() throws IOException {
        String object = "gs://gcs-connector-private-test-bucket-do-not-delete/tpch_customer_1.parquet";
        GcsFileSystemOptions options =
                GcsFileSystemOptions.builder()
                        .setGcsClientOptions(GcsClientOptions.builder().build())
                        .build();
        GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(NoCredentials.getInstance(), options);

        AccessDeniedException exception =
                assertThrows(AccessDeniedException.class, () -> gcsFileSystem.getFileInfo(URI.create(object)));

        assertThat(exception).hasMessageThat().contains("Access denied to object during metadata lookup");
    }

    @Test
    @EnabledIfSystemProperty(named = "gcs.integration.test.bucket", matches = ".+")
    void create_object_canWriteContent() throws IOException {
        TestWriteContext ctx = new TestWriteContext(System.getProperty("gcs.integration.test.bucket"), blobsToDelete);
        GcsFileSystemImpl gcsFileSystem = createFileSystem(GcsClientOptions.builder().build());
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
    @EnabledIfSystemProperty(named = "gcs.integration.test.bucket", matches = ".+")
    void create_overwriteDisabled_throwsFileAlreadyExistsException() throws IOException {
        TestWriteContext ctx = new TestWriteContext(System.getProperty("gcs.integration.test.bucket"), blobsToDelete);
        GcsFileSystemImpl gcsFileSystem = createFileSystem(GcsClientOptions.builder().build());
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
    @EnabledIfSystemProperty(named = "gcs.integration.test.bucket", matches = ".+")
    void create_withParallelCompositeUpload_success() throws IOException {
        TestWriteContext ctx = new TestWriteContext(System.getProperty("gcs.integration.test.bucket"), blobsToDelete);
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
        GcsFileSystemImpl gcsFileSystem = createFileSystem(GcsClientOptions.builder().build());
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

    @Test
    @EnabledIfSystemProperty(named = "gcs.integration.test.bucket", matches = ".+")
    void mkdirs_createsDirectoryMarkerAndIsIdempotent_success() throws IOException {
        String bucketName = System.getProperty("gcs.integration.test.bucket");
        String dirObjectName = "test-mkdirs-" + UUID.randomUUID() + "/subdir/";
        URI dirUri = URI.create("gs://" + bucketName + "/" + dirObjectName);
        blobsToDelete.add(BlobId.of(bucketName, dirObjectName));
        GcsFileSystemImpl gcsFileSystem = createFileSystem(GcsClientOptions.builder().build());

        // Verify initial directory marker creation
        gcsFileSystem.mkdirs(dirUri);
        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(dirUri);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(dirObjectName);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);

        // Verify calling mkdirs on an existing directory is idempotent
        assertDoesNotThrow(() -> gcsFileSystem.mkdirs(dirUri));
    }

    @Test
    @EnabledIfSystemProperty(named = "gcs.integration.test.bucket", matches = ".+")
    void mkdirs_withConflictingFile_throwsFileAlreadyExistsException() throws IOException {
        String bucketName = System.getProperty("gcs.integration.test.bucket");
        TestWriteContext ctx = new TestWriteContext(bucketName, blobsToDelete);
        GcsFileSystemImpl gcsFileSystem = createFileSystem(GcsClientOptions.builder().build());
        try (WritableByteChannel channel =
                gcsFileSystem.create(ctx.itemId, GcsWriteOptions.builder().build())) {
            channel.write(ByteBuffer.wrap("file content".getBytes(StandardCharsets.UTF_8)));
        }
        URI subdirUri = URI.create(ctx.uri.toString() + "/subdir");

        assertThrows(FileAlreadyExistsException.class, () -> gcsFileSystem.mkdirs(subdirUri));
    }

    @Test
    @EnabledIfSystemProperty(named = "gcs.integration.test.hns.bucket", matches = ".+")
    void mkdirs_createsHnsFolderAndIsIdempotent_success() throws IOException {
        String bucketName = System.getProperty("gcs.integration.test.hns.bucket");
        TestWriteContext ctx = new TestWriteContext(bucketName, blobsToDelete, foldersToDelete);
        URI dirUri = URI.create("gs://" + bucketName + "/" + ctx.folderName + "/");
        GcsFileSystemOptions options = GcsFileSystemOptions.builder()
                .setHnsApiEnabled(true)
                .build();
        GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(options);

        // Verify initial folder creation
        gcsFileSystem.mkdirs(dirUri);
        GcsItemInfo folderInfo =
                gcsFileSystem.getGcsClient().getFolderInfo(UriUtil.getItemIdFromUri(dirUri));
        assertThat(folderInfo.getItemId().getObjectName()).hasValue(ctx.folderName + "/");
        assertThat(folderInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.NATIVE_FOLDER);

        // Verify calling mkdirs on an existing folder is idempotent
        assertDoesNotThrow(() -> gcsFileSystem.mkdirs(dirUri));
    }

    @Test
    @EnabledIfSystemProperty(named = "gcs.integration.test.project-id", matches = ".+")
    void mkdirs_newBucket_createsBucket_success() throws IOException {
        String bucketName = "test-mkdirs-bucket-" + UUID.randomUUID();
        URI bucketUri = URI.create("gs://" + bucketName);
        bucketsToDelete.add(bucketName);
        GcsFileSystemImpl gcsFileSystem = createFileSystem(GcsClientOptions.builder().build());

        gcsFileSystem.mkdirs(bucketUri);

        GcsItemInfo bucketInfo =
                gcsFileSystem
                        .getGcsClient()
                        .getBucketInfo(GcsItemId.builder().setBucketName(bucketName).build());
        assertThat(bucketInfo.getItemId().getBucketName()).isEqualTo(bucketName);
        assertThat(bucketInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.BUCKET);
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
            if (blobsToDelete != null) {
                blobsToDelete.add(BlobId.of(bucketName, objectName));
            }
            if (foldersToDelete != null) {
                foldersToDelete.add("projects/_/buckets/" + bucketName + "/folders/" + folderName);
            }
        }
    }

    private GcsFileSystemImpl createFileSystem(GcsClientOptions clientOptions) {
        GcsClientOptions.Builder builder = clientOptions.toBuilder();
        String projectId = System.getProperty("gcs.integration.test.project-id");
        if (projectId != null && !projectId.isEmpty()) {
            builder.setProjectId(projectId);
        }
        GcsFileSystemOptions options = GcsFileSystemOptions.builder()
                .setGcsClientOptions(builder.build())
                .build();
        return new GcsFileSystemImpl(options);
    }
}
