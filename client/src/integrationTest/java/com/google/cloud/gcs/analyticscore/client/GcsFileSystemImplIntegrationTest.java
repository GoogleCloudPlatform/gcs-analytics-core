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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

// TODO: Setup buckets and test data as part of setup on place of relying on existing bucket.
class GcsFileSystemImplIntegrationTest {

    private static final String GCS_INTEGRATION_TEST_BUCKET_PROPERTY = "gcs.integration.test.bucket";
    private static final String GCS_INTEGRATION_TEST_PROJECT_ID_PROPERTY = "gcs.integration.test.project-id";

    private static final byte[] TEST_CONTENT =
        "Hello, GCS Analytics Core Write Path!".getBytes(StandardCharsets.UTF_8);
    private static final byte[] OVERWRITE_CONTENT =
        "Second write attempts to overwrite".getBytes(StandardCharsets.UTF_8);
    private static final String NON_EXISTENT_BUCKET =
        "non-existent-bucket-random-name-12345";

    private List<BlobId> blobsToDelete;
    private GcsFileSystemImpl gcsFileSystem;

    @BeforeEach
    void setUp() {
        blobsToDelete = new ArrayList<>();
        GcsFileSystemOptions options = GcsFileSystemOptions.builder()
            .setGcsClientOptions(GcsClientOptions.builder().build())
            .build();
        gcsFileSystem = new GcsFileSystemImpl(options);
    }

    @AfterEach
    void tearDown() {
        if (gcsFileSystem != null) {
            try {
                GcsClientImpl clientImpl = (GcsClientImpl) gcsFileSystem.getGcsClient();
                for (BlobId blobId : blobsToDelete) {
                    try {
                        clientImpl.storage.delete(blobId);
                    } catch (Exception e) {
                        // Ignore cleanup exceptions
                    }
                }
            } catch (Exception e) {
                // Ignore initialization errors during tearDown
            }
        }
    }

    @Test
    public void open_publicObject_canReadContent() throws IOException {
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
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_PROJECT_ID_PROPERTY, matches = ".+")
    public void create_object_canWriteContent() throws IOException {
        String fileName = "test-public-write-" + UUID.randomUUID() + ".txt";
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        BlobId blobId = BlobId.of(bucketName, fileName);
        blobsToDelete.add(blobId);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();

        try (WritableByteChannel channel = gcsFileSystem.create(blobInfo, writeOptions)) {
            assertThat(channel.isOpen()).isTrue();
            ByteBuffer buffer = ByteBuffer.wrap(TEST_CONTENT);
            int bytesWritten = channel.write(buffer);
            assertThat(bytesWritten).isEqualTo(TEST_CONTENT.length);
        }
        URI fileUri = URI.create("gs://" + bucketName + "/" + fileName);
        GcsFileInfo writtenFileInfo = gcsFileSystem.getFileInfo(fileUri);
        assertThat(writtenFileInfo).isNotNull();
        assertThat(writtenFileInfo.getItemInfo().getSize()).isEqualTo((long) TEST_CONTENT.length);
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_PROJECT_ID_PROPERTY, matches = ".+")
    public void create_overwriteDisabled_throwsFileAlreadyExistsException() throws IOException {
        String fileName = "test-overwrite-disabled-" + UUID.randomUUID() + ".txt";
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        BlobId blobId = BlobId.of(bucketName, fileName);
        blobsToDelete.add(blobId);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        GcsWriteOptions defaultOptions = GcsWriteOptions.builder().build();

        try (WritableByteChannel channel = gcsFileSystem.create(blobInfo, defaultOptions)) {
            channel.write(ByteBuffer.wrap(TEST_CONTENT));
        }
        GcsWriteOptions noOverwriteOptions = GcsWriteOptions.builder()
            .setOverwriteExisting(false)
            .build();
        assertThrows(FileAlreadyExistsException.class, () -> {
            try (WritableByteChannel channel = gcsFileSystem.create(blobInfo, noOverwriteOptions)) {
                channel.write(ByteBuffer.wrap(OVERWRITE_CONTENT));
            }
        });
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_PROJECT_ID_PROPERTY, matches = ".+")
    public void create_withParallelCompositeUpload_success() throws IOException {
        String fileName = "test-pcu-write-" + UUID.randomUUID() + ".txt";
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        BlobId blobId = BlobId.of(bucketName, fileName);
        blobsToDelete.add(blobId);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        GcsWriteOptions writeOptions = GcsWriteOptions.builder()
            .setUploadType(GcsWriteOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD)
            .setPcuBufferCount(2)
            .setPcuBufferCapacity(16 * 1024 * 1024)
            .setPcuPartFileCleanupType(GcsWriteOptions.PartFileCleanupType.ALWAYS)
            .build();

        try (WritableByteChannel channel = gcsFileSystem.create(blobInfo, writeOptions)) {
            assertThat(channel.isOpen()).isTrue();
            ByteBuffer buffer = ByteBuffer.wrap(TEST_CONTENT);
            int bytesWritten = channel.write(buffer);
            assertThat(bytesWritten).isEqualTo(TEST_CONTENT.length);
        }
        // Verify the file actually appeared by reading its metadata using the client itself
        URI fileUri = URI.create("gs://" + bucketName + "/" + fileName);
        GcsFileInfo writtenFileInfo = gcsFileSystem.getFileInfo(fileUri);
        assertThat(writtenFileInfo).isNotNull();
        assertThat(writtenFileInfo.getItemInfo().getSize()).isEqualTo((long) TEST_CONTENT.length);
    }

    @Test
    public void create_nonExistentBucket_throwsFileNotFoundException() {
        BlobId blobId = BlobId.of(NON_EXISTENT_BUCKET, "test-file.txt");
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();

        assertThrows(FileNotFoundException.class, () -> {
            try (WritableByteChannel channel = gcsFileSystem.create(blobInfo, writeOptions)) {
                channel.write(ByteBuffer.wrap(TEST_CONTENT));
            }
        });
    }

    @Test
    public void getFileInfo_noCredentialProvided_urlPointsToPublicObject_success() throws IOException {
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
    public void getFileInfo_noCredentialProvided_urlPointsToPrivateObject_usesApplicationDefaultCredentials()
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
    public void getFileInfo_anonymousCredentialProvided_urlPointsToPublicObject_success() throws IOException {
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
    public void getFileInfo_anonymousCredentialProvided_urlPointsToPrivateObject_throws() throws IOException {
        String object = "gs://gcs-connector-private-test-bucket-do-not-delete/tpch_customer_1.parquet";
        GcsFileSystemOptions options =
                GcsFileSystemOptions.builder()
                        .setGcsClientOptions(GcsClientOptions.builder().build())
                        .build();
        GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(NoCredentials.getInstance(), options);

        IOException exception =
                assertThrows(IOException.class, () -> gcsFileSystem.getFileInfo(URI.create(object)));

        assertThat(exception).hasMessageThat().contains("Unable to access blob");
    }
}
