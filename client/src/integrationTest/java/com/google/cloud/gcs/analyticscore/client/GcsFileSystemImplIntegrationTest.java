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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.FileNotFoundException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.util.UUID;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

// TODO: Setup buckets and test data as part of setup on place of relying on existing bucket.
class GcsFileSystemImplIntegrationTest {

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

    @Test
    @EnabledIfSystemProperty(named = "gcs.integration.test.bucket", matches = ".+")
    public void create_object_canWriteContent() throws IOException {
        // Arrange
        String bucketName = System.getProperty("gcs.integration.test.bucket");
        String objectName = "test-folder/test-file-" + UUID.randomUUID() + ".txt";
        URI uri = URI.create("gs://" + bucketName + "/" + objectName);

        GcsFileSystemOptions options = GcsFileSystemOptions.builder()
                .setGcsClientOptions(GcsClientOptions.builder().build())
                .build();
        GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(options);

        GcsItemId itemId = GcsItemId.builder()
                .setBucketName(bucketName)
                .setObjectName(objectName)
                .build();

        GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);

        // Act
        try (WritableByteChannel channel = gcsFileSystem.create(itemId, writeOptions)) {
            channel.write(ByteBuffer.wrap(content));
        }

        // Assert
        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(uri);
        assertThat(fileInfo.getItemInfo().getSize().get()).isEqualTo((long) content.length);
    }

    @Test
    @EnabledIfSystemProperty(named = "gcs.integration.test.bucket", matches = ".+")
    public void create_overwriteDisabled_throwsFileAlreadyExistsException() throws IOException {
        // Arrange
        String bucketName = System.getProperty("gcs.integration.test.bucket");
        String objectName = "test-folder/test-file-" + UUID.randomUUID() + ".txt";

        GcsFileSystemOptions options = GcsFileSystemOptions.builder()
                .setGcsClientOptions(GcsClientOptions.builder().build())
                .build();
        GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(options);

        GcsItemId itemId = GcsItemId.builder()
                .setBucketName(bucketName)
                .setObjectName(objectName)
                .build();

        // Act & Assert
        // We do a preliminary setup write (Arrange)
        GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
        try (WritableByteChannel channel = gcsFileSystem.create(itemId, writeOptions)) {
            channel.write(ByteBuffer.wrap("test".getBytes(StandardCharsets.UTF_8)));
        }

        GcsWriteOptions noOverwriteOptions = GcsWriteOptions.builder()
                .setOverwriteExisting(false)
                .build();

        // Act & Assert
        assertThrows(FileAlreadyExistsException.class, () -> {
            gcsFileSystem.create(itemId, noOverwriteOptions);
        });
    }

    @Test
    @EnabledIfSystemProperty(named = "gcs.integration.test.bucket", matches = ".+")
    public void create_withParallelCompositeUpload_success() throws IOException {
        // Arrange
        String bucketName = System.getProperty("gcs.integration.test.bucket");
        String objectName = "test-folder/test-file-" + UUID.randomUUID() + ".txt";
        URI uri = URI.create("gs://" + bucketName + "/" + objectName);

        GcsFileSystemOptions options = GcsFileSystemOptions.builder()
                .setGcsClientOptions(GcsClientOptions.builder().build())
                .build();
        GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(options);

        GcsItemId itemId = GcsItemId.builder()
                .setBucketName(bucketName)
                .setObjectName(objectName)
                .build();

        GcsWriteOptions writeOptions = GcsWriteOptions.builder()
                .setUploadStrategy(GcsWriteOptions.UploadStrategy.PARALLEL_COMPOSITE_UPLOAD)
                .build();
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);

        // Act
        try (WritableByteChannel channel = gcsFileSystem.create(itemId, writeOptions)) {
            channel.write(ByteBuffer.wrap(content));
        }

        // Assert
        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(uri);
        assertThat(fileInfo.getItemInfo().getSize().get()).isEqualTo((long) content.length);
    }

    @Test
    public void create_nonExistentBucket_throwsFileNotFoundException() throws IOException {
        // Arrange
        String bucketName = "non-existent-bucket-" + UUID.randomUUID();
        String objectName = "test-folder/test-file-" + UUID.randomUUID() + ".txt";

        GcsFileSystemOptions options = GcsFileSystemOptions.builder()
                .setGcsClientOptions(GcsClientOptions.builder().build())
                .build();
        GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(options);

        GcsItemId itemId = GcsItemId.builder()
                .setBucketName(bucketName)
                .setObjectName(objectName)
                .build();

        GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();

        // Act & Assert
        assertThrows(FileNotFoundException.class, () -> {
            try (WritableByteChannel channel = gcsFileSystem.create(itemId, writeOptions)) {
                channel.write(ByteBuffer.wrap("test".getBytes(StandardCharsets.UTF_8)));
            }
        });
    }
}
