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
}
