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

package com.google.cloud.gcs.analyticscore.core;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.common.collect.ImmutableList;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegrationTestHelper {
    public static final String BUCKET_NAME = System.getProperty("gcs.integration.test.bucket");
    public static final String PROJECT_ID = System.getProperty("gcs.integration.test.project-id");
    public static final String BUCKET_FOLDER = System.getProperty("gcs.integration.test.bucket.folder");

    private static final Logger logger = LoggerFactory.getLogger(IntegrationTestHelper.class);
    private static final Storage storage = StorageOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();

    /**
     * Retrieves a file from the test resources.
     *
     * @param fileName The name of the file to retrieve.
     * @return The File object representing the resource.
     */
    public static File getFileFromResources(String fileName) {
        URL resource = IntegrationTestHelper.class.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("Resource not found: " + fileName);
        }
        try {
            return new File(resource.toURI());
        } catch (java.net.URISyntaxException e) {
            throw new RuntimeException("Failed to convert resource URL to URI", e);
        }
    }

    /**
     * Constructs a GCS URI for a given file name in the test bucket and folder.
     *
     * @param fileName The name of the file.
     * @return The GCS URI for the file.
     */
    public static URI getGcsObjectUriForFile(String fileName) {
        String folderName = BUCKET_FOLDER.endsWith("/") ?  BUCKET_FOLDER : BUCKET_FOLDER + "/";
        return URI.create(BlobId.of(BUCKET_NAME, folderName + fileName).toGsUtilUri());
    }

    /**
     * Uploads a single file to the GCS test bucket.
     *
     * @param file The file to upload.
     * @throws IOException if an I/O error occurs during file reading.
     */
    public static void uploadFileToGcs(File file) throws FileNotFoundException, IOException {
        BlobId blobId = BlobId.of(BUCKET_NAME, BUCKET_FOLDER + "/" + file.getName());
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        try(InputStream inputStream =  new FileInputStream(file)) {
            try {
                storage.create(blobInfo, inputStream);
                logger.info("Successfully uploaded file {} to bucket {}", file.getName(), BUCKET_NAME);
            } catch (StorageException e) {
                logger.error("Failed to upload file {} to bucket {}", file.getName(), BUCKET_NAME, e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Deletes all files from the GCS test bucket folder.
     */
    public static void deleteUploadedFilesFromGcs() {
        String folderName = BUCKET_FOLDER.endsWith("/") ?  BUCKET_FOLDER : BUCKET_FOLDER + "/";
        Page<Blob> blobs = storage.list(BUCKET_NAME, Storage.BlobListOption.prefix(folderName));
        for (Blob blob : blobs.iterateAll()) {
            storage.delete(blob.getBlobId());
            logger.info("Successfully deleted file {} from bucket {}", blob.getName(), BUCKET_NAME);
        }
    }

    /**
     * Measures the execution time of a given task.
     *
     * @param task The Runnable task to execute.
     * @return The execution time in milliseconds.
     */
    public static long measureExecutionTime(Runnable task) {
        // 1. Get the start time in nanoseconds
        long startTime = System.nanoTime();
        // 2. Execute the lambda function
        task.run();
        // 3. Get the end time in nanoseconds
        long endTime = System.nanoTime();
        // 4. Calculate duration and convert to milliseconds
        long durationNanos = endTime - startTime;
        return TimeUnit.NANOSECONDS.toMillis(durationNanos);
    }
}
