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

import com.google.cloud.storage.Blob;

import java.io.*;
import java.net.URI;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegrationTestHelper {
    public static final String TPCDS_CUSTOMER_SMALL_FILE = "tpcds_customer_small.parquet";
    public static final String TPCDS_CUSTOMER_MEDIUM_FILE = "tpcds_customer_medium.parquet";
    public static final String TPCDS_CUSTOMER_LARGE_FILE = "tpcds_customer_large.parquet";

    public static final String BUCKET_NAME = System.getProperty("gcs.integration.test.bucket");
    public static final String PROJECT_ID = System.getProperty("gcs.integration.test.project-id");
    private static final String FOLDER_NAME  = getFolderName();

    private static final Logger logger = LoggerFactory.getLogger(IntegrationTestHelper.class);
    private static final Storage storage = StorageOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();

    /**
     * Constructs a GCS URI for a given file name in the test bucket and folder.
     *
     * @param fileName The name of the file.
     * @return The GCS URI for the file.
     */
    public static URI getGcsObjectUriForFile(String fileName) {
        return URI.create(BlobId.of(BUCKET_NAME, FOLDER_NAME + fileName).toGsUtilUri());
    }

    /**
     * Uploads a single file to the GCS test bucket.
     *
     * @param file The file to upload.
     * @throws IOException if an I/O error occurs during file reading.
     */
    public static void uploadFileToGcs(File file) throws FileNotFoundException, IOException {
        try(InputStream inputStream =  new FileInputStream(file)) {
            uploadFileToGcs(inputStream, file.getName());
        }
    }

    /**
     * Uploads content of inputStream to GCS test bucket as fileName.
     * @param inputStream InputStream to fetch content from
     * @param fileName the name of the GCS object
     */
    public static void uploadFileToGcs(InputStream inputStream, String fileName) {
        BlobId blobId = BlobId.of(BUCKET_NAME, FOLDER_NAME + fileName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        try {
            storage.create(blobInfo, inputStream);
            logger.info("Successfully uploaded file {} to bucket {}", fileName, BUCKET_NAME);
        } catch (StorageException e) {
            logger.error("Failed to upload file {} to bucket {}", fileName, BUCKET_NAME, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks if an object is present in the GCS test bucket folder.
     *
     * @param objectName The name of the object.
     * @return True if the object exists, false otherwise.
     */
    public static boolean objectPresentInBucket(String objectName) {
        BlobId blobId = BlobId.of(BUCKET_NAME, FOLDER_NAME + objectName);
        Blob blob = storage.get(blobId);
        return blob != null && blob.exists();
    }

    /**
     * Uploads sample Parquet files (small, medium, large) to the GCS test bucket if they do not
     * already exist. The files are generated using {@link TpcdsCustomerParquetWriter}.
     *
     * @throws IOException if an I/O error occurs during file generation or upload.
     */
    public static void uploadSampleParquetFilesIfNotExists() throws IOException {
        TpcdsCustomerParquetWriter writer = new TpcdsCustomerParquetWriter();
        uploadParquetFileIfNotExists(writer, TPCDS_CUSTOMER_SMALL_FILE, 100000, "small");
        uploadParquetFileIfNotExists(writer, TPCDS_CUSTOMER_MEDIUM_FILE, 1000000, "medium");
        uploadParquetFileIfNotExists(writer, TPCDS_CUSTOMER_LARGE_FILE, 10000000, "large");
    }

    private static void uploadParquetFileIfNotExists(
            TpcdsCustomerParquetWriter writer, String fileName, int recordCount, String sizeLabel)
            throws IOException {
        if (!IntegrationTestHelper.objectPresentInBucket(fileName)) {
            logger.info("Uploading {} size parquet files to Google Cloud Storage", sizeLabel);
            File generatedFile = writer.createSampleParquetFile(recordCount, fileName);
            try {
                IntegrationTestHelper.uploadFileToGcs(generatedFile);
            }  finally {
                generatedFile.delete();
            }
        }
    }

    private static String getFolderName() {
        String folderName = System.getProperty("gcs.integration.test.bucket.folder");
        if (folderName == null) { return "test/"; }
        return folderName.endsWith("/") ? folderName : folderName + "/";
    }
}
