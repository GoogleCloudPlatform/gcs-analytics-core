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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.nio.charset.StandardCharsets;

public class StorageTestUtils {

  public static void createBlobInStorage(Storage storage, BlobId blobId, String content) {
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, content.getBytes(StandardCharsets.UTF_8));
  }

  public static void createBlobInStorage(Storage storage, GcsItemId itemId, String content) {
    BlobId blobId = BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L);
    createBlobInStorage(storage, blobId, content);
  }
}
