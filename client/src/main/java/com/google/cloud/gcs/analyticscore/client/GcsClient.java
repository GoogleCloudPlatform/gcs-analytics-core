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

import com.google.cloud.gcs.analyticscore.common.BucketCapabilities;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@VisibleForTesting
public interface GcsClient {
  /** Opens a new read channel. */
  VectoredSeekableByteChannel openReadChannel(GcsItemInfo itemInfo, GcsReadOptions readOptions)
      throws IOException;

  /** Opens a new read channel. */
  VectoredSeekableByteChannel openReadChannel(GcsItemId itemId, GcsReadOptions readOptions)
      throws IOException;

  /** Fetches object metadata. */
  GcsItemInfo getGcsItemInfo(GcsItemId itemId) throws IOException;

  /** Lists objects under the given directory. */
  List<GcsItemInfo> listObjects(GcsItemId id) throws IOException;

  /** Deletes a batch of objects specified by their identifiers. */
  void deleteObjects(List<GcsItemId> ids) throws IOException;

  /** Updates the custom metadata for the specified object. */
  void updateObjectMetadata(GcsItemId id, Map<String, byte[]> metadata) throws IOException;

  /** Fetches metadata for a specific folder. */
  GcsItemInfo getFolderMetadata(GcsItemId id) throws IOException;

  /** Creates a new folder with the specified identifier. */
  void createFolder(GcsItemId id) throws IOException;

  /** Deletes the specified folder. */
  void deleteFolder(GcsItemId id) throws IOException;

  /** Renames a folder from the source identifier to the destination identifier. */
  void renameFolder(GcsItemId src, GcsItemId dst) throws IOException;

  /**
   * Probes and returns the capabilities supported by the specified bucket (e.g., whether
   * Hierarchical Namespace is enabled).
   */
  BucketCapabilities getBucketCapabilities(String bucketName) throws IOException;

  /** Close the client. */
  void close();
}
