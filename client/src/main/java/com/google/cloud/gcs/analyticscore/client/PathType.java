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

import static com.google.common.base.Preconditions.checkNotNull;

/** Represents the type of a path based on its string format (e.g., trailing slash, extensions). */
public enum PathType {
  /** A path that is definitively known to be a file/object. */
  FILE,
  /** A path that is definitively known to be a directory (e.g., ends with a trailing slash). */
  DIRECTORY,
  /** A path that represents a bucket (no object name). */
  BUCKET,
  /** A path that represents the root namespace (no bucket name). */
  ROOT,
  /** The path type cannot be definitively determined from its format. */
  UNKNOWN;

  public static PathType resolve(GcsItemId itemId) {
    checkNotNull(itemId, "itemId cannot be null");
    if (itemId.getBucketName().isEmpty()) {
      return ROOT;
    }
    if (itemId.isBucket()) {
      return BUCKET;
    }
    String objectName = itemId.getObjectName().get();
    if (objectName.endsWith("/")) {
      return DIRECTORY;
    }
    String lower = objectName.toLowerCase(java.util.Locale.US);
    if (lower.endsWith(".parquet")
        || lower.endsWith(".csv")
        || lower.endsWith(".json")
        || lower.endsWith(".avro")
        || lower.endsWith(".orc")
        || lower.endsWith(".txt")) {
      return FILE;
    }
    return UNKNOWN;
  }
}
