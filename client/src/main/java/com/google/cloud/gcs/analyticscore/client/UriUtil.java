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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utility class for parsing and formatting GCS URIs. */
final class UriUtil {

  private UriUtil() {}

  // Pattern that parses out bucket and object names.
  // Given 'gs://foo-bucket/foo/bar/baz', matcher.group(x) will return:
  // 0 = gs://foo-bucket/foo/bar/baz
  // 1 = foo-bucket/foo/bar/baz
  // 2 = foo-bucket
  // 3 = /foo/bar/baz
  // 4 = foo/bar/baz
  // Groups 2(bucket) and 4(objects) can be used to create an instance.
  private static final Pattern GCS_PATH_PATTERN = Pattern.compile("gs://(([^/]+)(/(.+)?)?)?");

  private static final String GCS_SCHEME = "gs://";

  /** Canonical string form of the GCS global root. */
  private static final String GCS_ROOT_PATH = "gs:/";

  /** Parses {@link GcsItemId} from specified string. */
  static GcsItemId getItemIdFromString(String path) {
    checkArgument(path != null, "path should not be null");

    if (path.equals(GCS_ROOT_PATH)) {
      return GcsItemId.ROOT;
    }

    Matcher matcher = GCS_PATH_PATTERN.matcher(path);
    checkArgument(matcher.matches(), "Invalid GCS path: %s", path);
    checkArgument(
        !path.substring(5).contains("//"),
        "GCS path must not have consecutive '/' characters: %s",
        path);

    String bucketName = matcher.group(2);
    String relativePath = matcher.group(4);
    if (bucketName == null) {
      return GcsItemId.ROOT;
    } else if (relativePath != null) {
      return GcsItemId.builder().setBucketName(bucketName).setObjectName(relativePath).build();
    }
    return GcsItemId.builder().setBucketName(bucketName).build();
  }

  /**
   * Formats the given {@link GcsItemId} as a {@code gs://} path string.
   *
   * @param itemId the item to format
   * @return {@code gs:/} for the root, {@code gs://bucket} for a bucket, otherwise {@code
   *     gs://bucket/object}
   */
  static String getStringPath(GcsItemId itemId) {
    checkArgument(itemId != null, "itemId should not be null");

    if (itemId.isRoot()) {
      return GCS_ROOT_PATH;
    }
    if (itemId.isBucket()) {
      return GCS_SCHEME + itemId.getBucketName();
    }
    return GCS_SCHEME + itemId.getBucketName() + "/" + itemId.getObjectName().orElse("");
  }

  static String removeTrailingSlash(String path) {
    if (path != null && path.endsWith("/")) {
      return path.substring(0, path.length() - 1);
    }
    return path;
  }

  static String toDirectoryPath(String path) {
    if (path != null && !path.endsWith("/")) {
      return path + "/";
    }
    return path;
  }
}
