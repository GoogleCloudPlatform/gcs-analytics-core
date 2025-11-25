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

  /** Parses {@link GcsItemId} from specified string. */
  public static GcsItemId getItemIdFromString(String path) {
    checkArgument(path != null, "path should not be null");

    Matcher matcher = GCS_PATH_PATTERN.matcher(path);
    checkArgument(matcher.matches(), "Invalid GCS path: %s", path);

    String bucketName = matcher.group(2);
    String relativePath = matcher.group(4);
    checkArgument(bucketName != null, "GCS path must include a bucket name: %s", path);

    if (relativePath == null) {
      return GcsItemId.builder().setBucketName(bucketName).build();
    }
    return GcsItemId.builder().setBucketName(bucketName).setObjectName(relativePath).build();
  }
}
