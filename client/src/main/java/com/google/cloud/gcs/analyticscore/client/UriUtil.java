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
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Utility class for parsing and formatting GCS URIs.
 *
 * <p>A {@link URI} follows the hadoop-connectors convention: the bucket is the authority, and the
 * object name is the percent-decoded path without its leading {@code /}. {@link #toUri}
 * percent-encodes the characters of a name that are not legal in a URI path, always including
 * {@code %}, {@code ?} and {@code #}, and {@link #getItemIdFromUri} decodes them: the object {@code
 * a b#1.txt} has the URI {@code gs://bucket/a%20b%231.txt}. A string, as in {@link #getStringPath},
 * holds the name literally and is not a URI.
 */
final class UriUtil {

  static final String PATH_DELIMITER = "/";

  private UriUtil() {}

  private static final String GCS_SCHEME = "gs://";

  /** The scheme of a GCS URI, without the {@code ://} that {@link #GCS_SCHEME} ends with. */
  private static final String GCS_SCHEME_NAME = "gs";

  /** Canonical string form of the GCS global root. */
  private static final String GCS_ROOT_PATH = "gs:/";

  private static final boolean[] PLAIN_IN_BUCKET = plainCharacters("-_.~");

  private static final boolean[] PLAIN_IN_PATH = plainCharacters("-_.~/=");

  /**
   * Parses a {@link GcsItemId} from a {@code gs://} URI: the bucket is {@link URI#getAuthority()},
   * and the object name is {@link URI#getPath()}, which is percent-decoded, without its leading
   * {@code /}.
   *
   * @param uri {@code gs:/} for the root, {@code gs://bucket} or {@code gs://bucket/} for a bucket,
   *     otherwise {@code gs://bucket/object}
   * @return the item that {@code uri} names
   * @throws NullPointerException if {@code uri} is {@code null}
   * @throws IllegalArgumentException if the scheme is not {@code gs}, if there is no bucket (as in
   *     {@code gs:///}), if the decoded path has consecutive {@code /} characters, or if {@code
   *     uri} has a query or a fragment
   */
  static GcsItemId getItemIdFromUri(URI uri) {
    checkNotNull(uri, "uri should not be null");
    checkArgument(GCS_SCHEME_NAME.equals(uri.getScheme()), "Invalid GCS path: %s", uri);
    // A '?' or '#' that a caller meant as part of an object name would cut the name short, and the
    // rest of the URI would then name another object.
    checkArgument(
        uri.getRawQuery() == null && uri.getRawFragment() == null,
        "GCS URI must not have a query or a fragment; percent-encode '?' as %s and '#' as %s in an"
            + " object name: %s",
        "%3F",
        "%23",
        uri);
    String bucketName = uri.getAuthority();
    if (bucketName == null) {
      // "gs:///" has the same URI components as the root "gs:/", so compare the strings.
      checkArgument(GCS_ROOT_PATH.equals(uri.toString()), "Invalid GCS path: %s", uri);
      return GcsItemId.ROOT;
    }
    String path = uri.getPath();
    checkArgument(
        !path.contains(PATH_DELIMITER + PATH_DELIMITER),
        "GCS path must not have consecutive '/' characters: %s",
        uri);
    if (path.length() > PATH_DELIMITER.length()) {
      String objectName = path.substring(PATH_DELIMITER.length());
      return GcsItemId.builder().setBucketName(bucketName).setObjectName(objectName).build();
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

  /**
   * Returns the {@code gs://} URI of the given {@link GcsItemId}, built as hadoop-connectors builds
   * it: the characters of the object name that are not legal in a URI path are percent-encoded,
   * always including {@code %}, {@code ?} and {@code #}. {@link #getItemIdFromUri} is the inverse.
   *
   * <p>For an object name made only of characters that are legal in a URI path (so without {@code
   * %}, {@code ?} and {@code #}), the string of the URI equals {@link #getStringPath}. Any content
   * generation is left out. An object name that starts with {@code /} or contains {@code //} gives
   * a URI with consecutive slashes, which {@link #getItemIdFromUri} rejects.
   *
   * @param itemId the item to format
   * @return {@code gs:/} for the root, {@code gs://bucket/} for a bucket, otherwise {@code
   *     gs://bucket/object} with the object name percent-encoded
   * @throws NullPointerException if {@code itemId} is {@code null}
   * @throws IllegalArgumentException if the bucket name can't be the authority of a URI
   */
  static URI toUri(GcsItemId itemId) {
    checkNotNull(itemId, "itemId should not be null");
    if (itemId.isRoot()) {
      return GcsFileInfo.GCS_ROOT_URI;
    }
    String bucketName = itemId.getBucketName();
    String objectName = itemId.getObjectName().orElse("");
    if (isPlain(bucketName, PLAIN_IN_BUCKET) && isPlain(objectName, PLAIN_IN_PATH)) {
      // The constructor below would quote nothing and parse this same text. Parsing it directly
      // skips the quoting pass and avoids allocating an intermediate path string, which matters
      // because opening a stream by item id builds a URI. The path of a bucket is "/", as in
      // hadoop-connectors.
      return URI.create(GCS_SCHEME + bucketName + PATH_DELIMITER + objectName);
    }
    try {
      // Unlike URI.create, this constructor quotes the characters that are illegal in a path, and
      // always '%', so that URI.getPath() returns the object name unchanged.
      return new URI(
          GCS_SCHEME_NAME,
          bucketName,
          PATH_DELIMITER + objectName,
          /* query= */ null,
          /* fragment= */ null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Cannot create a URI for " + getStringPath(itemId), e);
    }
  }

  /**
   * Builds a lookup table of ASCII letters, digits and {@code punctuation}. The {@link URI}
   * constructors leave these characters as they are, in a bucket name and in a path. This is a
   * subset of such characters on purpose: any other character takes the quoting constructor.
   */
  private static boolean[] plainCharacters(String punctuation) {
    boolean[] plain = new boolean[128];
    for (char c = 'a'; c <= 'z'; c++) {
      plain[c] = true;
    }
    for (char c = 'A'; c <= 'Z'; c++) {
      plain[c] = true;
    }
    for (char c = '0'; c <= '9'; c++) {
      plain[c] = true;
    }
    for (int i = 0; i < punctuation.length(); i++) {
      plain[punctuation.charAt(i)] = true;
    }
    return plain;
  }

  private static boolean isPlain(String text, boolean[] plainCharacters) {
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c >= plainCharacters.length || !plainCharacters[c]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Validates that {@code itemId} names a directory within a bucket (with or without a trailing
   * {@code "/"}) whose object name neither starts with {@code "/"} nor contains consecutive {@code
   * "/"} characters.
   *
   * @param itemId the directory identifier to validate
   * @throws NullPointerException if {@code itemId} is {@code null}
   * @throws IllegalArgumentException if {@code itemId} is the root, a bucket, or has an empty
   *     bucket name, or if its object name starts with {@code "/"} or contains consecutive {@code
   *     "/"} characters
   */
  static void validateDirectoryItemId(GcsItemId itemId) {
    checkNotNull(itemId, "itemId should not be null");
    checkArgument(
        itemId.isGcsObject(), "Expected a directory itemId within a bucket but got: %s", itemId);
    String objectName = itemId.getObjectName().get();
    checkArgument(
        !objectName.startsWith(PATH_DELIMITER)
            && !objectName.contains(PATH_DELIMITER + PATH_DELIMITER),
        "Object name cannot start with '/' or contain consecutive '/': %s",
        itemId);
  }

  static String removeTrailingSlash(String path) {
    if (path != null && path.endsWith(PATH_DELIMITER)) {
      return path.substring(0, path.length() - PATH_DELIMITER.length());
    }
    return path;
  }

  static String toDirectoryPath(String path) {
    return isNullOrEmpty(path) || path.endsWith(PATH_DELIMITER) ? path : path + PATH_DELIMITER;
  }
}
