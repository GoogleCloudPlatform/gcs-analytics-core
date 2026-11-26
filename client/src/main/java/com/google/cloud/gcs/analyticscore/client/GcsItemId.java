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

import com.google.auto.value.AutoValue;
import java.net.URI;
import java.util.Optional;

/** Represents an item identifier for a resource within Google Cloud Storage. */
@AutoValue
public abstract class GcsItemId {

  public static final GcsItemId ROOT = builder().setBucketName("").setObjectName("").build();

  // Name of the bucket.
  public abstract String getBucketName();

  // Name of the object in the bucket.
  public abstract Optional<String> getObjectName();

  // Returns objects's content generation, used for versioning.
  public abstract Optional<Long> getContentGeneration();

  public static Builder builder() {
    return new AutoValue_GcsItemId.Builder();
  }

  /**
   * Parses the identifier that the given {@code gs://} URI names.
   *
   * <p>The bucket is the authority of the URI, and the object name is its percent-decoded path
   * ({@link URI#getPath()}) without the leading {@code /}: {@code gs://bucket/a%20b.txt} names the
   * object {@code a b.txt}. {@link #toUri()} is the inverse.
   *
   * @param uri {@code gs:/} for the root, {@code gs://bucket} or {@code gs://bucket/} for a bucket,
   *     otherwise {@code gs://bucket/object}
   * @return the identifier that {@code uri} names, without a content generation
   * @throws NullPointerException if {@code uri} is {@code null}
   * @throws IllegalArgumentException if {@code uri} is not a valid {@code gs://} URI (for example,
   *     {@code gs:///} or consecutive slashes), or if it has a query or a fragment; write a {@code
   *     ?} or {@code #} of an object name as {@code %3F} or {@code %23}
   */
  public static GcsItemId fromUri(URI uri) {
    return UriUtil.getItemIdFromUri(uri);
  }

  /**
   * Returns the {@code gs://} URI of this identifier: {@code gs:/} for the root, {@code
   * gs://bucket/} for a bucket, otherwise {@code gs://bucket/object}.
   *
   * <p>Characters of the object name that are not legal in a URI path are percent-encoded, always
   * including {@code %}, {@code ?} and {@code #}: the object {@code a b.txt} has the URI {@code
   * gs://bucket/a%20b.txt}. {@link #fromUri(URI)} is the inverse, except for an object name that
   * starts with {@code /} or contains {@code //}: its URI has consecutive slashes, which {@link
   * #fromUri(URI)} rejects. Any content generation is left out.
   *
   * @return the URI of this identifier
   * @throws IllegalArgumentException if the bucket name can't be the authority of a URI
   */
  public URI toUri() {
    return UriUtil.toUri(this);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setBucketName(String bucketName);

    public abstract Builder setObjectName(String objectName);

    public abstract Builder setContentGeneration(Long contentGeneration);

    public abstract GcsItemId build();
  }

  /**
   * Returns true if this identifier represents a GCS object/folder within a bucket.
   *
   * <p>An item is a GCS object if it has a non-empty bucket name and a present, non-empty object
   * name.
   */
  public boolean isGcsObject() {
    return !this.getBucketName().isEmpty()
        && this.getObjectName().isPresent()
        && !this.getObjectName().get().isEmpty();
  }

  /**
   * Returns true if this identifier represents the GCS global root.
   *
   * <p>An item is root if it has an empty bucket name and no object path (the object name is either
   * absent or empty).
   */
  public boolean isRoot() {
    return this.getBucketName().isEmpty()
        && (this.getObjectName().isEmpty() || this.getObjectName().get().isEmpty());
  }

  /**
   * Returns true if this identifier represents a GCS bucket.
   *
   * <p>An item is a bucket if it has a non-empty bucket name and no object path (the object name is
   * either absent or empty).
   */
  public boolean isBucket() {
    return !this.getBucketName().isEmpty()
        && (this.getObjectName().isEmpty() || this.getObjectName().get().isEmpty());
  }

  /**
   * Returns true if this identifier represents a directory (root, a bucket, or an object path
   * ending with a trailing slash).
   */
  public boolean isDirectory() {
    return isRoot() || isBucket() || this.getObjectName().orElse("").endsWith("/");
  }

  /**
   * Resolves the {@link PathType} of this identifier based on its string format (e.g., trailing
   * slash).
   */
  PathType resolvePathType() {
    if (isRoot()) {
      return PathType.ROOT;
    }
    if (isBucket()) {
      return PathType.BUCKET;
    }
    if (getObjectName().orElse("").endsWith("/")) {
      return PathType.DIRECTORY;
    }
    return PathType.UNKNOWN;
  }

  /** Returns a new {@link GcsItemId} with the object name formatted as a directory path. */
  GcsItemId toDirectoryId() {
    if (isRoot() || isBucket()) {
      return this;
    }
    return builder()
        .setBucketName(getBucketName())
        .setObjectName(UriUtil.toDirectoryPath(getObjectName().orElse("")))
        .build();
  }
}
