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
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.Map;

/**
 * Contains information about a GCS file.
 *
 * <p>Note: This class wraps GcsItemInfo, adds file system specific information and hides
 * bucket/object specific information.
 */
@AutoValue
public abstract class GcsFileInfo {

  public static final URI GCS_ROOT_URI = URI.create("gs:/");

  public static final GcsFileInfo ROOT_INFO =
      builder()
          .setItemInfo(GcsItemInfo.ROOT_INFO)
          .setUri(GCS_ROOT_URI)
          .setAttributes(ImmutableMap.of())
          .build();

  public abstract GcsItemInfo getItemInfo();

  /**
   * Gets the {@code gs://} URI of this file or directory.
   *
   * <p>The URIs that this library builds are {@code gs:/} for the root, {@code gs://bucket/} for a
   * bucket, and {@code gs://bucket/object} otherwise. In them, the characters of the object name
   * that are not legal in a URI path are percent-encoded, always including {@code %}, {@code ?} and
   * {@code #}: the object {@code a b.txt} has the URI {@code gs://bucket/a%20b.txt}. {@link
   * URI#getPath()} returns the decoded name after a leading {@code /}; {@link #getItemInfo()} has
   * the name as well.
   */
  public abstract URI getUri();

  /**
   * Retrieve file attributes for this file.
   *
   * @return A map of file attributes
   */
  public abstract ImmutableMap<String, byte[]> getAttributes();

  /** Indicates whether this file or directory exists. */
  public boolean exists() {
    return getItemInfo().exists();
  }

  /**
   * Creates a not-found {@link GcsFileInfo} for the given {@code gs://} URI.
   *
   * <p>{@code uri} is parsed as in {@link GcsItemId#fromUri(URI)} (its path is percent-decoded) and
   * stored as-is for {@link #getUri()}.
   *
   * @param uri the {@code gs://} URI of the missing item
   * @return a {@link GcsFileInfo} whose {@link #exists()} is {@code false}
   * @throws NullPointerException if {@code uri} is {@code null}
   * @throws IllegalArgumentException if {@code uri} is not a valid {@code gs://} URI or has a query
   *     or a fragment
   */
  public static GcsFileInfo createNotFound(URI uri) {
    GcsItemId itemId = UriUtil.getItemIdFromUri(uri);
    return builder()
        .setItemInfo(GcsItemInfo.createNotFound(itemId))
        .setUri(uri)
        .setAttributes(ImmutableMap.of())
        .build();
  }

  /**
   * Creates a not-found {@link GcsFileInfo} for the given {@link GcsItemId}, building {@link
   * #getUri()} as in {@link GcsItemId#toUri()}.
   *
   * @param itemId the identifier of the missing item
   * @return a {@link GcsFileInfo} whose {@link #exists()} is {@code false}
   * @throws NullPointerException if {@code itemId} is {@code null}
   * @throws IllegalArgumentException if the bucket name can't be the authority of a URI
   */
  public static GcsFileInfo createNotFound(GcsItemId itemId) {
    return builder()
        .setItemInfo(GcsItemInfo.createNotFound(itemId))
        .setUri(UriUtil.toUri(itemId))
        .setAttributes(ImmutableMap.of())
        .build();
  }

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_GcsFileInfo.Builder();
  }

  /** Builder for {@link GcsFileInfo}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setItemInfo(GcsItemInfo itemInfo);

    public abstract Builder setUri(URI uri);

    public abstract Builder setAttributes(Map<String, byte[]> attributes);

    public abstract GcsFileInfo build();
  }
}
