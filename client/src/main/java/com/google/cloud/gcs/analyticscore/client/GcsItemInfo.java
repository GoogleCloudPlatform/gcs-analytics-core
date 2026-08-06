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
import java.util.Optional;

/** Represents metadata of a GCS Item. */
@AutoValue
public abstract class GcsItemInfo {

  /** Returns the identifier of the GCS item. */
  public abstract GcsItemId getItemId();

  /** Size of an object in bytes. Returns -1 for items that do not exist. */
  public abstract long getSize();

  /** Content type of the object. */
  public abstract Optional<String> getContentType();

  /** Content encoding of the object. */
  public abstract Optional<String> getContentEncoding();

  /** Location of the object. */
  public abstract Optional<String> getLocation();

  /** Storage class of the object. */
  public abstract Optional<String> getStorageClass();

  /** Verification attributes for the object. */
  public abstract Optional<VerificationAttributes> getVerificationAttributes();

  /** Generation ID of the object when the metadata is read. */
  public abstract Optional<Long> getContentGeneration();

  public abstract long getMetaGeneration();

  public enum ItemType {
    /** A standard storage object. */
    OBJECT,
    /**
     * An inferred directory, typically represented by a trailing slash in its name or empty object.
     */
    INFERRED_DIRECTORY,
    /**
     * An explicit directory/folder (e.g., an HNS folder or an explicit folder in a flat bucket).
     */
    EXPLICIT_DIRECTORY,
    /** A GCS bucket. */
    BUCKET,
    /** The global root namespace. */
    ROOT
  }

  /** Returns the type of this item. */
  public abstract ItemType getItemType();

  /** Returns the custom extended attributes (metadata) associated with the item. */
  public abstract ImmutableMap<String, byte[]> getExtendedAttributes();

  /** Returns the creation time of the object in milliseconds since epoch, or 0 if not available. */
  public abstract long getCreationTime();

  /**
   * Returns the modification time of the object in milliseconds since epoch, or 0 if not available.
   */
  public abstract long getModificationTime();

  public boolean isInferredDirectory() {
    return getItemType() == ItemType.INFERRED_DIRECTORY;
  }

  public boolean isExplicitDirectory() {
    return getItemType() == ItemType.EXPLICIT_DIRECTORY;
  }

  public static GcsItemInfo createInferredDirectory(GcsItemId itemId) {
    return builder().setItemId(itemId).setSize(0).setItemType(ItemType.INFERRED_DIRECTORY).build();
  }

  public static GcsItemInfo createBucket(GcsItemId itemId) {
    return builder().setItemId(itemId).setSize(0).setItemType(ItemType.BUCKET).build();
  }

  public static final GcsItemInfo ROOT_INFO = createRoot(GcsItemId.ROOT);

  public static GcsItemInfo createRoot(GcsItemId itemId) {
    return builder().setItemId(itemId).setSize(0).setItemType(ItemType.ROOT).build();
  }

  public abstract Builder toBuilder();

  public static Builder builder() {
    // By default, set size to -1, indicating a non-existent item.
    return new AutoValue_GcsItemInfo.Builder()
        .setSize(-1L)
        .setItemType(ItemType.OBJECT)
        .setExtendedAttributes(ImmutableMap.of())
        .setCreationTime(0L)
        .setModificationTime(0L)
        .setMetaGeneration(0L);
  }

  /** Builder for {@link GcsItemInfo}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setItemId(GcsItemId itemId);

    public abstract Builder setSize(long size);

    public abstract Builder setContentType(String contentType);

    public abstract Builder setContentEncoding(String contentEncoding);

    public abstract Builder setLocation(String location);

    public abstract Builder setStorageClass(String storageClass);

    public abstract Builder setVerificationAttributes(
        VerificationAttributes verificationAttributes);

    public abstract Builder setContentGeneration(long contentGeneration);

    public abstract Builder setMetaGeneration(long metaGeneration);

    public abstract Builder setItemType(ItemType itemType);

    public abstract Builder setExtendedAttributes(ImmutableMap<String, byte[]> extendedAttributes);

    public abstract Builder setCreationTime(long creationTime);

    public abstract Builder setModificationTime(long modificationTime);

    public abstract GcsItemInfo build();
  }
}
