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
import com.google.common.io.BaseEncoding;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Represents metadata of a GCS Item. */
@AutoValue
public abstract class GcsItemInfo {
  private static final Logger LOG = LoggerFactory.getLogger(GcsItemInfo.class);

  public static final GcsItemInfo ROOT_INFO = createRoot(GcsItemId.ROOT);

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

  /** Storage class of the object or default storage class of the bucket. */
  public abstract Optional<String> getStorageClass();

  /** Verification attributes for the object. */
  public abstract Optional<VerificationAttributes> getVerificationAttributes();

  /** Generation ID of the object when the metadata is read. */
  public abstract Optional<Long> getContentGeneration();

  public abstract long getMetaGeneration();

  public enum ItemType {
    /** A standard storage object. */
    OBJECT,
    /** An inferred directory, which only exists because of other objects with this prefix */
    INFERRED_DIRECTORY,
    /** A 0-byte placeholder object created in a flat bucket to explicitly represent a directory */
    PLACEHOLDER_DIRECTORY,
    /** A native directory resource in a Hierarchical Namespace (HNS) enabled bucket */
    NATIVE_FOLDER,
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

  public static GcsItemInfo createFolder(
      GcsItemId itemId, long creationTime, long modificationTime, long metaGeneration) {
    return builder()
        .setItemId(itemId)
        .setSize(0)
        .setItemType(ItemType.NATIVE_FOLDER)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setMetaGeneration(metaGeneration)
        .build();
  }

  public static GcsItemInfo createInferredDirectory(GcsItemId itemId) {
    return builder().setItemId(itemId).setSize(0).setItemType(ItemType.INFERRED_DIRECTORY).build();
  }

  public static GcsItemInfo createPlaceholderDirectory(GcsItemId itemId) {
    return builder()
        .setItemId(itemId)
        .setSize(0)
        .setItemType(ItemType.PLACEHOLDER_DIRECTORY)
        .build();
  }

  public static GcsItemInfo createBucket(GcsItemId itemId) {
    return builder().setItemId(itemId).setSize(0).setItemType(ItemType.BUCKET).build();
  }

  public static GcsItemInfo createRoot(GcsItemId itemId) {
    return builder().setItemId(itemId).setSize(0).setItemType(ItemType.ROOT).build();
  }

  public static GcsItemInfo createNotFound(GcsItemId itemId) {
    return builder().setItemId(itemId).setSize(-1L).build();
  }

  /** Indicates whether this item exists. */
  public boolean exists() {
    return getSize() >= 0;
  }

  /**
   * @deprecated Use {@link #getItemType()} and check against {@link ItemType#INFERRED_DIRECTORY}
   *     instead.
   */
  @Deprecated
  public boolean isInferredDirectory() {
    return getItemType() == ItemType.INFERRED_DIRECTORY;
  }

  /**
   * @deprecated Use {@link #getItemType()} and check against {@link ItemType#NATIVE_FOLDER}
   *     instead.
   */
  @Deprecated
  public boolean isNativeHnsFolder() {
    return getItemType() == ItemType.NATIVE_FOLDER;
  }

  public static ImmutableMap<String, String> encodeMetadata(ImmutableMap<String, byte[]> metadata) {
    ImmutableMap.Builder<String, String> encoded = ImmutableMap.builder();
    metadata.forEach((k, v) -> encoded.put(k, BaseEncoding.base64().encode(v)));
    return encoded.build();
  }

  public static ImmutableMap<String, byte[]> decodeMetadata(Map<String, String> metadata) {
    if (metadata == null || metadata.isEmpty()) {
      return ImmutableMap.of();
    }
    ImmutableMap.Builder<String, byte[]> decoded = ImmutableMap.builder();
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      String key = entry.getKey();
      if (key != null) {
        String value = entry.getValue();
        try {
          byte[] decodedValue = (value == null) ? new byte[0] : BaseEncoding.base64().decode(value);
          decoded.put(key, decodedValue);
        } catch (IllegalArgumentException e) {
          LOG.error("Failed to parse base64 encoded attribute value for key {}: {}", key, value, e);
        }
      }
    }
    return decoded.build();
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
