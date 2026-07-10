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

  /** Generation ID of the object when the metadata is read. */
  public abstract Optional<Long> getContentGeneration();

  public enum ItemType {
    /** A standard storage object. */
    OBJECT,
    /**
     * An inferred directory, typically represented by a trailing slash in its name or empty object.
     */
    INFERRED_DIRECTORY,
    /** A native Hierarchical Namespace (HNS) folder. */
    NATIVE_FOLDER
  }

  /** Returns the type of this item. */
  public abstract ItemType getItemType();

  /** Returns the custom extended attributes (metadata) associated with the item. */
  public abstract ImmutableMap<String, byte[]> getExtendedAttributes();

  public boolean isInferredDirectory() {
    return getItemType() == ItemType.INFERRED_DIRECTORY;
  }

  public boolean isNativeHnsFolder() {
    return getItemType() == ItemType.NATIVE_FOLDER;
  }

  public static Builder builder() {
    // By default, set size to -1, indicating a non-existent item.
    return new AutoValue_GcsItemInfo.Builder()
        .setSize(-1L)
        .setItemType(ItemType.OBJECT)
        .setExtendedAttributes(ImmutableMap.of());
  }

  /** Builder for {@link GcsItemInfo}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setItemId(GcsItemId itemId);

    public abstract Builder setSize(long size);

    public abstract Builder setContentGeneration(long contentGeneration);

    public abstract Builder setItemType(ItemType itemType);

    public abstract Builder setExtendedAttributes(ImmutableMap<String, byte[]> extendedAttributes);

    public abstract GcsItemInfo build();
  }
}
