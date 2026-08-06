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
    return this.getBucketName() != null
        && this.getObjectName().isPresent()
        && !this.getObjectName().get().isEmpty();
  }

  /**
   * Returns true if this identifier represents a GCS bucket.
   *
   * <p>An item is a bucket if it has a non-empty bucket name and no object path (the object name is
   * either absent or empty).
   */
  public boolean isBucket() {
    return this.getBucketName() != null
        && !this.getBucketName().isEmpty()
        && (this.getObjectName().isEmpty() || this.getObjectName().get().isEmpty());
  }
}
