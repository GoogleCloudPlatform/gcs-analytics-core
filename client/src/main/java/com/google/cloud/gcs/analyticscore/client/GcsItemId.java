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

  // Name of the bucket.
  public abstract String getBucketName();

  // Name of the object in the bucket.
  public abstract Optional<String> getObjectName();

  public static Builder builder() {
    return new AutoValue_GcsItemId.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setBucketName(String bucketName);

    public abstract Builder setObjectName(String objectName);

    public abstract GcsItemId build();
  }

  public boolean isGcsObject() {
    return this.getBucketName() != null && !this.getObjectName().isEmpty();
  }
}
