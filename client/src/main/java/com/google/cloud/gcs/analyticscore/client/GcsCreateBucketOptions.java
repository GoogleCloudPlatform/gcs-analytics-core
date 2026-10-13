/*
 * Copyright 2026 Google LLC
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

import com.google.auto.value.AutoValue;
import java.time.Duration;
import java.util.Optional;

/** Options that can be specified when creating a bucket in {@link GcsClient}. */
@AutoValue
public abstract class GcsCreateBucketOptions {

  /** Create bucket with all default settings. */
  public static final GcsCreateBucketOptions DEFAULT = builder().build();

  /** Returns the bucket location. */
  public abstract Optional<String> getLocation();

  /** Returns the bucket storage class. */
  public abstract Optional<String> getStorageClass();

  /** Returns whether Hierarchical Namespace (HNS) is enabled for the bucket. */
  public abstract boolean isHierarchicalNamespaceEnabled();

  /** Returns the bucket retention period (TTL). */
  public abstract Optional<Duration> getTtl();

  /** Returns the zone for a zonal bucket. */
  public abstract Optional<String> getZonalPlacement();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_GcsCreateBucketOptions.Builder().setHierarchicalNamespaceEnabled(false);
  }

  /** Builder for {@link GcsCreateBucketOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setLocation(String location);

    public abstract Builder setStorageClass(String storageClass);

    public abstract Builder setHierarchicalNamespaceEnabled(boolean enabled);

    public abstract Builder setTtl(Duration ttl);

    public abstract Builder setZonalPlacement(String zonalPlacement);

    abstract GcsCreateBucketOptions autoBuild();

    public GcsCreateBucketOptions build() {
      GcsCreateBucketOptions options = autoBuild();
      options
          .getTtl()
          .ifPresent(
              ttl ->
                  checkArgument(ttl.toDays() >= 1, "TTL must be at least 1 day, but got: %s", ttl));
      return options;
    }
  }
}
