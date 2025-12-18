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
import java.util.Map;

/**
 * Contains information about a GCS file.
 *
 * <p>Note: This class wraps GcsItemInfo, adds file system specific information and hides
 * bucket/object specific information.
 */
@AutoValue
public abstract class GcsFileInfo {

  public abstract GcsItemInfo getItemInfo();

  /** Gets the path of this file or directory. */
  public abstract URI getUri();

  /**
   * Retrieve file attributes for this file.
   *
   * @return A map of file attributes
   */
  @SuppressWarnings("AutoValueImmutableFields")
  public abstract Map<String, byte[]> getAttributes();

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
