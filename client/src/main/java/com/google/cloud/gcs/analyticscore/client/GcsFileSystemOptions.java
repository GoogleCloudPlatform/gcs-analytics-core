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
import java.util.Map;

/** Configuration options for the GCS File System. */
@AutoValue
public abstract class GcsFileSystemOptions {

  private static final String READ_THREAD_COUNT_KEY = "analytics-core.read.thread.count";
  private static final String CLIENT_TYPE_KEY = "client.type";

  /** Cloud Storage client to use. */
  public enum ClientType {
    HTTP_CLIENT,
    GRPC_CLIENT,
  }

  public abstract int getReadThreadCount();

  public abstract ClientType getClientType();

  public abstract GcsClientOptions getGcsClientOptions();

  public static Builder builder() {
    return new AutoValue_GcsFileSystemOptions.Builder()
        .setReadThreadCount(16)
        .setClientType(ClientType.HTTP_CLIENT)
        .setGcsClientOptions(GcsClientOptions.builder().build());
  }

  public static GcsFileSystemOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    GcsFileSystemOptions.Builder optionsBuilder = GcsFileSystemOptions.builder();
    if (analyticsCoreOptions.containsKey(prefix + READ_THREAD_COUNT_KEY)) {
      optionsBuilder.setReadThreadCount(
          Integer.parseInt(analyticsCoreOptions.get(prefix + READ_THREAD_COUNT_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + CLIENT_TYPE_KEY)) {
      optionsBuilder.setClientType(
          ClientType.valueOf(analyticsCoreOptions.get(prefix + CLIENT_TYPE_KEY)));
    }
    optionsBuilder.setGcsClientOptions(
        GcsClientOptions.createFromOptions(analyticsCoreOptions, prefix));

    return optionsBuilder.build();
  }

  /** Builder for {@link GcsFileSystemOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setClientType(ClientType clientType);

    public abstract Builder setReadThreadCount(int readThreadCount);

    public abstract Builder setGcsClientOptions(GcsClientOptions gcsClientOptions);

    public abstract GcsFileSystemOptions build();
  }
}
