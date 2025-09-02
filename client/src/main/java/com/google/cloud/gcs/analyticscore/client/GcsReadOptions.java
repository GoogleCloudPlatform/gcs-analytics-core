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
import java.util.Optional;

/** Configuration options for the GCS read options. */
@AutoValue
public abstract class GcsReadOptions {

  private static final String GCS_CHANNEL_READ_CHUNK_SIZE = "channel.read.chunk-size-bytes";
  private static final String DECRYPTION_KEY = "decryption.key";
  private static final String PROJECT_ID_KEY = "project.id";

  public abstract Optional<Integer> getChunkSize();

  public abstract Optional<String> getDecryptionKey();

  public abstract Optional<String> getProjectId();

  public abstract GcsVectoredReadOptions getGcsVectoredReadOptions();

  public static Builder builder() {
    return new AutoValue_GcsReadOptions.Builder()
        .setGcsVectoredReadOptions(GcsVectoredReadOptions.builder().build());
  }

  public static GcsReadOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String appendPrefix) {
    GcsReadOptions.Builder optionsBuilder = builder();
    if (analyticsCoreOptions.containsKey(appendPrefix + GCS_CHANNEL_READ_CHUNK_SIZE)) {
      optionsBuilder.setChunkSize(
          Integer.parseInt(analyticsCoreOptions.get(appendPrefix + GCS_CHANNEL_READ_CHUNK_SIZE)));
    }
    if (analyticsCoreOptions.containsKey(appendPrefix + DECRYPTION_KEY)) {
      optionsBuilder.setDecryptionKey(analyticsCoreOptions.get(appendPrefix + DECRYPTION_KEY));
    }
    if (analyticsCoreOptions.containsKey(appendPrefix + PROJECT_ID_KEY)) {
      optionsBuilder.setProjectId(analyticsCoreOptions.get(appendPrefix + PROJECT_ID_KEY));
    }
    optionsBuilder.setGcsVectoredReadOptions(
        GcsVectoredReadOptions.createFromOptions(analyticsCoreOptions, appendPrefix));

    return optionsBuilder.build();
  }

  /** Builder for {@link GcsReadOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setChunkSize(Integer chunkSize);

    public abstract Builder setDecryptionKey(String decryptionKey);

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setGcsVectoredReadOptions(GcsVectoredReadOptions vectoredReadOptions);

    public abstract GcsReadOptions build();
  }
}
