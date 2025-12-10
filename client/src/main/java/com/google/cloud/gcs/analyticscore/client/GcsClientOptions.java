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

/** Configuration options for the GCS client. */
@AutoValue
public abstract class GcsClientOptions {

  private static final String CLIENT_LIB_TOKEN_KEY = "client-lib-token";
  private static final String SERVICE_HOST_KEY = "service.host";
  private static final String USER_AGENT_KEY = "user-agent";
  static final String PROJECT_ID_KEY = "project-id";
  private static final String CLIENT_TYPE_KEY = "client.type";
  private static final String DIRECT_PATH_ENABLED_KEY = "client.grpc.direct-path-enabled";

  /** Cloud Storage client to use. */
  public enum ClientType {
    HTTP_CLIENT,
    GRPC_CLIENT,
  }

  public abstract Optional<String> getProjectId();

  public abstract Optional<String> getClientLibToken();

  public abstract Optional<String> getServiceHost();

  public abstract Optional<String> getUserAgent();

  public abstract ClientType getClientType();

  public abstract boolean isDirectPathEnabled();

  public abstract GcsReadOptions getGcsReadOptions();

  public static Builder builder() {
    return new AutoValue_GcsClientOptions.Builder()
        .setClientType(ClientType.HTTP_CLIENT)
        .setDirectPathEnabled(true)
        .setGcsReadOptions(GcsReadOptions.builder().build());
  }

  public static GcsClientOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    GcsClientOptions.Builder optionsBuilder = builder();
    if (analyticsCoreOptions.containsKey(prefix + PROJECT_ID_KEY)) {
      optionsBuilder.setProjectId(analyticsCoreOptions.get(prefix + PROJECT_ID_KEY));
    }
    if (analyticsCoreOptions.containsKey(prefix + CLIENT_LIB_TOKEN_KEY)) {
      optionsBuilder.setClientLibToken(analyticsCoreOptions.get(prefix + CLIENT_LIB_TOKEN_KEY));
    }
    if (analyticsCoreOptions.containsKey(prefix + SERVICE_HOST_KEY)) {
      optionsBuilder.setServiceHost(analyticsCoreOptions.get(prefix + SERVICE_HOST_KEY));
    }
    if (analyticsCoreOptions.containsKey(prefix + USER_AGENT_KEY)) {
      optionsBuilder.setUserAgent(analyticsCoreOptions.get(prefix + USER_AGENT_KEY));
    }
    if (analyticsCoreOptions.containsKey(prefix + CLIENT_TYPE_KEY)) {
      optionsBuilder.setClientType(
          ClientType.valueOf(
              analyticsCoreOptions.get(prefix + CLIENT_TYPE_KEY).trim().toUpperCase()));
    }
    if (analyticsCoreOptions.containsKey(prefix + DIRECT_PATH_ENABLED_KEY)) {
      optionsBuilder.setDirectPathEnabled(
          Boolean.parseBoolean(analyticsCoreOptions.get(prefix + DIRECT_PATH_ENABLED_KEY)));
    }
    optionsBuilder.setGcsReadOptions(
        GcsReadOptions.createFromOptions(analyticsCoreOptions, prefix));

    return optionsBuilder.build();
  }

  /** Builder for {@link GcsClientOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setClientLibToken(String clientLibToken);

    public abstract Builder setServiceHost(String serviceHost);

    public abstract Builder setUserAgent(String userAgent);

    public abstract Builder setClientType(ClientType clientType);

    public abstract Builder setDirectPathEnabled(boolean directPathEnabled);

    public abstract Builder setGcsReadOptions(GcsReadOptions readOptions);

    public abstract GcsClientOptions build();
  }
}
