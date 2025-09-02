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

  private static final String PROJECT_ID_KEY = "project-id";
  private static final String CLIENT_LIB_TOKEN_KEY = "client-lib-token";
  private static final String SERVICE_HOST_KEY = "service.host";
  private static final String USER_AGENT_KEY = "user-agent.key";

  public abstract Optional<String> getProjectId();

  public abstract Optional<String> getClientLibToken();

  public abstract Optional<String> getServiceHost();

  public abstract Optional<String> getUserAgent();

  public static Builder builder() {
    return new AutoValue_GcsClientOptions.Builder();
  }

  public static GcsClientOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String appendPrefix) {
    GcsClientOptions.Builder optionsBuilder = builder();
    if (analyticsCoreOptions.containsKey(appendPrefix + PROJECT_ID_KEY)) {
      optionsBuilder.setProjectId(analyticsCoreOptions.get(appendPrefix + PROJECT_ID_KEY));
    }
    if (analyticsCoreOptions.containsKey(appendPrefix + CLIENT_LIB_TOKEN_KEY)) {
      optionsBuilder.setClientLibToken(
          analyticsCoreOptions.get(appendPrefix + CLIENT_LIB_TOKEN_KEY));
    }
    if (analyticsCoreOptions.containsKey(appendPrefix + SERVICE_HOST_KEY)) {
      optionsBuilder.setServiceHost(analyticsCoreOptions.get(appendPrefix + SERVICE_HOST_KEY));
    }
    if (analyticsCoreOptions.containsKey(appendPrefix + USER_AGENT_KEY)) {
      optionsBuilder.setUserAgent(analyticsCoreOptions.get(appendPrefix + USER_AGENT_KEY));
    }

    return optionsBuilder.build();
  }

  /** Builder for {@link GcsClientOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setClientLibToken(String clientLibToken);

    public abstract Builder setServiceHost(String serviceHost);

    public abstract Builder setUserAgent(String userAgent);

    public abstract GcsClientOptions build();
  }
}
