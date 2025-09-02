package com.google.cloud.gcs.analyticscore.core;

import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class GcsAnalyticsCoreOptions {
  private String appendPrefix;
  private ImmutableMap<String, String> analyticsCoreOptions;

  public GcsAnalyticsCoreOptions(String appendPrefix, Map<String, String> analyticsCoreOptions) {
    this.appendPrefix = appendPrefix;
    this.analyticsCoreOptions = ImmutableMap.copyOf(analyticsCoreOptions);
  }

  public GcsFileSystemOptions getFileSystemOptions() {
    return GcsFileSystemOptions.createFromOptions(analyticsCoreOptions, appendPrefix);
  }
}
