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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.annotations.VisibleForTesting;

final class VersionHelper {
  private static final Logger LOG = LoggerFactory.getLogger(VersionHelper.class);
  
  @VisibleForTesting
  static final String PACKAGE_POM_PATH =
      "/META-INF/maven/com.google.cloud.gcs.analytics/client/pom.properties";
  
  @VisibleForTesting
  static final String DEFAULT_VERSION = "unknown";

  static final String VERSION = loadVersion(PACKAGE_POM_PATH);

  private VersionHelper() {}

  static String loadVersion(String pomPath) {
    try (InputStream stream = VersionHelper.class.getResourceAsStream(pomPath)) {
      return loadVersion(stream);
    } catch (IOException e) {
      LOG.warn("Failed to close or access resource stream", e);
      return DEFAULT_VERSION;
    }
  }

  @VisibleForTesting
  static String loadVersion(InputStream stream) {
    String version = DEFAULT_VERSION;
    if (stream != null) {
      try {
        Properties properties = new Properties();
        properties.load(stream);
        version = properties.getProperty("version", DEFAULT_VERSION);
      } catch (IOException e) {
        LOG.warn("Failed to load client version", e);
      }
    }
    return version;
  }
}
