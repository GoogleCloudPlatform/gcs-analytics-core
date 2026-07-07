/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class GcsFileSystemOptionsTest {

  private static final long KB = 1024L;
  private static final long MB = 1024L * KB;

  @Test
  void createFromOptions_withValidProperties_shouldCreateCorrectOptions() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "fs.gs.project-id", "test-project",
            "fs.gs.client.type", "GRPC_CLIENT",
            "fs.gs.analytics-core.read.thread.count", "32");

    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    assertThat(options.getGcsClientOptions().getProjectId().get()).isEqualTo("test-project");
    assertThat(options.getClientType()).isEqualTo(GcsFileSystemOptions.ClientType.GRPC_CLIENT);
    assertThat(options.getReadThreadCount()).isEqualTo(32);
  }

  @Test
  void createFromOptions_cacheProperties_createsCorrectOptions() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "fs.gs.analytics-core.footer.cache.enabled",
            "false",
            "fs.gs.analytics-core.footer.cache.max-size-bytes",
            String.valueOf(500 * MB),
            "fs.gs.analytics-core.small-file.cache.enabled",
            "true",
            "fs.gs.analytics-core.small-file.cache.max-size-bytes",
            String.valueOf(200 * MB));

    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    GcsCacheOptions cacheOptions = options.getGcsCacheOptions();
    assertThat(cacheOptions.isFooterCacheEnabled()).isFalse();
    assertThat(cacheOptions.getFooterCacheMaxSizeBytes()).isEqualTo(500 * MB);
    assertThat(cacheOptions.isSmallObjectCacheEnabled()).isTrue();
    assertThat(cacheOptions.getSmallObjectCacheMaxSizeBytes()).isEqualTo(200 * MB);
  }

  @Test
  void builder_withFewProcessors_setsDefaultThreadCountTo16() {
    try (MockedStatic<GcsFileSystemOptions> mockedStatic =
        mockStatic(GcsFileSystemOptions.class, CALLS_REAL_METHODS)) {
      mockedStatic.when(GcsFileSystemOptions::getAvailableProcessors).thenReturn(2);

      GcsFileSystemOptions options = GcsFileSystemOptions.builder().build();

      assertThat(options.getReadThreadCount()).isEqualTo(16);
    }
  }

  @Test
  void builder_withManyProcessors_scalesDefaultThreadCount() {
    try (MockedStatic<GcsFileSystemOptions> mockedStatic =
        mockStatic(GcsFileSystemOptions.class, CALLS_REAL_METHODS)) {
      mockedStatic.when(GcsFileSystemOptions::getAvailableProcessors).thenReturn(10);

      GcsFileSystemOptions options = GcsFileSystemOptions.builder().build();

      assertThat(options.getReadThreadCount()).isEqualTo(40);
    }
  }
}
