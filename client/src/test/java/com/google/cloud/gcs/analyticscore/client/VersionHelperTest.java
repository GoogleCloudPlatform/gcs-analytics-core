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

import static com.google.common.truth.Truth.assertThat;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class VersionHelperTest {

  @Test
  void loadVersion_validPomStream_returnsVersion() {
    String pomContent = "version=1.2.3-TEST\n";
    InputStream inputStream = new ByteArrayInputStream(pomContent.getBytes(StandardCharsets.UTF_8));
    String version = VersionHelper.loadVersion(inputStream);
    assertThat(version).isEqualTo("1.2.3-TEST");
  }

  @Test
  void loadVersion_nullStream_returnsDefault() {
    String version = VersionHelper.loadVersion((InputStream) null);
    assertThat(version).isEqualTo(VersionHelper.DEFAULT_VERSION);
  }
}
