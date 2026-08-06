/*
 * Copyright 2026 Google LLC
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

import org.junit.jupiter.api.Test;

class GcsFileInfoTest {

  @Test
  void rootInfo_hasRootUriAndRootItemInfo() {
    GcsFileInfo rootInfo = GcsFileInfo.ROOT_INFO;

    assertThat(rootInfo.getUri()).isEqualTo(GcsFileInfo.GCS_ROOT_URI);
    assertThat(rootInfo.getItemInfo()).isEqualTo(GcsItemInfo.ROOT_INFO);
    assertThat(rootInfo.getAttributes()).isEmpty();
  }
}
