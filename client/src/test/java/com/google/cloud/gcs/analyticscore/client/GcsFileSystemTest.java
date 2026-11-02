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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URI;
import org.junit.jupiter.api.Test;

/** Tests the default methods of {@link GcsFileSystem}. */
class GcsFileSystemTest {

  private static final URI DIR_URI = URI.create("gs://test-bucket/dir/");
  private static final GcsItemId DIR_ITEM_ID =
      GcsItemId.builder().setBucketName("test-bucket").setObjectName("dir/").build();

  @Test
  void mkdirs_itemIdNotOverridden_throwsUnsupportedOperationException() {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class, CALLS_REAL_METHODS);

    UnsupportedOperationException e =
        assertThrows(UnsupportedOperationException.class, () -> fileSystem.mkdirs(DIR_ITEM_ID));
    assertThat(e).hasMessageThat().startsWith("mkdirs is not supported by ");
  }

  @Test
  void mkdirs_uri_delegatesToItemIdOverload() throws IOException {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class, CALLS_REAL_METHODS);
    doNothing().when(fileSystem).mkdirs(DIR_ITEM_ID);

    fileSystem.mkdirs(DIR_URI);

    verify(fileSystem).mkdirs(DIR_ITEM_ID);
  }

  @Test
  void mkdirs_uriWithItemIdNotOverridden_throwsUnsupportedOperationException() {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class, CALLS_REAL_METHODS);

    assertThrows(UnsupportedOperationException.class, () -> fileSystem.mkdirs(DIR_URI));
  }

  @Test
  void mkdirs_nullUri_throwsNullPointerException() {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class, CALLS_REAL_METHODS);

    assertThrows(NullPointerException.class, () -> fileSystem.mkdirs((URI) null));
  }
}
