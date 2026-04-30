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

import com.google.cloud.storage.StorageException;
import org.junit.jupiter.api.Test;

public class GcsExceptionUtilTest {

  @Test
  public void testGetErrorType() {
    // Test all branches of the HTTP code switch statement
    assertThat(GcsExceptionUtil.getErrorType(new StorageException(404, "Not Found")))
        .isEqualTo(GcsExceptionUtil.ErrorType.NOT_FOUND);

    assertThat(GcsExceptionUtil.getErrorType(new StorageException(412, "Precondition Failed")))
        .isEqualTo(GcsExceptionUtil.ErrorType.ALREADY_EXISTS);

    assertThat(GcsExceptionUtil.getErrorType(new StorageException(403, "Forbidden")))
        .isEqualTo(GcsExceptionUtil.ErrorType.ACCESS_DENIED);

    assertThat(GcsExceptionUtil.getErrorType(new StorageException(401, "Unauthorized")))
        .isEqualTo(GcsExceptionUtil.ErrorType.ACCESS_DENIED);

    assertThat(GcsExceptionUtil.getErrorType(new StorageException(500, "Internal Error")))
        .isEqualTo(GcsExceptionUtil.ErrorType.UNKNOWN);
  }
}
