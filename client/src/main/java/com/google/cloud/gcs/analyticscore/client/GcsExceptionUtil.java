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

import com.google.cloud.storage.StorageException;
import java.net.HttpURLConnection;

/** Centralized utility for classifying GCS transport exceptions. */
public class GcsExceptionUtil {

  public enum ErrorType {
    NOT_FOUND,
    ALREADY_EXISTS,
    ACCESS_DENIED,
    UNKNOWN
  }

  /** Determines the logical error type from a StorageException. */
  public static ErrorType getErrorType(StorageException e) {
    switch (e.getCode()) {
      case HttpURLConnection.HTTP_NOT_FOUND: // 404
        return ErrorType.NOT_FOUND;
      case HttpURLConnection.HTTP_PRECON_FAILED: // 412
        return ErrorType.ALREADY_EXISTS;
      case HttpURLConnection.HTTP_FORBIDDEN: // 403
      case HttpURLConnection.HTTP_UNAUTHORIZED: // 401
        return ErrorType.ACCESS_DENIED;
      default:
        return ErrorType.UNKNOWN;
    }
  }
}
