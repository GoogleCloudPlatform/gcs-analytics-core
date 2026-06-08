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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;

/** Centralized utility for classifying GCS transport exceptions. */
class GcsExceptionUtil {

  private GcsExceptionUtil() {}

  enum ErrorType {
    NOT_FOUND,
    ALREADY_EXISTS,
    PRECONDITION_FAILED,
    ACCESS_DENIED,
    UNKNOWN
  }

  /** Determines the logical error type from a StorageException. */
  static ErrorType getErrorType(StorageException e) {
    switch (e.getCode()) {
      case HttpURLConnection.HTTP_NOT_FOUND: // 404
        return ErrorType.NOT_FOUND;
      case HttpURLConnection.HTTP_CONFLICT: // 409
        return ErrorType.ALREADY_EXISTS;
      case HttpURLConnection.HTTP_PRECON_FAILED: // 412
        return ErrorType.PRECONDITION_FAILED;
      case HttpURLConnection.HTTP_FORBIDDEN: // 403
      case HttpURLConnection.HTTP_UNAUTHORIZED: // 401
        return ErrorType.ACCESS_DENIED;
      default:
        return ErrorType.UNKNOWN;
    }
  }

  /** Translates a StorageException into a standard Java IOException subclass. */
  static IOException translateException(
      StorageException e,
      String context,
      String bucket,
      String name,
      Long generation,
      boolean overwriteExisting,
      long position) {
    ErrorType errorType = getErrorType(e);

    switch (errorType) {
      case NOT_FOUND:
        return (FileNotFoundException)
            new FileNotFoundException(
                    String.format(
                        "Location does not exist or generation not found: gs://%s/%s",
                        bucket, name))
                .initCause(e);

      case ACCESS_DENIED:
        return (AccessDeniedException)
            new AccessDeniedException(
                    String.format("gs://%s/%s", bucket, name),
                    null,
                    String.format("Access denied to object during %s: %s", context, e.getMessage()))
                .initCause(e);

      case ALREADY_EXISTS:
        return (FileAlreadyExistsException)
            new FileAlreadyExistsException(
                    String.format("Object gs://%s/%s already exists.", bucket, name))
                .initCause(e);

      case PRECONDITION_FAILED:
        if (!overwriteExisting) {
          return (FileAlreadyExistsException)
              new FileAlreadyExistsException(
                      String.format("Object gs://%s/%s already exists.", bucket, name))
                  .initCause(e);
        } else if (generation != null) {
          return new IOException(
              String.format(
                  "Generation mismatch for object gs://%s/%s. The file may have been modified concurrently.",
                  bucket, name),
              e);
        }
        break;
      default:
        break;
    }

    return new IOException(
        String.format(
            "Error during %s to GCS for gs://%s/%s at position %d",
            context, bucket, name, position),
        e);
  }

  /** Extracts a StorageException from a Throwable if present directly or as a cause. */
  static StorageException getStorageException(Throwable t) {
    if (t instanceof StorageException) {
      return (StorageException) t;
    }
    if (t instanceof IOException && t.getCause() instanceof StorageException) {
      return (StorageException) t.getCause();
    }
    return null;
  }

  /** Translates a generic Exception, handling unwrapped StorageExceptions. */
  static IOException translateException(
      Exception e,
      String context,
      String bucket,
      String name,
      Long generation,
      boolean overwriteExisting,
      long position) {
    StorageException se = getStorageException(e);
    if (se != null) {
      return translateException(se, context, bucket, name, generation, overwriteExisting, position);
    }
    if (e instanceof IOException) {
      return (IOException) e;
    }
    return new IOException(
        String.format(
            "Error during %s to GCS for gs://%s/%s at position %d",
            context, bucket, name, position),
        e);
  }
}
