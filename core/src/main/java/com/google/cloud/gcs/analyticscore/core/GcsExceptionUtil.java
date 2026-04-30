package com.google.cloud.gcs.analyticscore.core;

import com.google.cloud.storage.StorageException;
import java.net.HttpURLConnection;

/**
 * Centralized utility for classifying GCS transport exceptions.
 */
public class GcsExceptionUtil {

  public enum ErrorType {
    NOT_FOUND,
    ALREADY_EXISTS,
    ACCESS_DENIED,
    UNKNOWN
  }

  /**
   * Determines the logical error type from a StorageException.
   */
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
