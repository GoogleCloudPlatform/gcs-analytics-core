package com.google.cloud.gcs.analyticscore.core;

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
