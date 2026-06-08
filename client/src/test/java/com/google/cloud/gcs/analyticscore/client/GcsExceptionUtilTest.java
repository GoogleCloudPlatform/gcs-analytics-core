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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import org.junit.jupiter.api.Test;

class GcsExceptionUtilTest {

  private static final String CONTEXT = "write";
  private static final String BUCKET = "test-bucket";
  private static final String NAME = "test-object";
  private static final long POSITION = 100L;

  @Test
  void getErrorType_404_returnsNotFound() {
    assertThat(GcsExceptionUtil.getErrorType(new StorageException(404, "Not Found")))
        .isEqualTo(GcsExceptionUtil.ErrorType.NOT_FOUND);
  }

  @Test
  void getErrorType_409_returnsAlreadyExists() {
    assertThat(GcsExceptionUtil.getErrorType(new StorageException(409, "Conflict")))
        .isEqualTo(GcsExceptionUtil.ErrorType.ALREADY_EXISTS);
  }

  @Test
  void getErrorType_412_returnsPreconditionFailed() {
    assertThat(GcsExceptionUtil.getErrorType(new StorageException(412, "Precondition Failed")))
        .isEqualTo(GcsExceptionUtil.ErrorType.PRECONDITION_FAILED);
  }

  @Test
  void getErrorType_403_returnsAccessDenied() {
    assertThat(GcsExceptionUtil.getErrorType(new StorageException(403, "Forbidden")))
        .isEqualTo(GcsExceptionUtil.ErrorType.ACCESS_DENIED);
  }

  @Test
  void getErrorType_401_returnsAccessDenied() {
    assertThat(GcsExceptionUtil.getErrorType(new StorageException(401, "Unauthorized")))
        .isEqualTo(GcsExceptionUtil.ErrorType.ACCESS_DENIED);
  }

  @Test
  void getErrorType_500_returnsUnknown() {
    assertThat(GcsExceptionUtil.getErrorType(new StorageException(500, "Internal Error")))
        .isEqualTo(GcsExceptionUtil.ErrorType.UNKNOWN);
  }

  @Test
  void translateException_when404_throwsFileNotFound() {
    IOException exception =
        GcsExceptionUtil.translateException(
            new StorageException(404, "Not Found"), CONTEXT, BUCKET, NAME, null, true, POSITION);
    assertThat(exception).isInstanceOf(FileNotFoundException.class);
    assertThat(exception.getMessage())
        .contains("Location does not exist or generation not found: gs://test-bucket/test-object");
  }

  @Test
  void translateException_when403_throwsAccessDenied() {
    IOException exception =
        GcsExceptionUtil.translateException(
            new StorageException(403, "Forbidden"), CONTEXT, BUCKET, NAME, null, true, POSITION);
    assertThat(exception).isInstanceOf(AccessDeniedException.class);
    assertThat(exception.getMessage()).contains("Access denied to object during write");
  }

  @Test
  void translateException_when409_throwsFileAlreadyExists() {
    IOException exception =
        GcsExceptionUtil.translateException(
            new StorageException(409, "Conflict"), CONTEXT, BUCKET, NAME, null, true, POSITION);
    assertThat(exception).isInstanceOf(FileAlreadyExistsException.class);
    assertThat(exception.getMessage())
        .contains("Object gs://test-bucket/test-object already exists");
  }

  @Test
  void translateException_when412WithoutOverwrite_throwsFileAlreadyExists() {
    IOException exception =
        GcsExceptionUtil.translateException(
            new StorageException(412, "Precondition Failed"),
            CONTEXT,
            BUCKET,
            NAME,
            null,
            false,
            POSITION);
    assertThat(exception).isInstanceOf(FileAlreadyExistsException.class);
  }

  @Test
  void
      translateException_when412WithOverwriteAndGeneration_throwsIOExceptionWithGenerationMismatch() {
    IOException exception =
        GcsExceptionUtil.translateException(
            new StorageException(412, "Precondition Failed"),
            CONTEXT,
            BUCKET,
            NAME,
            12345L,
            true,
            POSITION);
    assertThat(exception).isNotInstanceOf(FileAlreadyExistsException.class);
    assertThat(exception.getMessage())
        .contains("Generation mismatch for object gs://test-bucket/test-object");
  }

  @Test
  void translateException_when500_throwsGenericIOException() {
    IOException exception =
        GcsExceptionUtil.translateException(
            new StorageException(500, "Internal Server Error"),
            CONTEXT,
            BUCKET,
            NAME,
            null,
            true,
            POSITION);
    assertThat(exception).isInstanceOf(IOException.class);
    assertThat(exception.getMessage())
        .contains("Error during write to GCS for gs://test-bucket/test-object at position 100");
  }

  @Test
  void constructor_isPrivate() throws Exception {
    Constructor<GcsExceptionUtil> constructor = GcsExceptionUtil.class.getDeclaredConstructor();
    assertThat(Modifier.isPrivate(constructor.getModifiers())).isTrue();
    constructor.setAccessible(true);
    constructor.newInstance();
  }
}
