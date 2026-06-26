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

import static com.google.cloud.gcs.analyticscore.client.GcsExceptionUtil.ErrorType;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.StorageException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class GcsExceptionUtilTest {

  private static final String CONTEXT = "write";
  private static final String BUCKET = "test-bucket";
  private static final String NAME = "test-object";
  private static final long POSITION = 100L;

  @Test
  void getErrorType_404_returnsNotFound() {
    StorageException se = new StorageException(404, "Not Found");
    ErrorType errorType = GcsExceptionUtil.getErrorType(se);
    assertThat(errorType).isEqualTo(ErrorType.NOT_FOUND);
  }

  @Test
  void getErrorType_409_returnsAlreadyExists() {
    StorageException se = new StorageException(409, "Conflict");
    ErrorType errorType = GcsExceptionUtil.getErrorType(se);
    assertThat(errorType).isEqualTo(ErrorType.ALREADY_EXISTS);
  }

  @Test
  void getErrorType_412_returnsPreconditionFailed() {
    StorageException se = new StorageException(412, "Precondition Failed");
    ErrorType errorType = GcsExceptionUtil.getErrorType(se);
    assertThat(errorType).isEqualTo(ErrorType.PRECONDITION_FAILED);
  }

  @Test
  void getErrorType_403_returnsAccessDenied() {
    StorageException se = new StorageException(403, "Forbidden");
    ErrorType errorType = GcsExceptionUtil.getErrorType(se);
    assertThat(errorType).isEqualTo(ErrorType.ACCESS_DENIED);
  }

  @Test
  void getErrorType_401_returnsAccessDenied() {
    StorageException se = new StorageException(401, "Unauthorized");
    ErrorType errorType = GcsExceptionUtil.getErrorType(se);
    assertThat(errorType).isEqualTo(ErrorType.ACCESS_DENIED);
  }

  @Test
  void getErrorType_500_returnsUnknown() {
    StorageException se = new StorageException(500, "Internal Error");
    ErrorType errorType = GcsExceptionUtil.getErrorType(se);
    assertThat(errorType).isEqualTo(ErrorType.UNKNOWN);
  }

  @Test
  void translateException_when404_throwsFileNotFound() {
    StorageException se = new StorageException(404, "Not Found");

    IOException exception =
        GcsExceptionUtil.translateException(se, CONTEXT, BlobId.of(BUCKET, NAME), POSITION);

    assertThat(exception).isInstanceOf(FileNotFoundException.class);
    assertThat(exception.getMessage())
        .contains(
            String.format(
                "Location does not exist or generation not found: gs://%s/%s", BUCKET, NAME));
  }

  @Test
  void translateException_when403_throwsAccessDenied() {
    StorageException se = new StorageException(403, "Forbidden");

    IOException exception =
        GcsExceptionUtil.translateException(se, CONTEXT, BlobId.of(BUCKET, NAME), POSITION);

    assertThat(exception).isInstanceOf(AccessDeniedException.class);
    assertThat(exception.getMessage())
        .contains(String.format("Access denied to object during %s", CONTEXT));
  }

  @Test
  void translateException_when409_throwsFileAlreadyExists() {
    StorageException se = new StorageException(409, "Conflict");

    IOException exception =
        GcsExceptionUtil.translateException(se, CONTEXT, BlobId.of(BUCKET, NAME), POSITION);

    assertThat(exception).isInstanceOf(FileAlreadyExistsException.class);
    assertThat(exception.getMessage())
        .contains(String.format("Object gs://%s/%s already exists", BUCKET, NAME));
  }

  @Test
  void translateException_when412WithoutOverwrite_throwsFileAlreadyExists() {
    StorageException se = new StorageException(412, "Precondition Failed");

    IOException exception =
        GcsExceptionUtil.translateException(se, CONTEXT, BlobId.of(BUCKET, NAME), POSITION);

    assertThat(exception).isInstanceOf(FileAlreadyExistsException.class);
  }

  @Test
  void
      translateException_when412WithOverwriteAndGeneration_throwsIOExceptionWithGenerationMismatch() {
    StorageException se = new StorageException(412, "Precondition Failed");

    IOException exception =
        GcsExceptionUtil.translateExceptionWithOverwrite(
            se, CONTEXT, BlobId.of(BUCKET, NAME, 12345L), POSITION);

    assertThat(exception).isNotInstanceOf(FileAlreadyExistsException.class);
    assertThat(exception.getMessage())
        .isEqualTo(
            String.format(
                "Generation mismatch for object gs://%s/%s. Concurrent modification detected.",
                BUCKET, NAME));
  }

  @Test
  void translateException_when500_throwsGenericIOException() {
    StorageException se = new StorageException(500, "Internal Server Error");

    IOException exception =
        GcsExceptionUtil.translateException(se, CONTEXT, BlobId.of(BUCKET, NAME), POSITION);

    assertThat(exception).isInstanceOf(IOException.class);
    assertThat(exception.getMessage())
        .contains(
            String.format(
                "Error during %s to GCS for gs://%s/%s at position %d",
                CONTEXT, BUCKET, NAME, POSITION));
  }

  @Test
  void getStorageException_withStorageException_returnsItself() {
    StorageException se = new StorageException(404, "Not Found");
    Optional<StorageException> result = GcsExceptionUtil.getStorageException(se);
    assertThat(result).hasValue(se);
  }

  @Test
  void getStorageException_withWrappedStorageExceptionInIOException_returnsCause() {
    StorageException se = new StorageException(404, "Not Found");
    IOException ioe = new IOException("Wrapped", se);

    Optional<StorageException> result = GcsExceptionUtil.getStorageException(ioe);

    assertThat(result).hasValue(se);
  }

  @Test
  void getStorageException_withOtherException_returnsNull() {
    IOException ioe = new IOException("Generic I/O error");
    Optional<StorageException> result = GcsExceptionUtil.getStorageException(ioe);
    assertThat(result).isEmpty();
  }

  @Test
  void translateException_withUpcastedStorageException_translatesStorageException() {
    Exception se = new StorageException(404, "Not Found");

    IOException exception =
        GcsExceptionUtil.translateException(se, CONTEXT, BlobId.of(BUCKET, NAME), POSITION);

    assertThat(exception).isInstanceOf(FileNotFoundException.class);
  }

  @Test
  void translateException_withWrappedStorageExceptionInIOException_translatesStorageException() {
    StorageException se = new StorageException(404, "Not Found");
    Exception ioe = new IOException("Wrapped exception", se);

    IOException exception =
        GcsExceptionUtil.translateException(ioe, CONTEXT, BlobId.of(BUCKET, NAME), POSITION);

    assertThat(exception).isInstanceOf(FileNotFoundException.class);
  }

  @Test
  void translateException_withGenericIOException_returnsSameIOException() {
    Exception genericIoe = new IOException("Generic Connection Error");

    IOException exception =
        GcsExceptionUtil.translateException(genericIoe, CONTEXT, BlobId.of(BUCKET, NAME), POSITION);

    assertThat(exception).isSameInstanceAs(genericIoe);
  }

  @Test
  void translateException_withGenericException_returnsWrappedIOException() {
    Exception genericException = new RuntimeException("Something went wrong");

    IOException exception =
        GcsExceptionUtil.translateException(
            genericException, CONTEXT, BlobId.of(BUCKET, NAME), POSITION);

    assertThat(exception).hasCauseThat().isSameInstanceAs(genericException);
    assertThat(exception.getMessage())
        .contains(
            String.format(
                "Error during %s to GCS for gs://%s/%s at position %d",
                CONTEXT, BUCKET, NAME, POSITION));
  }

  @Test
  void translateExceptionWithOverwrite_withGenericIOException_returnsSameIOException() {
    Exception genericIoe = new IOException("Generic Connection Error");

    IOException exception =
        GcsExceptionUtil.translateExceptionWithOverwrite(
            genericIoe, CONTEXT, BlobId.of(BUCKET, NAME), POSITION);

    assertThat(exception).isSameInstanceAs(genericIoe);
  }

  @Test
  void translateExceptionWithOverwrite_withGenericException_returnsWrappedIOException() {
    Exception genericException = new RuntimeException("Something went wrong");

    IOException exception =
        GcsExceptionUtil.translateExceptionWithOverwrite(
            genericException, CONTEXT, BlobId.of(BUCKET, NAME), POSITION);

    assertThat(exception).hasCauseThat().isSameInstanceAs(genericException);
    assertThat(exception.getMessage())
        .contains(
            String.format(
                "Error during %s to GCS for gs://%s/%s at position %d",
                CONTEXT, BUCKET, NAME, POSITION));
  }

  @Test
  void constructor_isPrivate() throws Exception {
    Constructor<GcsExceptionUtil> constructor = GcsExceptionUtil.class.getDeclaredConstructor();

    assertThat(Modifier.isPrivate(constructor.getModifiers())).isTrue();
  }
}
