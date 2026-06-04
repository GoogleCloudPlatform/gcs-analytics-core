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

public class GcsExceptionUtilTest {

  @Test
  public void testGetErrorType() {
    assertThat(GcsExceptionUtil.getErrorType(new StorageException(404, "Not Found")))
        .isEqualTo(GcsExceptionUtil.ErrorType.NOT_FOUND);

    assertThat(GcsExceptionUtil.getErrorType(new StorageException(409, "Conflict")))
        .isEqualTo(GcsExceptionUtil.ErrorType.ALREADY_EXISTS);

    assertThat(GcsExceptionUtil.getErrorType(new StorageException(412, "Precondition Failed")))
        .isEqualTo(GcsExceptionUtil.ErrorType.PRECONDITION_FAILED);

    assertThat(GcsExceptionUtil.getErrorType(new StorageException(403, "Forbidden")))
        .isEqualTo(GcsExceptionUtil.ErrorType.ACCESS_DENIED);

    assertThat(GcsExceptionUtil.getErrorType(new StorageException(401, "Unauthorized")))
        .isEqualTo(GcsExceptionUtil.ErrorType.ACCESS_DENIED);

    assertThat(GcsExceptionUtil.getErrorType(new StorageException(500, "Internal Error")))
        .isEqualTo(GcsExceptionUtil.ErrorType.UNKNOWN);
  }

  @Test
  public void testTranslateException() {
    String context = "write";
    String bucket = "test-bucket";
    String name = "test-object";
    long position = 100L;

    // 404 -> FileNotFoundException
    IOException e404 =
        GcsExceptionUtil.translateException(
            new StorageException(404, "Not Found"), context, bucket, name, null, true, position);
    assertThat(e404).isInstanceOf(FileNotFoundException.class);
    assertThat(e404.getMessage())
        .contains("Location does not exist or generation not found: gs://test-bucket/test-object");

    // 403 -> AccessDeniedException
    IOException e403 =
        GcsExceptionUtil.translateException(
            new StorageException(403, "Forbidden"), context, bucket, name, null, true, position);
    assertThat(e403).isInstanceOf(AccessDeniedException.class);
    assertThat(e403.getMessage()).contains("Access denied to object during write");

    // 409 -> FileAlreadyExistsException
    IOException e409 =
        GcsExceptionUtil.translateException(
            new StorageException(409, "Conflict"), context, bucket, name, null, true, position);
    assertThat(e409).isInstanceOf(FileAlreadyExistsException.class);
    assertThat(e409.getMessage()).contains("Object gs://test-bucket/test-object already exists");

    // 412 with overwriteExisting = false -> FileAlreadyExistsException
    IOException e412NoOverwrite =
        GcsExceptionUtil.translateException(
            new StorageException(412, "Precondition Failed"),
            context,
            bucket,
            name,
            null,
            false,
            position);
    assertThat(e412NoOverwrite).isInstanceOf(FileAlreadyExistsException.class);

    // 412 with overwriteExisting = true and generation != null -> IOException (Generation mismatch)
    IOException e412GenMismatch =
        GcsExceptionUtil.translateException(
            new StorageException(412, "Precondition Failed"),
            context,
            bucket,
            name,
            12345L,
            true,
            position);
    assertThat(e412GenMismatch).isNotInstanceOf(FileAlreadyExistsException.class);
    assertThat(e412GenMismatch.getMessage())
        .contains("Generation mismatch for object gs://test-bucket/test-object");

    // 500 -> generic IOException
    IOException e500 =
        GcsExceptionUtil.translateException(
            new StorageException(500, "Internal Server Error"),
            context,
            bucket,
            name,
            null,
            true,
            position);
    assertThat(e500).isInstanceOf(IOException.class);
    assertThat(e500.getMessage())
        .contains("Error during write to GCS for gs://test-bucket/test-object at position 100");
  }

  @Test
  public void testConstructorIsPrivate() throws Exception {
    Constructor<GcsExceptionUtil> constructor = GcsExceptionUtil.class.getDeclaredConstructor();
    assertThat(Modifier.isPrivate(constructor.getModifiers())).isTrue();
    constructor.setAccessible(true);
    constructor.newInstance();
  }
}
