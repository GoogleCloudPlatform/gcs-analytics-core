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

import static com.google.cloud.gcs.analyticscore.client.GcsExceptionUtil.getErrorType;

import com.google.cloud.gcs.analyticscore.client.GcsExceptionUtil.ErrorType;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import java.nio.file.FileAlreadyExistsException;
import com.google.cloud.gcs.analyticscore.client.GcsWriteOptions;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Attribute;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Operation;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableMap;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A unified WritableByteChannel for writing objects to Google Cloud Storage. */
public class GcsWriteChannel implements WritableByteChannel {

  private static final Logger LOG = LoggerFactory.getLogger(GcsWriteChannel.class);

  private final BlobInfo blobInfo;
  private final ImmutableMap<String, String> commonAttributes;
  private WritableByteChannel internalWriteChannel;
  private final Telemetry telemetry;
  private final GcsWriteOptions writeOptions;
  private final AutoCloseable resourcesToClose;

  private long bytesWritten = 0;
  private volatile boolean closed = false;

  GcsWriteChannel(
      WritableByteChannel internalWriteChannel,
      BlobInfo blobInfo,
      GcsWriteOptions writeOptions,
      Telemetry telemetry) {
    this(internalWriteChannel, blobInfo, writeOptions, telemetry, null);
  }

  GcsWriteChannel(
      WritableByteChannel internalWriteChannel,
      BlobInfo blobInfo,
      GcsWriteOptions writeOptions,
      Telemetry telemetry,
      AutoCloseable resourcesToClose) {
    this.internalWriteChannel = internalWriteChannel;
    this.blobInfo = blobInfo;
    this.writeOptions = writeOptions;
    this.telemetry = telemetry;
    this.resourcesToClose = resourcesToClose;
    this.commonAttributes =
        ImmutableMap.of(
            Attribute.CLASS_NAME.name(), GcsWriteChannel.class.getName());

    LOG.debug(
        "Initializing GcsWriteChannel for object: gs://{}/{}",
        blobInfo.getBucket(),
        blobInfo.getName());
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    if (!isOpen()) {
      LOG.warn("Attempted to write to a closed channel for object: {}", blobInfo.getBlobId());
      throw new ClosedChannelException();
    }

    return this.telemetry
        .measure(
            Operation.WRITE.name(),
            Metric.WRITE_DURATION,
            commonAttributes,
            recorder -> {
              try {
                int written = internalWriteChannel.write(src);
                if (written > 0) {
                  bytesWritten += written;
                  recorder.record(Metric.WRITE_BYTES, written, Collections.emptyMap());
                }

                LOG.trace(
                    "{} bytes were written out of provided buffer of capacity {}. Total: {}",
                    written,
                    src.capacity(),
                    bytesWritten);
                return written;
              } catch (Exception e) {
                StorageException se = null;
                if (e instanceof StorageException) {
                  se = (StorageException) e;
                } else if (e.getCause() instanceof StorageException) {
                  se = (StorageException) e.getCause();
                }

                if (se != null) {
                  LOG.error(
                      "StorageException while writing to object: {} at position: {}",
                      blobInfo.getBlobId(),
                      bytesWritten,
                      se);
                  handleStorageException(se, "write");
                  return 0; // Unreachable, but required by the compiler
                }

                if (e instanceof IOException) {
                  throw (IOException) e;
                }

                if (e instanceof RuntimeException) {
                  throw (RuntimeException) e;
                }

                throw new IOException("Unexpected exception during write", e);
              }
            });
  }

  @Override
  public boolean isOpen() {
    return !closed && internalWriteChannel != null && internalWriteChannel.isOpen();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    this.telemetry
        .measure(
            Operation.CLOSE.name(),
            Metric.CLOSE_DURATION,
            commonAttributes,
            recorder -> {
              LOG.debug(
                  "Closing GoogleCloudStorageWriteChannel for object: {}. Final byte count: {}",
                  blobInfo.getBlobId(),
                  bytesWritten);
              closed = true;
              try {
                if (internalWriteChannel != null) {
                  internalWriteChannel.close();
                }
                LOG.debug("Successfully closed and finalized object: {}", blobInfo.getBlobId());
              } catch (Exception e) {
                StorageException se = null;
                if (e instanceof StorageException) {
                  se = (StorageException) e;
                } else if (e.getCause() instanceof StorageException) {
                  se = (StorageException) e.getCause();
                }

                if (se != null) {
                  LOG.error(
                      "Failed to close and finalize upload for object: {}", blobInfo.getBlobId(), se);
                  handleStorageException(se, "close");
                }

                if (e instanceof IOException) {
                  throw (IOException) e;
                }

                if (e instanceof RuntimeException) {
                  throw (RuntimeException) e;
                }

                throw new IOException("Unexpected exception during close", e);
              } finally {
                internalWriteChannel = null;
                if (resourcesToClose != null) {
                  try {
                    resourcesToClose.close();
                  } catch (Exception e) {
                    LOG.warn("Failed to close resources associated with channel", e);
                  }
                }
              }
              return null;
            });
  }

  private void handleStorageException(StorageException e, String context) throws IOException {
    ErrorType errorType = getErrorType(e);

    if (errorType == ErrorType.NOT_FOUND) {
      throw (FileNotFoundException)
          new FileNotFoundException(
              String.format(
                  "Location does not exist or generation not found: gs://%s/%s",
                  blobInfo.getBucket(), blobInfo.getName()))
              .initCause(e);

    } else if (errorType == ErrorType.ACCESS_DENIED) {
      throw (AccessDeniedException)
          new AccessDeniedException(
              String.format("gs://%s/%s", blobInfo.getBucket(), blobInfo.getName()),
              null,
              String.format("Access denied to object during %s: %s", context, e.getMessage()))
              .initCause(e);

    } else if (errorType == ErrorType.ALREADY_EXISTS) {
      // 409 Conflict: The gRPC transport explicitly tells us the file already exists
      throw (FileAlreadyExistsException)
          new FileAlreadyExistsException(
              String.format(
                  "Object gs://%s/%s already exists.",
                  blobInfo.getBucket(), blobInfo.getName()))
              .initCause(e);

    } else if (errorType == ErrorType.PRECONDITION_FAILED) {
      // 412 Precondition Failed: We must use our local state to infer the failure reason
      if (writeOptions != null && !writeOptions.isOverwriteExisting()) {
        // In the JSON API, "does not exist" preconditions manifest as a 412.
        throw (FileAlreadyExistsException)
            new FileAlreadyExistsException(
                String.format(
                    "Object gs://%s/%s already exists.",
                    blobInfo.getBucket(), blobInfo.getName()))
                .initCause(e);
      } else if (blobInfo.getBlobId().getGeneration() != null) {
        throw new IOException(
            String.format(
                "Generation mismatch for object gs://%s/%s. The file may have been modified concurrently.",
                blobInfo.getBucket(), blobInfo.getName()),
            e);
      }
    }

    // Safe fallback for unmapped or generic transport errors
    throw new IOException(
        String.format(
            "Error during %s to GCS for %s at position %d",
            context, blobInfo.getBlobId(), bytesWritten),
        e);
  }

  public long getBytesWritten() {
    return bytesWritten;
  }
}
