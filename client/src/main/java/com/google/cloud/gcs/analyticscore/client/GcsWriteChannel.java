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

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A unified WritableByteChannel for writing objects to Google Cloud Storage. */
public class GcsWriteChannel implements WritableByteChannel {

  private static final Logger LOG = LoggerFactory.getLogger(GcsWriteChannel.class);

  private final BlobInfo blobInfo;
  private final BlobWriteSession blobWriteSession;
  private volatile WritableByteChannel sdkWriteChannel;
  private final GcsWriteOptions writeOptions;

  private volatile long bytesWritten = 0;
  private volatile boolean closed = false;

  GcsWriteChannel(
      BlobWriteSession blobWriteSession,
      WritableByteChannel sdkWriteChannel,
      BlobInfo blobInfo,
      GcsWriteOptions writeOptions) {
    this.blobWriteSession = blobWriteSession;
    this.sdkWriteChannel = sdkWriteChannel;
    this.blobInfo = blobInfo;
    this.writeOptions = writeOptions;

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

    int bytesToDraft = src.remaining();
    try {
      int written = sdkWriteChannel.write(src);
      if (written > 0) {
        bytesWritten += written;
      }

      LOG.trace(
          "{} bytes were written out of provided buffer of capacity {}. Total: {}",
          written,
          bytesToDraft,
          bytesWritten);
      return written;
    } catch (StorageException e) {
      LOG.error(
          "StorageException while writing to object: {} at position: {}",
          blobInfo.getBlobId(),
          bytesWritten,
          e);
      throw handleStorageException(e, "write");
    } catch (IOException e) {
      if (e.getCause() instanceof StorageException) {
        StorageException se = (StorageException) e.getCause();
        LOG.error(
            "StorageException while writing to object: {} at position: {}",
            blobInfo.getBlobId(),
            bytesWritten,
            se);
        throw handleStorageException(se, "write");
      }
      throw e;
    }
  }

  @Override
  public boolean isOpen() {
    return !closed && sdkWriteChannel != null && sdkWriteChannel.isOpen();
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    LOG.debug(
        "Closing GoogleCloudStorageWriteChannel for object: {}. Final byte count: {}",
        blobInfo.getBlobId(),
        bytesWritten);
    closed = true;
    try {
      if (sdkWriteChannel != null) {
        sdkWriteChannel.close();
      }
      if (blobWriteSession != null) {
        blobWriteSession.getResult().get();
      }
      LOG.debug("Successfully closed and finalized object: {}", blobInfo.getBlobId());
    } catch (InterruptedException e) {
      LOG.error(
          "Interrupted waiting for upload finalization for object: {}", blobInfo.getBlobId(), e);
      Thread.currentThread().interrupt();
      throw new InterruptedIOException(
          "Thread interrupted waiting for upload finalization: " + e.getMessage());
    } catch (ExecutionException e) {
      LOG.error("Failed to finalize upload session for object: {}", blobInfo.getBlobId(), e);
      Throwable cause = e.getCause();
      if (cause instanceof StorageException) {
        throw handleStorageException((StorageException) cause, "close");
      } else if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException("GCS failed to finalize the upload session", cause);
    } catch (StorageException e) {
      LOG.error("Failed to close and finalize upload for object: {}", blobInfo.getBlobId(), e);
      throw handleStorageException(e, "close");
    } catch (IOException e) {
      if (e.getCause() instanceof StorageException) {
        StorageException se = (StorageException) e.getCause();
        LOG.error("Failed to close and finalize upload for object: {}", blobInfo.getBlobId(), se);
        throw handleStorageException(se, "close");
      }
      throw e;
    } finally {
      sdkWriteChannel = null;
    }
  }

  private IOException handleStorageException(StorageException e, String context) {
    return GcsExceptionUtil.translateException(
        e,
        context,
        blobInfo.getBucket(),
        blobInfo.getName(),
        blobInfo.getBlobId().getGeneration(),
        writeOptions == null || writeOptions.isOverwriteExisting(),
        bytesWritten);
  }

  public long getBytesWritten() {
    return bytesWritten;
  }
}
