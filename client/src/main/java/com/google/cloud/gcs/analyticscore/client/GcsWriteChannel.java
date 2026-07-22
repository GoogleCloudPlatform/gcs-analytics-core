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
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A unified WritableByteChannel for writing objects to Google Cloud Storage. */
public class GcsWriteChannel implements WritableByteChannel {

  private static final Logger LOG = LoggerFactory.getLogger(GcsWriteChannel.class);

  protected final BlobInfo blobInfo;
  private final BlobWriteSession blobWriteSession;
  private volatile WritableByteChannel sdkWriteChannel;
  protected final GcsWriteOptions writeOptions;

  protected volatile long bytesWritten = 0;
  protected volatile boolean closed = false;

  GcsWriteChannel(
      BlobWriteSession blobWriteSession,
      WritableByteChannel sdkWriteChannel,
      BlobInfo blobInfo,
      GcsWriteOptions writeOptions) {
    this.blobWriteSession = blobWriteSession;
    this.sdkWriteChannel = sdkWriteChannel;
    this.blobInfo = blobInfo;
    this.writeOptions = writeOptions;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    if (!isOpen()) {
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
    } catch (StorageException | IOException e) {
      throw handleException(e, "write");
    }
  }

  @Override
  public boolean isOpen() {
    return !closed
        && Optional.ofNullable(sdkWriteChannel).map(WritableByteChannel::isOpen).orElse(false);
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

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
      Thread.currentThread().interrupt();
      throw new InterruptedIOException(
          "Thread interrupted waiting for upload finalization: " + e.getMessage());
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof StorageException || cause instanceof IOException) {
        throw handleException((Exception) cause, "close");
      }
      throw new IOException("GCS failed to finalize the upload session", cause);
    } catch (StorageException | IOException e) {
      throw handleException(e, "close");
    } finally {
      sdkWriteChannel = null;
    }
  }

  protected IOException handleException(Exception e, String context) {
    return GcsExceptionUtil.translateWriteException(
        e, context, blobInfo.getBlobId(), bytesWritten, writeOptions);
  }

  public long getBytesWritten() {
    return bytesWritten;
  }
}
