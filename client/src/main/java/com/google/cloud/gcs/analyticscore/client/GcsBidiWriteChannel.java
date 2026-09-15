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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.StorageChannelUtils;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * A write channel that supports bidirectional/appendable upload to Google Cloud Storage.
 *
 * <p>This channel utilizes the {@link BlobAppendableUpload} session from the GCS client library,
 * allowing incremental, bidirectional writes that can optionally be finalized on close.
 */
public class GcsBidiWriteChannel extends GcsWriteChannel {

  private volatile BlobAppendableUpload.AppendableUploadWriteableByteChannel gcsAppendChannel;

  public GcsBidiWriteChannel(
      @NonNull Storage storage, @NonNull BlobInfo blobInfo, @NonNull GcsWriteOptions writeOptions)
      throws IOException {
    this(storage, blobInfo, writeOptions, new BlobWriteOption[0]);
  }

  public GcsBidiWriteChannel(
      @NonNull Storage storage,
      @NonNull BlobInfo blobInfo,
      @NonNull GcsWriteOptions writeOptions,
      @NonNull BlobWriteOption[] sdkWriteOptions)
      throws IOException {
    super(
        null,
        null,
        checkNotNull(blobInfo, "blobInfo cannot be null"),
        checkNotNull(writeOptions, "writeOptions cannot be null"));
    checkNotNull(storage, "storage cannot be null");
    checkNotNull(sdkWriteOptions, "sdkWriteOptions cannot be null");

    BlobAppendableUploadConfig.CloseAction closeAction =
        writeOptions.isBidiFinalizeOnClose()
            ? BlobAppendableUploadConfig.CloseAction.FINALIZE_WHEN_CLOSING
            : BlobAppendableUploadConfig.CloseAction.CLOSE_WITHOUT_FINALIZING;

    try {
      BlobAppendableUpload session =
          storage.blobAppendableUpload(
              blobInfo,
              BlobAppendableUploadConfig.of().withCloseAction(closeAction),
              sdkWriteOptions);
      this.gcsAppendChannel = session.open();
    } catch (StorageException e) {
      throw handleException(e, "init");
    }
  }

  @Override
  public int write(@NonNull ByteBuffer src) throws IOException {
    checkNotNull(src, "src cannot be null");
    if (!isOpen()) {
      throw new ClosedChannelException();
    }

    try {
      int written = StorageChannelUtils.blockingEmptyTo(src, gcsAppendChannel);
      if (written > 0) {
        bytesWritten.addAndGet(written);
      }
      return written;
    } catch (StorageException | IOException e) {
      throw handleException(e, "write");
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Whether the object is finalized is determined by {@code
   * gcs.channel.write.bidi.finalize-on-close}. When it is disabled the object is left unfinalized
   * and remains appendable; use {@link #finalizeAndClose()} to finalize regardless of the
   * configuration.
   */
  @Override
  public void close() throws IOException {
    doClose(/* finalizeObject= */ false);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Finalizes the object even when {@code gcs.channel.write.bidi.finalize-on-close} is disabled.
   */
  @Override
  public void finalizeAndClose() throws IOException {
    doClose(/* finalizeObject= */ true);
  }

  /**
   * Closes the underlying appendable upload channel exactly once.
   *
   * @param finalizeObject when true the object is finalized regardless of the configured {@link
   *     BlobAppendableUploadConfig.CloseAction}; when false the configured close action applies.
   */
  private void doClose(boolean finalizeObject) throws IOException {
    if (closed) {
      return;
    }

    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      BlobAppendableUpload.AppendableUploadWriteableByteChannel channel = gcsAppendChannel;
      if (channel != null) {
        try {
          if (finalizeObject) {
            channel.finalizeAndClose();
          } else {
            channel.close();
          }
        } catch (StorageException | IOException e) {
          throw handleException(e, finalizeObject ? "finalizeAndClose" : "close");
        } finally {
          gcsAppendChannel = null;
        }
      }
    }
  }

  @Override
  public boolean isOpen() {
    return !closed && gcsAppendChannel != null && gcsAppendChannel.isOpen();
  }
}
