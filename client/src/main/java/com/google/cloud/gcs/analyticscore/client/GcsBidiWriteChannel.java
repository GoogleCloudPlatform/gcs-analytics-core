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
import com.google.cloud.storage.BlobAppendableUpload.AppendableUploadWriteableByteChannel;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobAppendableUploadConfig.CloseAction;
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
class GcsBidiWriteChannel extends GcsWriteChannel {

  private volatile AppendableUploadWriteableByteChannel gcsAppendChannel;

  GcsBidiWriteChannel(
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

    CloseAction closeAction =
        writeOptions.isBidiFinalizeOnClose()
            ? CloseAction.FINALIZE_WHEN_CLOSING
            : CloseAction.CLOSE_WITHOUT_FINALIZING;

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
    // Read the delegate exactly once. A concurrent close() nulls the field, and re-reading it
    // after the open check would surface an untranslated NullPointerException instead of the
    // documented ClosedChannelException.
    AppendableUploadWriteableByteChannel channel = gcsAppendChannel;
    if (closed || channel == null || !channel.isOpen()) {
      throw new ClosedChannelException();
    }

    try {
      int written = StorageChannelUtils.blockingEmptyTo(src, channel);
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
   * gcs.channel.write.bidi.finalize-on-close}, which selects the {@link CloseAction} applied when
   * the upload session is opened. When it is disabled the object is left unfinalized and remains
   * appendable.
   */
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      AppendableUploadWriteableByteChannel channel = gcsAppendChannel;
      if (channel != null) {
        try {
          channel.close();
        } catch (StorageException | IOException e) {
          throw handleException(e, "close");
        } finally {
          gcsAppendChannel = null;
        }
      }
    }
  }

  @Override
  public boolean isOpen() {
    // Snapshot for the same reason as write(): the null check must guard the same reference the
    // isOpen() call is made on.
    AppendableUploadWriteableByteChannel channel = gcsAppendChannel;
    return !closed && channel != null && channel.isOpen();
  }
}
