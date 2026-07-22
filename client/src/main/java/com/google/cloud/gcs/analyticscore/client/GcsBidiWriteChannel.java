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

import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageChannelUtils;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

/**
 * A write channel that supports bidirectional/appendable upload to Google Cloud Storage.
 *
 * <p>This channel utilizes the {@link BlobAppendableUpload} session from the GCS client library,
 * allowing incremental, bidirectional writes that can optionally be finalized on close.
 */
public class GcsBidiWriteChannel extends GcsWriteChannel {

  private final BlobAppendableUpload.AppendableUploadWriteableByteChannel gcsAppendChannel;

  public GcsBidiWriteChannel(Storage storage, BlobInfo blobInfo, GcsWriteOptions writeOptions)
      throws IOException {
    this(storage, blobInfo, writeOptions, new Storage.BlobWriteOption[0]);
  }

  public GcsBidiWriteChannel(
      Storage storage,
      BlobInfo blobInfo,
      GcsWriteOptions writeOptions,
      Storage.BlobWriteOption[] sdkWriteOptions)
      throws IOException {
    super(null, null, blobInfo, writeOptions);

    BlobAppendableUploadConfig.CloseAction closeAction =
        writeOptions.isFinalizeOnClose()
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
  public int write(ByteBuffer src) throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }

    try {
      int written = StorageChannelUtils.blockingEmptyTo(src, gcsAppendChannel);
      if (written > 0) {
        bytesWritten += written;
      }
      return written;
    } catch (StorageException | IOException e) {
      throw handleException(e, "write");
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (this.closed) {
      return;
    }

    this.closed = true;
    try {
      if (gcsAppendChannel != null) {
        gcsAppendChannel.close();
      }
    } catch (StorageException | IOException e) {
      throw handleException(e, "close");
    }
  }

  @Override
  public boolean isOpen() {
    return !this.closed && gcsAppendChannel != null && gcsAppendChannel.isOpen();
  }
}
