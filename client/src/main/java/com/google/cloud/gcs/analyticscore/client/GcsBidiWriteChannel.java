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
import java.util.concurrent.atomic.AtomicLong;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A write channel that supports bidirectional/appendable upload to Google Cloud Storage.
 *
 * <p>This channel utilizes the {@link BlobAppendableUpload} session from the GCS client library,
 * allowing incremental, bidirectional writes that can optionally be finalized on close.
 */
public class GcsBidiWriteChannel extends GcsWriteChannel {

  private static final Logger LOG = LoggerFactory.getLogger(GcsBidiWriteChannel.class);

  private volatile BlobAppendableUpload.AppendableUploadWriteableByteChannel gcsAppendChannel;
  private final AtomicLong bidiBytesWritten = new AtomicLong(0);

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
  public int write(@NonNull ByteBuffer src) throws IOException {
    checkNotNull(src, "src cannot be null");
    if (!isOpen()) {
      throw new ClosedChannelException();
    }

    try {
      int written = StorageChannelUtils.blockingEmptyTo(src, gcsAppendChannel);
      if (written > 0) {
        bidiBytesWritten.addAndGet(written);
        this.bytesWritten = bidiBytesWritten.get();
      }
      return written;
    } catch (StorageException | IOException e) {
      throw handleException(e, "write");
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    synchronized (this) {
      closed = true;
      try {
        super.close();
      } finally {
        if (gcsAppendChannel != null) {
          try {
            gcsAppendChannel.close();
          } catch (StorageException | IOException e) {
            throw handleException(e, "close");
          } finally {
            gcsAppendChannel = null;
          }
        }
      }
    }
  }

  @Override
  public boolean isOpen() {
    return !closed && gcsAppendChannel != null && gcsAppendChannel.isOpen();
  }

  @Override
  public long getBytesWritten() {
    return bidiBytesWritten.get();
  }

  @Override
  protected IOException handleException(@NonNull Exception e, @NonNull String context) {
    return GcsExceptionUtil.translateWriteException(
        e, context, blobInfo.getBlobId(), bidiBytesWritten.get(), writeOptions);
  }
}
