/*
 * Copyright 2025 Google LLC
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
import static java.lang.Math.min;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GcsAdaptiveReadChannel extends GcsReadChannel {
  private static final Logger LOG = LoggerFactory.getLogger(GcsAdaptiveReadChannel.class);
  private ContentReadChannel contentReadChannel;
  private boolean open = false;

  GcsAdaptiveReadChannel(
      Storage storage,
      GcsItemInfo itemInfo,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier) {
    super(
        storage,
        checkNotNull(itemInfo, "itemInfo cannot be null"),
        readOptions,
        executorServiceSupplier);
    this.contentReadChannel = new ContentReadChannel();
    this.open = true;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    throwIfNotOpen();
    if (dst.remaining() == 0) {
      return 0;
    }

    return contentReadChannel.readContent(dst);
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    throwIfNotOpen();
    if (newPosition == position) {
      return this;
    }
    validatePosition(newPosition);
    position = newPosition;
    return this;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void close() throws IOException {
    if (open) {
      try {
        contentReadChannel.closeContentChannel();
      } finally {
        contentReadChannel = null;
        open = false;
      }
    }
  }

  private void throwIfNotOpen() throws IOException {
    if (!isOpen()) {
      throw new java.nio.channels.ClosedChannelException();
    }
  }

  @VisibleForTesting
  ContentReadChannel getContentReadChannel() {
    return contentReadChannel;
  }

  class ContentReadChannel {
    private static final int SKIP_BUFFER_SIZE = 8192;
    private long contentChannelCurrentPosition = -1;
    private long contentChannelEnd = -1;
    private ReadChannel byteChannel = null;
    private byte[] skipBuffer = null;
    private final AdaptiveRangeReadStrategy adaptiveRangeReadStrategy;

    ContentReadChannel() {
      this.adaptiveRangeReadStrategy = new AdaptiveRangeReadStrategy(readOptions, itemInfo);
    }

    int readContent(ByteBuffer dst) throws IOException {
      performPendingSeeks();
      int totalBytesRead = 0;
      while (dst.hasRemaining()) {
        try {
          if (byteChannel == null) {
            byteChannel = openByteChannel(dst.remaining());
          }
          int bytesRead = byteChannel.read(dst);
          if (bytesRead == 0) {
            LOG.atDebug().log(
                "Read %d from storage-client's byte channel at position: %d with channel ending at: %d for resourceId: %s of size: %d",
                bytesRead, position, contentChannelEnd, itemId, itemInfo.getSize());
          }
          if (bytesRead < 0) {
            if (position != contentChannelEnd && position != itemInfo.getSize()) {

              throw new IOException(
                  String.format(
                      "Received end of stream result before all requestedBytes were received;"
                          + "EndOf stream signal received at offset: %d where as stream was suppose to end at: %d for resource: %s of size: %d",
                      position, contentChannelEnd, itemId, itemInfo.getSize()));
            }
            if (contentChannelEnd != itemInfo.getSize() && position == contentChannelEnd) {
              closeContentChannel();
              continue;
            } else {
              if (totalBytesRead == 0) {
                return -1;
              }
              break;
            }
          }
          totalBytesRead += bytesRead;
          position += bytesRead;
          contentChannelCurrentPosition += bytesRead;
        } catch (IOException e) {
          closeContentChannel();
          throw e;
        }
      }
      return totalBytesRead;
    }

    private ReadChannel openByteChannel(long bytesToRead) throws IOException {
      contentChannelCurrentPosition = position;
      contentChannelEnd =
          adaptiveRangeReadStrategy.calculateRangeRequestEnd(
              position, bytesToRead, itemInfo.getSize());
      ReadChannel channel = openReadChannel(itemId, readOptions);
      try {
        channel.seek(contentChannelCurrentPosition);
        channel.limit(contentChannelEnd);
        return channel;
      } catch (Exception e) {
        throw new IOException(
            String.format("Unable to update the boundaries/Range of contentChannel %s", itemId), e);
      }
    }

    private void performPendingSeeks() throws IOException {
      if (position == contentChannelCurrentPosition && byteChannel != null) {
        return;
      }
      if (canSeekInPlace()) {
        skipInPlace();
      } else {
        adaptiveRangeReadStrategy.detectRandomAccess(position, contentChannelCurrentPosition);
        closeContentChannel();
      }
    }

    private boolean canSeekInPlace() {
      return byteChannel != null
          && adaptiveRangeReadStrategy.shouldSeekInPlace(
              position, contentChannelCurrentPosition, contentChannelEnd);
    }

    private void skipInPlace() throws IOException {
      if (skipBuffer == null) {
        skipBuffer = new byte[SKIP_BUFFER_SIZE];
      }
      long seekDistance = position - contentChannelCurrentPosition;
      while (seekDistance > 0 && byteChannel != null) {
        try {
          int bufferSize = (int) min((long) skipBuffer.length, seekDistance);
          int bytesRead = byteChannel.read(ByteBuffer.wrap(skipBuffer, 0, bufferSize));
          if (bytesRead < 0) {
            closeContentChannel();
          } else {
            seekDistance -= bytesRead;
            contentChannelCurrentPosition += bytesRead;
          }
        } catch (IOException e) {
          closeContentChannel();
          throw e;
        }
      }
    }

    void closeContentChannel() {
      if (byteChannel != null) {
        try {
          byteChannel.close();
        } catch (Exception e) {
          LOG.debug("Got an exception on contentChannel.close() for '{}'; ignoring it.", itemId, e);
        } finally {
          byteChannel = null;
          contentChannelCurrentPosition = -1;
          contentChannelEnd = -1;
        }
      }
    }
  }
}
