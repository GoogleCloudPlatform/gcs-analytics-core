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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GcsAdaptiveReadChannel extends GcsReadChannel {
  private static final Logger LOG = LoggerFactory.getLogger(GcsAdaptiveReadChannel.class);
  private AdaptiveReadSession adaptiveReadSession;
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
    this.adaptiveReadSession = new AdaptiveReadSession();
    this.open = true;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    throwIfNotOpen();
    if (dst.remaining() == 0) {
      return 0;
    }

    return adaptiveReadSession.readContent(dst);
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    throwIfNotOpen();
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
        adaptiveReadSession.closeSession();
      } finally {
        adaptiveReadSession = null;
        open = false;
      }
    }
  }

  private void throwIfNotOpen() throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
  }

  @VisibleForTesting
  AdaptiveReadSession getAdaptiveReadSession() {
    return adaptiveReadSession;
  }

  class AdaptiveReadSession {
    private static final int SKIP_BUFFER_SIZE = 8192;
    private long sessionPosition = -1;
    private long sessionEnd = -1;
    private ReadChannel sessionReadChannel = null;
    private byte[] skipBuffer = null;
    private final AdaptiveRangeReadStrategy adaptiveRangeReadStrategy;

    AdaptiveReadSession() {
      this.adaptiveRangeReadStrategy = new AdaptiveRangeReadStrategy(readOptions, itemInfo);
    }

    int readContent(ByteBuffer dst) throws IOException {
      performPendingSeeks();
      int totalBytesRead = 0;
      while (dst.hasRemaining()) {
        try {
          if (sessionReadChannel == null) {
            sessionReadChannel = openBoundedReadChannel(dst.remaining());
          }
          int bytesRead = sessionReadChannel.read(dst);
          if (bytesRead == 0) {
            LOG.atDebug().log(
                "Read %d from storage-client's byte channel at position: %d with channel ending at: %d for resourceId: %s of size: %d",
                bytesRead, sessionPosition, sessionEnd, itemId, itemInfo.getSize());
          }
          if (bytesRead < 0) {
            // Check if EOF is reached.
            if (sessionPosition == itemInfo.getSize()) {
              // Return -1 if EOF is reached before reading any bytes.
              if (totalBytesRead == 0) {
                return -1;
              }
              break;
            }
            // Check if the current read channel reached the end of the specified range,
            // but there is still more data in the object to read, then close the current channel
            // and continue.
            if (sessionPosition == sessionEnd) {
              closeSession();
              continue;
            }
            // If EOF is reached before end of channel and before the end of object then throw an IO
            // exception.
            throw new IOException(
                String.format(
                    "Received end of stream result before all requestedBytes were received;"
                        + "EndOf stream signal received at offset: %d where as stream was suppose to end at: %d for resource: %s of size: %d",
                    sessionPosition, sessionEnd, itemId, itemInfo.getSize()));
          }
          totalBytesRead += bytesRead;
          sessionPosition += bytesRead;
          position += bytesRead;
        } catch (IOException e) {
          closeSession();
          throw e;
        }
      }
      return totalBytesRead;
    }

    private ReadChannel openBoundedReadChannel(long bytesToRead) throws IOException {
      sessionPosition = position;
      adaptiveRangeReadStrategy.detectSequentialAccess(sessionPosition);
      sessionEnd =
          adaptiveRangeReadStrategy.calculateAdaptiveReadSessionEnd(
              sessionPosition, bytesToRead, itemInfo.getSize());
      ReadChannel channel = openUnboundedReadChannel(itemId, readOptions);
      try {
        channel.seek(sessionPosition);
        channel.limit(sessionEnd);
        return channel;
      } catch (Exception e) {
        throw new IOException(
            String.format("Unable to update the boundaries/Range of contentChannel %s", itemId), e);
      }
    }

    private void performPendingSeeks() throws IOException {
      if (sessionReadChannel != null && position == sessionPosition) {
        return;
      }
      if (canSeekInPlace()) {
        skipInPlace();
        return;
      }
      adaptiveRangeReadStrategy.detectRandomAccess(position, sessionPosition);
      closeSession();
    }

    private boolean canSeekInPlace() {
      return sessionReadChannel != null
          && adaptiveRangeReadStrategy.shouldSeekInPlace(position, sessionPosition, sessionEnd);
    }

    private void skipInPlace() throws IOException {
      if (skipBuffer == null) {
        skipBuffer = new byte[SKIP_BUFFER_SIZE];
      }
      long seekDistance = position - sessionPosition;
      while (seekDistance > 0 && sessionReadChannel != null) {
        try {
          int bufferSize = (int) min((long) skipBuffer.length, seekDistance);
          int bytesRead = sessionReadChannel.read(ByteBuffer.wrap(skipBuffer, 0, bufferSize));
          if (bytesRead < 0) {
            closeSession();
            return;
          }
          seekDistance -= bytesRead;
          sessionPosition += bytesRead;
        } catch (IOException e) {
          closeSession();
          throw e;
        }
      }
    }

    void closeSession() {
      if (sessionReadChannel != null) {
        try {
          sessionReadChannel.close();
        } catch (Exception e) {
          LOG.debug(
              "Got an exception on closing AdaptiveReadChannelSession for '{}'; ignoring it.",
              itemId,
              e);
        } finally {
          sessionReadChannel = null;
          sessionPosition = -1;
          sessionEnd = -1;
        }
      }
    }
  }
}
