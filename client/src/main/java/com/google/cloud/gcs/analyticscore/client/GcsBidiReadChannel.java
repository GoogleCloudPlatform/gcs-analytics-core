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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.ReadChannel;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadProjectionConfigs;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.ZeroCopySupport.DisposableByteString;
import com.google.common.base.Supplier;
import com.google.protobuf.ByteString;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GcsBidiReadChannel extends GcsReadChannel {
  private static final Logger logger = LoggerFactory.getLogger(GcsBidiReadChannel.class);

  private final long bidiClientTimeoutSeconds;
  private final BlobId blobId;
  private volatile BlobReadSession blobReadSession;
  private volatile boolean closed = false;
  private volatile long position = 0;
  private final ApiFuture<BlobReadSession> sessionFuture;

  private volatile long objectSize = -1;
  private volatile boolean metadataInitialized = false;

  GcsBidiReadChannel(
      Storage storage,
      GcsItemInfo itemInfo,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry)
      throws IOException {
    super(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);
    this.bidiClientTimeoutSeconds = readOptions.getBidiTimeout();
    this.blobId = initBlobId();
    this.sessionFuture = storage.blobReadSession(blobId);
  }

  GcsBidiReadChannel(
      Storage storage,
      GcsItemId itemId,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry)
      throws IOException {
    this(storage, itemId, readOptions, executorServiceSupplier, telemetry, null);
  }

  GcsBidiReadChannel(
      Storage storage,
      GcsItemId itemId,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry,
      ItemInfoProvider itemInfoProvider)
      throws IOException {
    super(storage, itemId, readOptions, executorServiceSupplier, telemetry, itemInfoProvider);
    this.bidiClientTimeoutSeconds = readOptions.getBidiTimeout();
    this.blobId = initBlobId();
    this.sessionFuture = storage.blobReadSession(blobId);
  }

  @Override
  protected ReadStrategy createReadStrategy(
      Storage storage, GcsItemId itemId, GcsReadOptions readOptions, GcsItemInfo itemInfo) {
    return new ReadStrategy() {
      @Override
      public ReadChannel getReadChannel(long requestedPosition, int bytesToRead) {
        throw new UnsupportedOperationException(
            "Standard read is not supported on Bidi channel yet.");
      }

      @Override
      public void position(long newPosition) {}

      @Override
      public long getLimit() {
        return 0;
      }

      @Override
      public boolean isEof(long position) {
        return true;
      }

      @Override
      public ReadChannel getSdkReadChannel() {
        throw new UnsupportedOperationException(
            "Standard read is not supported on Bidi channel yet.");
      }

      @Override
      public void close() {}
    };
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    if (!dst.hasRemaining()) {
      return 0;
    }
    long objectSize = size();
    if (position >= objectSize) {
      return -1;
    }
    long bytesToRequest = Math.min((long) dst.remaining(), objectSize - position);
    int bytesRead = readBytesFromSession(dst, position, bytesToRequest);
    if (bytesRead > 0) {
      position += bytesRead;
    }
    return bytesRead;
  }

  @Override
  public long position() throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    return position;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    if (newPosition < 0) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be >= 0 for '%s'",
              newPosition, itemId));
    }
    this.position = newPosition;
    return this;
  }

  @Override
  public long size() throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    ensureMetadataInitialized();
    return objectSize;
  }

  @Override
  public void readVectored(List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    if (closed) {
      ClosedChannelException e =
          new ClosedChannelException() {
            @Override
            public String getMessage() {
              return "Reader is closed.";
            }
          };
      ranges.forEach(range -> range.getByteBufferFuture().completeExceptionally(e));
      throw e;
    }
    BlobReadSession session;
    try {
      session = getBlobReadSession();
    } catch (IOException | RuntimeException e) {
      ranges.forEach(range -> range.getByteBufferFuture().completeExceptionally(e));
      throw e;
    }
    ranges.forEach(range -> readAndAttachCallback(range, session, allocate));
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
        if (blobReadSession != null) {
          blobReadSession.close();
          blobReadSession = null;
        }
      }
    }
  }

  private BlobId initBlobId() {
    String bucketName = itemId.getBucketName();
    checkArgument(itemId.getObjectName().isPresent(), "ObjectName cannot be empty");
    String objectName = itemId.getObjectName().get();
    return itemId
        .getContentGeneration()
        .map(gen -> BlobId.of(bucketName, objectName, gen))
        .orElse(BlobId.of(bucketName, objectName));
  }

  private int readBytesFromSession(ByteBuffer dst, long offset, long length) throws IOException {
    BlobReadSession session = getBlobReadSession();
    ApiFuture<DisposableByteString> futureBytes =
        session.readAs(
            ReadProjectionConfigs.asFutureByteString().withRangeSpec(RangeSpec.of(offset, length)));
    try (DisposableByteString dbs = futureBytes.get(bidiClientTimeoutSeconds, TimeUnit.SECONDS)) {
      ByteString byteString = dbs.byteString();
      int bytesRead = byteString.size();
      if (bytesRead > 0) {
        byteString.copyTo(dst);
      }
      return bytesRead;
    } catch (InterruptedException e) {
      futureBytes.cancel(true);
      Thread.currentThread().interrupt();
      throw new IOException("Thread interrupted while reading from stream", e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof StorageException && ((StorageException) cause).getCode() == 404) {
        throw new FileNotFoundException("Object not found during read: " + blobId);
      }
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new IOException("Failed to read bytes from bidirectional session", e);
    } catch (TimeoutException e) {
      futureBytes.cancel(true);
      throw new IOException("Timed out waiting for bytes from bidirectional stream", e);
    }
  }

  private void ensureMetadataInitialized() throws IOException {
    if (metadataInitialized) {
      return;
    }
    synchronized (this) {
      try {
        BlobReadSession session = getBlobReadSession();
        BlobInfo blobInfo = (session != null) ? session.getBlobInfo() : null;

        this.objectSize = blobInfo == null ? super.size() : blobInfo.getSize();
      } catch (IOException e) {
        this.objectSize = super.size();
      }
      this.metadataInitialized = true;
    }
  }

  private BlobReadSession getBlobReadSession() throws IOException {
    if (blobReadSession != null) {
      return blobReadSession;
    }
    synchronized (this) {
      if (closed) {
        throw new IOException("Reader is closed.");
      }
      if (blobReadSession == null) {
        try {
          blobReadSession = this.sessionFuture.get(bidiClientTimeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Failed to get BlobReadSession due to thread interruption", e);
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof StorageException && ((StorageException) cause).getCode() == 404) {
            throw new FileNotFoundException("Object not found: " + blobId);
          }
          throw new IOException("Failed to get BlobReadSession", e);
        } catch (TimeoutException e) {
          throw new IOException("Failed to get BlobReadSession due to client timeout limit", e);
        }
      }
    }
    return blobReadSession;
  }

  private void readAndAttachCallback(
      GcsObjectRange range, BlobReadSession session, IntFunction<ByteBuffer> allocate) {
    ApiFuture<DisposableByteString> futureBytes =
        session.readAs(
            ReadProjectionConfigs.asFutureByteString()
                .withRangeSpec(RangeSpec.of(range.getOffset(), range.getLength())));

    ApiFutures.addCallback(
        futureBytes,
        new ApiFutureCallback<DisposableByteString>() {
          @Override
          public void onFailure(Throwable t) {
            range.getByteBufferFuture().completeExceptionally(t);
            logger.debug(
                "Vectored Read failed for range starting from {} with length {}",
                range.getOffset(),
                range.getLength());
          }

          @Override
          public void onSuccess(DisposableByteString disposableByteString) {
            try {
              processBytesAndCompleteRange(disposableByteString, range, allocate);
            } catch (Throwable t) {
              range.getByteBufferFuture().completeExceptionally(t);
            }
          }
        },
        executorServiceSupplier.get());
  }

  private void processBytesAndCompleteRange(
      DisposableByteString disposableByteString,
      GcsObjectRange range,
      IntFunction<ByteBuffer> allocate)
      throws IOException {
    try (DisposableByteString dbs = disposableByteString) {
      ByteString byteString = dbs.byteString();
      int size = byteString.size();
      ByteBuffer buf = allocate.apply(size);
      if (buf == null) {
        throw new NullPointerException("Allocator returned a null ByteBuffer!");
      }
      for (ByteBuffer b : byteString.asReadOnlyByteBufferList()) {
        buf.put(b);
      }
      buf.flip();
      range.getByteBufferFuture().complete(buf);
    }
  }
}
