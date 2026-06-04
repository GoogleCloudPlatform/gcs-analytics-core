package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadProjectionConfigs;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.ZeroCopySupport.DisposableByteString;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GcsBidiVectoredReader implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(GcsBidiVectoredReader.class);

  private final Storage storage;
  private final ExecutorService executorService;
  private volatile BlobReadSession blobReadSession;
  private final BlobId blobId;

  GcsBidiVectoredReader(Storage storage, GcsItemId itemId, ExecutorService executorService) {
    this.storage = checkNotNull(storage, "Storage instance cannot be null");
    this.executorService = checkNotNull(executorService, "Executor service cannot be null");
    checkNotNull(itemId, "ItemId cannot be null");

    String bucketName = itemId.getBucketName();
    String objectName = itemId.getObjectName().get();
    this.blobId =
        itemId
            .getContentGeneration()
            .map(gen -> BlobId.of(bucketName, objectName, gen))
            .orElse(BlobId.of(bucketName, objectName));
  }

  private BlobReadSession getBlobReadSession() throws IOException {
    if (blobReadSession == null) {
      synchronized (this) {
        if (blobReadSession == null) {
          try {
            ApiFuture<BlobReadSession> sessionFuture = storage.blobReadSession(blobId);
            blobReadSession = sessionFuture.get(10, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt status
            throw new IOException("Failed to get BlobReadSession due to thread interruption", e);
          } catch (ExecutionException | TimeoutException e) {
            throw new IOException("Failed to get BlobReadSession", e);
          }
        }
      }
    }
    return blobReadSession;
  }

  public void readVectored(List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    BlobReadSession session = getBlobReadSession();
    ranges.forEach(
        range -> {
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
              executorService);
        });
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
      for (ByteBuffer b : byteString.asReadOnlyByteBufferList()) {
        buf.put(b);
      }
      buf.flip();
      range.getByteBufferFuture().complete(buf);
    }
  }

  @Override
  public void close() throws IOException {
    synchronized (this) {
      if (blobReadSession != null) {
        blobReadSession.close();
        blobReadSession = null;
      }
    }
  }
}
