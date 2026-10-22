/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.UnaryOperator;

/**
 * In-memory {@link Storage} fake for hermetic unit tests and benchmarks: only {@code reader(BlobId,
 * BlobSourceOption...)} and {@code close()} are supported; other methods throw {@link
 * UnsupportedOperationException}. Readers open lazily on {@code read} (and reopen after {@code
 * seek}/{@code limit} changes on an active stream); unknown objects throw a 404 {@link
 * StorageException} on {@code read}.
 */
public final class InMemoryStorage {
  private final ImmutableMap<GcsItemId, byte[]> objects;
  private final UnaryOperator<ReadChannel> readChannelDecorator;
  private final Storage storage;
  // TODO: Track metadata (storage.get / getGcsItemInfo) and bucketProperties lookup counts, and
  // support StorageControlClient directory operations in follow-up PRs.
  private final LongAdder readerOpens = new LongAdder();
  private final LongAdder bytesServed = new LongAdder();

  private InMemoryStorage(Map<GcsItemId, byte[]> objects, UnaryOperator<ReadChannel> decorator) {
    ImmutableMap.Builder<GcsItemId, byte[]> builder = ImmutableMap.builder();
    objects.forEach(
        (id, data) -> {
          String name =
              id.getObjectName()
                  .orElseThrow(() -> new IllegalArgumentException("Expected GCS object: " + id));
          builder.put(key(id.getBucketName(), name), data);
        });
    this.objects = builder.buildOrThrow();
    this.readChannelDecorator = checkNotNull(decorator, "decorator must not be null");
    this.storage =
        (Storage)
            Proxy.newProxyInstance(
                Storage.class.getClassLoader(), new Class<?>[] {Storage.class}, this::invoke);
  }

  public static InMemoryStorage create(Map<GcsItemId, byte[]> objects) {
    return new InMemoryStorage(objects, UnaryOperator.identity());
  }

  public static InMemoryStorage create(
      Map<GcsItemId, byte[]> objects, UnaryOperator<ReadChannel> readChannelDecorator) {
    return new InMemoryStorage(objects, readChannelDecorator);
  }

  public Storage storage() {
    return storage;
  }

  public Optional<byte[]> find(GcsItemId itemId) {
    return itemId.getObjectName().map(name -> objects.get(key(itemId.getBucketName(), name)));
  }

  public long readerOpens() {
    return readerOpens.sum();
  }

  public long bytesServed() {
    return bytesServed.sum();
  }

  public void resetCounters() {
    readerOpens.reset();
    bytesServed.reset();
  }

  private Object invoke(Object proxy, Method method, Object[] args) {
    if (method.getDeclaringClass() == Object.class) {
      switch (method.getName()) {
        case "equals":
          return proxy == args[0];
        case "hashCode":
          return System.identityHashCode(proxy);
        default:
          return "InMemoryStorage{objects=" + objects.size() + "}";
      }
    }
    if (method.getName().equals("reader")
        && args != null
        && args.length > 0
        && args[0] instanceof BlobId) {
      BlobId blobId = (BlobId) args[0];
      byte[] data = objects.get(key(blobId.getBucket(), blobId.getName()));
      return readChannelDecorator.apply(new ByteArrayReadChannel(data, readerOpens, bytesServed));
    }
    if (method.getName().equals("close")) {
      return null;
    }
    throw new UnsupportedOperationException(
        "InMemoryStorage does not support Storage." + method.getName());
  }

  private static GcsItemId key(String bucket, String object) {
    return GcsItemId.builder().setBucketName(bucket).setObjectName(object).build();
  }

  /** Zero-copy {@link ReadChannel} over a byte array; {@code limit} is an absolute end offset. */
  static final class ByteArrayReadChannel implements ReadChannel {
    private final byte[] data;
    private final LongAdder readerOpens;
    private final LongAdder bytesServed;
    private long position;
    private long limit = Long.MAX_VALUE;
    private boolean open = true;
    private boolean streamOpen;

    ByteArrayReadChannel(byte[] data, LongAdder readerOpens, LongAdder bytesServed) {
      this.data = data;
      this.readerOpens = readerOpens;
      this.bytesServed = bytesServed;
    }

    @Override
    public int read(ByteBuffer dst) throws ClosedChannelException {
      if (!open) {
        throw new ClosedChannelException();
      }
      if (data == null) {
        throw new StorageException(404, "Unknown object");
      }
      long end = Math.min(limit, data.length);
      if (position >= end) {
        return -1;
      }
      int count = (int) Math.min(dst.remaining(), end - position);
      if (count > 0 && !streamOpen) {
        streamOpen = true;
        readerOpens.increment();
      }
      dst.put(data, (int) position, count);
      position += count;
      bytesServed.add(count);
      return count;
    }

    @Override
    public void seek(long position) {
      checkArgument(position >= 0, "position must be >= 0, got %s", position);
      if (position != this.position) {
        this.position = position;
        streamOpen = false;
      }
    }

    @Override
    public ReadChannel limit(long limit) {
      if (limit != this.limit) {
        this.limit = limit;
        streamOpen = false;
      }
      return this;
    }

    @Override
    public void setChunkSize(int chunkSize) {}

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public void close() {
      open = false;
    }

    @Override
    public RestorableState<ReadChannel> capture() {
      throw new UnsupportedOperationException("capture() is not supported");
    }
  }
}
