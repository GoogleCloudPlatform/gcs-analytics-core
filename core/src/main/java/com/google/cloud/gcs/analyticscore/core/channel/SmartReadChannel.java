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

package com.google.cloud.gcs.analyticscore.core.channel;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.core.optimizer.FormatOptimizer;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;
import javax.annotation.Nullable;

/**
 * A {@link VectoredSeekableByteChannel} decorator that orchestrates {@link FormatOptimizer}s to
 * apply format-specific optimizations to read operations.
 */
public class SmartReadChannel implements VectoredSeekableByteChannel {

  private final VectoredSeekableByteChannel delegate;
  private final List<FormatOptimizer> optimizers;

  private SmartReadChannel(
      VectoredSeekableByteChannel delegate,
      GcsItemId itemId,
      AnalyticsCacheManager cacheManager,
      List<FormatOptimizer> optimizers)
      throws IOException {
    this.delegate = checkNotNull(delegate, "delegate cannot be null");
    this.optimizers =
        optimizers.stream()
            .filter(optimizer -> optimizer.isApplicable(itemId))
            .collect(ImmutableList.toImmutableList());
    int openedCount = 0;
    try {
      for (FormatOptimizer optimizer : this.optimizers) {
        optimizer.onOpen(itemId, cacheManager);
        openedCount++;
      }
    } catch (IOException | RuntimeException | Error e) {
      cleanupOnInitializationFailure(this.optimizers, openedCount, e);
      throw e;
    }
  }

  private SmartReadChannel(
      VectoredSeekableByteChannel delegate,
      GcsFileInfo fileInfo,
      AnalyticsCacheManager cacheManager,
      List<FormatOptimizer> optimizers)
      throws IOException {
    this.delegate = checkNotNull(delegate, "delegate cannot be null");
    this.optimizers =
        optimizers.stream()
            .filter(optimizer -> optimizer.isApplicable(fileInfo))
            .collect(ImmutableList.toImmutableList());
    int openedCount = 0;
    try {
      for (FormatOptimizer optimizer : this.optimizers) {
        optimizer.onOpen(fileInfo, cacheManager);
        openedCount++;
      }
    } catch (IOException | RuntimeException | Error e) {
      cleanupOnInitializationFailure(this.optimizers, openedCount, e);
      throw e;
    }
  }

  /** Returns a new builder for {@link SmartReadChannel}. */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    long position = delegate.position();
    for (FormatOptimizer optimizer : optimizers) {
      int bytesRead = optimizer.read(position, dst, delegate);
      if (bytesRead > 0) {
        delegate.position(position + bytesRead);
        return bytesRead;
      }
      if (bytesRead < 0) {
        return bytesRead;
      }
      if (delegate.position() != position) {
        delegate.position(position);
      }
    }
    return delegate.read(dst);
  }

  @Override
  public void readVectored(List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    delegate.readVectored(ranges, allocate);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return delegate.write(src);
  }

  @Override
  public long position() throws IOException {
    return delegate.position();
  }

  @Override
  public VectoredSeekableByteChannel position(long newPosition) throws IOException {
    delegate.position(newPosition);
    return this;
  }

  @Override
  public long size() throws IOException {
    return delegate.size();
  }

  @Override
  public VectoredSeekableByteChannel truncate(long size) throws IOException {
    delegate.truncate(size);
    return this;
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public void close() throws IOException {
    if (!isOpen()) {
      return;
    }

    Throwable closeException = null;

    for (FormatOptimizer optimizer : optimizers) {
      closeException = closeAndAccumulate(optimizer::onClose, closeException);
    }
    closeException = closeAndAccumulate(delegate, closeException);

    if (closeException != null) {
      rethrow(closeException);
    }
  }

  private static void cleanupOnInitializationFailure(
      List<FormatOptimizer> optimizers, int openedCount, Throwable e) {
    for (FormatOptimizer openedOptimizer : optimizers.subList(0, openedCount)) {
      Throwable unused = closeAndAccumulate(openedOptimizer::onClose, e);
    }
  }

  private static Throwable closeAndAccumulate(
      java.io.Closeable closeable, @Nullable Throwable accumulated) {
    try {
      closeable.close();
    } catch (IOException | RuntimeException | Error e) {
      if (accumulated == null) {
        return e;
      }
      accumulated.addSuppressed(e);
    }
    return accumulated;
  }

  private static void rethrow(Throwable t) throws IOException {
    if (t instanceof IOException) {
      throw (IOException) t;
    }
    if (t instanceof Error) {
      throw (Error) t;
    }
    throw (RuntimeException) t;
  }

  /** Builder for {@link SmartReadChannel}. */
  public static class Builder {
    private VectoredSeekableByteChannel delegate;
    private GcsItemId itemId;
    @Nullable private GcsFileInfo fileInfo;
    private AnalyticsCacheManager cacheManager;
    private final ImmutableList.Builder<FormatOptimizer> optimizers = ImmutableList.builder();

    public Builder setDelegate(VectoredSeekableByteChannel delegate) {
      this.delegate = delegate;
      return this;
    }

    public Builder setItemId(GcsItemId itemId) {
      this.itemId = itemId;
      return this;
    }

    public Builder setFileInfo(@Nullable GcsFileInfo fileInfo) {
      this.fileInfo = fileInfo;
      return this;
    }

    public Builder setCacheManager(AnalyticsCacheManager cacheManager) {
      this.cacheManager = cacheManager;
      return this;
    }

    public Builder addOptimizer(FormatOptimizer optimizer) {
      this.optimizers.add(optimizer);
      return this;
    }

    public SmartReadChannel build() throws IOException {
      checkNotNull(delegate, "delegate must be set");
      checkNotNull(cacheManager, "cacheManager must be set");
      List<FormatOptimizer> optimizerList = optimizers.build();

      if (fileInfo != null) {
        return new SmartReadChannel(delegate, fileInfo, cacheManager, optimizerList);
      }
      checkNotNull(itemId, "itemId must be set if fileInfo is missing");
      return new SmartReadChannel(delegate, itemId, cacheManager, optimizerList);
    }
  }
}
