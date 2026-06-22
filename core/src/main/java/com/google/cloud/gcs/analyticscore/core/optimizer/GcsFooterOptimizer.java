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

package com.google.cloud.gcs.analyticscore.core.optimizer;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

/** A {@link FormatOptimizer} that caches and serves GCS object footers (e.g., for Parquet). */
public class GcsFooterOptimizer implements FormatOptimizer {

  private static final int LARGE_FILE_SIZE_THRESHOLD = 1024 * 1024 * 1024; // 1 GB

  private final GcsReadOptions readOptions;
  private final Telemetry telemetry;

  private AnalyticsCacheManager cacheManager;
  private GcsItemId gcsItemId;
  private long fileSize = -1;
  private long prefetchSize = -1;

  public GcsFooterOptimizer(GcsReadOptions readOptions, Telemetry telemetry) {
    this.readOptions = checkNotNull(readOptions, "readOptions cannot be null");
    this.telemetry = checkNotNull(telemetry, "telemetry cannot be null");
  }

  @Override
  public boolean isApplicable(GcsItemId itemId) {
    return readOptions.isFooterPrefetchEnabled();
  }

  @Override
  public void onOpen(GcsItemId itemId, AnalyticsCacheManager cacheManager) {
    this.gcsItemId = itemId;
    this.cacheManager = cacheManager;
  }

  @Override
  public void onOpen(GcsFileInfo fileInfo, AnalyticsCacheManager cacheManager) {
    this.gcsItemId = fileInfo.getItemInfo().getItemId();
    this.cacheManager = cacheManager;
    this.fileSize = fileInfo.getItemInfo().getSize();
    this.prefetchSize = calculatePrefetchSize(fileSize, readOptions);
  }

  @Override
  public int read(long position, ByteBuffer dst, VectoredSeekableByteChannel source)
      throws IOException {
    if (fileSize == -1) {
      fileSize = source.size();
      prefetchSize = calculatePrefetchSize(fileSize, readOptions);
    }

    if (prefetchSize <= 0 || position < (fileSize - prefetchSize)) {
      return 0;
    }

    if (position >= fileSize) {
      return -1;
    }

    ByteBuffer footer = cacheManager.getFooter(gcsItemId, itemId -> loadFooter(source));

    telemetry.recordMetric(Metric.FOOTER_CACHE_HIT, 1L, Collections.emptyMap());

    ByteBuffer footerLean = footer.duplicate();
    int readStartPosition = (int) (position - (fileSize - prefetchSize));
    footerLean.position(readStartPosition);

    if (footerLean.remaining() == 0) {
      return -1;
    }

    int bytesToRead = Math.min(dst.remaining(), footerLean.remaining());
    footerLean.limit(footerLean.position() + bytesToRead);
    dst.put(footerLean);
    return bytesToRead;
  }

  private ByteBuffer loadFooter(VectoredSeekableByteChannel source) throws IOException {
    telemetry.recordMetric(Metric.FOOTER_CACHE_MISS, 1L, Collections.emptyMap());
    long startPosition = fileSize - prefetchSize;
    int bufferSize = (int) (fileSize - startPosition);
    ByteBuffer cacheBuffer = ByteBuffer.allocate(bufferSize);
    long originalPosition = source.position();
    try {
      source.position(startPosition);
      while (cacheBuffer.hasRemaining()) {
        if (source.read(cacheBuffer) == -1) {
          throw new IOException("Unexpected EOF encountered while reading footer.");
        }
      }
      cacheBuffer.flip();
      return cacheBuffer;
    } finally {
      source.position(originalPosition);
    }
  }

  private static long calculatePrefetchSize(long fileSize, GcsReadOptions readOptions) {
    if (readOptions.getSmallObjectCacheSize() >= fileSize) {
      return fileSize;
    }
    if (!readOptions.isFooterPrefetchEnabled()) {
      return 0;
    }
    return fileSize > LARGE_FILE_SIZE_THRESHOLD
        ? Math.min(readOptions.getFooterPrefetchSizeLargeFile(), fileSize)
        : Math.min(readOptions.getFooterPrefetchSizeSmallFile(), fileSize);
  }
}
