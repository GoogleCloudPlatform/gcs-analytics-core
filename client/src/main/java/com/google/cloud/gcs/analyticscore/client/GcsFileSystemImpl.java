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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auth.Credentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class GcsFileSystemImpl implements GcsFileSystem {

  private final GcsClient gcsClient;
  private final GcsFileSystemOptions fileSystemOptions;
  private Supplier<ExecutorService> executorService;

  public GcsFileSystemImpl(GcsFileSystemOptions fileSystemOptions) {
    initializeExecutorService();
    this.gcsClient = new GcsClientImpl(getGcsClientOptions(fileSystemOptions), executorService);
    this.fileSystemOptions = fileSystemOptions;
  }

  public GcsFileSystemImpl(Credentials credentials, GcsFileSystemOptions fileSystemOptions) {
    this.fileSystemOptions = fileSystemOptions;
    initializeExecutorService();
    this.gcsClient =
        new GcsClientImpl(credentials, getGcsClientOptions(fileSystemOptions), executorService);
  }

  @VisibleForTesting
  GcsFileSystemImpl(GcsClient gcsClient, GcsFileSystemOptions fileSystemOptions) {
    this.gcsClient = gcsClient;
    this.fileSystemOptions = fileSystemOptions;
    initializeExecutorService();
  }

  @Override
  public VectoredSeekableByteChannel open(GcsFileInfo gcsFileInfo, GcsReadOptions readOptions)
      throws IOException {
    checkNotNull(gcsFileInfo, "fileInfo should not be null");
    GcsItemId itemId = UriUtil.getItemIdFromString(gcsFileInfo.getUri().toString());
    checkArgument(itemId.isGcsObject(), "Expected GCS object to be provided. But got: " + itemId);
    return gcsClient.openReadChannel(gcsFileInfo.getItemInfo(), readOptions);
  }

  @Override
  public GcsFileInfo getFileInfo(URI path) throws IOException {
    checkNotNull(path, "path should not be null");
    GcsItemId itemId = UriUtil.getItemIdFromString(path.toString());
    GcsItemInfo gcsItemInfo = gcsClient.getGcsItemInfo(itemId);
    return GcsFileInfo.builder()
        .setItemInfo(gcsItemInfo)
        .setUri(path)
        .setAttributes(Collections.emptyMap())
        .build();
  }

  @Override
  public GcsFileSystemOptions getFileSystemOptions() {
    return this.fileSystemOptions;
  }

  @Override
  public GcsClient getGcsClient() {
    return this.gcsClient;
  }

  private static GcsClientOptions getGcsClientOptions(GcsFileSystemOptions fileSystemOptions) {
    return fileSystemOptions.getGcsClientOptions() == null
        ? GcsClientOptions.builder().build()
        : fileSystemOptions.getGcsClientOptions();
  }

  private void initializeExecutorService() {
    this.executorService =
        Suppliers.memoize(
            () ->
                new ThreadPoolExecutor(
                    fileSystemOptions.getReadThreadCount(),
                    fileSystemOptions.getReadThreadCount(),
                    0L,
                    TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new ThreadFactoryBuilder()
                        .setNameFormat("gcs-filesystem-range-pool-%d")
                        .setDaemon(true)
                        .build()));
  }
}
