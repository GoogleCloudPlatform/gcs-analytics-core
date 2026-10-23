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

import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.WritableByteChannel;

public interface GcsFileSystem extends AutoCloseable {

  /**
   * Opens an object for reading.
   *
   * @param gcsFileInfo Contains information about a GCS File.
   * @param options Fine-grained read options for behaviors of retries, decryption, etc.
   * @return A channel for reading from the given object.
   * @throws FileNotFoundException if the given path does not exist.
   * @throws IOException if object exists but cannot be opened.
   */
  VectoredSeekableByteChannel open(GcsFileInfo gcsFileInfo, GcsReadOptions options)
      throws IOException;

  /**
   * Opens an object for reading.
   *
   * @param gcsItemId gcs object identifier.
   * @param options Fine-grained read options for behaviors of retries, decryption, etc.
   * @return A channel for reading from the given object.
   * @throws FileNotFoundException if the given path does not exist.
   * @throws IOException if object exists but cannot be opened.
   */
  VectoredSeekableByteChannel open(GcsItemId gcsItemId, GcsReadOptions options) throws IOException;

  /**
   * Gets metadata for the given path, which may be an object, a directory, a bucket or the root.
   *
   * @param path the {@code gs://} path to look up
   * @return metadata for the item at {@code path}
   * @throws FileNotFoundException if nothing exists at {@code path}
   * @throws IOException if the metadata cannot be fetched
   */
  GcsFileInfo getFileInfo(URI path) throws IOException;

  /**
   * Gets metadata for the given item, which may be an object, a directory, a bucket or the root.
   *
   * @param itemId the item to look up
   * @return metadata for the item
   * @throws FileNotFoundException if nothing exists at {@code itemId}
   * @throws IOException if the metadata cannot be fetched
   */
  GcsFileInfo getFileInfo(GcsItemId itemId) throws IOException;

  /**
   * Gets metadata for the object at the given path, without checking for a directory there.
   *
   * <p>Unlike {@link #getFileInfo(URI)}, this sends a single object metadata request and never
   * infers a directory. Use it when only an object is acceptable, for example before opening a file
   * for reading.
   *
   * @param path the {@code gs://} path of the object to look up
   * @return metadata for the object at {@code path}
   * @throws IllegalArgumentException if {@code path} is the root, a bucket or a directory path
   *     (ending in {@code /})
   * @throws FileNotFoundException if no object exists at {@code path}
   * @throws IOException if the metadata cannot be fetched
   */
  GcsFileInfo getFileInfoObject(URI path) throws IOException;

  /**
   * Gets metadata for the given object, without checking for a directory at its path.
   *
   * <p>Unlike {@link #getFileInfo(GcsItemId)}, this sends a single object metadata request and
   * never infers a directory. Use it when only an object is acceptable, for example before opening
   * a file for reading.
   *
   * @param itemId the object to look up
   * @return metadata for the object
   * @throws IllegalArgumentException if {@code itemId} is the root, a bucket or a directory (its
   *     object name ends in {@code /})
   * @throws FileNotFoundException if no object exists at {@code itemId}
   * @throws IOException if the metadata cannot be fetched
   */
  GcsFileInfo getFileInfoObject(GcsItemId itemId) throws IOException;

  /** Retrieve the options that were used to create this GcsFileSystem. */
  GcsFileSystemOptions getFileSystemOptions();

  /** Retrieve the gcs client used to create this GcsFileSystem. */
  GcsClient getGcsClient();

  /** Retrieve the telemetry instance used by this file system. */
  Telemetry getTelemetry();

  /** Returns the cache manager used by this file system. */
  AnalyticsCacheManager getCacheManager();

  /** Close the file system. */
  @Override
  void close();

  /**
   * Creates a new GCS object and returns a WritableByteChannel for writing to it.
   *
   * @param itemId the identity of the GCS object to be created
   * @param options configuration options for controlling upload strategies and integrity checks
   * @return a channel for writing data to the newly created object
   * @throws IOException if an I/O error occurs during channel initialization or translation
   */
  WritableByteChannel create(GcsItemId itemId, GcsWriteOptions options) throws IOException;

  /**
   * Creates a directory at the specified URI path.
   *
   * @param path the GCS URI of the directory to create
   * @throws IOException if an I/O error occurs
   */
  void mkdirs(URI path) throws IOException;

  /**
   * Creates a directory for the given {@link GcsItemId}.
   *
   * @param itemId the GCS item identifier of the directory to create
   * @throws IOException if an I/O error occurs
   */
  void mkdirs(GcsItemId itemId) throws IOException;
}
