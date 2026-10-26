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
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;

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
   * Creates the target directory or bucket (and any missing parent folders on hierarchical
   * namespace buckets) at the specified {@code gs://} URI.
   *
   * <p>Semantics follow {@link #mkdirs(GcsItemId)}. Consistent with {@link #getFileInfo(URI)} and
   * {@link #open(GcsFileInfo, GcsReadOptions)}, {@code path.toString()} is parsed literally without
   * percent-decoding; callers with object names containing spaces or pre-decoded characters should
   * construct a {@link GcsItemId} and call {@link #mkdirs(GcsItemId)} instead.
   *
   * @param path the {@code gs://} URI of the directory or bucket to create
   * @throws IllegalArgumentException if {@code path} is not a valid {@code gs://} URI (for example,
   *     {@code gs:///} or consecutive slashes)
   * @throws FileNotFoundException if creating a directory inside a bucket that does not exist
   * @throws FileAlreadyExistsException if an object already exists at a path prefix (when {@link
   *     GcsFileSystemOptions#isCreateItemsConflictCheckEnabled()} is {@code true}) or a non-empty
   *     object already exists at the flat directory marker path
   * @throws AccessDeniedException if the caller lacks required GCS permissions
   * @throws IOException if an I/O error occurs
   */
  void mkdirs(URI path) throws IOException;

  /**
   * Creates the target directory or bucket for the given {@link GcsItemId}, succeeding idempotently
   * if it already exists:
   *
   * <ul>
   *   <li><b>Root ({@link GcsItemId#ROOT})</b>: returns immediately as a no-op.
   *   <li><b>Bucket</b>: invokes {@link GcsClient#createBucket(String)} with {@link
   *       GcsCreateBucketOptions#DEFAULT} (requiring a configured {@code projectId} and {@code
   *       storage.buckets.create}; HTTP {@code 409 Conflict} on an existing bucket, including one
   *       owned by another project, is treated as success, whereas object-only IAM roles fail with
   *       {@link AccessDeniedException}).
   *   <li><b>Directory</b>: on hierarchical namespace (HNS) buckets (when {@link
   *       GcsFileSystemOptions#isHnsApiEnabled()} is {@code true} and bucket metadata lookup via
   *       {@code storage.buckets.get} succeeds; {@code 401}/{@code 403} falls back to flat),
   *       recursively creates native folders via the Storage Control API. On flat buckets, creates
   *       only the leaf 0-byte directory marker object ({@code "dir/"}).
   * </ul>
   *
   * <p>When {@link GcsFileSystemOptions#isCreateItemsConflictCheckEnabled()} is {@code true},
   * performs a best-effort prefix check ({@code O(depth)} {@code storage.objects.get} lookups) for
   * conflicting files before creating the directory; this check is not atomic with respect to
   * concurrent writes.
   *
   * @param itemId the GCS item identifier of the directory, bucket, or root to create
   * @throws IllegalArgumentException if {@code itemId} has an empty bucket name with a non-empty
   *     object name
   * @throws FileNotFoundException if creating a directory inside a bucket that does not exist
   * @throws FileAlreadyExistsException if an object already exists at a path prefix (when conflict
   *     checking is enabled) or a non-empty object already exists at the flat directory marker path
   * @throws AccessDeniedException if the caller lacks required GCS permissions
   * @throws IOException if an I/O error occurs
   */
  void mkdirs(GcsItemId itemId) throws IOException;
}
