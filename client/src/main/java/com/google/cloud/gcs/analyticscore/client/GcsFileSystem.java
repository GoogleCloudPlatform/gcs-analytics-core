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
import java.io.InterruptedIOException;
import java.net.URI;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;

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

  /**
   * Lists the object, or the immediate children of the directory or bucket, at the given {@code
   * gs://} path.
   *
   * <p>Semantics follow {@link #listFileInfo(GcsItemId)}. Consistent with {@link
   * #getFileInfo(URI)}, {@code path.toString()} is parsed literally without percent-decoding.
   *
   * <p>The default implementation parses {@code path} and calls {@link #listFileInfo(GcsItemId)}.
   *
   * @param path the {@code gs://} path of the object, directory or bucket to list
   * @return an unmodifiable list of the listed items, as described in {@link
   *     #listFileInfo(GcsItemId)}
   * @throws IllegalArgumentException if {@code path} is not a valid {@code gs://} URI (for example,
   *     {@code gs:///} or consecutive slashes), or if a listed item can't be returned as described
   *     in {@link #listFileInfo(GcsItemId)}
   * @throws FileNotFoundException if neither an object nor a directory exists at {@code path}, or
   *     if the bucket does not exist
   * @throws AccessDeniedException if the caller lacks permission to read or list {@code path}
   * @throws UnsupportedOperationException if {@code path} is the root ({@code gs:/}), or if this
   *     implementation does not support {@link #listFileInfo(GcsItemId)}
   * @throws InterruptedIOException if the calling thread is interrupted before a page of the
   *     listing is requested or while it waits for the background listing (its interrupt status
   *     stays set), or if {@link #close()} interrupts a running background listing (the calling
   *     thread's interrupt status is not set)
   * @throws IOException if the object, bucket, folder or listing request fails
   */
  default List<GcsFileInfo> listFileInfo(URI path) throws IOException {
    return listFileInfo(UriUtil.getItemIdFromUri(path));
  }

  /**
   * Lists the given object, or the immediate children of the given directory or bucket.
   *
   * <p>If {@code itemId} names an existing object (its object name does not end in {@code /}), the
   * result holds only that object, even if a directory with the same name also exists. Otherwise
   * {@code itemId} is listed as a directory, and the result holds its immediate children:
   *
   * <ul>
   *   <li>Objects, including the placeholder objects of subdirectories (such as {@code dir/sub/}).
   *   <li>Subdirectories without a placeholder object, as {@link
   *       GcsItemInfo.ItemType#INFERRED_DIRECTORY} items. These are implicit directories and, on
   *       hierarchical namespace (HNS) buckets, folders. Such items have no timestamps; use {@link
   *       #getFileInfo(GcsItemId)} for the metadata of a folder.
   * </ul>
   *
   * <p>The placeholder object of the listed directory itself is not returned, so an existing empty
   * directory or bucket gives an empty list. The children are sorted by {@link
   * GcsFileInfo#getUri()}: shorter URI strings first, then in lexicographic order.
   *
   * <p>A bucket is listed as an HNS bucket only when {@link GcsFileSystemOptions#isHnsApiEnabled()}
   * is {@code true} and the bucket metadata lookup ({@code storage.buckets.get}) succeeds; {@code
   * 401}/{@code 403} falls back to flat. When an HNS bucket is listed as a flat bucket, a folder
   * with no objects under it is not returned, and listing such a folder throws {@link
   * FileNotFoundException}.
   *
   * <p>Consistent with {@link #getFileInfo(GcsItemId)}, each item's {@link GcsFileInfo#getUri()} is
   * built literally from its GCS object name without percent-encoding. As a result:
   *
   * <ul>
   *   <li>If the name of a listed item cannot be written as a {@link URI} (for example, because it
   *       contains a space), the call throws {@link IllegalArgumentException} with no partial
   *       result. {@link GcsClient#listObjectsPage} can list such names.
   *   <li>If an object name contains URI syntax characters such as {@code %XX}, {@code #} or {@code
   *       ?}, {@link URI#getPath()} differs from the GCS object name; use {@code
   *       fileInfo.getItemInfo().getItemId()} for the exact object name.
   * </ul>
   *
   * <p>Listing the root, which means listing buckets, is not supported. Calling this method during
   * or after {@link #close()} has implementation-defined behavior: on {@link GcsFileSystemImpl},
   * object-form paths fail with {@link java.util.concurrent.RejectedExecutionException} once the
   * status executor shuts down, while directory and bucket paths do not use that executor.
   *
   * <p>The default implementation throws {@link UnsupportedOperationException}, so that
   * implementations written against earlier versions of this interface keep compiling; {@link
   * GcsFileSystemImpl} overrides it.
   *
   * @param itemId the object, directory or bucket to list; any content generation is ignored
   * @return an unmodifiable list with the object, or with the children of the directory or bucket
   * @throws IllegalArgumentException if {@code itemId} has an empty bucket name with a non-empty
   *     object name, or if the name of a listed item can't be written as a {@link
   *     GcsFileInfo#getUri()} (for example, because it contains a space)
   * @throws FileNotFoundException if neither an object nor a directory exists at {@code itemId}, or
   *     if the bucket does not exist
   * @throws AccessDeniedException if the caller lacks permission to read or list {@code itemId}
   * @throws UnsupportedOperationException if {@code itemId} is the root, or if this implementation
   *     does not support listing
   * @throws InterruptedIOException if the calling thread is interrupted before a page of the
   *     listing is requested or while it waits for the background listing (its interrupt status
   *     stays set), or if {@link #close()} interrupts a running background listing (the calling
   *     thread's interrupt status is not set)
   * @throws IOException if the object, bucket, folder or listing request fails
   */
  default List<GcsFileInfo> listFileInfo(GcsItemId itemId) throws IOException {
    throw new UnsupportedOperationException(
        "listFileInfo is not supported by " + getClass().getName());
  }

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
   * <p>The default implementation parses {@code path} and calls {@link #mkdirs(GcsItemId)}.
   *
   * @param path the {@code gs://} URI of the directory or bucket to create
   * @throws IllegalArgumentException if {@code path} is not a valid {@code gs://} URI (for example,
   *     {@code gs:///} or consecutive slashes)
   * @throws FileNotFoundException if creating a directory inside a bucket that does not exist
   * @throws FileAlreadyExistsException if an object already exists at a path prefix (when {@link
   *     GcsFileSystemOptions#isCreateItemsConflictCheckEnabled()} is {@code true}) or a non-empty
   *     object already exists at the flat directory marker path
   * @throws AccessDeniedException if the caller lacks required GCS permissions
   * @throws UnsupportedOperationException if this implementation does not support {@link
   *     #mkdirs(GcsItemId)}
   * @throws IOException if an I/O error occurs
   */
  default void mkdirs(URI path) throws IOException {
    mkdirs(UriUtil.getItemIdFromUri(path));
  }

  /**
   * Creates the target directory or bucket for the given {@link GcsItemId}, succeeding idempotently
   * if it already exists:
   *
   * <ul>
   *   <li><b>Root ({@link GcsItemId#ROOT})</b>: returns immediately as a no-op.
   *   <li><b>Bucket</b>: invokes {@link GcsClient#createBucket(String)} with {@link
   *       GcsCreateBucketOptions#DEFAULT} (requiring {@code storage.buckets.create}; HTTP {@code
   *       409 Conflict} on an existing bucket, including one owned by another project, is treated
   *       as success, whereas object-only IAM roles fail with {@link AccessDeniedException}).
   *   <li><b>Directory</b>: on hierarchical namespace (HNS) buckets (when {@link
   *       GcsFileSystemOptions#isHnsApiEnabled()} is {@code true} and bucket metadata lookup via
   *       {@code storage.buckets.get} succeeds; {@code 401}/{@code 403} falls back to flat),
   *       recursively creates native folders via the Storage Control API. On flat buckets, creates
   *       only the leaf 0-byte directory marker object ({@code "dir/"}); if a non-empty object
   *       already exists there, the marker can't be created and {@link FileAlreadyExistsException}
   *       is thrown. HNS buckets succeed in that case, because writing such an object also creates
   *       the native folder.
   * </ul>
   *
   * <p>When {@link GcsFileSystemOptions#isCreateItemsConflictCheckEnabled()} is {@code true},
   * performs a best-effort prefix check ({@code O(depth)} {@code storage.objects.get} lookups) for
   * conflicting files before creating the directory; this check is not atomic with respect to
   * concurrent writes, and it skips paths the caller can't read (for example, outside a Credential
   * Access Boundary or with a role that lacks {@code storage.objects.get}).
   *
   * <p>The default implementation throws {@link UnsupportedOperationException}, so that
   * implementations written against earlier versions of this interface keep compiling; {@link
   * GcsFileSystemImpl} overrides it.
   *
   * @param itemId the GCS item identifier of the directory, bucket, or root to create
   * @throws IllegalArgumentException if {@code itemId} has an empty bucket name with a non-empty
   *     object name, or if the object name starts with {@code "/"} or contains consecutive {@code
   *     "/"} characters
   * @throws FileNotFoundException if creating a directory inside a bucket that does not exist
   * @throws FileAlreadyExistsException if an object already exists at a path prefix (when conflict
   *     checking is enabled) or a non-empty object already exists at the flat directory marker path
   * @throws AccessDeniedException if the caller lacks required GCS permissions
   * @throws UnsupportedOperationException if this implementation does not support creating
   *     directories
   * @throws IOException if an I/O error occurs
   */
  default void mkdirs(GcsItemId itemId) throws IOException {
    throw new UnsupportedOperationException("mkdirs is not supported by " + getClass().getName());
  }
}
