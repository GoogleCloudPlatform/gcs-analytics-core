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

import com.google.common.annotations.VisibleForTesting;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;
import java.util.Optional;

@VisibleForTesting
public interface GcsClient {
  /** Opens a new read channel. */
  VectoredSeekableByteChannel openReadChannel(GcsItemInfo itemInfo, GcsReadOptions readOptions)
      throws IOException;

  /** Opens a new read channel. */
  VectoredSeekableByteChannel openReadChannel(GcsItemId itemId, GcsReadOptions readOptions)
      throws IOException;

  /**
   * Creates a new GCS object and returns a WritableByteChannel for writing to it.
   *
   * @param itemId the identity of the GCS object to be created
   * @param options configuration options for controlling upload strategies and integrity checks
   * @return a channel for writing data to the newly created object
   * @throws IOException if an I/O error occurs during channel initialization or translation
   */
  WritableByteChannel createWriteChannel(GcsItemId itemId, GcsWriteOptions options)
      throws IOException;

  /**
   * Moves an object to a new name in the same bucket (GCS {@code objects.move}).
   *
   * <p>The move is a server-side, metadata-only operation: no object bytes are copied. The object
   * keeps its encryption, including a customer-supplied key: a move never re-encrypts.
   *
   * <p>The content generation of {@code srcId}, if present, is sent as a source generation
   * precondition. The content generation of {@code dstId}, if present, is sent as a destination
   * generation precondition, where {@code 0} means the destination object must not exist. Without a
   * destination generation, an existing destination object is replaced.
   *
   * <p>The client library retries a move only when {@code dstId} has a generation. If an attempt
   * moved the object but its response was lost, the retry finds no source object, so this method
   * throws {@link FileNotFoundException} although the move succeeded. If the source name was
   * re-created in between, the retry fails a generation precondition instead. A caller that needs
   * to know whether the move happened must check the destination object, for example by comparing
   * its checksum with the source's. A move without a destination generation is never retried.
   *
   * @param srcId the object to move
   * @param dstId the new name of the object. It must be in the same bucket as {@code srcId}: the
   *     GCS {@code objects.move} API only moves an object within its bucket.
   * @throws IllegalArgumentException if {@code srcId} or {@code dstId} is not a GCS object, the two
   *     IDs name different buckets or the same object, the source generation is not positive, or
   *     the destination generation is negative
   * @throws FileNotFoundException if the source object does not exist
   * @throws FileAlreadyExistsException if {@code dstId} has generation {@code 0}, the destination
   *     exists, and {@code srcId} has no generation. If {@code srcId} also has a generation, GCS
   *     does not say which check failed, so a plain {@link IOException} is thrown.
   * @throws AccessDeniedException if the caller is not allowed to move the object
   * @throws IOException if any other generation precondition fails or the move fails for another
   *     reason
   * @see <a href="https://cloud.google.com/storage/docs/json_api/v1/objects/move">objects.move</a>
   */
  default void moveObject(GcsItemId srcId, GcsItemId dstId) throws IOException {
    throw new UnsupportedOperationException(
        "moveObject is not implemented by " + getClass().getName());
  }

  /**
   * Fetches metadata for the given GCS object.
   *
   * <p>This method supports GCS objects only. A missing object is reported by raising a {@link
   * FileNotFoundException}, which aligns with standard Java I/O expectations.
   *
   * <p>Buckets and folders are queried via {@link #getBucketInfo(GcsItemId)} and {@link
   * #getFolderInfo(GcsItemId)} respectively.
   *
   * @param itemId the identity of the object to fetch metadata for
   * @return metadata for the given object
   * @throws FileNotFoundException if {@code itemId} denotes an object that does not exist
   * @throws IOException if the metadata cannot be fetched
   * @throws UnsupportedOperationException if {@code itemId} does not denote a GCS object
   */
  GcsItemInfo getGcsItemInfo(GcsItemId itemId) throws IOException;

  /**
   * Fetches bucket metadata.
   *
   * @throws FileNotFoundException if {@code itemId} denotes a bucket that does not exist
   * @throws IOException if the metadata cannot be fetched
   */
  GcsItemInfo getBucketInfo(GcsItemId itemId) throws IOException;

  /**
   * Fetches folder metadata for HNS buckets.
   *
   * <p>{@code itemId} may name the folder with or without a trailing slash. The returned metadata
   * always uses the directory form of the ID (ending in {@code /}).
   *
   * @throws FileNotFoundException if {@code itemId} denotes a folder that does not exist
   * @throws IOException if the metadata cannot be fetched
   */
  GcsItemInfo getFolderInfo(GcsItemId itemId) throws IOException;

  /**
   * Lists native folders in a Hierarchical Namespace (HNS) bucket.
   *
   * <p>Returns every folder whose name starts with the directory form of {@code prefixId}'s object
   * name (for example, {@code dir} lists {@code dir/} and all folders below it), or every folder in
   * the bucket when {@code prefixId} is a bucket. All result pages under the prefix are fetched and
   * returned together. The folder named exactly by the prefix is included when it exists, and
   * folders keep the order returned by GCS. Any {@link GcsItemId#getContentGeneration()} on {@code
   * prefixId} is ignored.
   *
   * @param prefixId the bucket or folder to list folders under
   * @return an unmodifiable list of {@link GcsItemInfo.ItemType#NATIVE_FOLDER} items whose item IDs
   *     are in directory form (ending in {@code /}), or an empty list if no folder matches
   * @throws IllegalArgumentException if {@code prefixId} is the root or has an empty bucket name
   * @throws FileNotFoundException if the bucket does not exist
   * @throws AccessDeniedException if listing is denied (401/403)
   * @throws IOException if the folders cannot be listed, including when the bucket is a flat
   *     (non-HNS) bucket
   */
  default List<GcsItemInfo> listFolders(GcsItemId prefixId) throws IOException {
    throw new UnsupportedOperationException(
        "listFolders is not implemented by " + getClass().getName());
  }

  /**
   * Fetches metadata for the first object in unsigned UTF-8 byte order matching {@code prefixId}:
   * returns the placeholder directory object itself (for example, {@code "b/"}) if it exists,
   * otherwise the first nested object under that prefix (for example, {@code "b/c/obj"}), or {@link
   * Optional#empty()} if no object matches. Any {@link GcsItemId#getContentGeneration()} on {@code
   * prefixId} is ignored.
   */
  Optional<GcsItemInfo> listFirstObjectWithPrefix(GcsItemId prefixId) throws IOException;

  /**
   * Lists a single page of GCS objects and collapsed prefixes under {@code prefixId} (either a
   * bucket root {@code gs://bucket} or an object/directory prefix such as {@code gs://bucket/dir/})
   * in a single RPC (retried automatically per {@link GcsRetryOptions}), requesting up to {@link
   * GcsListOptions#DEFAULT_MAX_LIST_ITEMS_PER_CALL} ({@code 5,000}) entries when {@link
   * GcsListOptions#getPageSize()} is empty.
   *
   * <p>Results preserve GCS unsigned UTF-8 ordering. Collapsed prefixes matching a placeholder
   * object in {@link GcsListPage#getItems()} or the listed directory prefix itself are omitted from
   * {@link GcsListPage#getPrefixes()}. No synthetic self-directory is fabricated: when {@link
   * GcsListOptions#isIncludePrefixObject()} is {@code false}, a real self-marker object is surfaced
   * via {@link GcsListPage#getPrefixObject()} rather than {@link GcsListPage#getItems()}. A missing
   * prefix returns an empty page. Any {@link GcsItemId#getContentGeneration()} on {@code prefixId}
   * is ignored.
   *
   * <p>When {@link GcsListOptions#getFields()} omits optional metadata fields, unprojected fields
   * are reported as absent or {@code 0} on returned items. In particular, omitting {@link
   * GcsListOptions.ListField#SIZE} yields items with {@link GcsItemInfo#getSize()} equal to {@code
   * 0L} (with {@link GcsItemInfo#exists()} {@code true}); callers must not pass such partial {@link
   * GcsItemInfo} instances to {@link #openReadChannel(GcsItemInfo, GcsReadOptions)} or file-sizing
   * paths.
   *
   * @param prefixId bucket or object/directory prefix to list (must have a non-empty bucket name;
   *     any content generation is ignored)
   * @param options listing options controlling delimiter, projection, filters, and pagination
   * @return a single {@link GcsListPage} of object metadata and deduplicated prefix IDs
   * @throws IllegalArgumentException if {@code prefixId} has an empty bucket name (including root)
   * @throws FileNotFoundException if the target bucket does not exist ({@code 404})
   * @throws AccessDeniedException if listing is denied ({@code 401}/{@code 403})
   * @throws IOException if an I/O error occurs while listing
   */
  default GcsListPage<GcsItemInfo> listObjectsPage(GcsItemId prefixId, GcsListOptions options)
      throws IOException {
    throw new UnsupportedOperationException(
        "listObjectsPage is not implemented by " + getClass().getName());
  }

  /**
   * Creates a GCS bucket with {@link GcsCreateBucketOptions#DEFAULT}.
   *
   * @param bucketName name of the bucket to create
   * @throws FileAlreadyExistsException if a bucket with {@code bucketName} already exists
   * @throws IOException if an I/O error occurs while creating the bucket
   */
  void createBucket(String bucketName) throws IOException;

  /**
   * Creates a GCS bucket with the specified options.
   *
   * @param bucketName name of the bucket to create
   * @param options bucket creation options (location, storage class, HNS, zonal placement, TTL)
   * @throws UnsupportedOperationException if zonal placement is configured without HNS enabled,
   *     with a non-{@code RAPID} storage class, or with a TTL
   * @throws IllegalArgumentException if {@code bucketName} is empty or a non-zonal storage class is
   *     unrecognized
   * @throws FileAlreadyExistsException if a bucket with {@code bucketName} already exists
   * @throws IOException if an I/O error occurs while creating the bucket
   */
  void createBucket(String bucketName, GcsCreateBucketOptions options) throws IOException;

  /**
   * Creates a 0-byte empty object (such as a flat-namespace directory marker) with default write
   * options, inheriting {@code kmsKeyName}, {@code encryptionKey}, and {@code userProject} from the
   * client's configured {@link GcsClientOptions#getGcsWriteOptions()}.
   *
   * <p>For directory items ({@link GcsItemId#isDirectory()}) without an explicit content
   * generation, creation uses a {@code doesNotExist} precondition and is idempotent if a 0-byte
   * marker already exists; if a non-empty object already exists at {@code itemId}, {@link
   * FileAlreadyExistsException} is thrown.
   *
   * @param itemId the identity of the 0-byte GCS object to create
   * @throws FileAlreadyExistsException if a conflicting object already exists
   * @throws IOException if an I/O error occurs while creating the object
   */
  void createEmptyObject(GcsItemId itemId) throws IOException;

  /**
   * Creates a 0-byte empty object with the specified write options.
   *
   * <p>When {@link GcsWriteOptions#isOverwriteExisting()} is {@code false}, or when {@code itemId}
   * is a directory without an explicit content generation, creation uses a {@code doesNotExist}
   * precondition. If a 0-byte object already exists at {@code itemId} on a concurrent directory
   * marker creation, the conflict is ignored; otherwise, conflicts throw {@link
   * FileAlreadyExistsException}.
   *
   * @param itemId the identity of the 0-byte GCS object to create
   * @param options write options controlling overwrite behavior, headers, and encryption
   * @throws FileAlreadyExistsException if overwrite is disabled (or {@code itemId} is a directory
   *     with a non-empty existing object) and the target object already exists
   * @throws IOException if an I/O error occurs while creating the object
   */
  void createEmptyObject(GcsItemId itemId, GcsWriteOptions options) throws IOException;

  /**
   * Creates a native folder resource in a Hierarchical Namespace (HNS) enabled bucket.
   *
   * <p>This operation only applies to buckets with Hierarchical Namespace enabled.
   *
   * @param itemId the identity of the folder to create
   * @param recursive if {@code true}, automatically creates any missing parent folders along the
   *     path; if {@code false}, requires the immediate parent folder to already exist
   * @throws FileAlreadyExistsException if the target folder already exists
   * @throws IOException if an I/O error occurs while creating the folder
   */
  void createFolder(GcsItemId itemId, boolean recursive) throws IOException;

  /**
   * Fetches bucket capability properties (such as HNS and Rapid storage class).
   *
   * <p>If bucket metadata cannot be retrieved because the caller lacks bucket-level permissions
   * (401/403), this method degrades gracefully to {@code BucketProperties.create(false, false)}
   * (flat namespace and non-RAPID storage class).
   *
   * @throws FileNotFoundException if {@code bucketName} denotes a bucket that does not exist (404)
   * @throws IOException if the bucket properties cannot be fetched
   */
  BucketProperties getBucketProperties(String bucketName) throws IOException;

  /** Close the client. */
  void close();
}
