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
import java.nio.file.FileAlreadyExistsException;
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
   * Fetches metadata for the given GCS object.
   *
   * <p>This method supports GCS objects only. A missing object is reported by raising a {@link
   * FileNotFoundException}, which aligns with standard Java I/O expectations.
   *
   * <p>Buckets and folders are queried via {@link #getBucketInfo(GcsItemId)} and {@link
   * #getFolderInfo(GcsItemId)} respectively. Those methods report a missing item differently, by
   * returning a not-found {@link GcsItemInfo} instead of throwing.
   *
   * @param itemId the identity of the object to fetch metadata for
   * @return metadata for the given object
   * @throws FileNotFoundException if {@code itemId} denotes an object that does not exist
   * @throws IOException if the metadata cannot be fetched
   * @throws UnsupportedOperationException if {@code itemId} does not denote a GCS object
   */
  GcsItemInfo getGcsItemInfo(GcsItemId itemId) throws IOException;

  /** Fetches bucket metadata. */
  GcsItemInfo getBucketInfo(GcsItemId itemId) throws IOException;

  /** Fetches folder metadata for HNS buckets. */
  GcsItemInfo getFolderInfo(GcsItemId itemId) throws IOException;

  /**
   * Fetches metadata for at most one object matching the given prefix without pagination. Optimized
   * for fast directory emptiness checks.
   */
  Optional<GcsItemInfo> listFirstObjectWithPrefix(GcsItemId prefixId) throws IOException;

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
   * @throws FileAlreadyExistsException if a bucket with {@code bucketName} already exists
   * @throws IOException if an I/O error occurs while creating the bucket
   */
  void createBucket(String bucketName, GcsCreateBucketOptions options) throws IOException;

  /**
   * Creates a 0-byte empty object (such as a flat-namespace directory marker) with default write
   * options.
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

  /** Fetches bucket capability properties (such as HNS and Rapid storage class). */
  BucketProperties getBucketProperties(String bucketName) throws IOException;

  /** Close the client. */
  void close();
}
