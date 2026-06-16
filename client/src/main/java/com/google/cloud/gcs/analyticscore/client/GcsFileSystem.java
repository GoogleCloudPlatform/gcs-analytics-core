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
import java.util.Map;

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
   * Gets Metadata about the given path item.
   *
   * @param path The path we want Metadata about.
   * @return Metadata about the given path item.
   */
  GcsFileInfo getFileInfo(URI path) throws IOException;

  /** Gets Metadata about the given gcs object represented by itemId. */
  GcsFileInfo getFileInfo(GcsItemId itemId) throws IOException;

  /**
   * Gets Metadata about the given GCS object, interpreting it as the specified PathType.
   *
   * @param itemId The identifier of the GCS object.
   * @param pathType The type of path (e.g., file or directory).
   * @return Metadata about the given path item.
   */
  GcsItemInfo getFileInfo(GcsItemId itemId, PathType pathType) throws IOException;

  /**
   * Lists the statuses of the files/directories in the given path.
   *
   * @param path The path we want to list.
   * @return A list of GcsFileInfo.
   */
  java.util.List<GcsFileInfo> listStatus(URI path) throws IOException;

  /**
   * Lists the statuses of the files/directories in the given path.
   *
   * @param itemId The identifier of the given path.
   * @return A list of GcsFileInfo.
   */
  java.util.List<GcsFileInfo> listStatus(GcsItemId itemId) throws IOException;

  /**
   * Creates the directory named by the given identifier, including any necessary but nonexistent
   * parent directories.
   *
   * @param id The identifier for the directory to create.
   */
  void mkdirs(GcsItemId id) throws IOException;

  /**
   * Deletes the item denoted by the given identifier. If recursive is true and the item is a
   * directory, all contents will be deleted.
   *
   * @param id The identifier of the item to delete.
   * @param recursive Whether to recursively delete contents if the item is a directory.
   */
  void delete(GcsItemId id, boolean recursive) throws IOException;

  /**
   * Renames the item from the source identifier to the destination identifier.
   *
   * @param src The current identifier of the item.
   * @param dst The new identifier for the item.
   */
  void rename(GcsItemId src, GcsItemId dst) throws IOException;

  /**
   * Retrieves the value of an extended attribute (custom metadata) for the given item.
   *
   * @param id The identifier of the item.
   * @param name The name of the extended attribute.
   * @return The byte array value of the extended attribute, or null if it does not exist.
   */
  byte[] getXAttr(GcsItemId id, String name) throws IOException;

  /**
   * Sets an extended attribute (custom metadata) for the given item.
   *
   * @param id The identifier of the item.
   * @param name The name of the extended attribute.
   * @param value The byte array value to set for the attribute.
   */
  void setXAttr(GcsItemId id, String name, byte[] value) throws IOException;

  /**
   * Retrieves all extended attributes (custom metadata) for the given item.
   *
   * @param id The identifier of the item.
   * @return A map of extended attribute names to their byte array values.
   */
  Map<String, byte[]> getXAttrs(GcsItemId id) throws IOException;

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
}
