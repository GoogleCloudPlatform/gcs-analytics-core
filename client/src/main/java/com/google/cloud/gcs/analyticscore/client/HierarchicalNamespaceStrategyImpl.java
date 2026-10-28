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

import static com.google.cloud.gcs.analyticscore.client.UriUtil.PATH_DELIMITER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HierarchicalNamespaceStrategyImpl implements NamespaceStrategy {
  private static final Logger LOG =
      LoggerFactory.getLogger(HierarchicalNamespaceStrategyImpl.class);

  private final GcsClient gcsClient;

  HierarchicalNamespaceStrategyImpl(GcsClient gcsClient) {
    this.gcsClient = gcsClient;
  }

  @Override
  public void createDirectory(GcsItemId id) throws IOException {
    checkNotNull(id, "id must not be null");
    checkArgument(id.isGcsObject(), "Expected a directory folder itemId but got: %s", id);
    String rawObjectName = id.getObjectName().orElse("");
    checkArgument(
        !rawObjectName.startsWith(PATH_DELIMITER)
            && !rawObjectName.contains(PATH_DELIMITER + PATH_DELIMITER),
        "Object name cannot start with '/' or contain consecutive '/': %s",
        id);
    String folderName = UriUtil.removeTrailingSlash(rawObjectName);
    GcsItemId folderItemId =
        GcsItemId.builder().setBucketName(id.getBucketName()).setObjectName(folderName).build();
    try {
      gcsClient.createFolder(folderItemId, /* recursive= */ true);
    } catch (FileAlreadyExistsException e) {
      LOG.debug("Folder already exists for item: {}", folderItemId, e);
    }
  }

  @Override
  public GcsItemInfo getDirectoryInfo(GcsItemId id, boolean inferImplicitDirectories)
      throws IOException {
    checkNotNull(id, "Item ID must not be null.");
    // In HNS buckets, directories are always native folder resources; pass the ID as given so a
    // not-found error names the path the caller asked for, as in FlatNamespaceStrategyImpl.
    return gcsClient.getFolderInfo(id);
  }
}
