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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class FlatNamespaceStrategyImpl implements NamespaceStrategy {

  private static final Logger logger = LoggerFactory.getLogger(FlatNamespaceStrategyImpl.class);

  private final GcsClient gcsClient;

  FlatNamespaceStrategyImpl(GcsClient gcsClient) {
    this.gcsClient = gcsClient;
  }

  @Override
  public GcsItemInfo getDirectoryInfo(GcsItemId id) throws IOException {
    checkNotNull(id, "Item ID must not be null.");
    if (isImplicitDirectory(id)) {
      return GcsItemInfo.createInferredDirectory(id);
    }
    throw new FileNotFoundException("Directory not found: " + id);
  }

  // Checks if a path is an implicit directory by seeing if any objects exist with it as a prefix
  private boolean isImplicitDirectory(GcsItemId id) {
    String prefix = UriUtil.ensureTrailingSlash(id.getObjectName().orElse(""));
    GcsItemId prefixId =
        GcsItemId.builder().setBucketName(id.getBucketName()).setObjectName(prefix).build();
    try {
      return !gcsClient.listObjectInfo(prefixId, 1).isEmpty();
    } catch (IOException e) {
      logger.warn("Failed to check if {} is an implicit directory, returning false", id, e);
      return false;
    }
  }
}
