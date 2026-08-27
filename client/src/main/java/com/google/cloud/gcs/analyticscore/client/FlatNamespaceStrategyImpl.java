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

import java.io.IOException;

final class FlatNamespaceStrategyImpl implements NamespaceStrategy {

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
    return GcsItemInfo.createNotFound(id);
  }

  // Checks if a path is an implicit directory by seeing if any objects exist with it as a prefix
  private boolean isImplicitDirectory(GcsItemId id) throws IOException {
    String prefix = UriUtil.toDirectoryPath(id.getObjectName().orElse(""));
    GcsItemId prefixId =
        GcsItemId.builder().setBucketName(id.getBucketName()).setObjectName(prefix).build();
    return !gcsClient.listFirstObjectWithPrefix(prefixId).isEmpty();
  }
}
