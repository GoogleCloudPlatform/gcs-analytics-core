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
import java.util.Optional;

final class FlatNamespaceStrategyImpl implements NamespaceStrategy {

  private final GcsClient gcsClient;

  FlatNamespaceStrategyImpl(GcsClient gcsClient) {
    this.gcsClient = gcsClient;
  }

  /**
   * Resolves directory metadata in a flat namespace bucket by listing the first object under the
   * directory prefix (e.g. {@code dir/}).
   *
   * <ul>
   *   <li>If the first listed object is the placeholder object itself ({@code dir/}), its real
   *       metadata (including generation) is returned.
   *   <li>If the first listed object is a child (e.g. {@code dir/obj.txt}), an inferred directory
   *       for {@code dir/} is returned (no generation, zero timestamps).
   * </ul>
   */
  @Override
  public GcsItemInfo getDirectoryInfo(GcsItemId id) throws IOException {
    checkNotNull(id, "Item ID must not be null.");
    GcsItemId dirId = id.toDirectoryId();
    // Returns the placeholder object "dir/" (if present) or the first child object under "dir/".
    Optional<GcsItemInfo> firstObject = gcsClient.listFirstObjectWithPrefix(dirId);
    if (firstObject.isEmpty()) {
      throw GcsExceptionUtil.createFileNotFoundException(id);
    }
    GcsItemInfo firstItemInfo = firstObject.get();
    // The placeholder object "dir/" sorts first under its own prefix, so if it exists it is listed.
    if (firstItemInfo.getItemId().getObjectName().equals(dirId.getObjectName())) {
      return firstItemInfo;
    }
    return GcsItemInfo.createInferredDirectory(dirId);
  }
}
