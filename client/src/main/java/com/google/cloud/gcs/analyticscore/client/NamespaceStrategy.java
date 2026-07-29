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

/**
 * Strategy interface for namespace operations.
 *
 * <p>Methods for directory operations will be added in follow-up PRs. These methods will include:
 *
 * <ul>
 *   <li>{@code GcsItemInfo getFileInfo(GcsItemId id, PathType pathType) throws IOException;}
 *   <li>{@code void createDirectory(GcsItemId id) throws IOException;}
 *   <li>{@code boolean isDirectoryEmpty(GcsItemId id) throws IOException;}
 *   <li>{@code void renameDirectory(GcsItemId src, GcsItemId dst) throws IOException;}
 *   <li>{@code java.util.List<GcsItemInfo> listObjectInfo(GcsItemId id) throws IOException;}
 *   <li>{@code java.util.List<GcsItemInfo> listRecursive(GcsItemId id) throws IOException;}
 * </ul>
 */
interface NamespaceStrategy {}
