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

import com.google.cloud.storage.BlobWriteSessionConfig;
import com.google.cloud.storage.BlobWriteSessionConfigs;
import com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig;
import com.google.cloud.storage.Storage.BlobWriteOption;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

final class GcsWriteConfigurationUtil {

  private GcsWriteConfigurationUtil() {
    // Utility class
  }

  static BlobWriteSessionConfig generateSessionConfig(
      GcsClientOptions clientOptions, boolean isHttpTransport) {
    switch (clientOptions.getUploadType()) {
      case PARALLEL_COMPOSITE_UPLOAD:
        return getParallelCompositeUploadSessionConfig(clientOptions);
      case WRITE_TO_DISK_THEN_UPLOAD:
        return getWriteToDiskSessionConfig(clientOptions);
      case JOURNALING:
        return getJournalingSessionConfig(clientOptions, isHttpTransport);
      case CHUNK_UPLOAD:
        return BlobWriteSessionConfigs.getDefault()
            .withChunkSize(clientOptions.getUploadChunkSize());
      default:
        return BlobWriteSessionConfigs.getDefault();
    }
  }

  private static BlobWriteSessionConfig getParallelCompositeUploadSessionConfig(
      GcsClientOptions clientOptions) {
    return BlobWriteSessionConfigs.parallelCompositeUpload()
        .withBufferAllocationStrategy(
            ParallelCompositeUploadBlobWriteSessionConfig.BufferAllocationStrategy.fixedPool(
                clientOptions.getPcuBufferCount(), clientOptions.getPcuBufferCapacity()))
        .withPartCleanupStrategy(getSdkCleanupStrategy(clientOptions.getPcuPartFileCleanupType()))
        .withPartNamingStrategy(
            ParallelCompositeUploadBlobWriteSessionConfig.PartNamingStrategy.prefix(
                clientOptions.getPcuPartFileNamePrefix()));
  }

  private static BlobWriteSessionConfig getWriteToDiskSessionConfig(
      GcsClientOptions clientOptions) {
    try {
      if (!clientOptions.getTemporaryPaths().isEmpty()) {
        List<Path> paths = toPaths(clientOptions.getTemporaryPaths());
        return BlobWriteSessionConfigs.bufferToDiskThenUpload(paths);
      } else {
        return BlobWriteSessionConfigs.bufferToTempDirThenUpload();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed while initializing configs for " + clientOptions.getUploadType(), e);
    }
  }

  private static BlobWriteSessionConfig getJournalingSessionConfig(
      GcsClientOptions clientOptions, boolean isHttpTransport) {
    // TODO: Add the isHttpTransport check and support for JOURNALING once gRPC support is added to
    // gcs-analytics-core.
    throw new UnsupportedOperationException(
        "JOURNALING upload type is not supported since it requires gRPC transport.");
  }

  private static List<Path> toPaths(Collection<String> pathStrings) {
    return pathStrings.stream().map(Paths::get).collect(Collectors.toList());
  }

  private static ParallelCompositeUploadBlobWriteSessionConfig.PartCleanupStrategy
      getSdkCleanupStrategy(GcsClientOptions.PartFileCleanupType cleanupType) {
    switch (cleanupType) {
      case NEVER:
        return ParallelCompositeUploadBlobWriteSessionConfig.PartCleanupStrategy.never();
      case ON_SUCCESS:
        return ParallelCompositeUploadBlobWriteSessionConfig.PartCleanupStrategy.onlyOnSuccess();
      case ALWAYS:
      default:
        return ParallelCompositeUploadBlobWriteSessionConfig.PartCleanupStrategy.always();
    }
  }

  static BlobWriteOption[] generateWriteOptions(GcsWriteOptions writeOptions, GcsItemId itemId) {
    List<BlobWriteOption> sdkWriteOptions = new ArrayList<>();

    if (writeOptions != null) {
      if (writeOptions.isDisableGzipContent()) {
        sdkWriteOptions.add(BlobWriteOption.disableGzipContent());
      }
      if (writeOptions.isChecksumValidationEnabled()) {
        sdkWriteOptions.add(BlobWriteOption.crc32cMatch());
      }
      writeOptions.getKmsKeyName().map(BlobWriteOption::kmsKeyName).ifPresent(sdkWriteOptions::add);
      writeOptions
          .getEncryptionKey()
          .map(BlobWriteOption::encryptionKey)
          .ifPresent(sdkWriteOptions::add);
      writeOptions
          .getUserProject()
          .map(BlobWriteOption::userProject)
          .ifPresent(sdkWriteOptions::add);
    }

    // Determine overwrite semantics based on exact generation ID or 'doesNotExist' flag
    if (itemId.getContentGeneration().isPresent()) {
      sdkWriteOptions.add(BlobWriteOption.generationMatch());
    } else if (writeOptions != null && !writeOptions.isOverwriteExisting()) {
      sdkWriteOptions.add(BlobWriteOption.doesNotExist());
    }

    return sdkWriteOptions.toArray(new BlobWriteOption[0]);
  }
}
