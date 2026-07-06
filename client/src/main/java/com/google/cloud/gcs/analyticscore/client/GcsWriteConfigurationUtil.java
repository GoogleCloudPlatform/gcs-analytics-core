package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.storage.BlobWriteSessionConfig;
import com.google.cloud.storage.BlobWriteSessionConfigs;
import com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig;
import com.google.cloud.storage.Storage.BlobWriteOption;
import java.io.IOException;
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
      GcsClientOptions clientOptions, boolean isHttpTransport) throws IOException {
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

  private static BlobWriteSessionConfig getWriteToDiskSessionConfig(GcsClientOptions clientOptions)
      throws IOException {
    if (!clientOptions.getTemporaryPaths().isEmpty()) {
      List<Path> paths = toPaths(clientOptions.getTemporaryPaths());
      return BlobWriteSessionConfigs.bufferToDiskThenUpload(paths);
    } else {
      return BlobWriteSessionConfigs.bufferToTempDirThenUpload();
    }
  }

  private static BlobWriteSessionConfig getJournalingSessionConfig(
      GcsClientOptions clientOptions, boolean isHttpTransport) throws IOException {
    if (isHttpTransport) {
      throw new UnsupportedOperationException(
          "JOURNALING upload type is not supported because it requires the gRPC "
              + "transport backend (HTTP transport is currently active).");
    }
    checkArgument(
        !clientOptions.getTemporaryPaths().isEmpty(),
        "Temporary paths must be configured for JOURNALING upload type");
    List<Path> paths = toPaths(clientOptions.getTemporaryPaths());
    return BlobWriteSessionConfigs.journaling(paths);
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
