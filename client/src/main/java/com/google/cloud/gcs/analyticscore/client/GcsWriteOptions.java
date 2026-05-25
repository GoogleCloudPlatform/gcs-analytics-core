/*
 * Copyright 2026 Google LLC
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

import com.google.common.collect.ImmutableSet;
import java.util.Collection;

/**
 * Configuration options for writing objects to Google Cloud Storage.
 *
 * <p>This class abstracts client-specific configurations into a unified, generic set of properties
 * utilized by {@code gcs-analytics-core}. By centralizing these options, it ensures that any
 * integrating analytics framework or compute engine can leverage the exact same underlying upload
 * strategies and performance optimizations.
 */
public class GcsWriteOptions {

  /**
   * Upload strategies matching the configurations offered by the google-cloud-storage Java client.
   */
  public enum UploadType {
    CHUNK_UPLOAD,
    WRITE_TO_DISK_THEN_UPLOAD,
    JOURNALING,
    PARALLEL_COMPOSITE_UPLOAD
  }

  /** Part file cleanup strategy for parallel composite upload. */
  public enum PartFileCleanupType {
    ALWAYS,
    NEVER,
    ON_SUCCESS
  }

  private final boolean checksumValidationEnabled;
  private final boolean disableGzipContent;
  private final boolean overwriteExisting;
  private final int uploadChunkSize;
  private final UploadType uploadType;
  // Metadata/Auth Configurations
  private final String kmsKeyName;
  private final String userProject;
  private final String encryptionKey;

  // PCU Configurations (Active only if uploadType == PARALLEL_COMPOSITE_UPLOAD)
  private final int pcuBufferCount;
  private final int pcuBufferCapacity;
  private final PartFileCleanupType pcuPartFileCleanupType;
  private final String pcuPartFileNamePrefix;

  // Disk Buffering Configurations (Active only if uploadType == WRITE_TO_DISK_THEN_UPLOAD or
  // JOURNALING)
  private final ImmutableSet<String> temporaryPaths;

  private GcsWriteOptions(Builder builder) {
    this.checksumValidationEnabled = builder.checksumValidationEnabled;
    this.disableGzipContent = builder.disableGzipContent;
    this.overwriteExisting = builder.overwriteExisting;
    this.uploadChunkSize = builder.uploadChunkSize;
    this.uploadType = builder.uploadType;
    this.kmsKeyName = builder.kmsKeyName;
    this.userProject = builder.userProject;
    this.encryptionKey = builder.encryptionKey;
    this.pcuBufferCount = builder.pcuBufferCount;
    this.pcuBufferCapacity = builder.pcuBufferCapacity;
    this.pcuPartFileCleanupType = builder.pcuPartFileCleanupType;
    this.pcuPartFileNamePrefix = builder.pcuPartFileNamePrefix;
    this.temporaryPaths = builder.temporaryPaths;
  }

  public boolean isChecksumValidationEnabled() {
    return checksumValidationEnabled;
  }

  public boolean isDisableGzipContent() {
    return disableGzipContent;
  }

  public boolean isOverwriteExisting() {
    return overwriteExisting;
  }

  public int getUploadChunkSize() {
    return uploadChunkSize;
  }

  public UploadType getUploadType() {
    return uploadType;
  }

  public int getPcuBufferCount() {
    return pcuBufferCount;
  }

  public int getPcuBufferCapacity() {
    return pcuBufferCapacity;
  }

  public PartFileCleanupType getPcuPartFileCleanupType() {
    return pcuPartFileCleanupType;
  }

  public String getPcuPartFileNamePrefix() {
    return pcuPartFileNamePrefix;
  }

  public ImmutableSet<String> getTemporaryPaths() {
    return temporaryPaths;
  }

  public String getKmsKeyName() {
    return kmsKeyName;
  }

  public String getUserProject() {
    return userProject;
  }

  public String getEncryptionKey() {
    return encryptionKey;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private boolean checksumValidationEnabled = false;
    private boolean disableGzipContent = true;
    private boolean overwriteExisting = true;
    private int uploadChunkSize = 24 * 1024 * 1024; // 24MB default
    private UploadType uploadType = UploadType.CHUNK_UPLOAD;
    private int pcuBufferCount = 1;
    private int pcuBufferCapacity = 32 * 1024 * 1024; // 32MB default
    private PartFileCleanupType pcuPartFileCleanupType = PartFileCleanupType.ALWAYS;
    private String pcuPartFileNamePrefix = "";
    private ImmutableSet<String> temporaryPaths = ImmutableSet.of();
    private String kmsKeyName = null;
    private String userProject = null;
    private String encryptionKey = null;

    public Builder setChecksumValidationEnabled(boolean enabled) {
      this.checksumValidationEnabled = enabled;
      return this;
    }

    public Builder setDisableGzipContent(boolean disable) {
      this.disableGzipContent = disable;
      return this;
    }

    public Builder setOverwriteExisting(boolean overwrite) {
      this.overwriteExisting = overwrite;
      return this;
    }

    public Builder setUploadChunkSize(int size) {
      this.uploadChunkSize = size;
      return this;
    }

    public Builder setUploadType(UploadType type) {
      this.uploadType = type;
      return this;
    }

    public Builder setPcuBufferCount(int count) {
      this.pcuBufferCount = count;
      return this;
    }

    public Builder setPcuBufferCapacity(int capacity) {
      this.pcuBufferCapacity = capacity;
      return this;
    }

    public Builder setPcuPartFileCleanupType(PartFileCleanupType cleanupType) {
      this.pcuPartFileCleanupType = cleanupType;
      return this;
    }

    public Builder setPcuPartFileNamePrefix(String prefix) {
      this.pcuPartFileNamePrefix = prefix;
      return this;
    }

    public Builder setTemporaryPaths(Collection<String> paths) {
      this.temporaryPaths = ImmutableSet.copyOf(paths);
      return this;
    }

    public Builder setKmsKeyName(String key) {
      this.kmsKeyName = key;
      return this;
    }

    public Builder setUserProject(String project) {
      this.userProject = project;
      return this;
    }

    public Builder setEncryptionKey(String key) {
      this.encryptionKey = key;
      return this;
    }

    public GcsWriteOptions build() {
      return new GcsWriteOptions(this);
    }
  }
}
