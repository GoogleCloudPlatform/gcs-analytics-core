package com.google.cloud.gcs.analyticscore.core;

/**
 * Configuration options for writing objects to Google Cloud Storage.
 * This class abstracts framework-specific configurations (like Hadoop's AsyncWriteChannelOptions 
 * or Iceberg's GCPProperties) into a generic set of properties that gcs-analytics-core can use.
 */
public class GcsWriteOptions {

  private final boolean checksumValidationEnabled;
  private final boolean disableGzipContent;
  private final int uploadChunkSize;

  private GcsWriteOptions(Builder builder) {
    this.checksumValidationEnabled = builder.checksumValidationEnabled;
    this.disableGzipContent = builder.disableGzipContent;
    this.uploadChunkSize = builder.uploadChunkSize;
  }

  /**
   * Returns whether end-to-end CRC32C checksum validation is enabled.
   */
  public boolean isChecksumValidationEnabled() {
    return checksumValidationEnabled;
  }

  /**
   * Returns whether GZIP compression is disabled at the transport layer.
   * True by default, as analytics formats (Parquet/ORC) are already compressed.
   */
  public boolean isDisableGzipContent() {
    return disableGzipContent;
  }

  /**
   * Returns the chunk size (in bytes) to be used for the upload session.
   */
  public int getUploadChunkSize() {
    return uploadChunkSize;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private boolean checksumValidationEnabled = false;
    private boolean disableGzipContent = true;
    
    // Default chunk size (e.g., 16MB) - can be tuned based on framework defaults
    private int uploadChunkSize = 16 * 1024 * 1024; 

    public Builder setChecksumValidationEnabled(boolean checksumValidationEnabled) {
      this.checksumValidationEnabled = checksumValidationEnabled;
      return this;
    }

    public Builder setDisableGzipContent(boolean disableGzipContent) {
      this.disableGzipContent = disableGzipContent;
      return this;
    }

    public Builder setUploadChunkSize(int uploadChunkSize) {
      this.uploadChunkSize = uploadChunkSize;
      return this;
    }

    public GcsWriteOptions build() {
      return new GcsWriteOptions(this);
    }
  }
}
