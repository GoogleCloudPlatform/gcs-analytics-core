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

package com.google.cloud.gcs.analyticscore.core;

import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Attribute;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Operation;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableMap;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A unified WritableByteChannel for writing objects to Google Cloud Storage.
 */
public class GoogleCloudStorageWriteChannel implements WritableByteChannel {

  private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudStorageWriteChannel.class);

  private final GcsFileSystem gcsFileSystem;
  private final BlobInfo blobInfo;
  private final ImmutableMap<String, String> commonAttributes;
  
  private WritableByteChannel internalWriteChannel;
  private long bytesWritten = 0;
  private volatile boolean closed = false;

  public GoogleCloudStorageWriteChannel(
      GcsFileSystem gcsFileSystem,
      Storage storage, 
      BlobInfo blobInfo, 
      GcsWriteOptions writeOptions) throws IOException {
    
    this.gcsFileSystem = gcsFileSystem;
    this.blobInfo = blobInfo;
    this.commonAttributes = ImmutableMap.of(
        Attribute.CLASS_NAME.name(), GoogleCloudStorageWriteChannel.class.getName());
    
    LOG.debug("Initializing GoogleCloudStorageWriteChannel for object: gs://{}/{}", 
        blobInfo.getBucket(), blobInfo.getName());

    try {
      BlobWriteOption[] options = generateWriteOptions(writeOptions, blobInfo);
      BlobWriteSession writeSession = storage.blobWriteSession(blobInfo, options);
      this.internalWriteChannel = writeSession.open();
    } catch (StorageException e) {
      LOG.error("Failed to initialize BlobWriteSession for object: gs://{}/{}", 
          blobInfo.getBucket(), blobInfo.getName(), e);
          
      GcsExceptionUtil.ErrorType errorType = GcsExceptionUtil.getErrorType(e);
      if (errorType == GcsExceptionUtil.ErrorType.ALREADY_EXISTS) {
        throw (FileAlreadyExistsException) new FileAlreadyExistsException(
            String.format("Object gs://%s/%s already exists.", blobInfo.getBucket(), blobInfo.getName()))
            .initCause(e);
      } else if (errorType == GcsExceptionUtil.ErrorType.ACCESS_DENIED) {
        throw (AccessDeniedException) new AccessDeniedException(
            String.format("gs://%s/%s", blobInfo.getBucket(), blobInfo.getName()), null, 
            String.format("Access denied to object during initialization: %s", e.getMessage()))
            .initCause(e);
      }
      
      throw new IOException("Failed to initialize BlobWriteSession for " + blobInfo.getBlobId(), e);
    }
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    if (!isOpen()) {
      LOG.warn("Attempted to write to a closed channel for object: {}", blobInfo.getBlobId());
      throw new ClosedChannelException();
    }

    return gcsFileSystem.getTelemetry().measure(
        Operation.WRITE.name(),
        Metric.WRITE_DURATION,
        commonAttributes,
        recorder -> {
            int bytesToDraft = src.remaining();
            try {
              int written = internalWriteChannel.write(src);
              if (written > 0) {
                bytesWritten += written;
                recorder.record(Metric.WRITE_BYTES, written, Collections.emptyMap());
              }
              
              LOG.trace("{} bytes were written out of provided buffer of capacity {}. Total: {}", 
                  written, src.capacity(), bytesWritten);
              return written;
            } catch (StorageException e) {
              LOG.error("StorageException while writing to object: {} at position: {}", 
                  blobInfo.getBlobId(), bytesWritten, e);
                  
              GcsExceptionUtil.ErrorType errorType = GcsExceptionUtil.getErrorType(e);
              if (errorType == GcsExceptionUtil.ErrorType.NOT_FOUND) {
                throw (FileNotFoundException) new FileNotFoundException(
                    String.format("Location does not exist or generation not found: gs://%s/%s", blobInfo.getBucket(), blobInfo.getName()))
                    .initCause(e);
              } else if (errorType == GcsExceptionUtil.ErrorType.ACCESS_DENIED) {
                throw (AccessDeniedException) new AccessDeniedException(
                    String.format("gs://%s/%s", blobInfo.getBucket(), blobInfo.getName()), null, 
                    String.format("Access denied to object during write: %s", e.getMessage()))
                    .initCause(e);
              }
              
              throw new IOException(
                  String.format("Error writing to GCS for %s at position %d", blobInfo.getBlobId(), bytesWritten), e);
            } catch (Exception e) {
              LOG.error("Unexpected error while writing to object: {} at position: {}", 
                  blobInfo.getBlobId(), bytesWritten, e);
              throw new IOException(
                  String.format("Error writing to GCS for %s at position %d", blobInfo.getBlobId(), bytesWritten), e);
            }
        });
  }

  @Override
  public boolean isOpen() {
    return !closed && internalWriteChannel != null && internalWriteChannel.isOpen();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    
    gcsFileSystem.getTelemetry().measure(
        Operation.CLOSE.name(),
        Metric.CLOSE_DURATION,
        commonAttributes,
        recorder -> {
            LOG.debug("Closing GoogleCloudStorageWriteChannel for object: {}. Final byte count: {}", 
                blobInfo.getBlobId(), bytesWritten);
            closed = true;
            try {
              if (internalWriteChannel != null) {
                  internalWriteChannel.close();
              }
              LOG.debug("Successfully closed and finalized object: {}", blobInfo.getBlobId());
            } catch (StorageException e) {
              LOG.error("Failed to close and finalize upload for object: {}", blobInfo.getBlobId(), e);
              
              GcsExceptionUtil.ErrorType errorType = GcsExceptionUtil.getErrorType(e);
              if (errorType == GcsExceptionUtil.ErrorType.NOT_FOUND) {
                throw (FileNotFoundException) new FileNotFoundException(
                    String.format("Location does not exist or generation not found: gs://%s/%s", blobInfo.getBucket(), blobInfo.getName()))
                    .initCause(e);
              }
              
              throw new IOException(String.format("Upload failed for '%s'. reason=%s", 
                  blobInfo.getBlobId(), e.getMessage()), e);
            } catch (Exception e) {
              LOG.error("Unexpected error closing upload for object: {}", blobInfo.getBlobId(), e);
              throw new IOException(String.format("Upload failed for '%s'. reason=%s", 
                  blobInfo.getBlobId(), e.getMessage()), e);
            } finally {
              internalWriteChannel = null;
            }
            return null;
        });
  }

  public long getBytesWritten() {
    return bytesWritten;
  }

  private BlobWriteOption[] generateWriteOptions(GcsWriteOptions writeOptions, BlobInfo blobInfo) {
    List<BlobWriteOption> options = new ArrayList<>();
    if (writeOptions == null) {
        return new BlobWriteOption[0];
    }
    if (writeOptions.isDisableGzipContent()) {
        options.add(BlobWriteOption.disableGzipContent());
    }
    
    // Determine overwrite semantics based on exact generation ID or 'doesNotExist' flag
    if (blobInfo.getBlobId().getGeneration() != null) {
        options.add(BlobWriteOption.generationMatch());
    } else if (!writeOptions.isOverwriteExisting()) {
        options.add(BlobWriteOption.doesNotExist());
    }

    if (writeOptions.isChecksumValidationEnabled()) {
        options.add(BlobWriteOption.crc32cMatch());
    }
    if (writeOptions.getKmsKeyName() != null) {
        options.add(BlobWriteOption.kmsKeyName(writeOptions.getKmsKeyName()));
    }
    if (writeOptions.getEncryptionKey() != null) {
        options.add(BlobWriteOption.encryptionKey(writeOptions.getEncryptionKey()));
    }
    if (writeOptions.getUserProject() != null) {
        options.add(BlobWriteOption.userProject(writeOptions.getUserProject()));
    }
    return options.toArray(new BlobWriteOption[0]);
  }
}
