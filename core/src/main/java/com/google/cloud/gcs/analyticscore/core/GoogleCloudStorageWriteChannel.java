package com.google.cloud.gcs.analyticscore.core;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A unified WritableByteChannel for writing objects to Google Cloud Storage.
 * This class delegates all transport logic to the Java SDK's BlobWriteSession.
 */
public class GoogleCloudStorageWriteChannel implements WritableByteChannel {

  private static final Logger logger = LoggerFactory.getLogger(GoogleCloudStorageWriteChannel.class);

  private final BlobInfo blobInfo;
  private WritableByteChannel internalWriteChannel;
  private long bytesWritten = 0;

  public GoogleCloudStorageWriteChannel(
      Storage storage, 
      BlobInfo blobInfo, 
      GcsWriteOptions writeOptions) throws IOException {
    
    this.blobInfo = blobInfo;
    
    logger.debug("Initializing GoogleCloudStorageWriteChannel for object: {}", blobInfo.getBlobId());

    try {
      BlobWriteOption[] options = generateWriteOptions(writeOptions);
      BlobWriteSession writeSession = storage.blobWriteSession(blobInfo, options);
      this.internalWriteChannel = writeSession.open();
      logger.debug("Successfully opened BlobWriteSession for object: {}", blobInfo.getBlobId());
    } catch (StorageException e) {
      logger.error("Failed to initialize BlobWriteSession for object: {}", blobInfo.getBlobId(), e);
      throw new IOException("Failed to initialize BlobWriteSession for " + blobInfo.getBlobId(), e);
    }
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    if (!isOpen()) {
      logger.warn("Attempted to write to a closed channel for object: {}", blobInfo.getBlobId());
      throw new ClosedChannelException();
    }

    int bytesToDraft = src.remaining();
    try {
      int written = internalWriteChannel.write(src);
      
      if (written > 0) {
        bytesWritten += written;
      }
      
      // Use trace or debug to avoid spamming the logs on every tiny chunk, 
      // but keep it available for deep debugging.
      logger.trace("Wrote {} bytes (requested {}) to object: {}. Total bytes written: {}", 
          written, bytesToDraft, blobInfo.getBlobId(), bytesWritten);
          
      return written;
      
    } catch (StorageException e) {
      logger.error("StorageException while writing to object: {} at position: {}", 
          blobInfo.getBlobId(), bytesWritten, e);
      throw new IOException("Error writing to GCS for " + blobInfo.getBlobId(), e);
    }
  }

  @Override
  public boolean isOpen() {
    return internalWriteChannel != null && internalWriteChannel.isOpen();
  }

  @Override
  public void close() throws IOException {
    if (!isOpen()) {
      logger.debug("Channel for object {} is already closed. Ignoring close().", blobInfo.getBlobId());
      return;
    }
    
    logger.debug("Closing GoogleCloudStorageWriteChannel for object: {}. Total bytes written: {}", 
        blobInfo.getBlobId(), bytesWritten);
        
    try {
      internalWriteChannel.close();
      logger.debug("Successfully finalized upload for object: {}", blobInfo.getBlobId());
    } catch (StorageException e) {
      logger.error("Failed to close and finalize upload for object: {}", blobInfo.getBlobId(), e);
      throw new IOException("Failed to close upload for " + blobInfo.getBlobId(), e);
    } finally {
      internalWriteChannel = null;
    }
  }

  public long getBytesWritten() {
    return bytesWritten;
  }

  private BlobWriteOption[] generateWriteOptions(GcsWriteOptions writeOptions) {
    List<BlobWriteOption> options = new ArrayList<>();
    
    options.add(BlobWriteOption.disableGzipContent());
    
    if (writeOptions != null && writeOptions.isChecksumValidationEnabled()) {
      logger.debug("Checksum validation is enabled for object: {}", blobInfo.getBlobId());
      options.add(BlobWriteOption.crc32cMatch());
    }
    
    return options.toArray(new BlobWriteOption[0]);
  }
}
