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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GcsWriteChannelTest {

  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_OBJECT = "test-object";
  private static final String TEST_WRITE_OBJECT = "test-write-object";
  private static final String TEST_WRITE_CHUNKS_OBJECT = "test-write-chunks-object";

  private BlobInfo blobInfo;
  private GcsWriteOptions writeOptions;
  private FakeWritableByteChannel fakeChannel;

  @BeforeEach
  void setUp() throws Exception {

    blobInfo = BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT)).build();
    writeOptions = GcsWriteOptions.builder().setChecksumValidationEnabled(true).build();
    fakeChannel = new FakeWritableByteChannel();
  }

  @Test
  void testWriteDelegationAndByteTracking() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);
    byte[] data = new byte[] {1, 2, 3, 4, 5};
    ByteBuffer buffer = ByteBuffer.wrap(data);

    int bytesWritten = channel.write(buffer);

    assertThat(bytesWritten).isEqualTo(5);
    assertThat(channel.getBytesWritten()).isEqualTo(5L);
    assertThat(fakeChannel.toByteArray()).isEqualTo(data);
  }

  @Test
  void testWriteWhenClosedThrowsException() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);
    fakeChannel.close();

    assertThrows(
        ClosedChannelException.class, () -> channel.write(ByteBuffer.wrap(new byte[] {1, 2, 3})));
  }

  @Test
  void testWriteWithFakeStorage() throws Exception {
    Storage fakeStorage = LocalStorageHelper.getOptions().getService();
    BlobId blobId = BlobId.of(TEST_BUCKET, TEST_WRITE_OBJECT);
    BlobInfo bInfo = BlobInfo.newBuilder(blobId).build();

    WritableByteChannel internalChannel = fakeStorage.blobWriteSession(bInfo).open();
    GcsWriteChannel channel = new GcsWriteChannel(internalChannel, bInfo, writeOptions);
    byte[] data = new byte[] {1, 2, 3, 4, 5};
    ByteBuffer buffer = ByteBuffer.wrap(data);

    int bytesWritten = channel.write(buffer);
    channel.close();

    assertThat(bytesWritten).isEqualTo(5);
    byte[] readData = fakeStorage.readAllBytes(blobId);
    assertThat(readData).isEqualTo(data);
  }

  @Test
  void testWriteChunksWithFakeStorage() throws Exception {
    Storage fakeStorage = LocalStorageHelper.getOptions().getService();
    BlobId blobId = BlobId.of(TEST_BUCKET, TEST_WRITE_CHUNKS_OBJECT);
    BlobInfo bInfo = BlobInfo.newBuilder(blobId).build();

    WritableByteChannel internalChannel = fakeStorage.blobWriteSession(bInfo).open();
    GcsWriteChannel channel = new GcsWriteChannel(internalChannel, bInfo, writeOptions);
    byte[] data1 = new byte[] {1, 2, 3};
    byte[] data2 = new byte[] {4, 5};

    channel.write(ByteBuffer.wrap(data1));
    channel.write(ByteBuffer.wrap(data2));
    channel.close();

    byte[] readData = fakeStorage.readAllBytes(blobId);
    byte[] expectedData = new byte[] {1, 2, 3, 4, 5};
    assertThat(readData).isEqualTo(expectedData);
  }

  @Test
  void testExceptionTranslation_AccessDeniedOnWrite() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e403 = new StorageException(403, "Forbidden");
    fakeChannel.setExceptionToThrowOnWrite(e403);

    assertThrows(AccessDeniedException.class, () -> channel.write(buffer));
  }

  @Test
  void testExceptionTranslation_NotFoundOnWrite() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e404 = new StorageException(404, "Not Found");
    fakeChannel.setExceptionToThrowOnWrite(e404);

    assertThrows(FileNotFoundException.class, () -> channel.write(buffer));
  }

  @Test
  void testExceptionTranslation_NotFoundOnClose() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);
    StorageException e404 = new StorageException(404, "Not Found");
    fakeChannel.setExceptionToThrowOnClose(e404);

    assertThrows(FileNotFoundException.class, channel::close);
  }

  @Test
  void testWrite_GenericStorageException() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e500 = new StorageException(500, "Internal Server Error");
    fakeChannel.setExceptionToThrowOnWrite(e500);

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown.getCause().getMessage()).contains("Internal Server Error");
  }

  @Test
  void testClose_GenericStorageException() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);
    StorageException e500 = new StorageException(500, "Internal Server Error");
    fakeChannel.setExceptionToThrowOnClose(e500);

    IOException thrown = assertThrows(IOException.class, channel::close);

    assertThat(thrown.getCause()).hasMessageThat().contains("Internal Server Error");
  }

  @Test
  void testClose_IOException() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);
    fakeChannel.setExceptionToThrowOnClose(new IOException("Generic Close Error"));

    IOException thrown = assertThrows(IOException.class, channel::close);

    assertThat(thrown).hasMessageThat().contains("Generic Close Error");
  }

  @Test
  void testWrite_IOException() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    fakeChannel.setExceptionToThrowOnWrite(new IOException("Generic I/O Error"));

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown).hasMessageThat().contains("Generic I/O Error");
  }

  @Test
  void testClose_AlreadyClosed() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);

    channel.close();
    channel.close();

    assertThat(fakeChannel.getCloseCount()).isEqualTo(1);
  }

  @Test
  void testExceptionTranslation_AccessDeniedOnWrite_Wrapped() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e403 = new StorageException(403, "Forbidden");
    IOException wrappedException = new IOException("Wrapper exception", e403);
    fakeChannel.setExceptionToThrowOnWrite(wrappedException);

    assertThrows(AccessDeniedException.class, () -> channel.write(buffer));
  }

  @Test
  void testExceptionTranslation_NotFoundOnWrite_Wrapped() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e404 = new StorageException(404, "Not Found");
    IOException wrappedException = new IOException("Wrapper exception", e404);
    fakeChannel.setExceptionToThrowOnWrite(wrappedException);

    assertThrows(FileNotFoundException.class, () -> channel.write(buffer));
  }

  @Test
  void testExceptionTranslation_NotFoundOnClose_Wrapped() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);
    StorageException e404 = new StorageException(404, "Not Found");
    IOException wrappedException = new IOException("Wrapper exception", e404);
    fakeChannel.setExceptionToThrowOnClose(wrappedException);

    assertThrows(FileNotFoundException.class, channel::close);
  }

  @Test
  void testWrite_GenericIOException_RethrownDirectly() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    IOException genericException = new IOException("Connection reset by peer");
    fakeChannel.setExceptionToThrowOnWrite(genericException);

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown).isSameInstanceAs(genericException);
  }

  @Test
  void testWrite_RuntimeException_PropagatedUnchanged() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    NullPointerException npe = new NullPointerException("Simulated NPE");
    fakeChannel.setExceptionToThrowOnWrite(npe);

    NullPointerException thrown =
        assertThrows(NullPointerException.class, () -> channel.write(buffer));

    assertThat(thrown).isSameInstanceAs(npe);
  }

  @Test
  void testClose_RuntimeException_PropagatedUnchanged() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);
    NullPointerException npe = new NullPointerException("Simulated NPE during close");
    fakeChannel.setExceptionToThrowOnClose(npe);

    NullPointerException thrown = assertThrows(NullPointerException.class, channel::close);

    assertThat(thrown).isSameInstanceAs(npe);
  }

  @Test
  void testExceptionTranslation_AlreadyExistsOnWrite() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e409 = new StorageException(409, "Conflict");
    fakeChannel.setExceptionToThrowOnWrite(e409);

    assertThrows(FileAlreadyExistsException.class, () -> channel.write(buffer));
  }

  @Test
  void testExceptionTranslation_PreconditionFailed_OverwriteDisabled() throws Exception {
    GcsWriteOptions overwriteDisabledOptions =
        GcsWriteOptions.builder().setOverwriteExisting(false).build();
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, overwriteDisabledOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e412 = new StorageException(412, "Precondition Failed");
    fakeChannel.setExceptionToThrowOnWrite(e412);

    assertThrows(FileAlreadyExistsException.class, () -> channel.write(buffer));
  }

  @Test
  void testExceptionTranslation_PreconditionFailed_GenerationMismatch() throws Exception {
    BlobInfo blobInfoWithGen =
        BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT, 123L)).build();
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfoWithGen, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e412 = new StorageException(412, "Precondition Failed");
    fakeChannel.setExceptionToThrowOnWrite(e412);

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown).hasMessageThat().contains("Generation mismatch");
  }

  @Test
  void testIsOpen_AfterClose_ReturnsFalse() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);

    assertThat(channel.isOpen()).isTrue();

    channel.close();

    assertThat(channel.isOpen()).isFalse();
  }

  @Test
  void testExceptionTranslation_PreconditionFailed_Fallback() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e412 = new StorageException(412, "Precondition Failed");
    fakeChannel.setExceptionToThrowOnWrite(e412);

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown).hasMessageThat().contains("Error during write to GCS");
  }

  @Test
  void testExceptionTranslation_PreconditionFailed_NullOptions() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, null);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e412 = new StorageException(412, "Precondition Failed");
    fakeChannel.setExceptionToThrowOnWrite(e412);

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown).hasMessageThat().contains("Error during write to GCS");
  }

  @Test
  void testIsOpen_NullSdkChannel_ReturnsFalse() {
    GcsWriteChannel channel = new GcsWriteChannel(null, blobInfo, writeOptions);
    assertThat(channel.isOpen()).isFalse();
  }

  @Test
  void testClose_NullSdkChannel_ReturnsSuccessfully() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(null, blobInfo, writeOptions);
    channel.close();
    assertThat(channel.isOpen()).isFalse();
  }

  @Test
  void testWrite_EmptyBuffer_ReturnsZeroAndDoesNotIncrementBytes() throws Exception {
    GcsWriteChannel channel = new GcsWriteChannel(fakeChannel, blobInfo, writeOptions);
    ByteBuffer emptyBuffer = ByteBuffer.allocate(0);

    int bytesWritten = channel.write(emptyBuffer);

    assertThat(bytesWritten).isEqualTo(0);
    assertThat(channel.getBytesWritten()).isEqualTo(0L);
  }
}
