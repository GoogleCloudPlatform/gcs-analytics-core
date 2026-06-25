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
import static org.mockito.Mockito.*;

import com.google.api.core.ApiFuture;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GcsWriteChannelTest {

  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_OBJECT = "test-object";
  private static final String TEST_WRITE_OBJECT = "test-write-object";
  private static final String TEST_WRITE_CHUNKS_OBJECT = "test-write-chunks-object";

  private BlobInfo blobInfo;
  private GcsWriteOptions writeOptions;
  private BlobWriteSession mockSession;
  private ApiFuture<BlobInfo> mockFuture;
  private WritableByteChannel mockChannel;
  private Storage fakeStorage;

  @BeforeEach
  void setUp() throws Exception {
    blobInfo = BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT)).build();
    writeOptions = GcsWriteOptions.builder().setChecksumValidationEnabled(true).build();
    mockSession = mock(BlobWriteSession.class);
    mockFuture = mock(ApiFuture.class);
    when(mockSession.getResult()).thenReturn(mockFuture);
    mockChannel = mock(WritableByteChannel.class);
    when(mockChannel.isOpen()).thenReturn(true);
    fakeStorage = LocalStorageHelper.getOptions().getService();
  }

  private GcsWriteChannel createChannel(
      WritableByteChannel sdkChannel, BlobInfo info, GcsWriteOptions options) {
    return new GcsWriteChannel(mockSession, sdkChannel, info, options);
  }

  private GcsWriteChannel createFakeStorageChannel(String objectName) throws IOException {
    BlobInfo bInfo = BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, objectName)).build();
    BlobWriteSession session = fakeStorage.blobWriteSession(bInfo);
    return new GcsWriteChannel(session, session.open(), bInfo, writeOptions);
  }

  @Test
  void write_delegatesToSdkChannelAndTracksBytesWritten() throws Exception {
    GcsWriteChannel channel = createFakeStorageChannel(TEST_WRITE_OBJECT);
    byte[] data = new byte[] {1, 2, 3, 4, 5};
    ByteBuffer buffer = ByteBuffer.wrap(data);

    int bytesWritten = channel.write(buffer);

    assertThat(bytesWritten).isEqualTo(5);
    assertThat(channel.getBytesWritten()).isEqualTo(5L);

    channel.close();
    byte[] readData = fakeStorage.readAllBytes(BlobId.of(TEST_BUCKET, TEST_WRITE_OBJECT));
    assertThat(readData).isEqualTo(data);
  }

  @Test
  void write_whenChannelClosed_throwsClosedChannelException() throws Exception {
    BlobInfo bInfo = BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_WRITE_OBJECT)).build();
    BlobWriteSession session = fakeStorage.blobWriteSession(bInfo);
    WritableByteChannel internalChannel = session.open();
    GcsWriteChannel channel = new GcsWriteChannel(session, internalChannel, bInfo, writeOptions);

    internalChannel.close();

    assertThrows(
        ClosedChannelException.class, () -> channel.write(ByteBuffer.wrap(new byte[] {1, 2, 3})));
  }

  @Test
  void write_usingFakeStorage_writesDataSuccessfully() throws Exception {
    GcsWriteChannel channel = createFakeStorageChannel(TEST_WRITE_OBJECT);
    byte[] data = new byte[] {1, 2, 3, 4, 5};
    ByteBuffer buffer = ByteBuffer.wrap(data);

    int bytesWritten = channel.write(buffer);
    channel.close();

    assertThat(bytesWritten).isEqualTo(5);
    byte[] readData = fakeStorage.readAllBytes(BlobId.of(TEST_BUCKET, TEST_WRITE_OBJECT));
    assertThat(readData).isEqualTo(data);
  }

  @Test
  void write_multipleChunksUsingFakeStorage_writesAllChunksSuccessfully() throws Exception {
    GcsWriteChannel channel = createFakeStorageChannel(TEST_WRITE_CHUNKS_OBJECT);
    byte[] data1 = new byte[] {1, 2, 3};
    byte[] data2 = new byte[] {4, 5};

    channel.write(ByteBuffer.wrap(data1));
    channel.write(ByteBuffer.wrap(data2));
    channel.close();

    byte[] readData = fakeStorage.readAllBytes(BlobId.of(TEST_BUCKET, TEST_WRITE_CHUNKS_OBJECT));
    byte[] expectedData = new byte[] {1, 2, 3, 4, 5};
    assertThat(readData).isEqualTo(expectedData);
  }

  @Test
  void write_onAccessDeniedStorageException_translatesToAccessDeniedException() throws Exception {
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e403 = new StorageException(403, "Forbidden");
    when(mockChannel.write(any(ByteBuffer.class))).thenThrow(e403);

    assertThrows(AccessDeniedException.class, () -> channel.write(buffer));
  }

  @Test
  void write_onNotFoundStorageException_translatesToFileNotFoundException() throws Exception {
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e404 = new StorageException(404, "Not Found");
    when(mockChannel.write(any(ByteBuffer.class))).thenThrow(e404);

    assertThrows(FileNotFoundException.class, () -> channel.write(buffer));
  }

  @Test
  void close_onNotFoundStorageException_translatesToFileNotFoundException() throws Exception {
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, writeOptions);
    StorageException e404 = new StorageException(404, "Not Found");
    doThrow(e404).when(mockChannel).close();

    assertThrows(FileNotFoundException.class, channel::close);
  }

  @Test
  void write_onGenericStorageException_translatesToIOExceptionWithCause() throws Exception {
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e500 = new StorageException(500, "Internal Server Error");
    when(mockChannel.write(any(ByteBuffer.class))).thenThrow(e500);

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown).hasCauseThat().hasMessageThat().contains("Internal Server Error");
  }

  @Test
  void close_onGenericStorageException_translatesToIOExceptionWithCause() throws Exception {
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, writeOptions);
    StorageException e500 = new StorageException(500, "Internal Server Error");
    doThrow(e500).when(mockChannel).close();

    IOException thrown = assertThrows(IOException.class, channel::close);

    assertThat(thrown).hasCauseThat().hasMessageThat().contains("Internal Server Error");
  }

  @Test
  void close_onIOException_propagatesIOException() throws Exception {
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, writeOptions);
    IOException genericException = new IOException("Generic Close Error");
    doThrow(genericException).when(mockChannel).close();

    IOException thrown = assertThrows(IOException.class, channel::close);

    assertThat(thrown).isSameInstanceAs(genericException);
  }

  @Test
  void write_onIOException_propagatesIOException() throws Exception {
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    IOException genericException = new IOException("Generic I/O Error");
    when(mockChannel.write(any(ByteBuffer.class))).thenThrow(genericException);

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown).isSameInstanceAs(genericException);
  }

  @Test
  void close_whenAlreadyClosed_doesNotCloseAgain() throws Exception {
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, writeOptions);

    channel.close();
    channel.close();

    verify(mockChannel, times(1)).close();
  }

  @Test
  void write_onWrappedAccessDeniedStorageException_translatesToAccessDeniedException()
      throws Exception {
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e403 = new StorageException(403, "Forbidden");
    IOException wrappedException = new IOException("Wrapper exception", e403);
    when(mockChannel.write(any(ByteBuffer.class))).thenThrow(wrappedException);

    assertThrows(AccessDeniedException.class, () -> channel.write(buffer));
  }

  @Test
  void write_onWrappedNotFoundStorageException_translatesToFileNotFoundException()
      throws Exception {
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e404 = new StorageException(404, "Not Found");
    IOException wrappedException = new IOException("Wrapper exception", e404);
    when(mockChannel.write(any(ByteBuffer.class))).thenThrow(wrappedException);

    assertThrows(FileNotFoundException.class, () -> channel.write(buffer));
  }

  @Test
  void close_onWrappedNotFoundStorageException_translatesToFileNotFoundException()
      throws Exception {
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, writeOptions);
    StorageException e404 = new StorageException(404, "Not Found");
    IOException wrappedException = new IOException("Wrapper exception", e404);
    doThrow(wrappedException).when(mockChannel).close();

    assertThrows(FileNotFoundException.class, channel::close);
  }

  @Test
  void write_onGenericIOException_propagatesIOExceptionDirectly() throws Exception {
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    IOException genericException = new IOException("Connection reset by peer");
    when(mockChannel.write(any(ByteBuffer.class))).thenThrow(genericException);

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown).isSameInstanceAs(genericException);
  }

  @Test
  void write_onRuntimeException_propagatesRuntimeException() throws Exception {
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    NullPointerException npe = new NullPointerException("Simulated NPE");
    when(mockChannel.write(any(ByteBuffer.class))).thenThrow(npe);

    NullPointerException thrown =
        assertThrows(NullPointerException.class, () -> channel.write(buffer));

    assertThat(thrown).isSameInstanceAs(npe);
  }

  @Test
  void close_onRuntimeException_propagatesRuntimeException() throws Exception {
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, writeOptions);
    NullPointerException npe = new NullPointerException("Simulated NPE during close");
    doThrow(npe).when(mockChannel).close();

    NullPointerException thrown = assertThrows(NullPointerException.class, channel::close);

    assertThat(thrown).isSameInstanceAs(npe);
  }

  @Test
  void write_onAlreadyExistsStorageException_translatesToFileAlreadyExistsException()
      throws Exception {
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e409 = new StorageException(409, "Conflict");
    when(mockChannel.write(any(ByteBuffer.class))).thenThrow(e409);

    assertThrows(FileAlreadyExistsException.class, () -> channel.write(buffer));
  }

  @Test
  void write_onPreconditionFailedWithOverwriteDisabled_translatesToFileAlreadyExistsException()
      throws Exception {
    GcsWriteOptions overwriteDisabledOptions =
        GcsWriteOptions.builder().setOverwriteExisting(false).build();
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, overwriteDisabledOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockChannel.write(any(ByteBuffer.class))).thenThrow(e412);

    assertThrows(FileAlreadyExistsException.class, () -> channel.write(buffer));
  }

  @Test
  void write_onPreconditionFailedWithGenerationMismatch_translatesToIOException() throws Exception {
    BlobInfo blobInfoWithGen =
        BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT, 123L)).build();
    GcsWriteChannel channel = createChannel(mockChannel, blobInfoWithGen, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockChannel.write(any(ByteBuffer.class))).thenThrow(e412);

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown).hasMessageThat().contains("Generation mismatch");
  }

  @Test
  void isOpen_afterClose_returnsFalse() throws Exception {
    GcsWriteChannel channel = createFakeStorageChannel(TEST_WRITE_OBJECT);

    channel.close();

    assertThat(channel.isOpen()).isFalse();
  }

  @Test
  void
      write_onPreconditionFailedWithNoGenerationAndOverwriteEnabled_translatesToIOExceptionWithGenericFallbackMessage()
          throws Exception {
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, writeOptions);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockChannel.write(any(ByteBuffer.class))).thenThrow(e412);

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown).hasMessageThat().contains("Error during write to GCS");
  }

  @Test
  void write_onPreconditionFailedWithNullOptions_translatesToIOExceptionWithGenericFallbackMessage()
      throws Exception {
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, null);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockChannel.write(any(ByteBuffer.class))).thenThrow(e412);

    IOException thrown = assertThrows(IOException.class, () -> channel.write(buffer));

    assertThat(thrown).hasMessageThat().contains("Error during write to GCS");
  }

  @Test
  void isOpen_whenSdkChannelIsNull_returnsFalse() {
    GcsWriteChannel channel = createChannel(null, blobInfo, writeOptions);

    assertThat(channel.isOpen()).isFalse();
  }

  @Test
  void close_whenSdkChannelIsNull_completesSuccessfully() throws Exception {
    GcsWriteChannel channel = createChannel(null, blobInfo, writeOptions);

    channel.close();

    assertThat(channel.isOpen()).isFalse();
  }

  @Test
  void write_emptyBuffer_returnsZeroAndDoesNotIncrementBytesWritten() throws Exception {
    GcsWriteChannel channel = createChannel(mockChannel, blobInfo, writeOptions);
    ByteBuffer emptyBuffer = ByteBuffer.allocate(0);

    int bytesWritten = channel.write(emptyBuffer);

    assertThat(bytesWritten).isEqualTo(0);
    assertThat(channel.getBytesWritten()).isEqualTo(0L);
  }

  @Test
  void close_blocksOnBlobWriteSessionResult() throws Exception {
    BlobWriteSession mockSession = mock(BlobWriteSession.class);
    ApiFuture<BlobInfo> mockFuture = mock(ApiFuture.class);
    when(mockSession.getResult()).thenReturn(mockFuture);
    GcsWriteChannel channel = new GcsWriteChannel(mockSession, mockChannel, blobInfo, writeOptions);

    channel.close();

    verify(mockSession).getResult();
    verify(mockFuture).get();
  }

  @Test
  void close_whenFutureThrowsExecutionExceptionWithStorageException_translatesException()
      throws Exception {
    BlobWriteSession mockSession = mock(BlobWriteSession.class);
    ApiFuture<BlobInfo> mockFuture = mock(ApiFuture.class);
    when(mockSession.getResult()).thenReturn(mockFuture);
    StorageException se = new StorageException(404, "Not Found");
    when(mockFuture.get()).thenThrow(new ExecutionException(se));
    GcsWriteChannel channel = new GcsWriteChannel(mockSession, mockChannel, blobInfo, writeOptions);

    FileNotFoundException exception =
        assertThrows(FileNotFoundException.class, () -> channel.close());

    assertThat(exception).hasCauseThat().isSameInstanceAs(se);
  }

  @Test
  void close_whenFutureThrowsExecutionExceptionWithIOException_propagatesIOException()
      throws Exception {
    BlobWriteSession mockSession = mock(BlobWriteSession.class);
    ApiFuture<BlobInfo> mockFuture = mock(ApiFuture.class);
    when(mockSession.getResult()).thenReturn(mockFuture);
    IOException ioe = new IOException("custom connection error");
    when(mockFuture.get()).thenThrow(new ExecutionException(ioe));
    GcsWriteChannel channel = new GcsWriteChannel(mockSession, mockChannel, blobInfo, writeOptions);

    IOException exception = assertThrows(IOException.class, () -> channel.close());

    assertThat(exception).isSameInstanceAs(ioe);
  }

  @Test
  void
      close_whenFutureThrowsExecutionExceptionWithGenericException_translatesToIOExceptionWithCustomMessage()
          throws Exception {
    BlobWriteSession mockSession = mock(BlobWriteSession.class);
    ApiFuture<BlobInfo> mockFuture = mock(ApiFuture.class);
    when(mockSession.getResult()).thenReturn(mockFuture);
    RuntimeException re = new RuntimeException("generic failure");
    when(mockFuture.get()).thenThrow(new ExecutionException(re));
    GcsWriteChannel channel = new GcsWriteChannel(mockSession, mockChannel, blobInfo, writeOptions);

    IOException exception = assertThrows(IOException.class, () -> channel.close());

    assertThat(exception).hasCauseThat().isSameInstanceAs(re);
    assertThat(exception).hasMessageThat().contains("GCS failed to finalize the upload session");
  }

  @Test
  void close_whenInterrupted_restoresInterruptStatusAndThrowsInterruptedIOException()
      throws Exception {
    BlobWriteSession mockSession = mock(BlobWriteSession.class);
    ApiFuture<BlobInfo> mockFuture = mock(ApiFuture.class);
    when(mockSession.getResult()).thenReturn(mockFuture);
    InterruptedException ie = new InterruptedException("finalization interrupted");
    when(mockFuture.get()).thenThrow(ie);
    GcsWriteChannel channel = new GcsWriteChannel(mockSession, mockChannel, blobInfo, writeOptions);
    // Clear interrupt status first just in case
    Thread.interrupted();

    InterruptedIOException exception =
        assertThrows(InterruptedIOException.class, () -> channel.close());

    assertThat(exception)
        .hasMessageThat()
        .contains("Thread interrupted waiting for upload finalization");
    assertThat(Thread.currentThread().isInterrupted()).isTrue();

    // Clean up interrupt status
    Thread.interrupted();
  }
}
