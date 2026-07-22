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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.AccessDeniedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

class GcsBidiWriteChannelTest {

  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_OBJECT = "test-object";

  @Mock private Storage storage;
  @Mock private BlobAppendableUpload mockSession;
  @Mock private BlobAppendableUpload.AppendableUploadWriteableByteChannel mockAppendChannel;

  private BlobInfo blobInfo;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    blobInfo = BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT)).build();
    when(storage.blobAppendableUpload(
            any(BlobInfo.class),
            any(BlobAppendableUploadConfig.class),
            any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockSession);
    when(mockSession.open()).thenReturn(mockAppendChannel);
    when(mockAppendChannel.isOpen()).thenReturn(true);
  }

  @Test
  void testConstructor_setsCloseActionBasedOnOptions_finalizeOnCloseTrue() throws Exception {
    GcsWriteOptions optionsTrue =
        GcsWriteOptions.builder().setBidiWriteEnabled(true).setFinalizeOnClose(true).build();
    ArgumentCaptor<BlobAppendableUploadConfig> configCaptor =
        ArgumentCaptor.forClass(BlobAppendableUploadConfig.class);

    GcsBidiWriteChannel channelTrue = new GcsBidiWriteChannel(storage, blobInfo, optionsTrue);

    verify(storage)
        .blobAppendableUpload(
            eq(blobInfo), configCaptor.capture(), any(Storage.BlobWriteOption[].class));
    assertThat(configCaptor.getValue().getCloseAction())
        .isEqualTo(BlobAppendableUploadConfig.CloseAction.FINALIZE_WHEN_CLOSING);
    assertThat(channelTrue.isOpen()).isTrue();
  }

  @Test
  void testConstructor_setsCloseActionBasedOnOptions_finalizeOnCloseFalse() throws Exception {
    GcsWriteOptions optionsFalse =
        GcsWriteOptions.builder().setBidiWriteEnabled(true).setFinalizeOnClose(false).build();
    ArgumentCaptor<BlobAppendableUploadConfig> configCaptorFalse =
        ArgumentCaptor.forClass(BlobAppendableUploadConfig.class);

    GcsBidiWriteChannel channelFalse = new GcsBidiWriteChannel(storage, blobInfo, optionsFalse);

    verify(storage)
        .blobAppendableUpload(
            eq(blobInfo), configCaptorFalse.capture(), any(Storage.BlobWriteOption[].class));
    assertThat(configCaptorFalse.getValue().getCloseAction())
        .isEqualTo(BlobAppendableUploadConfig.CloseAction.CLOSE_WITHOUT_FINALIZING);
    assertThat(channelFalse.isOpen()).isTrue();
  }

  @Test
  void testConstructor_initializationThrowsStorageException_translated() throws Exception {
    StorageException se = new StorageException(403, "Forbidden");
    when(storage.blobAppendableUpload(
            any(BlobInfo.class),
            any(BlobAppendableUploadConfig.class),
            any(Storage.BlobWriteOption[].class)))
        .thenThrow(se);

    GcsWriteOptions options = GcsWriteOptions.builder().build();

    assertThrows(
        AccessDeniedException.class, () -> new GcsBidiWriteChannel(storage, blobInfo, options));
  }

  @Test
  void testWrite_success_delegatesToAppendChannelAndTracksBytes() throws Exception {
    GcsWriteOptions options = GcsWriteOptions.builder().build();
    GcsBidiWriteChannel channel = new GcsBidiWriteChannel(storage, blobInfo, options);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5});
    when(mockAppendChannel.write(any(ByteBuffer.class)))
        .thenAnswer(
            i -> {
              ByteBuffer buf = i.getArgument(0);
              int rem = buf.remaining();
              buf.position(buf.position() + rem);
              return rem;
            });

    int written = channel.write(buffer);

    assertThat(written).isEqualTo(5);
    assertThat(channel.getBytesWritten()).isEqualTo(5L);
  }

  @Test
  void testWrite_whenClosed_throwsClosedChannelException() throws Exception {
    GcsWriteOptions options = GcsWriteOptions.builder().build();
    GcsBidiWriteChannel channel = new GcsBidiWriteChannel(storage, blobInfo, options);
    channel.close();

    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
    assertThrows(ClosedChannelException.class, () -> channel.write(buffer));
  }

  @Test
  void testWrite_failure_translatesException() throws Exception {
    GcsWriteOptions options = GcsWriteOptions.builder().build();
    GcsBidiWriteChannel channel = new GcsBidiWriteChannel(storage, blobInfo, options);

    StorageException se = new StorageException(403, "Forbidden");
    when(mockAppendChannel.write(any(ByteBuffer.class))).thenThrow(se);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
    assertThrows(AccessDeniedException.class, () -> channel.write(buffer));
  }

  @Test
  void testClose_success_closesAppendChannelAndUpdatesIsOpen() throws Exception {
    GcsWriteOptions options = GcsWriteOptions.builder().build();
    GcsBidiWriteChannel channel = new GcsBidiWriteChannel(storage, blobInfo, options);

    assertThat(channel.isOpen()).isTrue();
    channel.close();

    verify(mockAppendChannel).close();
    assertThat(channel.isOpen()).isFalse();

    // Secondary close is no-op
    channel.close();
    verify(mockAppendChannel).close(); // Still called only once
  }

  @Test
  void testClose_failure_translatesException() throws Exception {
    GcsWriteOptions options = GcsWriteOptions.builder().build();
    GcsBidiWriteChannel channel = new GcsBidiWriteChannel(storage, blobInfo, options);

    StorageException se = new StorageException(403, "Forbidden");
    Mockito.doThrow(se).when(mockAppendChannel).close();

    assertThrows(AccessDeniedException.class, () -> channel.close());
  }
}
