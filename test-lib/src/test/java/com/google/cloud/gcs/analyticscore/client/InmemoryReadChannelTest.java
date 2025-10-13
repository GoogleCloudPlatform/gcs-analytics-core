/*
 * Copyright 2025 Google LLC
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

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InmemoryReadChannelTest {

  private InmemoryReadChannel channel;
  private static final long FILE_SIZE = 1024;
  private ByteBuffer expectedData;

  @BeforeEach
  void setUp() {
    channel = new InmemoryReadChannel(FILE_SIZE);
    InmemoryReadChannel.resetCounts();
    expectedData = InmemoryReadChannel.getPredictableByteBuffer(FILE_SIZE);
  }

  @Test
  void isOpen_channelOpen_returnsTrue() {
    assertTrue(channel.isOpen());
  }

  @Test
  void isOpen_channelClosed_returnsFalse() {
    channel.close();
    assertFalse(channel.isOpen());
  }

  @Test
  void isOpen_anyState_incrementsCount() {
    channel.isOpen();
    assertEquals(1, InmemoryReadChannel.getIsOpenCount());
    channel.close();
    channel.isOpen();
    assertEquals(2, InmemoryReadChannel.getIsOpenCount());
  }

  @Test
  void close_anyState_closesChannelAndIncrementsCount() {
    channel.close();
    assertFalse(channel.isOpen());
    assertEquals(1, InmemoryReadChannel.getCloseCount());
  }

  @Test
  void seek_validPosition_updatesPosition() throws IOException {
    channel = new InmemoryReadChannel(1024);
    channel.seek(100);
    assertEquals(1, InmemoryReadChannel.getSeekCount());
  }

  @Test
  void seek_invalidPosition_throwsException() {
    channel = new InmemoryReadChannel(FILE_SIZE);
    assertThrows(IllegalArgumentException.class, () -> channel.seek(FILE_SIZE + 1));
    assertThrows(IllegalArgumentException.class, () -> channel.seek(-1));
    assertEquals(2, InmemoryReadChannel.getSeekCount());
  }

  @Test
  void setChunkSize_anyValue_incrementsCount() {
    channel.setChunkSize(128);
    assertEquals(1, InmemoryReadChannel.getSetChunkSizeCount());
  }

  @Test
  void capture_anyState_returnsNullAndIncrementsCount() {
    assertNull(channel.capture());
    assertEquals(1, InmemoryReadChannel.getCaptureCount());
  }

  @Test
  void read_bufferAvailable_readsBytesAndIncrementsCount() throws IOException {
    int readLength = 100;
    ByteBuffer buffer = ByteBuffer.allocate(readLength);

    int bytesRead = channel.read(buffer);

    assertEquals(readLength, bytesRead);
    assertEquals(1, InmemoryReadChannel.getReadCount());
    buffer.flip();
    ByteBuffer expectedData = InmemoryReadChannel.getPredictableByteBuffer(FILE_SIZE);
    assertBufferEquals(expectedData, buffer, 0, readLength);
  }

  @Test
  void read_partialRead_readsRemainingBytes() throws IOException {
    int offset = (int) FILE_SIZE - 50;
    int readLength = 50;
    channel.seek(offset);
    ByteBuffer buffer = ByteBuffer.allocate(100);

    int bytesRead = channel.read(buffer);

    assertEquals(readLength, bytesRead);
    assertEquals(1, InmemoryReadChannel.getReadCount());
    buffer.flip();
    ByteBuffer expectedData = InmemoryReadChannel.getPredictableByteBuffer(FILE_SIZE);
    assertBufferEquals(expectedData, buffer, offset, readLength);
  }

  @Test
  void read_eof_returnsMinusOne() throws IOException {
    channel.seek(FILE_SIZE);
    ByteBuffer buffer = ByteBuffer.allocate(100);
    int bytesRead = channel.read(buffer);
    assertEquals(-1, bytesRead);
    assertEquals(1, InmemoryReadChannel.getReadCount());
  }

  @Test
  void read_bufferMatchesFileSize_readsAllBytes() throws IOException {
    channel.seek(0);
    int readLength = (int) FILE_SIZE;
    ByteBuffer buffer = ByteBuffer.allocate(readLength);

    int bytesRead = channel.read(buffer);

    assertEquals(readLength, bytesRead);
    assertEquals(1, InmemoryReadChannel.getReadCount());
    buffer.flip();
    ByteBuffer expectedData = InmemoryReadChannel.getPredictableByteBuffer(FILE_SIZE);
    assertBufferEquals(expectedData, buffer, 0, readLength);
  }

  @Test
  void read_zeroSizeBuffer_readsZeroBytes() throws IOException {
    channel.seek(0);
    ByteBuffer buffer = ByteBuffer.allocate(0);
    int bytesRead = channel.read(buffer);
    assertEquals(0, bytesRead);
    assertEquals(1, InmemoryReadChannel.getReadCount());
  }

  @Test
  void getPredictableByteBuffer_sameSeedAndSize_returnsEqualBuffers() {
    ByteBuffer b1 = InmemoryReadChannel.getPredictableByteBuffer(100);
    ByteBuffer b2 = InmemoryReadChannel.getPredictableByteBuffer(100);
    assertEquals(b1, b2);
  }

  @Test
  void getPredictableByteBuffer_differentSize_returnsUnequalBuffers() {
    ByteBuffer b1 = InmemoryReadChannel.getPredictableByteBuffer(100);
    ByteBuffer b3 = InmemoryReadChannel.getPredictableByteBuffer(200);
    assertNotEquals(b1, b3);
  }

  private void assertBufferEquals(
      ByteBuffer expectedData, ByteBuffer actual, int expectedOffset, int expectedLength) {
    assertEquals(expectedLength, actual.remaining());
    ByteBuffer expected = expectedData.duplicate();
    expected.position(expectedOffset);
    expected.limit(expectedOffset + expectedLength);
    assertEquals(expected, actual);
  }
}
