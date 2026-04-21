/*
 * Copyright 2026 Google LLC
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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TrackingReadChannelTest {

  private ReadChannel delegate;
  private TrackingReadChannel trackingReadChannel;

  @BeforeEach
  void setUp() {
    delegate = mock(ReadChannel.class);
    trackingReadChannel = new TrackingReadChannel(delegate);
  }

  @Test
  void read_delegatesAndIncrementsCounter() throws IOException {
    ByteBuffer dst = ByteBuffer.allocate(10);
    when(delegate.read(dst)).thenReturn(1);

    int bytesRead = trackingReadChannel.read(dst);

    assertThat(bytesRead).isEqualTo(1);
    assertThat(trackingReadChannel.getReadCalls()).isEqualTo(1);
    verify(delegate).read(dst);
  }

  @Test
  void read_returnsEofWhenSimulated() throws IOException {
    ByteBuffer dst = ByteBuffer.allocate(10);
    trackingReadChannel.setEofAtCall(1);

    int bytesRead = trackingReadChannel.read(dst);

    assertThat(bytesRead).isEqualTo(-1);
    assertThat(trackingReadChannel.getReadCalls()).isEqualTo(1);
  }

  @Test
  void seek_delegatesAndIncrementsCounter() throws IOException {
    long position = 100L;

    trackingReadChannel.seek(position);

    assertThat(trackingReadChannel.getSeekCalls()).isEqualTo(1);
    verify(delegate).seek(position);
  }

  @Test
  void close_delegatesAndIncrementsCounter() {
    trackingReadChannel.close();

    assertThat(trackingReadChannel.getCloseCalls()).isEqualTo(1);
    verify(delegate).close();
  }

  @Test
  void isOpen_delegates() {
    when(delegate.isOpen()).thenReturn(true);

    boolean open = trackingReadChannel.isOpen();

    assertThat(open).isTrue();
    verify(delegate).isOpen();
  }

  @Test
  void setChunkSize_delegatesAndTracks() {
    int chunkSize = 1024;

    trackingReadChannel.setChunkSize(chunkSize);

    assertThat(trackingReadChannel.getLastChunkSize()).isEqualTo(chunkSize);
    verify(delegate).setChunkSize(chunkSize);
  }

  @Test
  void limit_delegatesAndTracks() {
    long limit = 500L;
    when(delegate.limit(limit)).thenReturn(delegate);

    ReadChannel result = trackingReadChannel.limit(limit);

    assertThat(result).isEqualTo(trackingReadChannel);
    assertThat(trackingReadChannel.getLastLimit()).isEqualTo(limit);
    verify(delegate).limit(limit);
  }

  @Test
  void capture_delegates() {
    when(delegate.capture()).thenReturn(null);

    RestorableState<ReadChannel> state = trackingReadChannel.capture();

    assertThat(state).isNull();
    verify(delegate).capture();
  }
}
