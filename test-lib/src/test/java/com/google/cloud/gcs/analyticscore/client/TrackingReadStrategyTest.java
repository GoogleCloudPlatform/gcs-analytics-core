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
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TrackingReadStrategyTest {

  private ReadStrategy delegate;
  private TrackingReadStrategy trackingReadStrategy;

  @BeforeEach
  void setUp() {
    TrackingReadStrategy.resetTotalGetReadChannelCalls();
    delegate = mock(ReadStrategy.class);
    trackingReadStrategy = new TrackingReadStrategy(delegate);
  }

  @AfterEach
  void tearDown() {
    TrackingReadStrategy.resetTotalGetReadChannelCalls();
  }

  @Test
  void constructor_initializesDelegateAndZeroCounters() {
    assertThat(trackingReadStrategy.getDelegate()).isSameInstanceAs(delegate);
    assertThat(trackingReadStrategy.getCloseCalls()).isEqualTo(0);
    assertThat(trackingReadStrategy.getGetReadChannelCalls()).isEqualTo(0);
    assertThat(trackingReadStrategy.getCurrentChannel()).isNull();
  }

  @Test
  void getSdkReadChannel_withDelegate_delegates() {
    ReadChannel channel = mock(ReadChannel.class);
    when(delegate.getSdkReadChannel()).thenReturn(channel);

    ReadChannel result = trackingReadStrategy.getSdkReadChannel();

    assertThat(result).isSameInstanceAs(channel);
    verify(delegate).getSdkReadChannel();
  }

  @Test
  void getSdkReadChannel_withNullDelegate_returnsNull() {
    TrackingReadStrategy strategyWithNullDelegate = new TrackingReadStrategy(null);

    ReadChannel result = strategyWithNullDelegate.getSdkReadChannel();

    assertThat(result).isNull();
  }

  @Test
  void getReadChannel_withoutEofSimulation_wrapsChannelAndIncrementsCounters() throws IOException {
    ReadChannel mockChannel = mock(ReadChannel.class);
    when(delegate.getReadChannel(100L, 50)).thenReturn(mockChannel);

    ReadChannel result = trackingReadStrategy.getReadChannel(100L, 50);

    assertThat(result).isInstanceOf(TrackingReadChannel.class);
    assertThat(trackingReadStrategy.getCurrentChannel()).isSameInstanceAs(result);
    assertThat(trackingReadStrategy.getGetReadChannelCalls()).isEqualTo(1);
    assertThat(TrackingReadStrategy.getTotalGetReadChannelCalls()).isEqualTo(1);
    verify(delegate).getReadChannel(100L, 50);
  }

  @Test
  void getReadChannel_withEofAtCallConfigured_propagatesEofThresholdToWrappedChannel()
      throws IOException {
    ReadChannel mockChannel = mock(ReadChannel.class);
    when(delegate.getReadChannel(0L, 10)).thenReturn(mockChannel);
    trackingReadStrategy.setEofAtCall(1);

    ReadChannel channel = trackingReadStrategy.getReadChannel(0L, 10);
    ByteBuffer dst = ByteBuffer.allocate(10);
    int bytesRead = channel.read(dst);

    assertThat(bytesRead).isEqualTo(-1);
  }

  @Test
  void totalGetReadChannelCalls_accumulatesAcrossInstancesAndResets() throws IOException {
    ReadStrategy secondDelegate = mock(ReadStrategy.class);
    TrackingReadStrategy secondStrategy = new TrackingReadStrategy(secondDelegate);
    ReadChannel mockChannel = mock(ReadChannel.class);
    when(delegate.getReadChannel(0L, 10)).thenReturn(mockChannel);
    when(secondDelegate.getReadChannel(10L, 20)).thenReturn(mockChannel);

    trackingReadStrategy.getReadChannel(0L, 10);
    secondStrategy.getReadChannel(10L, 20);

    assertThat(TrackingReadStrategy.getTotalGetReadChannelCalls()).isEqualTo(2);
    TrackingReadStrategy.resetTotalGetReadChannelCalls();
    assertThat(TrackingReadStrategy.getTotalGetReadChannelCalls()).isEqualTo(0);
  }

  @Test
  void position_delegates() {
    trackingReadStrategy.position(100L);

    verify(delegate).position(100L);
  }

  @Test
  void getLimit_delegates() {
    when(delegate.getLimit()).thenReturn(500L);

    long limit = trackingReadStrategy.getLimit();

    assertThat(limit).isEqualTo(500L);
    verify(delegate).getLimit();
  }

  @Test
  void isEof_delegates() {
    when(delegate.isEof(100L)).thenReturn(true);

    boolean eof = trackingReadStrategy.isEof(100L);

    assertThat(eof).isTrue();
    verify(delegate).isEof(100L);
  }

  @Test
  void close_delegatesAndIncrementsCounter() throws IOException {
    trackingReadStrategy.close();

    assertThat(trackingReadStrategy.getCloseCalls()).isEqualTo(1);
    verify(delegate).close();
  }
}
