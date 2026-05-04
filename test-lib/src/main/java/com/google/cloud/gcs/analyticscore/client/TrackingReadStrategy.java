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

import com.google.cloud.ReadChannel;
import java.io.IOException;

public class TrackingReadStrategy implements ReadStrategy {
  private final ReadStrategy delegate;
  private static int totalGetReadChannelCalls = 0;
  private int closeCalls = 0;
  private int getReadChannelCalls = 0;
  private int eofAtCall = -1;
  private TrackingReadChannel currentChannel;

  public TrackingReadStrategy(ReadStrategy delegate) {
    this.delegate = delegate;
  }

  public void setEofAtCall(int eofAtCall) {
    this.eofAtCall = eofAtCall;
  }

  @Override
  public ReadChannel getReadChannel(long requestedPosition, int bytesToRead) throws IOException {
    getReadChannelCalls++;
    totalGetReadChannelCalls++;
    ReadChannel ch = delegate.getReadChannel(requestedPosition, bytesToRead);
    currentChannel = new TrackingReadChannel(ch);
    if (eofAtCall != -1) {
      currentChannel.setEofAtCall(eofAtCall);
    }
    return currentChannel;
  }

  @Override
  public void position(long newPosition) {
    delegate.position(newPosition);
  }

  @Override
  public long getLimit() {
    return delegate.getLimit();
  }

  @Override
  public boolean isEof(long position) {
    return delegate.isEof(position);
  }

  @Override
  public void close() throws IOException {
    closeCalls++;
    delegate.close();
  }

  public int getCloseCalls() {
    return closeCalls;
  }

  public int getGetReadChannelCalls() {
    return getReadChannelCalls;
  }

  public static int getTotalGetReadChannelCalls() {
    return totalGetReadChannelCalls;
  }

  public static void resetTotalGetReadChannelCalls() {
    totalGetReadChannelCalls = 0;
  }

  public TrackingReadChannel getCurrentChannel() {
    return currentChannel;
  }

  public ReadStrategy getDelegate() {
    return delegate;
  }
}
