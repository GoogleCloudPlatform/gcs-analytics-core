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
import com.google.cloud.RestorableState;
import java.io.IOException;
import java.nio.ByteBuffer;

public class TrackingReadChannel implements ReadChannel {
  private final ReadChannel delegate;
  private int readCalls = 0;
  private int seekCalls = 0;
  private int eofAtCall = -1;
  private int closeCalls = 0;

  public TrackingReadChannel(ReadChannel delegate) {
    this.delegate = delegate;
  }

  public void setEofAtCall(int callNumber) {
    this.eofAtCall = callNumber;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    readCalls++;
    if (readCalls == eofAtCall) {
      return -1;
    }
    return delegate.read(dst);
  }

  @Override
  public void seek(long position) throws IOException {
    seekCalls++;
    delegate.seek(position);
  }

  @Override
  public void close() {
    closeCalls++;
    delegate.close();
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public void setChunkSize(int chunkSize) {
    delegate.setChunkSize(chunkSize);
  }

  @Override
  public ReadChannel limit(long limit) {
    delegate.limit(limit);
    return this;
  }

  @Override
  public RestorableState<ReadChannel> capture() {
    return delegate.capture();
  }

  public int getReadCalls() {
    return readCalls;
  }

  public int getSeekCalls() {
    return seekCalls;
  }

  public int getCloseCalls() {
    return closeCalls;
  }
}
