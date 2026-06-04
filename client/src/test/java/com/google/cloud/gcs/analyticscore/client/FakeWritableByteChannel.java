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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;

public class FakeWritableByteChannel implements WritableByteChannel {
  private final ByteArrayOutputStream bos = new ByteArrayOutputStream();
  private boolean open = true;
  private int closeCount = 0;
  private Throwable exceptionToThrowOnWrite;
  private Throwable exceptionToThrowOnClose;

  public void setExceptionToThrowOnWrite(Throwable exception) {
    this.exceptionToThrowOnWrite = exception;
  }

  public void setExceptionToThrowOnClose(Throwable exception) {
    this.exceptionToThrowOnClose = exception;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    if (exceptionToThrowOnWrite != null) {
      if (exceptionToThrowOnWrite instanceof IOException) {
        throw (IOException) exceptionToThrowOnWrite;
      }
      if (exceptionToThrowOnWrite instanceof RuntimeException) {
        throw (RuntimeException) exceptionToThrowOnWrite;
      }
      throw new RuntimeException(exceptionToThrowOnWrite);
    }
    if (!open) {
      throw new ClosedChannelException();
    }
    int remaining = src.remaining();
    byte[] bytes = new byte[remaining];
    src.get(bytes);
    bos.write(bytes);
    return remaining;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void close() throws IOException {
    if (exceptionToThrowOnClose != null) {
      if (exceptionToThrowOnClose instanceof IOException) {
        throw (IOException) exceptionToThrowOnClose;
      }
      if (exceptionToThrowOnClose instanceof RuntimeException) {
        throw (RuntimeException) exceptionToThrowOnClose;
      }
      throw new RuntimeException(exceptionToThrowOnClose);
    }
    open = false;
    closeCount++;
  }

  public byte[] toByteArray() {
    return bos.toByteArray();
  }

  public int getCloseCount() {
    return closeCount;
  }
}
