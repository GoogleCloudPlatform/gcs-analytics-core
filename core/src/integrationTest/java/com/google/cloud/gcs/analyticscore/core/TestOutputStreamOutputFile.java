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

package com.google.cloud.gcs.analyticscore.core;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * Adapts GoogleCloudStorageOutputStream to the Parquet OutputFile interface.
 */
public class TestOutputStreamOutputFile implements OutputFile {
  private final GoogleCloudStorageOutputStream gcsos;

  public TestOutputStreamOutputFile(GoogleCloudStorageOutputStream gcsos) {
    this.gcsos = gcsos;
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) throws IOException {
    return new PositionOutputStream() {
      @Override
      public long getPos() {
        return gcsos.getBytesWritten();
      }

      @Override
      public void write(int b) throws IOException {
        gcsos.write(b);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        gcsos.write(b, off, len);
      }

      @Override
      public void close() throws IOException {
        gcsos.close();
      }
    };
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
    return create(blockSizeHint);
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return 0;
  }
}
