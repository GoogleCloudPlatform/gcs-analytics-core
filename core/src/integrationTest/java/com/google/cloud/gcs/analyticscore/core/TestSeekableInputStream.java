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

package com.google.cloud.gcs.analyticscore.core;

import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.ParquetFileRange;
import org.apache.parquet.bytes.ByteBufferAllocator;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

public class TestSeekableInputStream extends SeekableInputStream {
    private final GoogleCloudStorageInputStream inputStream;
    private final boolean vectoredReadEnabled;

    public TestSeekableInputStream(GoogleCloudStorageInputStream inputStream, boolean enableVectoredRead) {
        this.inputStream = inputStream;
        this.vectoredReadEnabled = enableVectoredRead;
    }

    @Override
    public long getPos() throws IOException {
        return inputStream.getPos();
    }

    @Override
    public void seek(long l) throws IOException {
        inputStream.seek(l);
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
        inputStream.read(bytes);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes, start, len);
        while (buffer.hasRemaining()) {
            if (inputStream.read(buffer) == -1) {
                throw new EOFException();
            }
        }
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        return inputStream.read(byteBuffer);
    }

    @Override
    public void readFully(ByteBuffer byteBuffer) throws IOException {
        while (byteBuffer.hasRemaining()) {
            if(inputStream.read(byteBuffer) == -1) {
                throw new EOFException();
            };
        }
    }

    @Override
    public int read() throws IOException {
        return inputStream.read();
    }

    @Override
    public boolean readVectoredAvailable(ByteBufferAllocator allocator) {
        return vectoredReadEnabled;
    }

    @Override
    public void readVectored(List<ParquetFileRange> ranges, ByteBufferAllocator allocator) throws IOException {
        List<GcsObjectRange> gcsObjectRanges = new ArrayList<>();
        for (ParquetFileRange range : ranges) {
            GcsObjectRange.Builder builder = GcsObjectRange.builder()
                    .setOffset(range.getOffset())
                    .setLength(range.getLength());
            CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
            builder.setByteBufferFuture(future);
            range.setDataReadFuture(future);
            gcsObjectRanges.add(builder.build());
        }
        inputStream.readVectored(gcsObjectRanges, allocator::allocate);
    }
}
