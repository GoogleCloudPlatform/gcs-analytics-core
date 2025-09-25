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

import com.google.cloud.gcs.analyticscore.client.*;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.net.URI;

public class TestInputStreamInputFile implements InputFile {

    private GcsFileSystem gcsFileSystem;
    private final URI fileUri;
    private final boolean enableVectoredIO;
    private Long size;

    public TestInputStreamInputFile(URI filePath, boolean enableVectoredIO, int footerPrefetchSize) throws IOException {
        this.enableVectoredIO = enableVectoredIO;
        this.fileUri = filePath;
        GcsReadOptions gcsReadOptions = GcsReadOptions.builder().setFooterPrefetchSize(footerPrefetchSize).build();
        GcsFileSystemOptions gcsFileSystemOptions = GcsFileSystemOptions.builder().setGcsClientOptions(
                GcsClientOptions.builder().setGcsReadOptions(gcsReadOptions).build()
        ).build();

        this.gcsFileSystem = new GcsFileSystemImpl(gcsFileSystemOptions);
    }

    @Override
    public long getLength() throws IOException {
        if (size == null) {
            size = gcsFileSystem.getFileInfo(fileUri).getItemInfo().getSize();
        }
        return size;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        GoogleCloudStorageInputStream gcsInputStream = GoogleCloudStorageInputStream
                .create(gcsFileSystem, fileUri);
        return new TestSeekableInputStream(gcsInputStream, enableVectoredIO);
    }
}
