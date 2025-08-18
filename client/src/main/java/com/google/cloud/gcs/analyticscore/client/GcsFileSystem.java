/*
 * Copyright 2025 Google LLC
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;

public interface GcsFileSystem {

   /**
     * Opens an object for reading.
     *
     * @param path Object full path of the form gs://bucket/object-path.
     * @param options Fine-grained read options for behaviors of retries, decryption, etc.
     * @return A channel for reading from the given object.
     * @throws FileNotFoundException if the given path does not exist.
     * @throws IOException if object exists but cannot be opened.
     */
    SeekableByteChannel open(URI path, GcsReadOptions options) throws IOException;

    /**
     * Gets Metadata about the given path item.
     *
     * @param path The path we want Metadata about.
     * @return Metadata about the given path item.
     */
    GcsFileInfo getFileInfo(URI path) throws IOException;

    /** Retrieve the options that were used to create this GcsFileSystem. */
    GcsFileSystemOptions getOptions();

    /** Retrieve the gcs client used to create this GcsFileSystem. */
    GcsClient getGcsClient();
}
