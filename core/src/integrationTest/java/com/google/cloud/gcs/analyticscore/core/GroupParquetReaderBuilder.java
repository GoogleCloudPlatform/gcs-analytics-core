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

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.InputFile;

/**
 * A custom ParquetReader.Builder to create a reader for 'Group' objects
 * from an 'InputFile'.
 *
 * This is necessary because the default ParquetReader.builder() static method
 * only accepts a 'Path', and the protected Builder(InputFile) constructor
 * must be subclassed.
 */
public class GroupParquetReaderBuilder extends ParquetReader.Builder<Group> {

    private final GroupReadSupport readSupport = new GroupReadSupport();

    /**
     * Creates a builder for reading 'Group' objects from an InputFile.
     * @param file The InputFile to read from.
     */
    public GroupParquetReaderBuilder(InputFile file) {
        super(file);
    }

    @Override
    protected ReadSupport<Group> getReadSupport() {
        return readSupport;
    }
}
