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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class HermeticGcsFileSystemTest {
  private static final GcsItemId ITEM =
      GcsItemId.builder().setBucketName("bucket").setObjectName("obj").build();
  private static final GcsItemId MISSING =
      GcsItemId.builder().setBucketName("bucket").setObjectName("nope").build();
  private static final byte[] CONTENT = "hermetic-content".getBytes(StandardCharsets.UTF_8);
  private static final GcsReadOptions READ_OPTIONS = GcsReadOptions.builder().build();

  private final InMemoryStorage storage = InMemoryStorage.create(ImmutableMap.of(ITEM, CONTENT));
  private final GcsFileSystem fs =
      HermeticGcsFileSystem.create(storage, GcsFileSystemOptions.builder().build());

  @AfterEach
  void closeFileSystem() {
    fs.close();
  }

  @Test
  void getFileInfo_returnsSizeFromMap() throws Exception {
    assertThat(fs.getFileInfo(ITEM).getItemInfo().getSize()).isEqualTo(CONTENT.length);
  }

  @Test
  void getFileInfo_missingObject_throwsFileNotFoundLikeProduction() {
    assertThrows(FileNotFoundException.class, () -> fs.getFileInfo(MISSING));
  }

  @Test
  void open_readsAllBytes_withOneReaderOpen() throws Exception {
    ByteBuffer buffer = ByteBuffer.allocate(CONTENT.length);

    try (VectoredSeekableByteChannel channel = fs.open(ITEM, READ_OPTIONS)) {
      assertThat(channel.read(buffer)).isEqualTo(CONTENT.length);
    }

    assertThat(buffer.array()).isEqualTo(CONTENT);
    assertThat(storage.readerOpens()).isEqualTo(1);
  }

  @Test
  void open_missingObject_failsOnFirstReadWithFileNotFound() throws Exception {
    try (VectoredSeekableByteChannel channel = fs.open(MISSING, READ_OPTIONS)) {
      assertThat(storage.readerOpens()).isEqualTo(0);
      assertThrows(FileNotFoundException.class, () -> channel.read(ByteBuffer.allocate(1)));
    }
  }

  @Test
  void close_isIdempotent() {
    fs.close();
    assertDoesNotThrow(fs::close);
  }
}
