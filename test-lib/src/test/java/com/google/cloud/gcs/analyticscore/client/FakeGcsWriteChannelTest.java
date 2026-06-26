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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FakeGcsWriteChannelTest {

  private FakeGcsWriteChannel fakeGcsWriteChannel;
  private BlobInfo blobInfo;

  @BeforeEach
  void setUp() throws Exception {
    blobInfo = BlobInfo.newBuilder(BlobId.of("test-bucket", "test-object")).build();
    BlobWriteSession session =
        LocalStorageHelper.getOptions().getService().blobWriteSession(blobInfo);
    fakeGcsWriteChannel =
        new FakeGcsWriteChannel(
            session, session.open(), blobInfo, GcsWriteOptions.builder().build());
  }

  @Test
  void write_whenChannelWritten_incrementsWriteCallCount() throws Exception {
    fakeGcsWriteChannel.write(ByteBuffer.wrap(new byte[] {1, 2, 3}));

    assertThat(fakeGcsWriteChannel.getWriteCallCount()).isEqualTo(1);
  }

  @Test
  void close_whenChannelClosed_incrementsCloseCallCount() throws Exception {
    fakeGcsWriteChannel.close();

    assertThat(fakeGcsWriteChannel.getCloseCallCount()).isEqualTo(1);
  }
}
