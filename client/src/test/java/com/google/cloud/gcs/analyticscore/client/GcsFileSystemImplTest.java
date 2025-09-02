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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GcsFileSystemImplTest {

  private static final String TEST_PROJECT = "test-project";
  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_OBJECT = "test-dir/test-object.txt";
  private static final GcsClientOptions TEST_GCS_CLIENT_OPTIONS =
      GcsClientOptions.builder().setProjectId(TEST_PROJECT).build();
  private static final GcsFileSystemOptions TEST_GCS_FILESYSTEM_OPTIONS =
      GcsFileSystemOptions.builder().setGcsClientOptions(TEST_GCS_CLIENT_OPTIONS).build();

  @Mock private GcsClient mockClient;
  private GcsFileSystem gcsFileSystem;

  @BeforeEach
  void setUp() {
    gcsFileSystem = new GcsFileSystemImpl(mockClient, TEST_GCS_FILESYSTEM_OPTIONS);
  }


  @Test
  void constructor_withCredentials_createsClientWithProvidedCredentials() throws IOException {
    GcsFileSystemImpl gcsFileSystem =
        new GcsFileSystemImpl(NoCredentials.getInstance(), TEST_GCS_FILESYSTEM_OPTIONS);
    GcsClientImpl gcsClientImpl = (GcsClientImpl) gcsFileSystem.getGcsClient();
    assertThat(gcsClientImpl.storage.getOptions().getCredentials())
        .isEqualTo(NoCredentials.getInstance());
  }

  @Test
  void open_withObjectPath_shouldSucceedAndReadContent() throws IOException, URISyntaxException {
    String content = "hello world";
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();
    URI gcsPath = new URI("gs://" + TEST_BUCKET + "/" + TEST_OBJECT);
    GcsReadOptions readOptions = GcsReadOptions.builder().setProjectId("test-project").build();
    VectoredSeekableByteChannel mockChannel = mock(VectoredSeekableByteChannel.class);

    when(mockClient.openReadChannel(eq(itemId), eq(readOptions))).thenReturn(mockChannel);
    when(mockChannel.isOpen()).thenReturn(true);
    when(mockChannel.size()).thenReturn((long) content.length());
    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              buffer.put(content.getBytes(StandardCharsets.UTF_8));
              return content.length();
            });

    try (SeekableByteChannel channel = gcsFileSystem.open(gcsPath, readOptions)) {
      assertNotNull(channel);
      assertTrue(channel.isOpen());
      assertEquals(content.length(), channel.size());

      ByteBuffer buffer = ByteBuffer.allocate(content.length());
      int bytesRead = channel.read(buffer);
      assertEquals(content.length(), bytesRead);
      assertEquals(content, new String(buffer.array(), StandardCharsets.UTF_8));
    }
  }

  @Test
  void open_withNonExistentObjectPath_shouldThrowException()
      throws URISyntaxException, IOException {
    GcsItemId nonExistentItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("non-existent-object").build();
    URI nonExistentPath = new URI("gs://" + TEST_BUCKET + "/non-existent-object");
    GcsReadOptions readOptions = GcsReadOptions.builder().setProjectId("test-project").build();
    when(mockClient.openReadChannel(eq(nonExistentItemId), eq(readOptions)))
        .thenThrow(new IOException("Object not found:" + nonExistentItemId));

    IOException e =
        assertThrows(IOException.class, () -> gcsFileSystem.open(nonExistentPath, readOptions));

    assertThat(e).hasMessageThat().contains("Object not found:" + nonExistentItemId);
  }

  @Test
  void open_withNullPath_throwsException() {
    GcsReadOptions readOptions = GcsReadOptions.builder().setProjectId("test-project").build();

    NullPointerException e =
        assertThrows(NullPointerException.class, () -> gcsFileSystem.open(null, readOptions));

    assertThat(e).hasMessageThat().contains("path should not be null");
  }

  @Test
  void open_withBucketPath_shouldThrowException() throws URISyntaxException {
    URI bucketPath = new URI("gs://" + TEST_BUCKET);
    GcsReadOptions readOptions = GcsReadOptions.builder().setProjectId("test-project").build();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> gcsFileSystem.open(bucketPath, readOptions));
    assertTrue(e.getMessage().contains("Expected GCS object to be provided"));
  }

  @Test
  void getFileInfo_withValidPath_returnsGcsFileInfo() throws IOException, URISyntaxException {
    String content = "file info test";
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();
    URI gcsPath = new URI("gs://" + TEST_BUCKET + "/" + TEST_OBJECT);
    GcsItemInfo mockItemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize((long) content.length())
            .setContentGeneration(12345L) // A sample generation ID
            .build();
    when(mockClient.getGcsItemInfo(eq(itemId))).thenReturn(mockItemInfo);

    GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(gcsPath);

    assertNotNull(fileInfo);
    assertEquals(gcsPath, fileInfo.getUri());
    assertEquals(TEST_BUCKET, fileInfo.getItemInfo().getItemId().getBucketName());
    assertTrue(fileInfo.getItemInfo().getItemId().getObjectName().isPresent());
    assertEquals(TEST_OBJECT, fileInfo.getItemInfo().getItemId().getObjectName().get());
    assertEquals(content.length(), fileInfo.getItemInfo().getSize());
    assertNotNull(fileInfo.getAttributes());
    assertTrue(fileInfo.getAttributes().isEmpty());
  }

  @Test
  void getFileInfo_withNonExistentPath_shouldThrowException()
      throws URISyntaxException, IOException {
    GcsItemId nonExistentItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("non-existent-object").build();
    URI nonExistentPath = new URI("gs://" + TEST_BUCKET + "/non-existent-object");
    when(mockClient.getGcsItemInfo(eq(nonExistentItemId)))
        .thenThrow(new IOException("Object not found:" + nonExistentItemId));

    IOException e =
        assertThrows(IOException.class, () -> gcsFileSystem.getFileInfo(nonExistentPath));

    assertThat(e).hasMessageThat().contains("Object not found:" + nonExistentItemId);
  }

  @Test
  void getOptions_shouldReturnConfiguredOptions() {
    assertEquals(TEST_GCS_FILESYSTEM_OPTIONS, gcsFileSystem.getFileSystemOptions());
  }

  @Test
  void getGcsClient_shouldReturnConfiguredClient() {
    assertEquals(mockClient, gcsFileSystem.getGcsClient());
  }
}
