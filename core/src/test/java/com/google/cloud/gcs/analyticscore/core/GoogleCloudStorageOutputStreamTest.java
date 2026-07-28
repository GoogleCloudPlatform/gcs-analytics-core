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

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.client.FakeGcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Operation;
import com.google.cloud.gcs.analyticscore.common.telemetry.MetricsRecorder;
import com.google.cloud.gcs.analyticscore.common.telemetry.OperationSupplier;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GoogleCloudStorageOutputStreamTest {

  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_OBJECT = "test-object";
  private final GcsItemId itemId =
      GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();

  private GcsFileSystemOptions fileSystemOptions;
  private FakeGcsFileSystemImpl fakeFileSystem;

  @BeforeEach
  void setUp() {
    fileSystemOptions = GcsFileSystemOptions.createFromOptions(Map.of(), "");
    fakeFileSystem = new FakeGcsFileSystemImpl(fileSystemOptions);
  }

  @Test
  void write_validData_recordsTelemetryMetrics() throws IOException {
    GcsFileSystem mockFileSystem = mock(GcsFileSystem.class);
    WritableByteChannel mockChannel = mock(WritableByteChannel.class);
    when(mockFileSystem.getFileSystemOptions()).thenReturn(fileSystemOptions);
    when(mockFileSystem.create(any(GcsItemId.class), any())).thenReturn(mockChannel);

    Telemetry mockTelemetry = mock(Telemetry.class);
    when(mockFileSystem.getTelemetry()).thenReturn(mockTelemetry);

    when(mockChannel.write(any(ByteBuffer.class)))
        .thenAnswer(
            i -> {
              ByteBuffer buf = i.getArgument(0);
              int rem = buf.remaining();
              buf.position(buf.position() + rem);
              return rem;
            });

    // Stub measure to just execute the lambda and return its result
    when(mockTelemetry.measure(any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              MetricsRecorder recorder = mock(MetricsRecorder.class);
              OperationSupplier closure = invocation.getArgument(3);
              return closure.get(recorder);
            });

    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(mockFileSystem, itemId);

    stream.write(65);
    stream.close();

    verify(mockTelemetry)
        .measure(eq(Operation.CREATE.name()), eq(Metric.CREATE_DURATION), any(), any());
    verify(mockTelemetry)
        .measure(eq(Operation.WRITE.name()), eq(Metric.WRITE_DURATION), any(), any());
    verify(mockTelemetry)
        .measure(eq(Operation.WRITE_CLOSE.name()), eq(Metric.WRITE_CLOSE_DURATION), any(), any());
  }

  @Test
  void create_initializesWriteSession_returnsStream() throws IOException {
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeFileSystem, itemId);

    assertThat(stream).isNotNull();
    assertThat(stream.getBytesWritten()).isEqualTo(0L);
  }

  @Test
  void createWithUri_initializesWriteSession_returnsStream() throws IOException {
    URI uri = URI.create("gs://" + TEST_BUCKET + "/" + TEST_OBJECT);

    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeFileSystem, uri);

    assertThat(stream).isNotNull();
    assertThat(stream.getBytesWritten()).isEqualTo(0L);
  }

  @Test
  void createWithGcsFileInfo_initializesWriteSession_returnsStream() throws IOException {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(100).build();
    GcsFileInfo fileInfo =
        GcsFileInfo.builder()
            .setItemInfo(itemInfo)
            .setUri(URI.create("gs://" + TEST_BUCKET + "/" + TEST_OBJECT))
            .setAttributes(Collections.emptyMap())
            .build();

    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeFileSystem, fileInfo);

    assertThat(stream).isNotNull();
    assertThat(stream.getBytesWritten()).isEqualTo(0L);
  }

  @Test
  void create_nullFileSystem_throwsNullPointerException() {
    var exception =
        assertThrows(
            NullPointerException.class, () -> GoogleCloudStorageOutputStream.create(null, itemId));

    assertThat(exception).hasMessageThat().isEqualTo("GcsFileSystem shouldn't be null");
  }

  @Test
  void createWithUri_nullFileSystem_throwsNullPointerException() {
    var exception =
        assertThrows(
            NullPointerException.class,
            () -> GoogleCloudStorageOutputStream.create(null, URI.create("gs://bucket/object")));

    assertThat(exception).hasMessageThat().isEqualTo("GcsFileSystem shouldn't be null");
  }

  @Test
  void createWithGcsFileInfo_nullFileSystem_throwsNullPointerException() {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(100).build();
    GcsFileInfo fileInfo =
        GcsFileInfo.builder()
            .setItemInfo(itemInfo)
            .setUri(URI.create("gs://" + TEST_BUCKET + "/" + TEST_OBJECT))
            .setAttributes(Collections.emptyMap())
            .build();

    var exception =
        assertThrows(
            NullPointerException.class,
            () -> GoogleCloudStorageOutputStream.create(null, fileInfo));
    assertThat(exception).hasMessageThat().isEqualTo("GcsFileSystem shouldn't be null");
  }

  @Test
  void createWithGcsFileInfo_nullFileInfo_throwsNullPointerException() {
    var exception =
        assertThrows(
            NullPointerException.class,
            () -> GoogleCloudStorageOutputStream.create(fakeFileSystem, (GcsFileInfo) null));
    assertThat(exception).hasMessageThat().isEqualTo("GcsFileInfo shouldn't be null");
  }

  @Test
  void create_nullBlobInfo_throwsNullPointerException() {
    var exception =
        assertThrows(
            NullPointerException.class,
            () -> GoogleCloudStorageOutputStream.create(fakeFileSystem, (GcsItemId) null));
    assertThat(exception).hasMessageThat().isEqualTo("GcsItemId shouldn't be null");
  }

  @Test
  void create_nullFileSystemOptions_throwsNullPointerException() {
    GcsFileSystem mockFileSystem = mock(GcsFileSystem.class);
    when(mockFileSystem.getTelemetry()).thenReturn(new Telemetry(ImmutableList.of()));
    when(mockFileSystem.getFileSystemOptions()).thenReturn(null);

    assertThrows(
        NullPointerException.class,
        () -> GoogleCloudStorageOutputStream.create(mockFileSystem, itemId));
  }

  @Test
  void write_singleByte_partialWrites_loopsUntilFinished() throws IOException {
    GcsFileSystem mockFileSystem = mock(GcsFileSystem.class);
    when(mockFileSystem.getTelemetry()).thenReturn(new Telemetry(ImmutableList.of()));
    WritableByteChannel mockChannel = mock(WritableByteChannel.class);
    when(mockFileSystem.getFileSystemOptions()).thenReturn(fileSystemOptions);
    when(mockFileSystem.create(any(GcsItemId.class), any())).thenReturn(mockChannel);
    int[] callCount = {0};
    when(mockChannel.write(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              if (callCount[0] == 0) {
                callCount[0]++;
                return 0; // Simulate 0 bytes written on first try
              }
              int remaining = buffer.remaining();
              buffer.position(buffer.position() + remaining);
              return remaining;
            });
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(mockFileSystem, itemId);

    stream.write(65); // 'A'

    verify(mockChannel, times(2)).write(any(ByteBuffer.class));
  }

  @Test
  void write_byteArray_partialWrites_loopsUntilFinished() throws IOException {
    GcsFileSystem mockFileSystem = mock(GcsFileSystem.class);
    when(mockFileSystem.getTelemetry()).thenReturn(new Telemetry(ImmutableList.of()));
    WritableByteChannel mockChannel = mock(WritableByteChannel.class);
    when(mockFileSystem.getFileSystemOptions()).thenReturn(fileSystemOptions);
    when(mockFileSystem.create(any(GcsItemId.class), any())).thenReturn(mockChannel);
    int[] callCount = {0};
    when(mockChannel.write(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              if (callCount[0] == 0) {
                callCount[0]++;
                buffer.position(buffer.position() + 1);
                return 1; // Simulate 1 byte written
              }
              int remaining = buffer.remaining();
              buffer.position(buffer.position() + remaining);
              return remaining;
            });
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(mockFileSystem, itemId);
    byte[] data = new byte[] {1, 2, 3};

    stream.write(data, 0, 3);

    verify(mockChannel, times(2)).write(any(ByteBuffer.class));
  }

  @Test
  void write_byteArray_invalidArgs_throwsExceptions() throws IOException {
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeFileSystem, itemId);
    byte[] data = new byte[] {1, 2, 3};

    assertThrows(NullPointerException.class, () -> stream.write(null, 0, 1));
    assertThrows(IndexOutOfBoundsException.class, () -> stream.write(data, -1, 2));
    assertThrows(IndexOutOfBoundsException.class, () -> stream.write(data, 0, -1));
    assertThrows(IndexOutOfBoundsException.class, () -> stream.write(data, 2, 2));
  }

  @Test
  void write_byteArray_zeroLength_doesNothing() throws IOException {
    GcsFileSystem mockFileSystem = mock(GcsFileSystem.class);
    when(mockFileSystem.getTelemetry()).thenReturn(new Telemetry(ImmutableList.of()));
    WritableByteChannel mockChannel = mock(WritableByteChannel.class);
    when(mockFileSystem.getFileSystemOptions()).thenReturn(fileSystemOptions);
    when(mockFileSystem.create(any(GcsItemId.class), any())).thenReturn(mockChannel);
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(mockFileSystem, itemId);
    byte[] data = new byte[] {1, 2, 3};

    stream.write(data, 0, 0);

    verify(mockChannel, never()).write(any(ByteBuffer.class));
  }

  @Test
  void close_closesChannel() throws IOException {
    GcsFileSystem mockFileSystem = mock(GcsFileSystem.class);
    when(mockFileSystem.getTelemetry()).thenReturn(new Telemetry(ImmutableList.of()));
    WritableByteChannel mockChannel = mock(WritableByteChannel.class);
    when(mockFileSystem.getFileSystemOptions()).thenReturn(fileSystemOptions);
    when(mockFileSystem.create(any(GcsItemId.class), any())).thenReturn(mockChannel);
    when(mockChannel.isOpen()).thenReturn(true);

    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(mockFileSystem, itemId);

    stream.close();

    verify(mockChannel).close();
  }

  @Test
  void getBytesWritten_returnsAccumulatedBytes() throws IOException {
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeFileSystem, itemId);

    assertThat(stream.getBytesWritten()).isEqualTo(0L);

    stream.write(65); // 1 byte
    assertThat(stream.getBytesWritten()).isEqualTo(1L);

    byte[] data = new byte[] {1, 2, 3, 4, 5};
    stream.write(data, 1, 3); // 3 bytes
    assertThat(stream.getBytesWritten()).isEqualTo(4L);
  }

  @Test
  void write_withFakeGcsFileSystem_writesDataCorrectly() throws IOException {
    try (GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeFileSystem, itemId)) {
      stream.write("hello fake world".getBytes(UTF_8));
    }

    // Verify the data is present in the fake file system
    GcsFileInfo fileInfo = fakeFileSystem.getFileInfo(itemId);
    assertThat(fileInfo).isNotNull();
    assertThat(fileInfo.getItemInfo().getSize()).isEqualTo("hello fake world".length());
    // Read the data back using GoogleCloudStorageInputStream to verify
    try (GoogleCloudStorageInputStream inputStream =
        GoogleCloudStorageInputStream.create(fakeFileSystem, itemId)) {
      byte[] buffer = new byte[50];
      int read = inputStream.read(buffer, 0, buffer.length);
      assertThat(new String(buffer, 0, read, UTF_8)).isEqualTo("hello fake world");
    }
  }

  @Test
  void write_multipleChunksAndSingleBytes_withFakeGcsFileSystem() throws IOException {
    int valA = 65; // 'A'
    byte[] chunk1 = "hello".getBytes(UTF_8);
    int valB = 66; // 'B'
    byte[] chunk2 = "world".getBytes(UTF_8);
    int chunk2Offset = 0;
    int chunk2Length = 3; // "wor"
    ByteArrayOutputStream expectedStream = new ByteArrayOutputStream();
    expectedStream.write(valA);
    expectedStream.write(chunk1, 0, chunk1.length);
    expectedStream.write(valB);
    expectedStream.write(chunk2, chunk2Offset, chunk2Length);
    byte[] expectedBytes = expectedStream.toByteArray();

    try (GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeFileSystem, itemId)) {
      stream.write(valA);
      stream.write(chunk1);
      stream.write(valB);
      stream.write(chunk2, chunk2Offset, chunk2Length);
    }

    // Verify the data is present in the fake file system
    GcsFileInfo fileInfo = fakeFileSystem.getFileInfo(itemId);
    assertThat(fileInfo).isNotNull();
    assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(expectedBytes.length);
    // read the data back using GoogleCloudStorageInputStream to verify correctness
    try (GoogleCloudStorageInputStream inputStream =
        GoogleCloudStorageInputStream.create(fakeFileSystem, itemId)) {
      byte[] buffer = new byte[expectedBytes.length + 10];
      int read = inputStream.read(buffer, 0, buffer.length);
      assertThat(read).isEqualTo(expectedBytes.length);
      byte[] actualBytes = Arrays.copyOf(buffer, read);
      assertThat(actualBytes).isEqualTo(expectedBytes);
    }
  }
}
