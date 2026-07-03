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
package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.storage.BlobInfo;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class GcsReadChannelMetadataExtractorTest {

  @Test
  void extract_nullChannel_returnsNull() {
    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(null);

    assertThat(metadata).isNull();
  }

  @Test
  void extract_unsupportedChannelClass_returnsNull() {
    ReadChannel channel = Mockito.mock(ReadChannel.class);

    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(channel);

    assertThat(metadata).isNull();
  }

  @Test
  void extract_viaMethod_getBlobInfo_returnsMetadata() {
    BlobInfo blobInfo = Mockito.mock(BlobInfo.class);
    Mockito.when(blobInfo.getSize()).thenReturn(100L);
    Mockito.when(blobInfo.getGeneration()).thenReturn(200L);
    ReflectiveBlobInfoChannel channel = Mockito.mock(ReflectiveBlobInfoChannel.class);
    Mockito.when(channel.getBlobInfo()).thenReturn(blobInfo);

    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(channel);

    assertThat(metadata).isNotNull();
    assertThat(metadata.getSize()).isEqualTo(100L);
    assertThat(metadata.getGeneration()).isEqualTo(200L);
  }

  @Test
  void extract_viaField_result_returnsMetadata() {
    BlobInfo blobInfo = Mockito.mock(BlobInfo.class);
    Mockito.when(blobInfo.getSize()).thenReturn(300L);
    Mockito.when(blobInfo.getGeneration()).thenReturn(400L);
    ReadChannel channel =
        new ReadChannel() {
          private final BlobInfo result = blobInfo;

          @Override
          public void close() {}

          @Override
          public boolean isOpen() {
            return true;
          }

          @Override
          public int read(ByteBuffer dst) {
            return -1;
          }

          @Override
          public void seek(long position) {}

          @Override
          public void setChunkSize(int chunkSize) {}

          @Override
          public RestorableState<ReadChannel> capture() {
            return null;
          }
        };

    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(channel);

    assertThat(metadata).isNotNull();
    assertThat(metadata.getSize()).isEqualTo(300L);
    assertThat(metadata.getGeneration()).isEqualTo(400L);
  }

  @Test
  void extract_futureNotDone_returnsNull() {
    CompletableFuture<BlobInfo> future = new CompletableFuture<>();
    ReflectiveFutureChannel channel = Mockito.mock(ReflectiveFutureChannel.class);
    Mockito.when(channel.getObject()).thenReturn(future);

    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(channel);

    assertThat(metadata).isNull();
  }

  @Test
  void extract_futureDone_returnsMetadata() {
    BlobInfo blobInfo = Mockito.mock(BlobInfo.class);
    Mockito.when(blobInfo.getSize()).thenReturn(500L);
    Mockito.when(blobInfo.getGeneration()).thenReturn(600L);
    CompletableFuture<BlobInfo> future = CompletableFuture.completedFuture(blobInfo);
    ReflectiveFutureChannel channel = Mockito.mock(ReflectiveFutureChannel.class);
    Mockito.when(channel.getObject()).thenReturn(future);

    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(channel);

    assertThat(metadata).isNotNull();
    assertThat(metadata.getSize()).isEqualTo(500L);
    assertThat(metadata.getGeneration()).isEqualTo(600L);
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored")
  void extract_futureExecutionException_returnsNull() throws Exception {
    Future<BlobInfo> mockFuture = Mockito.mock(Future.class);
    Mockito.when(mockFuture.isDone()).thenReturn(true);
    Mockito.when(mockFuture.get())
        .thenThrow(new ExecutionException(new RuntimeException("failed")));
    ReflectiveFutureChannel channel = Mockito.mock(ReflectiveFutureChannel.class);
    Mockito.when(channel.getObject()).thenReturn(mockFuture);

    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(channel);

    assertThat(metadata).isNull();
  }

  @Test
  void extract_fallbackGetter_returnsSize() throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    stream.write(new byte[150]);
    ReflectiveFallbackChannel channel = Mockito.mock(ReflectiveFallbackChannel.class);
    Mockito.when(channel.getResolvedObject()).thenReturn(stream);

    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(channel);

    assertThat(metadata).isNotNull();
    assertThat(metadata.getSize()).isEqualTo(150L);
    assertThat(metadata.getGeneration()).isEqualTo(-1L);
  }

  @Test
  void extract_targetIsMap_doesNotUseFallbackGetter_returnsNull() {
    Map<String, Object> mapTarget = new HashMap<>();
    mapTarget.put("size", 1000L);
    ReflectiveMapChannel channel = Mockito.mock(ReflectiveMapChannel.class);
    Mockito.when(channel.getResolvedObject()).thenReturn(mapTarget);

    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(channel);

    assertThat(metadata).isNull();
  }

  @Test
  void extract_noSizeProperty_returnsNull() {
    ReflectiveNoSizeChannel channel = Mockito.mock(ReflectiveNoSizeChannel.class);
    Mockito.when(channel.getResolvedObject()).thenReturn("dummy-string-no-size-getter");

    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(channel);

    assertThat(metadata).isNull();
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored")
  void extract_futureInterruptedException_returnsNull() throws Exception {
    Future<BlobInfo> mockFuture = Mockito.mock(Future.class);
    Mockito.when(mockFuture.isDone()).thenReturn(true);
    Mockito.when(mockFuture.get()).thenThrow(new InterruptedException("interrupted"));
    ReflectiveFutureChannel channel = Mockito.mock(ReflectiveFutureChannel.class);
    Mockito.when(channel.getObject()).thenReturn(mockFuture);

    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(channel);

    assertThat(metadata).isNull();
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
    Thread.interrupted();
  }

  @Test
  void extract_getterThrowsException_returnsNull() {
    ThrowingModel throwingModel = new ThrowingModel();
    ReflectiveErrorChannel channel = Mockito.mock(ReflectiveErrorChannel.class);
    Mockito.when(channel.getResolvedObject()).thenReturn(throwingModel);

    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(channel);

    assertThat(metadata).isNull();
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored")
  void extract_futureCancelledException_returnsNull() throws Exception {
    Future<BlobInfo> mockFuture = Mockito.mock(Future.class);
    Mockito.when(mockFuture.isDone()).thenReturn(true);
    Mockito.when(mockFuture.get()).thenThrow(new CancellationException("cancelled"));
    ReflectiveFutureChannel channel = Mockito.mock(ReflectiveFutureChannel.class);
    Mockito.when(channel.getObject()).thenReturn(mockFuture);

    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(channel);

    assertThat(metadata).isNull();
  }

  @Test
  void extract_targetIsCollection_doesNotUseFallbackGetter_returnsNull() {
    List<Object> collectionTarget = new ArrayList<>();
    collectionTarget.add("item");
    ReflectiveCollectionChannel channel = Mockito.mock(ReflectiveCollectionChannel.class);
    Mockito.when(channel.getResolvedObject()).thenReturn(collectionTarget);

    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(channel);

    assertThat(metadata).isNull();
  }

  @Test
  void extract_fallbackGetterNegative_returnsNull() {
    FallbackNegativeModel target = new FallbackNegativeModel();
    ReflectiveGenericChannel channel = Mockito.mock(ReflectiveGenericChannel.class);
    Mockito.when(channel.getResolvedObject()).thenReturn(target);

    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(channel);

    assertThat(metadata).isNull();
  }

  @Test
  void extract_negativePrimaryPositiveFallback_returnsMetadata() {
    NegativePrimaryPositiveFallbackModel target = new NegativePrimaryPositiveFallbackModel();
    ReflectiveGenericChannel channel = Mockito.mock(ReflectiveGenericChannel.class);
    Mockito.when(channel.getResolvedObject()).thenReturn(target);

    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(channel);

    assertThat(metadata).isNotNull();
    assertThat(metadata.getSize()).isEqualTo(100L);
    assertThat(metadata.getGeneration()).isEqualTo(-1L);
  }

  @Test
  void extract_bothPrimaryAndFallbackNegative_returnsNull() {
    BothNegativeModel target = new BothNegativeModel();
    ReflectiveGenericChannel channel = Mockito.mock(ReflectiveGenericChannel.class);
    Mockito.when(channel.getResolvedObject()).thenReturn(target);

    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(channel);

    assertThat(metadata).isNull();
  }

  @Test
  void extract_fieldValueNull_fallsThroughToNextField() {
    BlobInfo blobInfo = Mockito.mock(BlobInfo.class);
    Mockito.when(blobInfo.getSize()).thenReturn(700L);
    Mockito.when(blobInfo.getGeneration()).thenReturn(800L);
    ReadChannel channel = new FakeChannelWithNullAndNonNullFields(blobInfo);

    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(channel);

    assertThat(metadata).isNotNull();
    assertThat(metadata.getSize()).isEqualTo(700L);
    assertThat(metadata.getGeneration()).isEqualTo(800L);
  }

  @Test
  void extract_methodValueNull_fallsThroughToNextMethod() {
    BlobInfo blobInfo = Mockito.mock(BlobInfo.class);
    Mockito.when(blobInfo.getSize()).thenReturn(900L);
    Mockito.when(blobInfo.getGeneration()).thenReturn(1000L);
    ReflectiveNullAndNonNullMethodsChannel channel =
        Mockito.mock(ReflectiveNullAndNonNullMethodsChannel.class);
    Mockito.when(channel.getObject()).thenReturn(null);
    Mockito.when(channel.getBlobInfo()).thenReturn(blobInfo);

    GcsReadChannelMetadataExtractor.ExtractedMetadata metadata =
        GcsReadChannelMetadataExtractor.extract(channel);

    assertThat(metadata).isNotNull();
    assertThat(metadata.getSize()).isEqualTo(900L);
    assertThat(metadata.getGeneration()).isEqualTo(1000L);
  }

  static class ThrowingModel {
    public long getSize() {
      throw new RuntimeException("size retrieval failed");
    }
  }

  static class FallbackNegativeModel {
    public long size() {
      return -5L;
    }
  }

  static class NegativePrimaryPositiveFallbackModel {
    public long getSize() {
      return -10L;
    }

    public long size() {
      return 100L;
    }
  }

  static class BothNegativeModel {
    public long getSize() {
      return -10L;
    }

    public long size() {
      return -5L;
    }
  }

  static class FakeChannelWithNullAndNonNullFields implements ReadChannel {
    private final Object storageObject = null;
    private final BlobInfo result;

    FakeChannelWithNullAndNonNullFields(BlobInfo result) {
      this.result = result;
    }

    @Override
    public void close() {}

    @Override
    public boolean isOpen() {
      return true;
    }

    @Override
    public int read(ByteBuffer dst) {
      return -1;
    }

    @Override
    public void seek(long position) {}

    @Override
    public void setChunkSize(int chunkSize) {}

    @Override
    public RestorableState<ReadChannel> capture() {
      return null;
    }
  }

  // Helper interfaces for Mockito stubbing of reflective getters
  interface ReflectiveBlobInfoChannel extends ReadChannel {
    BlobInfo getBlobInfo();
  }

  interface ReflectiveFutureChannel extends ReadChannel {
    Future<BlobInfo> getObject();
  }

  interface ReflectiveFallbackChannel extends ReadChannel {
    ByteArrayOutputStream getResolvedObject();
  }

  interface ReflectiveMapChannel extends ReadChannel {
    Map<String, Object> getResolvedObject();
  }

  interface ReflectiveCollectionChannel extends ReadChannel {
    List<Object> getResolvedObject();
  }

  interface ReflectiveNoSizeChannel extends ReadChannel {
    String getResolvedObject();
  }

  interface ReflectiveErrorChannel extends ReadChannel {
    ThrowingModel getResolvedObject();
  }

  interface ReflectiveGenericChannel extends ReadChannel {
    Object getResolvedObject();
  }

  interface ReflectiveNullAndNonNullMethodsChannel extends ReadChannel {
    Object getObject();

    BlobInfo getBlobInfo();
  }
}
