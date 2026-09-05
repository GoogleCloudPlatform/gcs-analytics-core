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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.google.cloud.NoCredentials;
import com.google.cloud.gcs.analyticscore.common.telemetry.CustomTelemetryOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.LoggingTelemetryOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.LoggingTelemetryReporter;
import com.google.cloud.gcs.analyticscore.common.telemetry.OpenTelemetryOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.OpenTelemetryReporter;
import com.google.cloud.gcs.analyticscore.common.telemetry.Operation;
import com.google.cloud.gcs.analyticscore.common.telemetry.OperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.gcs.analyticscore.common.telemetry.TelemetryOptions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
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

  @AfterEach
  void tearDown() {
    if (gcsFileSystem != null) {
      gcsFileSystem.close();
    }
  }

  @Test
  void constructor_withCredentials_createsClientWithProvidedCredentials() {
    try (GcsFileSystemImpl gcsFileSystem =
        new GcsFileSystemImpl(NoCredentials.getInstance(), TEST_GCS_FILESYSTEM_OPTIONS)) {
      GcsClientImpl gcsClientImpl = (GcsClientImpl) gcsFileSystem.getGcsClient();

      assertThat(gcsClientImpl.storage.getOptions().getCredentials())
          .isEqualTo(NoCredentials.getInstance());
      assertThat(gcsFileSystem.getTelemetry()).isNotNull();
      assertThat(gcsFileSystem.getCacheManager()).isNotNull();
      assertThat(gcsFileSystem.getFlatStrategy()).isNotNull();
      assertThat(gcsFileSystem.getHnsStrategy()).isNotNull();
    }
  }

  @Test
  void constructor_withFileSystemOptions_createsClientWithDefaultCredentials() {
    GcsClientOptions clientOptions =
        GcsClientOptions.builder().setProjectId("test-project-default").build();
    GcsFileSystemOptions fileSystemOptions =
        GcsFileSystemOptions.builder().setGcsClientOptions(clientOptions).build();

    try (GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(fileSystemOptions)) {
      GcsClientImpl gcsClient = (GcsClientImpl) gcsFileSystem.getGcsClient();

      assertThat(gcsFileSystem.getFileSystemOptions()).isSameInstanceAs(fileSystemOptions);
      assertThat(gcsClient).isNotNull();
      assertThat(gcsClient.storage.getOptions().getProjectId()).isEqualTo("test-project-default");
      assertThat(gcsFileSystem.getTelemetry()).isNotNull();
      assertThat(gcsFileSystem.getCacheManager()).isNotNull();
      assertThat(gcsFileSystem.getFlatStrategy()).isNotNull();
      assertThat(gcsFileSystem.getHnsStrategy()).isNotNull();
    }
  }

  @Test
  void constructor_withValidOptions_passesMemorizedExecutorServiceAndTelemetryToGcsClient() {
    final AtomicReference<Supplier<ExecutorService>> capturedSupplier = new AtomicReference<>();
    final AtomicReference<Telemetry> capturedTelemetry = new AtomicReference<>();
    try (MockedConstruction<GcsClientImpl> mockGcsClientConstruction =
        Mockito.mockConstruction(
            GcsClientImpl.class,
            (mock, context) -> {
              @SuppressWarnings("unchecked") // Safe cast due to constructor signature
              Supplier<ExecutorService> supplier =
                  (Supplier<ExecutorService>) context.arguments().get(1);
              capturedSupplier.set(supplier);

              Telemetry telemetry = (Telemetry) context.arguments().get(2);
              capturedTelemetry.set(telemetry);
            })) {

      try (GcsFileSystemImpl fs = new GcsFileSystemImpl(TEST_GCS_FILESYSTEM_OPTIONS)) {
        ExecutorService executorService1 = capturedSupplier.get().get();
        ExecutorService executorService2 = capturedSupplier.get().get();

        assertThat(mockGcsClientConstruction.constructed()).hasSize(1);
        assertThat(capturedSupplier.get()).isNotNull();
        assertThat(capturedSupplier.get().get()).isNotNull();
        assertThat(executorService1).isEqualTo(executorService2);
        assertThat(capturedTelemetry.get()).isNotNull();
        assertThat(capturedTelemetry.get()).isSameInstanceAs(fs.getTelemetry());
        assertThat(fs.getFlatStrategy()).isNotNull();
        assertThat(fs.getHnsStrategy()).isNotNull();
      }
    }
  }

  @Test
  void open_withFileInfo_callsGcsClientOpen() throws IOException {
    URI testUri = URI.create("gs://test-bucket/test-object");
    GcsItemInfo mockItemInfo = mock(GcsItemInfo.class);
    GcsFileInfo fileInfo =
        GcsFileInfo.builder()
            .setUri(testUri)
            .setItemInfo(mockItemInfo)
            .setAttributes(Collections.emptyMap())
            .build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId("test-project").build();
    VectoredSeekableByteChannel mockChannel = mock(VectoredSeekableByteChannel.class);
    when(mockClient.openReadChannel(eq(mockItemInfo), eq(readOptions))).thenReturn(mockChannel);

    VectoredSeekableByteChannel resultChannel = gcsFileSystem.open(fileInfo, readOptions);

    verify(mockClient).openReadChannel(mockItemInfo, readOptions);
    assertThat(resultChannel).isSameInstanceAs(mockChannel);
  }

  @Test
  void open_withNullFileInfo_throwsNullPointerException() {
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId("test-project").build();

    NullPointerException e =
        assertThrows(
            NullPointerException.class, () -> gcsFileSystem.open((GcsFileInfo) null, readOptions));

    assertThat(e).hasMessageThat().contains("fileInfo should not be null");
  }

  @Test
  void open_withNonObjectFileInfo_throwsIllegalArgumentException() throws URISyntaxException {
    URI bucketUri = new URI("gs://" + TEST_BUCKET);
    GcsItemInfo mockItemInfo = mock(GcsItemInfo.class);
    GcsFileInfo fileInfo =
        GcsFileInfo.builder()
            .setUri(bucketUri)
            .setItemInfo(mockItemInfo)
            .setAttributes(Collections.emptyMap())
            .build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId("test-project").build();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> gcsFileSystem.open(fileInfo, readOptions));

    assertThat(e).hasMessageThat().startsWith("Expected GCS object to be provided");
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

    assertThat(fileInfo.getUri()).isEqualTo(gcsPath);
    assertThat(fileInfo.getItemInfo()).isEqualTo(mockItemInfo);
    assertThat(fileInfo.getAttributes()).isEmpty();
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
  void open_withItemId_callsGcsClientOpen() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId("test-project").build();
    VectoredSeekableByteChannel mockChannel = mock(VectoredSeekableByteChannel.class);
    when(mockClient.openReadChannel(eq(itemId), eq(readOptions))).thenReturn(mockChannel);

    VectoredSeekableByteChannel resultChannel = gcsFileSystem.open(itemId, readOptions);

    verify(mockClient).openReadChannel(itemId, readOptions);
    assertThat(resultChannel).isSameInstanceAs(mockChannel);
  }

  @Test
  void open_withNullItemId_throwsNullPointerException() {
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId("test-project").build();

    NullPointerException e =
        assertThrows(
            NullPointerException.class, () -> gcsFileSystem.open((GcsItemId) null, readOptions));

    assertThat(e).hasMessageThat().contains("gcsItemId should not be null");
  }

  @Test
  void open_withNonObjectItemId_throwsIllegalArgumentException() {
    GcsItemId itemId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId("test-project").build();

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> gcsFileSystem.open(itemId, readOptions));

    assertThat(e).hasMessageThat().startsWith("Expected GCS object to be provided");
  }

  @Test
  void getFileInfo_withValidItemId_returnsGcsFileInfo() throws IOException {
    String content = "file info test";
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();
    GcsItemInfo mockItemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize((long) content.length())
            .setContentGeneration(12345L) // A sample generation ID
            .build();
    when(mockClient.getGcsItemInfo(eq(itemId))).thenReturn(mockItemInfo);

    GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(itemId);

    assertThat(fileInfo.getUri().toString()).isEqualTo("gs://" + TEST_BUCKET + "/" + TEST_OBJECT);
    assertThat(fileInfo.getItemInfo()).isEqualTo(mockItemInfo);
    assertThat(fileInfo.getAttributes()).isEmpty();
  }

  @Test
  void getFileInfo_withNonExistentItemId_shouldThrowException() throws IOException {
    GcsItemId nonExistentItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("non-existent-object").build();
    when(mockClient.getGcsItemInfo(eq(nonExistentItemId)))
        .thenThrow(new IOException("Object not found:" + nonExistentItemId));

    IOException e =
        assertThrows(IOException.class, () -> gcsFileSystem.getFileInfo(nonExistentItemId));

    assertThat(e).hasMessageThat().contains("Object not found:" + nonExistentItemId);
  }

  @Test
  void initializeReadExecutionServiceSupplier_shouldReturnMemoizedExecutorService() {
    GcsFileSystemImpl fileSystemImpl = (GcsFileSystemImpl) gcsFileSystem;

    Supplier<ExecutorService> readExecutorServiceSupplier =
        fileSystemImpl.initializeReadExecutionServiceSupplier();

    assertThat(readExecutorServiceSupplier).isNotNull();
    assertThat(readExecutorServiceSupplier.get()).isNotNull();
    assertThat(readExecutorServiceSupplier.get()).isInstanceOf(ThreadPoolExecutor.class);
    assertThat(((ThreadPoolExecutor) readExecutorServiceSupplier.get()).getCorePoolSize())
        .isEqualTo(16);
    assertThat(readExecutorServiceSupplier.get())
        .isSameInstanceAs(readExecutorServiceSupplier.get());
  }

  @Test
  void
      initializeStatusExecutionServiceSupplier_whenMetadataLookupParallelEnabled_shouldReturnCachedExecutorService() {
    GcsFileSystemImpl fileSystemImpl = (GcsFileSystemImpl) gcsFileSystem;

    Supplier<ExecutorService> statusExecutorServiceSupplier =
        fileSystemImpl.initializeStatusExecutionServiceSupplier();

    assertThat(statusExecutorServiceSupplier).isNotNull();
    assertThat(statusExecutorServiceSupplier.get()).isNotNull();
    assertThat(((ThreadPoolExecutor) statusExecutorServiceSupplier.get()).getQueue())
        .isInstanceOf(LinkedBlockingQueue.class);
    assertThat(statusExecutorServiceSupplier.get())
        .isSameInstanceAs(statusExecutorServiceSupplier.get());
  }

  @Test
  void
      initializeStatusExecutionServiceSupplier_whenMetadataLookupParallelDisabled_shouldReturnLazyExecutorService()
          throws Exception {
    GcsFileSystemOptions options =
        TEST_GCS_FILESYSTEM_OPTIONS.toBuilder().setMetadataLookupParallelEnabled(false).build();
    // Use try-with-resources to ensure GcsFileSystemImpl is closed with its internal executors.
    try (GcsFileSystemImpl fileSystemImpl = new GcsFileSystemImpl(mock(GcsClient.class), options)) {

      Supplier<ExecutorService> statusExecutorServiceSupplier =
          fileSystemImpl.initializeStatusExecutionServiceSupplier();

      assertThat(statusExecutorServiceSupplier).isNotNull();
      assertThat(statusExecutorServiceSupplier.get()).isNotNull();
      assertThat(statusExecutorServiceSupplier.get()).isInstanceOf(LazyExecutorService.class);
    }
  }

  @Test
  void close_whenTerminationSucceeds_shutsDownGracefully() throws InterruptedException {
    ExecutorService mockReadExecutorService = mock(ExecutorService.class);
    ExecutorService mockStatusExecutorService = mock(ExecutorService.class);
    when(mockReadExecutorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);
    when(mockStatusExecutorService.awaitTermination(anyLong(), any(TimeUnit.class)))
        .thenReturn(true);
    GcsFileSystemImpl fileSystemWithMockExecutor =
        new GcsFileSystemImpl(mockClient, TEST_GCS_FILESYSTEM_OPTIONS) {
          @Override
          Supplier<ExecutorService> initializeReadExecutionServiceSupplier() {
            return () -> mockReadExecutorService;
          }

          @Override
          Supplier<ExecutorService> initializeStatusExecutionServiceSupplier() {
            return () -> mockStatusExecutorService;
          }
        };

    fileSystemWithMockExecutor.close();
    InOrder inOrder = inOrder(mockReadExecutorService, mockStatusExecutorService, mockClient);

    inOrder.verify(mockReadExecutorService).shutdown();
    inOrder.verify(mockStatusExecutorService).shutdown();
    inOrder.verify(mockReadExecutorService).awaitTermination(anyLong(), any(TimeUnit.class));
    inOrder.verify(mockStatusExecutorService).awaitTermination(anyLong(), any(TimeUnit.class));
    inOrder.verify(mockClient).close();
    verify(mockReadExecutorService, never()).shutdownNow();
    verify(mockStatusExecutorService, never()).shutdownNow();
  }

  @Test
  void close_whenTerminationTimesOut_shutsDownNow() throws InterruptedException {
    ExecutorService mockReadExecutorService = mock(ExecutorService.class);
    ExecutorService mockStatusExecutorService = mock(ExecutorService.class);
    when(mockReadExecutorService.awaitTermination(anyLong(), any(TimeUnit.class)))
        .thenReturn(false);
    when(mockStatusExecutorService.awaitTermination(anyLong(), any(TimeUnit.class)))
        .thenReturn(false);
    GcsFileSystemImpl fileSystemWithMockExecutor =
        new GcsFileSystemImpl(mockClient, TEST_GCS_FILESYSTEM_OPTIONS) {
          @Override
          Supplier<ExecutorService> initializeReadExecutionServiceSupplier() {
            return () -> mockReadExecutorService;
          }

          @Override
          Supplier<ExecutorService> initializeStatusExecutionServiceSupplier() {
            return () -> mockStatusExecutorService;
          }
        };

    fileSystemWithMockExecutor.close();
    InOrder inOrder = inOrder(mockReadExecutorService, mockStatusExecutorService, mockClient);

    inOrder.verify(mockReadExecutorService).shutdown();
    inOrder.verify(mockStatusExecutorService).shutdown();
    inOrder.verify(mockReadExecutorService).awaitTermination(anyLong(), any(TimeUnit.class));
    inOrder.verify(mockReadExecutorService).shutdownNow();
    inOrder.verify(mockStatusExecutorService).awaitTermination(anyLong(), any(TimeUnit.class));
    inOrder.verify(mockStatusExecutorService).shutdownNow();
    inOrder.verify(mockClient).close();
  }

  @Test
  void close_whenInterrupted_reInterruptsThreadAndShutsDownNow() throws InterruptedException {
    ExecutorService mockReadExecutorService = mock(ExecutorService.class);
    ExecutorService mockStatusExecutorService = mock(ExecutorService.class);
    when(mockReadExecutorService.awaitTermination(anyLong(), any(TimeUnit.class)))
        .thenThrow(new InterruptedException());
    GcsFileSystemImpl fileSystemWithMockExecutor =
        new GcsFileSystemImpl(mockClient, TEST_GCS_FILESYSTEM_OPTIONS) {
          @Override
          Supplier<ExecutorService> initializeReadExecutionServiceSupplier() {
            return () -> mockReadExecutorService;
          }

          @Override
          Supplier<ExecutorService> initializeStatusExecutionServiceSupplier() {
            return () -> mockStatusExecutorService;
          }
        };

    fileSystemWithMockExecutor.close();
    InOrder inOrder = inOrder(mockReadExecutorService, mockStatusExecutorService, mockClient);

    inOrder.verify(mockReadExecutorService).shutdown();
    inOrder.verify(mockStatusExecutorService).shutdown();
    inOrder.verify(mockReadExecutorService).awaitTermination(anyLong(), any(TimeUnit.class));
    inOrder.verify(mockReadExecutorService).shutdownNow();
    inOrder.verify(mockStatusExecutorService).shutdownNow();
    inOrder.verify(mockClient).close();
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
    Thread.interrupted(); // Clear interrupted status to not affect other tests
  }

  @Test
  void getOptions_shouldReturnConfiguredOptions() {
    assertEquals(TEST_GCS_FILESYSTEM_OPTIONS, gcsFileSystem.getFileSystemOptions());
  }

  @Test
  void getGcsClient_shouldReturnConfiguredClient() {
    assertEquals(mockClient, gcsFileSystem.getGcsClient());
  }

  @Test
  void getCacheManager_default_returnsNonNullInstance() {
    assertThat(gcsFileSystem.getCacheManager()).isNotNull();
  }

  @Test
  void initializeTelemetry_registerListenersToTelemetry() {
    OperationListener mockListener = mock(OperationListener.class);
    CustomTelemetryOptions customTelemetryOptions =
        CustomTelemetryOptions.builder()
            .setOperationListeners(ImmutableList.of(mockListener))
            .build();
    TelemetryOptions telemetryOptions =
        TelemetryOptions.builder().setCustomTelemetryOptions(customTelemetryOptions).build();
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(TEST_GCS_CLIENT_OPTIONS)
            .setAnalyticsCoreTelemetryOptions(telemetryOptions)
            .build();

    try (GcsFileSystemImpl unused = new GcsFileSystemImpl(options)) {
      verify(mockListener, times(1)).onOperationStart(any(Operation.class));
    }
  }

  @Test
  void initializeTelemetry_loggingOptionsEnabled_registersLoggingTelemetryReporter() {
    LoggingTelemetryOptions loggingOptions =
        LoggingTelemetryOptions.builder().setEnabled(true).build();
    TelemetryOptions telemetryOptions =
        TelemetryOptions.builder().setLoggingTelemetryOptions(loggingOptions).build();
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(TEST_GCS_CLIENT_OPTIONS)
            .setAnalyticsCoreTelemetryOptions(telemetryOptions)
            .build();

    try (GcsFileSystemImpl fileSystem = new GcsFileSystemImpl(options)) {
      List<OperationListener> registeredListeners =
          getRegisteredTelemetryListeners(fileSystem.getTelemetry());
      OperationListener reporter = registeredListeners.get(0);

      assertThat(reporter).isInstanceOf(LoggingTelemetryReporter.class);
      assertThat(getRegisteredTelemetryListeners(fileSystem.getTelemetry())).contains(reporter);
    }
  }

  @Test
  void initializeTelemetry_loggingOptionsDisabled_doesNotRegisterReporter() {
    LoggingTelemetryOptions loggingOptions =
        LoggingTelemetryOptions.builder().setEnabled(false).build();
    TelemetryOptions telemetryOptions =
        TelemetryOptions.builder().setLoggingTelemetryOptions(loggingOptions).build();
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(TEST_GCS_CLIENT_OPTIONS)
            .setAnalyticsCoreTelemetryOptions(telemetryOptions)
            .build();

    try (GcsFileSystemImpl fileSystem = new GcsFileSystemImpl(options)) {
      assertThat(getRegisteredTelemetryListeners(fileSystem.getTelemetry())).isEmpty();
    }
  }

  @Test
  void initializeTelemetry_openTelemetryOptionsEnabled_registersOpenTelemetryReporter() {
    OpenTelemetryOptions openTelemetryOptions =
        OpenTelemetryOptions.builder().setEnabled(true).build();
    TelemetryOptions telemetryOptions =
        TelemetryOptions.builder().setOpenTelemetryOptions(openTelemetryOptions).build();
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(TEST_GCS_CLIENT_OPTIONS)
            .setAnalyticsCoreTelemetryOptions(telemetryOptions)
            .build();

    try (GcsFileSystemImpl fileSystem = new GcsFileSystemImpl(options)) {
      List<OperationListener> registeredListeners =
          getRegisteredTelemetryListeners(fileSystem.getTelemetry());
      OperationListener reporter = registeredListeners.get(0);

      assertThat(reporter).isInstanceOf(OpenTelemetryReporter.class);
    }
  }

  @Test
  void initializeTelemetry_openTelemetryOptionsDisabled_doesNotRegisterReporter() {
    OpenTelemetryOptions openTelemetryOptions =
        OpenTelemetryOptions.builder().setEnabled(false).build();
    TelemetryOptions telemetryOptions =
        TelemetryOptions.builder().setOpenTelemetryOptions(openTelemetryOptions).build();
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(TEST_GCS_CLIENT_OPTIONS)
            .setAnalyticsCoreTelemetryOptions(telemetryOptions)
            .build();

    try (GcsFileSystemImpl fileSystem = new GcsFileSystemImpl(options)) {
      assertThat(getRegisteredTelemetryListeners(fileSystem.getTelemetry())).isEmpty();
    }
  }

  @Test
  void close_openTelemetryEnabled_removesRegisteredReporters() {
    OpenTelemetryOptions openTelemetryOptions =
        OpenTelemetryOptions.builder().setEnabled(true).build();
    TelemetryOptions telemetryOptions =
        TelemetryOptions.builder().setOpenTelemetryOptions(openTelemetryOptions).build();
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(TEST_GCS_CLIENT_OPTIONS)
            .setAnalyticsCoreTelemetryOptions(telemetryOptions)
            .build();

    GcsFileSystemImpl fileSystem = new GcsFileSystemImpl(options);
    int listnerCountBeforeClose = getRegisteredTelemetryListeners(fileSystem.getTelemetry()).size();

    fileSystem.close();

    assertThat(listnerCountBeforeClose).isEqualTo(1);
    assertThat(getRegisteredTelemetryListeners(fileSystem.getTelemetry())).isEmpty();
  }

  @Test
  void close_loggingTelemetryEnabled_removesRegisteredReporters() {
    LoggingTelemetryOptions loggingOptions =
        LoggingTelemetryOptions.builder().setEnabled(true).build();
    TelemetryOptions telemetryOptions =
        TelemetryOptions.builder().setLoggingTelemetryOptions(loggingOptions).build();
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(TEST_GCS_CLIENT_OPTIONS)
            .setAnalyticsCoreTelemetryOptions(telemetryOptions)
            .build();

    GcsFileSystemImpl fileSystem = new GcsFileSystemImpl(options);
    List<OperationListener> registeredListeners =
        getRegisteredTelemetryListeners(fileSystem.getTelemetry());
    assertThat(registeredListeners).hasSize(1);
    OperationListener reporter = registeredListeners.get(0);

    assertThat(getRegisteredTelemetryListeners(fileSystem.getTelemetry())).contains(reporter);

    fileSystem.close();

    assertThat(getRegisteredTelemetryListeners(fileSystem.getTelemetry())).isEmpty();
    assertThat(getRegisteredTelemetryListeners(fileSystem.getTelemetry())).doesNotContain(reporter);
  }

  @Test
  void create_withValidArguments_delegatesToClient() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
    WritableByteChannel mockChannel = mock(WritableByteChannel.class);
    when(mockClient.createWriteChannel(eq(itemId), eq(writeOptions))).thenReturn(mockChannel);

    WritableByteChannel resultChannel = gcsFileSystem.create(itemId, writeOptions);

    verify(mockClient).createWriteChannel(itemId, writeOptions);
    assertThat(resultChannel).isSameInstanceAs(mockChannel);
  }

  @Test
  void create_nullItemId_throwsNullPointerException() {
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();

    NullPointerException e =
        assertThrows(
            NullPointerException.class, () -> gcsFileSystem.create((GcsItemId) null, writeOptions));

    assertThat(e).hasMessageThat().contains("itemId should not be null");
  }

  @Test
  void create_nullWriteOptions_delegatesToClientWithNullOptions() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();
    WritableByteChannel mockChannel = mock(WritableByteChannel.class);
    when(mockClient.createWriteChannel(eq(itemId), eq(null))).thenReturn(mockChannel);

    WritableByteChannel resultChannel = gcsFileSystem.create(itemId, null);

    verify(mockClient).createWriteChannel(itemId, null);
    assertThat(resultChannel).isSameInstanceAs(mockChannel);
  }

  @Test
  void resolveStrategy_hnsFlagEnabledAndHnsBucket_returnsHnsStrategy() throws IOException {
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(TEST_GCS_CLIENT_OPTIONS)
            .setHnsApiEnabled(true)
            .build();
    when(mockClient.isHnsBucket(TEST_BUCKET)).thenReturn(true);

    try (GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(mockClient, options)) {
      NamespaceStrategy strategy = gcsFileSystem.resolveStrategy(TEST_BUCKET);

      assertThat(strategy).isInstanceOf(HierarchicalNamespaceStrategyImpl.class);
    }
  }

  @Test
  void resolveStrategy_hnsFlagDisabled_returnsFlatStrategy() throws IOException {
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(TEST_GCS_CLIENT_OPTIONS)
            .setHnsApiEnabled(false)
            .build();

    try (GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(mockClient, options)) {
      NamespaceStrategy strategy = gcsFileSystem.resolveStrategy(TEST_BUCKET);

      assertThat(strategy).isInstanceOf(FlatNamespaceStrategyImpl.class);
      verify(mockClient, never()).isHnsBucket(anyString());
    }
  }

  @Test
  void resolveStrategy_hnsFlagEnabledAndFlatBucket_returnsFlatStrategy() throws IOException {
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(TEST_GCS_CLIENT_OPTIONS)
            .setHnsApiEnabled(true)
            .build();
    when(mockClient.isHnsBucket(TEST_BUCKET)).thenReturn(false);

    try (GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(mockClient, options)) {
      NamespaceStrategy strategy = gcsFileSystem.resolveStrategy(TEST_BUCKET);

      assertThat(strategy).isInstanceOf(FlatNamespaceStrategyImpl.class);
    }
  }

  @Test
  void resolveStrategy_isHnsBucketThrowsIoException_throwsIOException() throws IOException {
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(TEST_GCS_CLIENT_OPTIONS)
            .setHnsApiEnabled(true)
            .build();
    when(mockClient.isHnsBucket(TEST_BUCKET)).thenThrow(new IOException("test exception"));

    try (GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(mockClient, options)) {
      IOException exception =
          assertThrows(IOException.class, () -> gcsFileSystem.resolveStrategy(TEST_BUCKET));

      assertThat(exception).hasMessageThat().isEqualTo("test exception");
    }
  }

  @SuppressWarnings("unchecked")
  private List<OperationListener> getRegisteredTelemetryListeners(Telemetry telemetry) {
    try {
      java.lang.reflect.Field field = Telemetry.class.getDeclaredField("listeners");
      field.setAccessible(true);
      return (List<OperationListener>) field.get(telemetry);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get Telemetry listeners", e);
    }
  }
}
