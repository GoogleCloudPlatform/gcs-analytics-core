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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.api.gax.paging.Page;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.gcs.analyticscore.client.auth.GcsAuthOptions;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCache;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.http.HttpTransportOptions.DefaultHttpTransportFactory;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUpload.AppendableUploadWriteableByteChannel;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.BlobWriteSessionConfig;
import com.google.cloud.storage.BlobWriteSessionConfigs;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.BucketInfo.CustomPlacementConfig;
import com.google.cloud.storage.BucketInfo.HierarchicalNamespace;
import com.google.cloud.storage.BucketInfo.IamConfiguration;
import com.google.cloud.storage.BucketInfo.LifecycleRule;
import com.google.cloud.storage.BucketInfo.LifecycleRule.LifecycleAction;
import com.google.cloud.storage.BucketInfo.LifecycleRule.LifecycleCondition;
import com.google.cloud.storage.BufferToDiskThenUpload;
import com.google.cloud.storage.DefaultBlobWriteSessionConfig;
import com.google.cloud.storage.GrpcStorageOptions;
import com.google.cloud.storage.HttpStorageOptions;
import com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BucketGetOption;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.Timestamp;
import com.google.storage.control.v2.CreateFolderRequest;
import com.google.storage.control.v2.Folder;
import com.google.storage.control.v2.GetFolderRequest;
import com.google.storage.control.v2.StorageControlClient;
import com.google.storage.control.v2.StorageControlSettings;
import com.sun.net.httpserver.HttpServer;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

class GcsClientImplTest {

  private static final String TEST_PROJECT = "test-project";
  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_OBJECT = "test-object";
  private static final String TEST_BUCKET_NAME = "test-bucket-name";
  private static final String TEST_BUCKET_ID = "test-bucket-id";
  private static final String TEST_OBJECT_ID = "test-object-id";
  private static final String TEST_WRITE_OBJECT = "test-write-object";
  private static final String TEST_NON_EXISTENT_OBJECT = "non-existent-object";
  private static final String TEST_HNS_BUCKET = "hns-bucket";
  private static final String TEST_FLAT_BUCKET = "flat-bucket";
  private static final String TEST_NON_EXISTENT_BUCKET = "non-existent-bucket";
  private static final String TEST_FORBIDDEN_BUCKET = "forbidden-bucket";
  private static final String TEST_ERROR_BUCKET = "error-bucket";
  private static final String TEST_LOCATION = "US";
  private static final String TEST_FOLDER_NAME = "test-folder";
  private static final String TEST_DIR = "dir/";
  private static final String TEST_OBJECT_NAME = "test-object-name";
  private static final String BLOB_WRITE_SESSION_CONFIG_FIELD = "blobWriteSessionConfig";
  private static final int MB = 1024 * 1024;
  private static final String RAPID_STORAGE_CLASS = "RAPID";
  private static final String TEST_ENCRYPTION_KEY = "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI=";
  private static final String TEST_DIRECTORY_CONTENT_TYPE = "application/x-directory";
  private static final String TEST_CONTENT_ENCODING = "identity";
  private static final long TEST_CONTENT_GENERATION = 123L;

  private static final GcsClientOptions TEST_GCS_CLIENT_OPTIONS =
      GcsClientOptions.builder().setProjectId(TEST_PROJECT).build();
  private static final GcsItemId TEST_ITEM_ID =
      GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();
  private static final GcsItemId TEST_DIR_ITEM_ID =
      GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_DIR).build();
  private static final BlobInfo TEST_BLOB_INFO =
      BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT))
          .setContentType("application/octet-stream")
          .build();
  private static final GcsWriteOptions DEFAULT_WRITE_OPTIONS = GcsWriteOptions.builder().build();

  private final Storage storage = LocalStorageHelper.getOptions().getService();
  private final Supplier<ExecutorService> executorServiceSupplier =
      Suppliers.memoize(() -> Executors.newFixedThreadPool(30));
  private final Telemetry telemetry = new Telemetry(ImmutableList.of());

  private GcsClient gcsClient;

  @BeforeEach
  void setUp() throws IOException {
    gcsClient = createClientWithMockStorage(storage);
  }

  @Test
  void getGcsItemInfo_itemIdPointsToDirectory_throwsUnsupportedOperationException() {
    GcsItemId directoryItemId = GcsItemId.builder().setBucketName(TEST_BUCKET_ID).build();

    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class, () -> gcsClient.getGcsItemInfo(directoryItemId));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(String.format("Expected gcs object but got %s", directoryItemId));
  }

  @Test
  void getGcsItemInfo_gcsObjectExists_returnsItemInfo() throws IOException {
    String objectData = "hello world";
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_ID).setObjectName(TEST_OBJECT_ID).build();
    StorageTestUtils.createBlobInStorage(
        storage, BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);

    GcsItemInfo itemInfo = gcsClient.getGcsItemInfo(itemId);

    GcsItemId expectedItemId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET_ID)
            .setObjectName(TEST_OBJECT_ID)
            .setContentGeneration(itemInfo.getContentGeneration().get())
            .build();
    assertThat(itemInfo.getItemId()).isEqualTo(expectedItemId);
    assertThat(itemInfo.getSize()).isEqualTo(objectData.length());
    assertThat(itemInfo.getContentGeneration().get()).isEqualTo(0L);
    assertThat(itemInfo.getVerificationAttributes().get().getMd5hash()).isNull();
    assertThat(itemInfo.getVerificationAttributes().get().getCrc32c()).isNotNull();
  }

  @Test
  void getGcsItemInfo_placeholderDirectory_returnsPlaceholderDirectoryItemInfo()
      throws IOException {
    GcsItemId directoryId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/").build();
    Blob mockBlob = mock(Blob.class);
    when(mockBlob.getBucket()).thenReturn(TEST_BUCKET);
    when(mockBlob.getName()).thenReturn("dir/");
    when(mockBlob.getGeneration()).thenReturn(1L);
    when(mockBlob.isDirectory()).thenReturn(true);
    Storage mockStorage = mock(Storage.class);
    when(mockStorage.get(eq(BlobId.of(TEST_BUCKET, "dir/")), any(Storage.BlobGetOption[].class)))
        .thenReturn(mockBlob);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    GcsItemInfo itemInfo = clientWithMock.getGcsItemInfo(directoryId);

    assertThat(itemInfo.getItemId().getBucketName()).isEqualTo(TEST_BUCKET);
    assertThat(itemInfo.getItemId().getObjectName()).hasValue("dir/");
    assertThat(itemInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY);
    assertThat(itemInfo.getVerificationAttributes()).isEmpty();
  }

  @ParameterizedTest
  @CsvSource({"md5, crc", ", crc", "md5, "})
  void getGcsItemInfo_withChecksums_returnsVerificationAttributes(String md5, String crc32c)
      throws IOException {
    Blob mockBlob = mock(Blob.class);
    when(mockBlob.getBucket()).thenReturn(TEST_BUCKET);
    when(mockBlob.getName()).thenReturn(TEST_OBJECT);
    if (md5 != null) {
      when(mockBlob.getMd5()).thenReturn(BaseEncoding.base64().encode(md5.getBytes(UTF_8)));
    }
    if (crc32c != null) {
      when(mockBlob.getCrc32c()).thenReturn(BaseEncoding.base64().encode(crc32c.getBytes(UTF_8)));
    }
    Storage mockStorage = mock(Storage.class);
    when(mockStorage.get(any(BlobId.class), any(Storage.BlobGetOption[].class)))
        .thenReturn(mockBlob);

    GcsItemInfo itemInfo = createClientWithMockStorage(mockStorage).getGcsItemInfo(TEST_ITEM_ID);

    byte[] expectedMd5 = md5 != null ? md5.getBytes(UTF_8) : null;
    byte[] expectedCrc32c = crc32c != null ? crc32c.getBytes(UTF_8) : null;
    assertThat(itemInfo.getVerificationAttributes())
        .hasValue(VerificationAttributes.create(expectedMd5, expectedCrc32c));
  }

  @Test
  void getGcsItemInfo_nullSize_defaultsToZero() throws IOException {
    Blob mockBlob = mock(Blob.class);
    when(mockBlob.getBucket()).thenReturn(TEST_BUCKET);
    when(mockBlob.getName()).thenReturn(TEST_OBJECT);
    Storage mockStorage = mock(Storage.class);
    when(mockStorage.get(any(BlobId.class), any(Storage.BlobGetOption[].class)))
        .thenReturn(mockBlob);

    GcsItemInfo itemInfo = createClientWithMockStorage(mockStorage).getGcsItemInfo(TEST_ITEM_ID);

    assertThat(itemInfo.getSize()).isEqualTo(0L);
  }

  @Test
  void getGcsItemInfo_storageReturnsNull_throwsFileNotFoundException() throws IOException {
    GcsItemId nonExistentItemId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET_NAME)
            .setObjectName(TEST_NON_EXISTENT_OBJECT)
            .build();

    FileNotFoundException e =
        assertThrows(
            FileNotFoundException.class, () -> gcsClient.getGcsItemInfo(nonExistentItemId));
    assertThat(e.getCause()).isInstanceOf(StorageException.class);
    assertThat(((StorageException) e.getCause()).getCode()).isEqualTo(404);
  }

  @Test
  void openReadChannel_gcsObjectExists_returnsChannelWithCorrectSizeAndContent()
      throws IOException {
    String objectData = "hello world";
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId(TEST_PROJECT).build();
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT_NAME).build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    StorageTestUtils.createBlobInStorage(
        storage, BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    ByteBuffer buffer = ByteBuffer.allocate(objectData.length());

    SeekableByteChannel channel = gcsClient.openReadChannel(itemInfo, readOptions);
    int bytesRead = channel.read(buffer);

    assertThat(channel.size()).isEqualTo(objectData.length());
    assertThat(bytesRead).isEqualTo(objectData.length());
    assertThat(new String(buffer.array(), UTF_8)).isEqualTo(objectData);
  }

  @Test
  void openReadChannel_itemId_gcsObjectExists_returnsChannelWithCorrectSizeAndContent()
      throws IOException {
    String objectData = "hello world";
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId(TEST_PROJECT).build();
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT_NAME).build();
    StorageTestUtils.createBlobInStorage(
        storage, BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    ByteBuffer buffer = ByteBuffer.allocate(objectData.length());

    SeekableByteChannel channel = gcsClient.openReadChannel(itemId, readOptions);
    int bytesRead = channel.read(buffer);

    assertThat(channel.size()).isEqualTo(objectData.length());
    assertThat(channel.size())
        .isEqualTo(objectData.length()); // Call twice to cover cached size branch
    assertThat(bytesRead).isEqualTo(objectData.length());
    assertThat(new String(buffer.array(), UTF_8)).isEqualTo(objectData);
  }

  @Test
  void openReadChannel_nullItemId_throwsNullPointerException() {
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId(TEST_PROJECT).build();

    NullPointerException e =
        assertThrows(
            NullPointerException.class,
            () -> gcsClient.openReadChannel((GcsItemId) null, readOptions));
    assertThat(e).hasMessageThat().isEqualTo("gcsItemId should not be null");
  }

  @Test
  void openReadChannel_nullItemInfo_throwsNullPointerException() {
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId(TEST_PROJECT).build();

    NullPointerException e =
        assertThrows(
            NullPointerException.class,
            () -> gcsClient.openReadChannel((GcsItemInfo) null, readOptions));
    assertThat(e).hasMessageThat().isEqualTo("itemInfo should not be null");
  }

  @Test
  void openReadChannel_nullReadOptions_throwsNullPointerException() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT).build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(0L).setContentGeneration(0L).build();

    NullPointerException e =
        assertThrows(NullPointerException.class, () -> gcsClient.openReadChannel(itemInfo, null));
    assertThat(e).hasMessageThat().isEqualTo("readOptions should not be null");
  }

  @Test
  void openReadChannel_itemInfoPointsToDirectory_throwsIllegalArgumentException() {
    GcsItemId directoryItemId = GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).build();
    GcsItemInfo directoryItemInfo =
        GcsItemInfo.builder()
            .setItemId(directoryItemId)
            .setSize(0L)
            .setContentGeneration(-1L)
            .build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId(TEST_PROJECT).build();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> gcsClient.openReadChannel(directoryItemInfo, readOptions));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Expected GCS object to be provided. But got: " + directoryItemId);
  }

  @Test
  void getUserAgent_noOptionalUserAgent() throws Exception {
    GcsClientImpl client =
        new GcsClientImpl(
            TEST_GCS_CLIENT_OPTIONS,
            executorServiceSupplier,
            telemetry,
            newBucketPropertiesCache());

    String userAgent = client.getUserAgent();

    assertThat(userAgent).isEqualTo("gcs-analytics-core/" + VersionHelper.VERSION);
  }

  @Test
  void getUserAgent_withOptionalUserAgent() throws Exception {
    GcsClientOptions options =
        GcsClientOptions.builder()
            .setProjectId(TEST_PROJECT)
            .setUserAgent("custom-app/1.0")
            .build();
    GcsClientImpl client =
        new GcsClientImpl(options, executorServiceSupplier, telemetry, newBucketPropertiesCache());

    String userAgent = client.getUserAgent();

    assertThat(userAgent)
        .isEqualTo("gcs-analytics-core/" + VersionHelper.VERSION + " custom-app/1.0");
  }

  @Test
  void createStorage_withCredentials_usesProvidedCredentials() throws IOException {
    Credentials credentials = NoCredentials.getInstance();

    GcsClientImpl client =
        new GcsClientImpl(
            credentials,
            TEST_GCS_CLIENT_OPTIONS,
            executorServiceSupplier,
            telemetry,
            newBucketPropertiesCache());

    assertThat(client.storage.getOptions().getCredentials()).isEqualTo(credentials);
  }

  @Test
  void createStorage_withProxy_sendsRequestsThroughProxy() throws IOException {
    List<String> proxiedRequests = new CopyOnWriteArrayList<>();
    HttpServer proxy = startNotFoundServer(proxiedRequests);
    try {
      String serviceHost = "http://storage.example.invalid";
      GcsClientOptions clientOptions =
          GcsClientOptions.builder()
              .setProjectId("test-project")
              .setServiceHost(serviceHost)
              .build();
      GcsAuthOptions authOptions =
          GcsAuthOptions.builder()
              .setProxyAddress("localhost:" + proxy.getAddress().getPort())
              .build();
      GcsClientImpl client =
          new GcsClientImpl(
              NoCredentials.getInstance(),
              clientOptions,
              executorServiceSupplier,
              telemetry,
              newBucketPropertiesCache(),
              authOptions);

      client.storage.get(BlobId.of("test-bucket", "test-object"));

      assertThat(proxiedRequests)
          .containsExactly(
              "GET " + serviceHost + "/storage/v1/b/test-bucket/o/test-object?projection=full");
    } finally {
      proxy.stop(0);
    }
  }

  @Test
  void createStorage_httpProtocol_appliesConfiguredTimeouts() {
    GcsAuthOptions authOptions =
        GcsAuthOptions.builder()
            .setHttpConnectTimeout(Duration.ofSeconds(12))
            .setHttpReadTimeout(Duration.ofSeconds(34))
            .build();

    GcsClientImpl client =
        new GcsClientImpl(
            NoCredentials.getInstance(),
            TEST_GCS_CLIENT_OPTIONS,
            executorServiceSupplier,
            telemetry,
            newBucketPropertiesCache(),
            authOptions);

    HttpTransportOptions transportOptions =
        (HttpTransportOptions) client.storage.getOptions().getTransportOptions();
    assertThat(new int[] {transportOptions.getConnectTimeout(), transportOptions.getReadTimeout()})
        .asList()
        .containsExactly(12_000, 34_000)
        .inOrder();
  }

  @Test
  void createStorage_withoutProxy_keepsDefaultTransportFactory() {
    GcsClientImpl client =
        new GcsClientImpl(
            NoCredentials.getInstance(),
            TEST_GCS_CLIENT_OPTIONS,
            executorServiceSupplier,
            telemetry,
            newBucketPropertiesCache(),
            GcsAuthOptions.builder().build());

    HttpTransportOptions transportOptions =
        (HttpTransportOptions) client.storage.getOptions().getTransportOptions();
    assertThat(transportOptions.getHttpTransportFactory())
        .isInstanceOf(DefaultHttpTransportFactory.class);
  }

  @Test
  void createStorage_grpcProtocolWithProxy_createsGrpcClient() {
    GcsClientOptions grpcClientOptions =
        GcsClientOptions.builder()
            .setProjectId("test-project")
            .setProtocol(GcsClientOptions.Protocol.GRPC)
            .build();
    GcsAuthOptions authOptions =
        GcsAuthOptions.builder().setProxyAddress("http://proxy.example.com:8080").build();

    GcsClientImpl client =
        new GcsClientImpl(
            NoCredentials.getInstance(),
            grpcClientOptions,
            executorServiceSupplier,
            telemetry,
            newBucketPropertiesCache(),
            authOptions);

    assertThat(client.storage.getOptions()).isInstanceOf(GrpcStorageOptions.class);
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "configured.goog, environment.goog, configured.goog",
        "null, environment.goog, environment.goog",
        "null, ' environment.goog ', environment.goog",
        "null, '  ', null",
        "null, null, null",
      },
      nullValues = "null")
  void resolveUniverseDomain_prefersConfiguredValueThenEnvironment(
      String configured, String environment, String expected) {
    assertThat(GcsClientImpl.resolveUniverseDomain(Optional.ofNullable(configured), environment))
        .isEqualTo(Optional.ofNullable(expected));
  }

  @Test
  void createStorageControlClient_withUniverseDomain_setsUniverseDomain() throws IOException {
    GcsClientOptions clientOptions =
        GcsClientOptions.builder().setUniverseDomain("custom-universe.goog").build();
    GcsClientImpl client = createClientWithClientOptions(clientOptions);

    try (StorageControlClient storageControlClient =
        client.createStorageControlClient(Optional.of(NoCredentials.getInstance()))) {
      assertThat(storageControlClient.getSettings().getUniverseDomain())
          .isEqualTo("custom-universe.goog");
    }
  }

  @Test
  void getBucketProperties_nullBucketName_throwsNullPointerException() {
    GcsClientImpl localGcsClient = createClientWithMockStorage(mock(Storage.class));
    NullPointerException e =
        assertThrows(NullPointerException.class, () -> localGcsClient.getBucketProperties(null));
    assertThat(e).hasMessageThat().isEqualTo("bucketName cannot be null");
  }

  @Test
  void getBucketProperties_hnsBucket_returnsTrue() throws IOException {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl localGcsClient = createClientWithMockStorage(mockStorage);
    Bucket mockBucket = mockBucketWithHns(true);
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_HNS_BUCKET), any(BucketGetOption.class));

    BucketProperties bucketProperties = localGcsClient.getBucketProperties(TEST_HNS_BUCKET);

    assertThat(bucketProperties.isHnsEnabled()).isTrue();
  }

  @Test
  void getBucketProperties_flatBucket_returnsDisabledHnsAndRapid() throws IOException {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl localGcsClient = createClientWithMockStorage(mockStorage);
    Bucket mockBucket = mockBucketWithHns(false);
    when(mockBucket.getStorageClass()).thenReturn(StorageClass.STANDARD);
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_FLAT_BUCKET), any(BucketGetOption.class));

    BucketProperties bucketProperties = localGcsClient.getBucketProperties(TEST_FLAT_BUCKET);

    assertThat(bucketProperties.isHnsEnabled()).isFalse();
    assertThat(bucketProperties.isRapid()).isFalse();
  }

  @Test
  void getBucketProperties_rapidBucket_returnsRapidEnabled() throws IOException {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl localGcsClient = createClientWithMockStorage(mockStorage);
    Bucket mockBucket = mockBucketWithHns(true);
    when(mockBucket.getStorageClass()).thenReturn(StorageClass.valueOf(RAPID_STORAGE_CLASS));
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_BUCKET), any(BucketGetOption.class));

    BucketProperties bucketProperties = localGcsClient.getBucketProperties(TEST_BUCKET);

    assertThat(bucketProperties.isHnsEnabled()).isTrue();
    assertThat(bucketProperties.isRapid()).isTrue();
  }

  @Test
  void getBucketProperties_missingHnsProperty_returnsFalse() throws IOException {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl localGcsClient = createClientWithMockStorage(mockStorage);
    Bucket mockBucket = mockBucketWithHns(null);
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_BUCKET), any(BucketGetOption.class));

    BucketProperties bucketProperties = localGcsClient.getBucketProperties(TEST_BUCKET);

    assertThat(bucketProperties.isHnsEnabled()).isFalse();
  }

  @Test
  void getBucketProperties_bucketNotFound_returnsDisabledHnsAndRapid() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl localGcsClient = createClientWithMockStorage(mockStorage);
    doReturn(null).when(mockStorage).get(eq(TEST_NON_EXISTENT_BUCKET), any(BucketGetOption.class));

    BucketProperties properties = localGcsClient.getBucketProperties(TEST_NON_EXISTENT_BUCKET);

    assertThat(properties.isHnsEnabled()).isFalse();
    assertThat(properties.isRapid()).isFalse();
  }

  @Test
  void getBucketProperties_forbiddenAccess_returnsDisabledHnsAndRapid() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl localGcsClient = createClientWithMockStorage(mockStorage);
    doThrow(new StorageException(403, "Forbidden"))
        .when(mockStorage)
        .get(eq(TEST_FORBIDDEN_BUCKET), any(BucketGetOption.class));

    BucketProperties properties = localGcsClient.getBucketProperties(TEST_FORBIDDEN_BUCKET);

    assertThat(properties.isHnsEnabled()).isFalse();
    assertThat(properties.isRapid()).isFalse();
  }

  @Test
  void getBucketProperties_storageThrows500_throwsIOException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl localGcsClient = createClientWithMockStorage(mockStorage);
    doThrow(new StorageException(500, "Internal Error"))
        .when(mockStorage)
        .get(eq(TEST_ERROR_BUCKET), any(BucketGetOption.class));

    IOException e =
        assertThrows(
            IOException.class, () -> localGcsClient.getBucketProperties(TEST_ERROR_BUCKET));

    assertThat(e).hasMessageThat().contains("Unable to access bucket: " + TEST_ERROR_BUCKET);
  }

  private GcsClientImpl createClientWithMockStorage(Storage mockStorage) {
    return createClientWithMockStorage(mockStorage, TEST_GCS_CLIENT_OPTIONS);
  }

  private static GcsClientOptions bidiClientOptions() {
    return GcsClientOptions.builder()
        .setProjectId(TEST_PROJECT)
        .setProtocol(GcsClientOptions.Protocol.BIDI)
        .build();
  }

  private static AnalyticsCache<String, BucketProperties> newBucketPropertiesCache() {
    return new AnalyticsCacheManager(GcsCacheOptions.builder().build()).getBucketPropertiesCache();
  }

  private GcsClientImpl createClientWithMockStorage(
      Storage mockStorage, GcsClientOptions clientOptions) {
    return new GcsClientImpl(
        clientOptions, executorServiceSupplier, telemetry, newBucketPropertiesCache()) {
      @Override
      protected Storage createStorage(Optional<Credentials> credentials) {
        return mockStorage;
      }

      @Override
      protected Storage createGrpcStorage(Optional<Credentials> credentials) {
        return mockStorage;
      }
    };
  }

  private Bucket mockBucketWithHns(Boolean hnsEnabled) {
    Bucket mockBucket = mock(Bucket.class);
    if (hnsEnabled != null) {
      HierarchicalNamespace hns = mock(HierarchicalNamespace.class);
      when(hns.getEnabled()).thenReturn(hnsEnabled);
      when(mockBucket.getHierarchicalNamespace()).thenReturn(hns);
    } else {
      when(mockBucket.getHierarchicalNamespace()).thenReturn(null);
    }
    return mockBucket;
  }

  @Test
  void create_withLocalStorage_writesSuccessfully() throws Exception {
    GcsClientImpl client = createClientWithMockStorage(storage);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_WRITE_OBJECT).build();
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_WRITE_OBJECT)).build();
    byte[] data = "hello write world".getBytes(StandardCharsets.UTF_8);

    try (WritableByteChannel channel = client.createWriteChannel(itemId, DEFAULT_WRITE_OPTIONS)) {
      int bytesWritten = channel.write(ByteBuffer.wrap(data));
      assertThat(bytesWritten).isEqualTo(data.length);
    }

    assertThat(new String(storage.readAllBytes(blobInfo.getBlobId()), StandardCharsets.UTF_8))
        .isEqualTo("hello write world");
  }

  @Test
  void create_whenAccessDenied_throwsAccessDeniedException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    StorageException e403 = new StorageException(403, "Forbidden");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e403);

    assertThrows(
        AccessDeniedException.class,
        () -> clientWithMock.createWriteChannel(TEST_ITEM_ID, DEFAULT_WRITE_OPTIONS));
  }

  @Test
  void create_whenFileExists_throwsIOException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    StorageException e409 = new StorageException(409, "Conflict");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e409);

    assertThrows(
        IOException.class,
        () -> clientWithMock.createWriteChannel(TEST_ITEM_ID, DEFAULT_WRITE_OPTIONS));
  }

  @Test
  void create_whenPreconditionFailedAndNoOverwrite_throwsFileAlreadyExistsException()
      throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().setOverwriteExisting(false).build();
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e412);

    FileAlreadyExistsException exception =
        assertThrows(
            FileAlreadyExistsException.class,
            () -> clientWithMock.createWriteChannel(TEST_ITEM_ID, writeOptions));

    assertThat(exception).hasCauseThat().isSameInstanceAs(e412);
  }

  @Test
  void create_whenGenerationMismatch_throwsIOException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId itemIdWithGen =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_OBJECT)
            .setContentGeneration(12345L)
            .build();
    BlobInfo blobInfoWithGen =
        BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT, 12345L))
            .setContentType("application/octet-stream")
            .build();
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockStorage.blobWriteSession(eq(blobInfoWithGen), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e412);

    IOException exception =
        assertThrows(
            IOException.class,
            () -> clientWithMock.createWriteChannel(itemIdWithGen, DEFAULT_WRITE_OPTIONS));

    assertThat(exception).hasMessageThat().contains("Generation mismatch for object");
    assertThat(exception).hasCauseThat().isSameInstanceAs(e412);
  }

  @Test
  void create_whenBucketOrObjectNotFound_throwsFileNotFoundException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName(TEST_NON_EXISTENT_BUCKET)
            .setObjectName(TEST_NON_EXISTENT_OBJECT)
            .build();
    BlobInfo blobInfo =
        BlobInfo.newBuilder(BlobId.of(TEST_NON_EXISTENT_BUCKET, TEST_NON_EXISTENT_OBJECT))
            .setContentType("application/octet-stream")
            .build();
    StorageException e404 = new StorageException(404, "Not Found");
    when(mockStorage.blobWriteSession(eq(blobInfo), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e404);

    FileNotFoundException exception =
        assertThrows(
            FileNotFoundException.class,
            () -> clientWithMock.createWriteChannel(itemId, DEFAULT_WRITE_OPTIONS));

    assertThat(exception).hasCauseThat().isSameInstanceAs(e404);
  }

  @Test
  void create_whenStorageExceptionOccurs_throwsIOException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    StorageException e500 = new StorageException(500, "Internal Server Error");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e500);

    IOException thrown =
        assertThrows(
            IOException.class,
            () -> clientWithMock.createWriteChannel(TEST_ITEM_ID, DEFAULT_WRITE_OPTIONS));

    assertThat(thrown).hasMessageThat().contains("Error during initialization to GCS");
  }

  @Test
  void create_whenOpenThrowsIOExceptionWrappingStorageException_translatesStorageException()
      throws Exception {
    Storage mockStorage = mock(Storage.class);
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    StorageException nestedStorageException = new StorageException(404, "Not Found");
    IOException wrappingException = new IOException(nestedStorageException);
    when(mockSession.open()).thenThrow(wrappingException);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    IOException exception =
        assertThrows(
            IOException.class, () -> clientWithMock.createWriteChannel(TEST_ITEM_ID, null));

    assertThat(exception).isInstanceOf(FileNotFoundException.class);
    assertThat(exception).hasCauseThat().isSameInstanceAs(nestedStorageException);
  }

  @Test
  void create_nullWriteOptions_usesDefaultWriteOptions() throws Exception {
    Storage mockStorage = mock(Storage.class);
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    WritableByteChannel mockChannel = mock(WritableByteChannel.class);
    when(mockSession.open()).thenReturn(mockChannel);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    WritableByteChannel returnedChannel = clientWithMock.createWriteChannel(TEST_ITEM_ID, null);

    assertThat(returnedChannel).isInstanceOf(GcsWriteChannel.class);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
    when(mockChannel.isOpen()).thenReturn(true);
    returnedChannel.write(buffer);
    verify(mockChannel).write(buffer);
  }

  @Test
  void create_withMetadata_base64EncodesMetadata() throws Exception {
    Storage mockStorage = mock(Storage.class);
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    when(mockSession.open()).thenReturn(mock(WritableByteChannel.class));
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    Map<String, byte[]> customMetadata = new HashMap<>();
    customMetadata.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
    customMetadata.put("key2", new byte[] {0, 1, 2, 3});

    GcsWriteOptions options =
        GcsWriteOptions.builder()
            .setMetadata(customMetadata)
            .setContentType("text/plain")
            .setContentEncoding("gzip")
            .build();
    ArgumentCaptor<BlobInfo> blobInfoCaptor = ArgumentCaptor.forClass(BlobInfo.class);

    clientWithMock.createWriteChannel(TEST_ITEM_ID, options);

    verify(mockStorage).blobWriteSession(blobInfoCaptor.capture(), any());
    BlobInfo capturedBlobInfo = blobInfoCaptor.getValue();
    assertThat(capturedBlobInfo.getMetadata()).containsEntry("key1", "dmFsdWUx");
    assertThat(capturedBlobInfo.getMetadata()).containsEntry("key2", "AAECAw==");
    assertThat(capturedBlobInfo.getContentType()).isEqualTo("text/plain");
    assertThat(capturedBlobInfo.getContentEncoding()).isEqualTo("gzip");
  }

  @Test
  void create_withoutObjectName_throwsIllegalArgumentException() {
    GcsClientImpl client =
        new GcsClientImpl(
            TEST_GCS_CLIENT_OPTIONS,
            executorServiceSupplier,
            telemetry,
            newBucketPropertiesCache());
    GcsItemId itemIdWithoutName = GcsItemId.builder().setBucketName(TEST_BUCKET).build();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> client.createWriteChannel(itemIdWithoutName, DEFAULT_WRITE_OPTIONS));
    assertThat(e).hasMessageThat().contains("Object name must be present");
  }

  @Test
  void create_allWriteOptionsEnabled_generatesCorrectBlobWriteOptions() throws Exception {
    Storage mockStorage = mock(Storage.class);
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    when(mockSession.open()).thenReturn(mock(WritableByteChannel.class));
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsWriteOptions allOptions =
        GcsWriteOptions.builder()
            .setChecksumValidationEnabled(true)
            .setDisableGzipContent(true)
            .setOverwriteExisting(false)
            .setKmsKeyName("kms-key")
            .setEncryptionKey("MDEyMzQ1Njc4OUFCQ0RFRkdISUpLTE1OT1BRUlNUVVU=")
            .setUserProject("user-project")
            .build();

    clientWithMock.createWriteChannel(TEST_ITEM_ID, allOptions);

    String capturedOptionsString = captureBlobWriteOptions(mockStorage, TEST_BLOB_INFO);
    assertThat(capturedOptionsString).contains("Crc32cMatchExtractor");
    assertThat(capturedOptionsString).contains("IF_GENERATION_MATCH");
    assertThat(capturedOptionsString).contains("KMS_KEY_NAME");
    assertThat(capturedOptionsString).contains("CUSTOMER_SUPPLIED_KEY");
  }

  @Test
  void create_withDisableGzipContentFalse_doesNotAddDisableGzipOption() throws Exception {
    Storage mockStorage = mock(Storage.class);
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    when(mockSession.open()).thenReturn(mock(WritableByteChannel.class));
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().setDisableGzipContent(false).build();

    clientWithMock.createWriteChannel(TEST_ITEM_ID, writeOptions);

    String capturedOptionsString = captureBlobWriteOptions(mockStorage, TEST_BLOB_INFO);
    assertThat(capturedOptionsString).doesNotContain("disableGzipContent");
  }

  @Test
  void create_withGenerationId_generatesGenerationMatchOption() throws Exception {
    Storage mockStorage = mock(Storage.class);
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    when(mockSession.open()).thenReturn(mock(WritableByteChannel.class));
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId itemIdWithGen =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_OBJECT)
            .setContentGeneration(12345L)
            .build();
    BlobInfo blobInfoWithGen =
        BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT, 12345L))
            .setContentType("application/octet-stream")
            .build();

    clientWithMock.createWriteChannel(itemIdWithGen, DEFAULT_WRITE_OPTIONS);

    String capturedOptionsString = captureBlobWriteOptions(mockStorage, blobInfoWithGen);
    assertThat(capturedOptionsString)
        .contains("GenerationMatch{key=IF_GENERATION_MATCH, val=12345}");
  }

  @ParameterizedTest
  @EnumSource(GcsClientOptions.PartFileCleanupType.class)
  void createStorage_withParallelCompositeUpload_setsPcuSessionConfig(
      GcsClientOptions.PartFileCleanupType cleanupType) throws Exception {
    GcsClientOptions clientOptions =
        GcsClientOptions.builder()
            .setUploadType(GcsClientOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD)
            .setPcuBufferCount(5)
            .setPcuBufferCapacity(128 * MB)
            .setPcuPartFileCleanupType(cleanupType)
            .setPcuPartFileNamePrefix("custom-prefix-")
            .build();

    assertPcuSessionConfig(clientOptions);
  }

  @Test
  void createStorage_withWriteToDiskThenUpload_setsBufferToDiskSessionConfig() throws Exception {
    GcsClientOptions clientOptions =
        GcsClientOptions.builder()
            .setUploadType(GcsClientOptions.UploadType.WRITE_TO_DISK_THEN_UPLOAD)
            .setTemporaryPaths(ImmutableList.of("/tmp/path1"))
            .build();

    GcsClientImpl client = createClientWithClientOptions(clientOptions);

    assertThat(getBlobWriteSessionConfig(client.storage.getOptions()))
        .isInstanceOf(BufferToDiskThenUpload.class);
  }

  @Test
  void createStorage_whenBufferToTempDirThrowsIOException_throwsUncheckedIOException() {
    GcsClientOptions clientOptions =
        GcsClientOptions.builder()
            .setUploadType(GcsClientOptions.UploadType.WRITE_TO_DISK_THEN_UPLOAD)
            .build();

    try (MockedStatic<BlobWriteSessionConfigs> mocked = mockStatic(BlobWriteSessionConfigs.class)) {
      mocked
          .when(BlobWriteSessionConfigs::bufferToTempDirThenUpload)
          .thenThrow(new IOException("mock error"));
      UncheckedIOException e =
          assertThrows(
              UncheckedIOException.class, () -> createClientWithClientOptions(clientOptions));
      assertThat(e).hasMessageThat().contains("Failed while initializing configs");
    }
  }

  @Test
  void createStorage_withJournalingOnHttp_throwsUnsupportedOperationException() {
    GcsClientOptions clientOptions =
        GcsClientOptions.builder()
            .setUploadType(GcsClientOptions.UploadType.JOURNALING)
            .setTemporaryPaths(ImmutableList.of("/tmp/path1"))
            .build();

    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class,
            () -> createClientWithClientOptions(clientOptions));

    assertThat(e).hasMessageThat().contains("JOURNALING upload type is not supported");
  }

  @Test
  void getGcsItemInfo_storageThrows500_throwsIOException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();
    when(mockStorage.get(any(BlobId.class), any(Storage.BlobGetOption[].class)))
        .thenThrow(new StorageException(500, "Internal Server Error"));

    IOException e = assertThrows(IOException.class, () -> clientWithMock.getGcsItemInfo(itemId));

    assertThat(e).hasMessageThat().contains("Error during metadata lookup");
  }

  @Test
  void getGcsItemInfo_storageThrows404_throwsFileNotFoundException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("non-existent-bucket").setObjectName(TEST_OBJECT).build();
    when(mockStorage.get(any(BlobId.class), any(Storage.BlobGetOption[].class)))
        .thenThrow(new StorageException(404, "Not Found"));

    FileNotFoundException e =
        assertThrows(FileNotFoundException.class, () -> clientWithMock.getGcsItemInfo(itemId));

    assertThat(((StorageException) e.getCause()).getCode()).isEqualTo(404);
  }

  @Test
  void getGcsItemInfo_storageThrows403_throwsAccessDeniedException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();
    when(mockStorage.get(any(BlobId.class), any(Storage.BlobGetOption[].class)))
        .thenThrow(new StorageException(403, "Forbidden"));

    AccessDeniedException e =
        assertThrows(AccessDeniedException.class, () -> clientWithMock.getGcsItemInfo(itemId));

    assertThat(e).hasMessageThat().contains("Access denied to object during metadata lookup");
  }

  @Test
  void create_whenOpenThrowsIOException_propagatesIOException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    IOException ioException = new IOException("Open failed");
    when(mockSession.open()).thenThrow(ioException);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    IOException thrown =
        assertThrows(
            IOException.class,
            () -> clientWithMock.createWriteChannel(TEST_ITEM_ID, DEFAULT_WRITE_OPTIONS));

    assertThat(thrown).isSameInstanceAs(ioException);
  }

  @Test
  void createStorage_withWriteToDiskAndNoTempPaths_setsBufferToTempDirSessionConfig()
      throws Exception {
    GcsClientOptions clientOptions =
        GcsClientOptions.builder()
            .setUploadType(GcsClientOptions.UploadType.WRITE_TO_DISK_THEN_UPLOAD)
            .setTemporaryPaths(ImmutableList.of()) // Empty paths
            .build();

    GcsClientImpl client = createClientWithClientOptions(clientOptions);

    assertThat(getBlobWriteSessionConfig(client.storage.getOptions()))
        .isInstanceOf(BufferToDiskThenUpload.class);
  }

  @Test
  void createStorage_withUniverseDomain_setsUniverseDomainOnStorageOptions() {
    GcsClientOptions clientOptions =
        GcsClientOptions.builder().setUniverseDomain("custom-universe.goog").build();

    GcsClientImpl client = createClientWithClientOptions(clientOptions);

    assertThat(client.storage.getOptions().getUniverseDomain()).isEqualTo("custom-universe.goog");
  }

  @Test
  void createStorage_withChunkUpload_setsDefaultSessionConfig() throws Exception {
    int customChunkSize = 2 * MB;
    GcsClientOptions clientOptions =
        GcsClientOptions.builder()
            .setUploadType(GcsClientOptions.UploadType.CHUNK_UPLOAD)
            .setUploadChunkSize(customChunkSize)
            .build();

    GcsClientImpl client = createClientWithClientOptions(clientOptions);

    BlobWriteSessionConfig config = getBlobWriteSessionConfig(client.storage.getOptions());
    assertThat(config).isInstanceOf(DefaultBlobWriteSessionConfig.class);
    assertThat(((DefaultBlobWriteSessionConfig) config).getChunkSize()).isEqualTo(customChunkSize);
  }

  @Test
  void create_whenPreconditionFailedAndNullWriteOptions_throwsIOException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e412);

    IOException exception =
        assertThrows(
            IOException.class, () -> clientWithMock.createWriteChannel(TEST_ITEM_ID, null));

    assertThat(exception).hasCauseThat().isSameInstanceAs(e412);
  }

  @Test
  void create_whenPreconditionFailedAndDefaultOverwrite_throwsIOException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().setOverwriteExisting(true).build();
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e412);

    IOException exception =
        assertThrows(
            IOException.class, () -> clientWithMock.createWriteChannel(TEST_ITEM_ID, writeOptions));

    assertThat(exception).hasCauseThat().isSameInstanceAs(e412);
  }

  @Test
  void create_whenRuntimeExceptionOccurs_directlyPropagates() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    RuntimeException runtimeException = new NullPointerException("mock null pointer exception");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(runtimeException);

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> clientWithMock.createWriteChannel(TEST_ITEM_ID, DEFAULT_WRITE_OPTIONS));

    assertThat(exception).isSameInstanceAs(runtimeException);
  }

  private BlobWriteSessionConfig getBlobWriteSessionConfig(StorageOptions options) {
    Class<?> clazz = options.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(BLOB_WRITE_SESSION_CONFIG_FIELD);
        field.setAccessible(true);
        return (BlobWriteSessionConfig) field.get(options);
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    throw new RuntimeException(
        new NoSuchFieldException(
            "Field " + BLOB_WRITE_SESSION_CONFIG_FIELD + " not found in options hierarchy"));
  }

  private void assertPcuSessionConfig(GcsClientOptions clientOptions) throws Exception {
    GcsClientImpl client = createClientWithClientOptions(clientOptions);

    assertThat(getBlobWriteSessionConfig(client.storage.getOptions()))
        .isInstanceOf(ParallelCompositeUploadBlobWriteSessionConfig.class);
  }

  private GcsClientImpl createClientWithClientOptions(GcsClientOptions clientOptions) {
    return new GcsClientImpl(
        clientOptions, executorServiceSupplier, telemetry, newBucketPropertiesCache());
  }

  /** Starts a local HTTP server that records each request line and answers 404 Not Found. */
  private static HttpServer startNotFoundServer(List<String> requests) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
          byte[] body = "{\"error\":{\"code\":404,\"message\":\"Not Found\"}}".getBytes(UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(404, body.length);
          try (OutputStream responseBody = exchange.getResponseBody()) {
            responseBody.write(body);
          }
        });
    server.start();
    return server;
  }

  private BlobWriteSession mockBlobWriteSession(Storage mockStorage) {
    BlobWriteSession mockSession = mock(BlobWriteSession.class);
    when(mockStorage.blobWriteSession(any(BlobInfo.class), any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockSession);
    return mockSession;
  }

  private String captureBlobWriteOptions(Storage mockStorage, BlobInfo blobInfo) {
    ArgumentCaptor<Storage.BlobWriteOption[]> optionsCaptor =
        ArgumentCaptor.forClass(Storage.BlobWriteOption[].class);
    verify(mockStorage).blobWriteSession(eq(blobInfo), optionsCaptor.capture());
    return Arrays.toString(optionsCaptor.getValue());
  }

  @Test
  void createStorage_bidiDisabled_usesHttpTransport() throws IOException {
    GcsClientImpl client =
        new GcsClientImpl(
            NoCredentials.getInstance(),
            TEST_GCS_CLIENT_OPTIONS,
            executorServiceSupplier,
            telemetry,
            newBucketPropertiesCache());
    assertThat(client.storage.getOptions()).isInstanceOf(HttpStorageOptions.class);
  }

  @Test
  void createStorage_grpcProtocol_usesGrpcTransport() throws IOException {
    GcsClientOptions options =
        GcsClientOptions.builder()
            .setProjectId(TEST_PROJECT)
            .setProtocol(GcsClientOptions.Protocol.GRPC)
            .build();

    GcsClientImpl client =
        new GcsClientImpl(
            NoCredentials.getInstance(),
            options,
            executorServiceSupplier,
            telemetry,
            newBucketPropertiesCache());

    assertThat(client.storage.getOptions()).isInstanceOf(GrpcStorageOptions.class);
  }

  @Test
  void createStorage_bidiProtocolWithBidiWrite_usesGrpcTransport() throws IOException {
    GcsClientOptions options =
        GcsClientOptions.builder()
            .setProjectId(TEST_PROJECT)
            .setProtocol(GcsClientOptions.Protocol.BIDI)
            .setGcsWriteOptions(GcsWriteOptions.builder().setBidiWriteEnabled(true).build())
            .build();

    GcsClientImpl client =
        new GcsClientImpl(
            NoCredentials.getInstance(),
            options,
            executorServiceSupplier,
            telemetry,
            newBucketPropertiesCache());

    assertThat(client.storage.getOptions()).isInstanceOf(GrpcStorageOptions.class);
  }

  @Test
  void openReadChannel_bidiProtocol_returnsGcsBidiReadChannel() throws IOException {
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId(TEST_PROJECT).build();
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT_NAME).build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(100L).setContentGeneration(0L).build();
    Storage mockStorage = mock(Storage.class);
    ApiFuture<BlobReadSession> mockSessionFuture = mock(ApiFuture.class);
    when(mockStorage.blobReadSession(any(BlobId.class))).thenReturn(mockSessionFuture);
    GcsClient bidiClient = createClientWithMockStorage(mockStorage, bidiClientOptions());

    VectoredSeekableByteChannel channel = bidiClient.openReadChannel(itemInfo, readOptions);

    assertThat(channel).isInstanceOf(GcsBidiReadChannel.class);
  }

  @Test
  void openReadChannel_itemId_bidiProtocol_returnsGcsBidiReadChannel() throws IOException {
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId(TEST_PROJECT).build();
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT_NAME).build();
    Storage mockStorage = mock(Storage.class);
    ApiFuture<BlobReadSession> mockSessionFuture = mock(ApiFuture.class);
    when(mockStorage.blobReadSession(any(BlobId.class))).thenReturn(mockSessionFuture);
    GcsClient bidiClient = createClientWithMockStorage(mockStorage, bidiClientOptions());

    VectoredSeekableByteChannel channel = bidiClient.openReadChannel(itemId, readOptions);

    assertThat(channel).isInstanceOf(GcsBidiReadChannel.class);
  }

  @Test
  void openReadChannel_grpcProtocol_returnsStandardGcsReadChannel() throws IOException {
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId(TEST_PROJECT).build();
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT_NAME).build();
    GcsClientOptions clientOptions =
        GcsClientOptions.builder()
            .setProjectId(TEST_PROJECT)
            .setProtocol(GcsClientOptions.Protocol.GRPC)
            .build();
    Storage mockStorage = mock(Storage.class);
    when(mockStorage.reader(any(BlobId.class), any(Storage.BlobSourceOption[].class)))
        .thenReturn(mock(ReadChannel.class));
    GcsClient client = createClientWithMockStorage(mockStorage, clientOptions);

    VectoredSeekableByteChannel channel = client.openReadChannel(itemId, readOptions);

    assertThat(channel).isNotInstanceOf(GcsBidiReadChannel.class);
  }

  @Test
  void getBucketInfo_nullItemId_throwsNullPointerException() {
    NullPointerException e =
        assertThrows(NullPointerException.class, () -> gcsClient.getBucketInfo(null));

    assertThat(e).hasMessageThat().contains("Item ID must not be null");
  }

  @Test
  void getBucketInfo_notBucketItemId_throwsIllegalArgumentException() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> gcsClient.getBucketInfo(TEST_ITEM_ID));

    assertThat(e).hasMessageThat().contains("Expected a bucket itemId");
  }

  @Test
  void getBucketInfo_storageReturnsNull_returnsNotFoundItemInfo() throws IOException {
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_NON_EXISTENT_BUCKET).build();
    Storage mockStorage = mock(Storage.class);
    when(mockStorage.get(eq(TEST_NON_EXISTENT_BUCKET), any(Storage.BucketGetOption[].class)))
        .thenReturn(null);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    GcsItemInfo itemInfo = clientWithMock.getBucketInfo(bucketId);

    assertNotFound(itemInfo, bucketId);
  }

  @Test
  void getBucketInfo_bucketExists_returnsBucketInfo() throws IOException {
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    Bucket mockBucket = createMockBucket(TEST_BUCKET);
    Storage mockStorage = mock(Storage.class);
    when(mockStorage.get(eq(TEST_BUCKET), any(Storage.BucketGetOption[].class)))
        .thenReturn(mockBucket);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    GcsItemInfo itemInfo = clientWithMock.getBucketInfo(bucketId);

    assertThat(itemInfo.getItemId()).isEqualTo(bucketId);
    assertThat(itemInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.BUCKET);
    assertThat(itemInfo.getSize()).isEqualTo(0L);
    assertThat(itemInfo.getLocation()).hasValue(TEST_LOCATION);
    assertThat(itemInfo.getStorageClass()).hasValue(StorageClass.STANDARD.toString());
    assertThat(itemInfo.getMetaGeneration()).isEqualTo(2L);
    assertThat(itemInfo.getCreationTime())
        .isEqualTo(OffsetDateTime.parse("2026-08-01T10:00:00Z").toInstant().toEpochMilli());
    assertThat(itemInfo.getModificationTime())
        .isEqualTo(OffsetDateTime.parse("2026-08-02T10:00:00Z").toInstant().toEpochMilli());
  }

  @Test
  void getBucketInfo_storageThrows404_returnsNotFoundItemInfo() throws IOException {
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    Storage mockStorage = mock(Storage.class);
    when(mockStorage.get(eq(TEST_BUCKET), any(Storage.BucketGetOption[].class)))
        .thenThrow(new StorageException(404, "Not Found"));
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    GcsItemInfo itemInfo = clientWithMock.getBucketInfo(bucketId);

    assertNotFound(itemInfo, bucketId);
  }

  @Test
  void getBucketInfo_storageThrows500_throwsIOException() {
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    Storage mockStorage = mock(Storage.class);
    when(mockStorage.get(eq(TEST_BUCKET), any(Storage.BucketGetOption[].class)))
        .thenThrow(new StorageException(500, "Internal Error"));
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    IOException e = assertThrows(IOException.class, () -> clientWithMock.getBucketInfo(bucketId));

    assertThat(e).hasMessageThat().contains("Unable to access bucket: " + TEST_BUCKET);
  }

  @Test
  void getFolderInfo_nullItemId_throwsNullPointerException() {
    NullPointerException e =
        assertThrows(NullPointerException.class, () -> gcsClient.getFolderInfo(null));

    assertThat(e).hasMessageThat().contains("Item ID must not be null");
  }

  @Test
  void getFolderInfo_rootItemId_returnsRootInfo() throws IOException {
    GcsItemInfo itemInfo = gcsClient.getFolderInfo(GcsItemId.ROOT);

    assertThat(itemInfo).isEqualTo(GcsItemInfo.ROOT_INFO);
  }

  @Test
  void getFolderInfo_bucketItemId_returnsBucketInfo() throws IOException {
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    Bucket mockBucket = createMockBucket(TEST_BUCKET);
    Storage mockStorage = mock(Storage.class);
    when(mockStorage.get(eq(TEST_BUCKET), any(Storage.BucketGetOption[].class)))
        .thenReturn(mockBucket);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    GcsItemInfo itemInfo = clientWithMock.getFolderInfo(bucketId);

    assertThat(itemInfo.getItemId()).isEqualTo(bucketId);
    assertThat(itemInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.BUCKET);
  }

  @Test
  void getFolderInfo_emptyFolderName_throwsIllegalArgumentException() {
    GcsItemId rootSlashId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("/").build();

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> gcsClient.getFolderInfo(rootSlashId));

    assertThat(e).hasMessageThat().contains("Folder name cannot be empty");
  }

  @Test
  void getFolderInfo_folderExists_returnsFolderInfo() throws IOException {
    GcsItemId folderItemId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_FOLDER_NAME + "/")
            .build();
    Folder mockFolder = createMockFolder(TEST_BUCKET, TEST_FOLDER_NAME);
    StorageControlClient mockControlClient = mock(StorageControlClient.class);
    when(mockControlClient.getFolder(any(GetFolderRequest.class))).thenReturn(mockFolder);
    GcsClientImpl clientWithMockControl = createClientWithMockControl(mockControlClient);

    GcsItemInfo itemInfo = clientWithMockControl.getFolderInfo(folderItemId);

    assertThat(itemInfo.getItemId()).isEqualTo(folderItemId);
    assertThat(itemInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.NATIVE_FOLDER);
    assertThat(itemInfo.getSize()).isEqualTo(0L);
    assertThat(itemInfo.getMetaGeneration()).isEqualTo(3L);
    assertThat(itemInfo.getCreationTime())
        .isEqualTo(Instant.ofEpochSecond(1000, 500).toEpochMilli());
    assertThat(itemInfo.getModificationTime())
        .isEqualTo(Instant.ofEpochSecond(2000, 500).toEpochMilli());
  }

  @Test
  void getFolderInfo_folderWithoutCreateOrUpdateTime_setsTimestampsToZero() throws IOException {
    GcsItemId folderItemId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_FOLDER_NAME + "/")
            .build();
    StorageControlClient mockControlClient = mock(StorageControlClient.class);
    when(mockControlClient.getFolder(any(GetFolderRequest.class)))
        .thenReturn(Folder.getDefaultInstance());
    GcsClientImpl clientWithMockControl = createClientWithMockControl(mockControlClient);

    GcsItemInfo itemInfo = clientWithMockControl.getFolderInfo(folderItemId);

    assertThat(itemInfo.getCreationTime()).isEqualTo(0L);
    assertThat(itemInfo.getModificationTime()).isEqualTo(0L);
  }

  @Test
  void close_withStorageControlClient_closesBoth() throws Exception {
    Storage mockStorage = mock(Storage.class);
    StorageControlClient mockControlClient = mock(StorageControlClient.class);
    GcsClientImpl localClient = createClientWithMockStorage(mockStorage);
    localClient.storageControlClient = mockControlClient;

    localClient.close();

    verify(mockStorage).close();
    verify(mockControlClient).close();
  }

  @Test
  void close_whenUnderlyingClientsThrowException_suppressesException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    StorageControlClient mockControlClient = mock(StorageControlClient.class);
    doThrow(new RuntimeException("storage close error")).when(mockStorage).close();
    doThrow(new RuntimeException("control client close error")).when(mockControlClient).close();
    GcsClientImpl localClient = createClientWithMockStorage(mockStorage);
    localClient.storageControlClient = mockControlClient;

    localClient.close();

    verify(mockStorage).close();
    verify(mockControlClient).close();
  }

  @Test
  void close_idempotent_closesStorageControlClientOnlyOnce() throws Exception {
    Storage mockStorage = mock(Storage.class);
    StorageControlClient mockControlClient = mock(StorageControlClient.class);
    GcsClientImpl localClient = createClientWithMockStorage(mockStorage);
    localClient.storageControlClient = mockControlClient;

    localClient.close();
    localClient.close();

    verify(mockControlClient, times(1)).close();
  }

  @Test
  void lazyGetStorageControlClient_afterClose_throwsIOException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl localClient = createClientWithMockStorage(mockStorage);

    localClient.close();

    IOException e =
        assertThrows(IOException.class, () -> localClient.lazyGetStorageControlClient());
    assertThat(e).hasMessageThat().contains("StorageControlClient is closed");
  }

  @Test
  void lazyGetStorageControlClient_closedWhileWaitingForLock_throwsIOException() throws Exception {
    GcsClientImpl client = createClientWithMockStorage(mock(Storage.class));
    CountDownLatch started = new CountDownLatch(1);
    AtomicReference<IOException> thrown = new AtomicReference<>();

    CompletableFuture<Void> future;
    synchronized (client) {
      future =
          CompletableFuture.runAsync(
              () -> {
                started.countDown();
                thrown.set(assertThrows(IOException.class, client::lazyGetStorageControlClient));
              });
      started.await(5, TimeUnit.SECONDS);
      Thread.sleep(50);
      client.close();
    }
    future.get(5, TimeUnit.SECONDS);

    assertThat(thrown.get()).hasMessageThat().contains("StorageControlClient is closed");
  }

  @Test
  void createStorageControlClient_withCredentials_createsClient() throws Exception {
    GcsClientImpl localClient = createClientWithMockStorage(mock(Storage.class));

    try (StorageControlClient controlClient =
        localClient.createStorageControlClient(Optional.of(NoCredentials.getInstance()))) {
      assertThat(controlClient).isNotNull();
      assertThat(controlClient.getSettings().getHeaderProvider().getHeaders())
          .containsEntry("User-Agent", localClient.getUserAgent());
    }
  }

  @Test
  void createStorageControlClient_withoutCredentials_createsClient() throws Exception {
    GcsClientImpl localClient = createClientWithMockStorage(mock(Storage.class));
    StorageControlClient mockControlClient = mock(StorageControlClient.class);

    try (MockedStatic<StorageControlClient> mocked = mockStatic(StorageControlClient.class)) {
      ArgumentCaptor<StorageControlSettings> settingsCaptor =
          ArgumentCaptor.forClass(StorageControlSettings.class);
      mocked
          .when(() -> StorageControlClient.create(settingsCaptor.capture()))
          .thenReturn(mockControlClient);

      assertThat(localClient.createStorageControlClient(Optional.empty()))
          .isSameInstanceAs(mockControlClient);
      assertThat(settingsCaptor.getValue().getHeaderProvider().getHeaders())
          .containsEntry("User-Agent", localClient.getUserAgent());
    }
  }

  @Test
  void createStorageControlClient_withCustomUserAgent_setsUserAgentHeader() throws Exception {
    GcsClientOptions options =
        GcsClientOptions.builder()
            .setProjectId(TEST_PROJECT)
            .setUserAgent("custom-app/1.0")
            .build();
    GcsClientImpl localClient = createClientWithMockStorage(mock(Storage.class), options);
    StorageControlClient mockControlClient = mock(StorageControlClient.class);

    try (MockedStatic<StorageControlClient> mocked = mockStatic(StorageControlClient.class)) {
      ArgumentCaptor<StorageControlSettings> settingsCaptor =
          ArgumentCaptor.forClass(StorageControlSettings.class);
      mocked
          .when(() -> StorageControlClient.create(settingsCaptor.capture()))
          .thenReturn(mockControlClient);

      assertThat(localClient.createStorageControlClient(Optional.empty()))
          .isSameInstanceAs(mockControlClient);
      assertThat(settingsCaptor.getValue().getHeaderProvider().getHeaders())
          .containsEntry(
              "User-Agent", "gcs-analytics-core/" + VersionHelper.VERSION + " custom-app/1.0");
    }
  }

  @Test
  void lazyGetStorageControlClient_initializesWhenNullAndReusesInstance() throws Exception {
    StorageControlClient mockControlClient = mock(StorageControlClient.class);
    GcsClientImpl mockClient = createClientWithMockControl(mockControlClient);

    StorageControlClient createdInstance = mockClient.lazyGetStorageControlClient();
    StorageControlClient cachedInstance = mockClient.lazyGetStorageControlClient();

    assertThat(createdInstance).isSameInstanceAs(mockControlClient);
    assertThat(cachedInstance).isSameInstanceAs(mockControlClient);
  }

  @Test
  void lazyGetStorageControlClient_concurrentCalls_instantiatesExactlyOnce() throws Exception {
    StorageControlClient mockClientInstance = mock(StorageControlClient.class);
    AtomicInteger factoryInvocations = new AtomicInteger();
    GcsClientImpl client =
        new GcsClientImpl(
            TEST_GCS_CLIENT_OPTIONS,
            executorServiceSupplier,
            telemetry,
            newBucketPropertiesCache()) {
          @Override
          protected StorageControlClient createStorageControlClient(
              Optional<Credentials> credentials) {
            factoryInvocations.incrementAndGet();
            try {
              Thread.sleep(50);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return mockClientInstance;
          }
        };
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    Callable<StorageControlClient> task =
        () -> {
          ready.countDown();
          start.await();
          return client.lazyGetStorageControlClient();
        };

    try {
      Future<StorageControlClient> f1 = executor.submit(task);
      Future<StorageControlClient> f2 = executor.submit(task);
      ready.await(5, TimeUnit.SECONDS);
      start.countDown();
      StorageControlClient result1 = f1.get(5, TimeUnit.SECONDS);
      StorageControlClient result2 = f2.get(5, TimeUnit.SECONDS);

      assertThat(result1).isSameInstanceAs(mockClientInstance);
      assertThat(result2).isSameInstanceAs(mockClientInstance);
      assertThat(factoryInvocations.get()).isEqualTo(1);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void getFolderInfo_notFound_returnsNotFoundItemInfo() throws IOException {
    GcsItemId folderItemId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_NON_EXISTENT_OBJECT)
            .build();
    StorageControlClient mockControlClient = mock(StorageControlClient.class);
    NotFoundException notFoundException = mock(NotFoundException.class);
    when(mockControlClient.getFolder(any(GetFolderRequest.class))).thenThrow(notFoundException);
    GcsClientImpl clientWithMockControl = createClientWithMockControl(mockControlClient);

    GcsItemInfo itemInfo = clientWithMockControl.getFolderInfo(folderItemId);

    assertNotFound(itemInfo, folderItemId);
  }

  @Test
  void getFolderInfo_otherRpcException_throwsIOException() throws IOException {
    StorageControlClient mockControlClient = mock(StorageControlClient.class);
    when(mockControlClient.getFolder(any(GetFolderRequest.class)))
        .thenThrow(new RuntimeException("RPC error"));
    GcsClientImpl clientWithMockControl = createClientWithMockControl(mockControlClient);

    IOException e =
        assertThrows(IOException.class, () -> clientWithMockControl.getFolderInfo(TEST_ITEM_ID));

    assertThat(e).hasMessageThat().contains("Failed to get folder info for: " + TEST_ITEM_ID);
  }

  private GcsClientImpl createClientWithMockControl(StorageControlClient mockControlClient) {
    return new GcsClientImpl(
        TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry, newBucketPropertiesCache()) {
      @Override
      protected Storage createStorage(Optional<Credentials> credentials) {
        return GcsClientImplTest.this.storage;
      }

      @Override
      protected StorageControlClient createStorageControlClient(Optional<Credentials> credentials) {
        return mockControlClient;
      }
    };
  }

  @Test
  void listFirstObjectWithPrefix_hasObjects_returnsSingleItem() throws IOException {
    GcsItemId prefixId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_DIR).build();
    Blob mockBlob = createMockBlob(TEST_BUCKET, TEST_DIR + "file.txt");
    Storage mockStorage = createMockStorageWithBlobs(TEST_BUCKET, mockBlob);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    Optional<GcsItemInfo> result = clientWithMock.listFirstObjectWithPrefix(prefixId);

    assertThat(result).isPresent();
    assertThat(result.get().getItemId().getObjectName()).hasValue(TEST_DIR + "file.txt");
    assertThat(result.get().getSize()).isEqualTo(0L);
  }

  @Test
  void listFirstObjectWithPrefix_empty_returnsEmptyList() throws IOException {
    GcsItemId prefixId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_DIR).build();
    Storage mockStorage = createMockStorageWithBlobs(TEST_BUCKET);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    Optional<GcsItemInfo> result = clientWithMock.listFirstObjectWithPrefix(prefixId);

    assertThat(result).isEmpty();
  }

  @Test
  void listFirstObjectWithPrefix_nullPrefixId_throwsNullPointerException() {
    NullPointerException e =
        assertThrows(NullPointerException.class, () -> gcsClient.listFirstObjectWithPrefix(null));

    assertThat(e).hasMessageThat().contains("prefixId must not be null");
  }

  @Test
  void listFirstObjectWithPrefix_storageThrows404_throwsFileNotFoundException() {
    Storage mockStorage = mock(Storage.class);
    when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
        .thenThrow(new StorageException(404, "Bucket not found"));
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    FileNotFoundException e =
        assertThrows(
            FileNotFoundException.class,
            () -> clientWithMock.listFirstObjectWithPrefix(TEST_ITEM_ID));

    assertThat(e).hasMessageThat().contains("Bucket not found: " + TEST_BUCKET);
  }

  @Test
  void listFirstObjectWithPrefix_storageThrows500_throwsIOException() {
    Storage mockStorage = mock(Storage.class);
    when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
        .thenThrow(new StorageException(500, "Internal error"));
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    IOException e =
        assertThrows(
            IOException.class, () -> clientWithMock.listFirstObjectWithPrefix(TEST_ITEM_ID));

    assertThat(e)
        .hasMessageThat()
        .contains("Failed to list the first object for prefix: " + TEST_ITEM_ID);
  }

  private static Blob createMockBlob(String bucket, String name) {
    Blob mockBlob = mock(Blob.class);
    when(mockBlob.getBucket()).thenReturn(bucket);
    when(mockBlob.getName()).thenReturn(name);
    return mockBlob;
  }

  private static Bucket createMockBucket(String bucketName) {
    Bucket mockBucket = mock(Bucket.class);
    when(mockBucket.getName()).thenReturn(bucketName);
    when(mockBucket.getLocation()).thenReturn(TEST_LOCATION);
    when(mockBucket.getStorageClass()).thenReturn(StorageClass.STANDARD);
    when(mockBucket.getMetageneration()).thenReturn(2L);
    when(mockBucket.getCreateTimeOffsetDateTime())
        .thenReturn(OffsetDateTime.parse("2026-08-01T10:00:00Z"));
    when(mockBucket.getUpdateTimeOffsetDateTime())
        .thenReturn(OffsetDateTime.parse("2026-08-02T10:00:00Z"));
    return mockBucket;
  }

  private static Folder createMockFolder(String bucketName, String folderName) {
    return Folder.newBuilder()
        .setName("projects/_/buckets/" + bucketName + "/folders/" + folderName)
        .setMetageneration(3L)
        .setCreateTime(Timestamp.newBuilder().setSeconds(1000).setNanos(500).build())
        .setUpdateTime(Timestamp.newBuilder().setSeconds(2000).setNanos(500).build())
        .build();
  }

  @SuppressWarnings("unchecked")
  private static Storage createMockStorageWithBlobs(String bucket, Blob... blobs) {
    Page<Blob> mockPage = mock(Page.class);
    when(mockPage.getValues()).thenReturn(ImmutableList.copyOf(blobs));
    Storage mockStorage = mock(Storage.class);
    when(mockStorage.list(eq(bucket), any(Storage.BlobListOption[].class))).thenReturn(mockPage);
    return mockStorage;
  }

  private static void assertNotFound(GcsItemInfo itemInfo, GcsItemId expectedItemId) {
    assertThat(itemInfo.getItemId()).isEqualTo(expectedItemId);
    assertThat(itemInfo.exists()).isFalse();
    assertThat(itemInfo.getSize()).isEqualTo(-1L);
  }

  @Test
  void createWriteChannel_bidiEnabled_returnsGcsBidiWriteChannel() throws IOException {
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().setBidiWriteEnabled(true).build();
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName("test-bucket-name")
            .setObjectName("test-object-name")
            .build();

    Storage mockStorage = mock(Storage.class);
    BlobAppendableUpload mockSession = mock(BlobAppendableUpload.class);
    AppendableUploadWriteableByteChannel mockAppendChannel =
        mock(AppendableUploadWriteableByteChannel.class);

    when(mockStorage.blobAppendableUpload(
            any(BlobInfo.class),
            any(BlobAppendableUploadConfig.class),
            any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockSession);
    when(mockSession.open()).thenReturn(mockAppendChannel);
    when(mockAppendChannel.isOpen()).thenReturn(true);

    GcsClient bidiClient = createClientWithMockStorage(mockStorage, bidiClientOptions());

    try (WritableByteChannel channel = bidiClient.createWriteChannel(itemId, writeOptions)) {
      assertThat(channel).isInstanceOf(GcsBidiWriteChannel.class);
    }
  }

  @Test
  void createWriteChannel_bidiWriteWithoutBidiProtocol_throwsIllegalArgumentException() {
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().setBidiWriteEnabled(true).build();
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName("test-bucket-name")
            .setObjectName("test-object-name")
            .build();
    GcsClient client = createClientWithMockStorage(mock(Storage.class), TEST_GCS_CLIENT_OPTIONS);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> client.createWriteChannel(itemId, writeOptions));

    assertThat(exception).hasMessageThat().contains("client.protocol");
  }

  @Test
  void createWriteChannel_bidiDisabled_returnsStandardGcsWriteChannel() throws IOException {
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().setBidiWriteEnabled(false).build();
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName("test-bucket-name")
            .setObjectName("test-object-name")
            .build();

    Storage mockStorage = mock(Storage.class);
    BlobWriteSession mockSession = mock(BlobWriteSession.class);
    WritableByteChannel mockChannel = mock(WritableByteChannel.class);
    ApiFuture<BlobInfo> mockFuture = mock(ApiFuture.class);

    when(mockStorage.blobWriteSession(any(BlobInfo.class), any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockSession);
    when(mockSession.open()).thenReturn(mockChannel);
    when(mockSession.getResult()).thenReturn(mockFuture);

    GcsClient client =
        new GcsClientImpl(
            TEST_GCS_CLIENT_OPTIONS,
            executorServiceSupplier,
            telemetry,
            newBucketPropertiesCache()) {
          @Override
          protected Storage createStorage(Optional<Credentials> credentials) {
            return mockStorage;
          }
        };

    try (WritableByteChannel channel = client.createWriteChannel(itemId, writeOptions)) {
      assertThat(channel).isInstanceOf(GcsWriteChannel.class);
      assertThat(channel).isNotInstanceOf(GcsBidiWriteChannel.class);
    }
  }

  @Test
  void createWriteChannel_nullOptions_returnsStandardGcsWriteChannel() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName("test-bucket-name")
            .setObjectName("test-object-name")
            .build();

    Storage mockStorage = mock(Storage.class);
    BlobWriteSession mockSession = mock(BlobWriteSession.class);
    WritableByteChannel mockChannel = mock(WritableByteChannel.class);
    ApiFuture<BlobInfo> mockFuture = mock(ApiFuture.class);

    when(mockStorage.blobWriteSession(any(BlobInfo.class), any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockSession);
    when(mockSession.open()).thenReturn(mockChannel);
    when(mockSession.getResult()).thenReturn(mockFuture);

    GcsClient client =
        new GcsClientImpl(
            TEST_GCS_CLIENT_OPTIONS,
            executorServiceSupplier,
            telemetry,
            newBucketPropertiesCache()) {
          @Override
          protected Storage createStorage(Optional<Credentials> credentials) {
            return mockStorage;
          }
        };

    try (WritableByteChannel channel = client.createWriteChannel(itemId, null)) {
      assertThat(channel).isInstanceOf(GcsWriteChannel.class);
      assertThat(channel).isNotInstanceOf(GcsBidiWriteChannel.class);
    }
  }

  @Test
  void createBucket_success() throws IOException {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    clientWithMock.createBucket(TEST_BUCKET_NAME);

    verify(mockStorage).create(BucketInfo.of(TEST_BUCKET_NAME));
  }

  @Test
  void createBucket_withNonZonalOptions_setsHnsLocationStorageClassAndTtl() throws IOException {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsCreateBucketOptions options =
        GcsCreateBucketOptions.builder()
            .setHierarchicalNamespaceEnabled(true)
            .setLocation("us-central1")
            .setStorageClass("STANDARD")
            .setTtl(Duration.ofDays(7))
            .build();

    clientWithMock.createBucket(TEST_BUCKET_NAME, options);

    BucketInfo expectedBucketInfo =
        BucketInfo.newBuilder(TEST_BUCKET_NAME)
            .setLocation("us-central1")
            .setStorageClass(StorageClass.STANDARD)
            .setIamConfiguration(
                IamConfiguration.newBuilder().setIsUniformBucketLevelAccessEnabled(true).build())
            .setHierarchicalNamespace(HierarchicalNamespace.newBuilder().setEnabled(true).build())
            .setLifecycleRules(
                ImmutableList.of(
                    new LifecycleRule(
                        LifecycleAction.newDeleteAction(),
                        LifecycleCondition.newBuilder().setAge(7).build())))
            .build();
    verify(mockStorage).create(expectedBucketInfo);
  }

  @ParameterizedTest
  @ValueSource(strings = {"RAPID", "rapid"})
  void createBucket_withZonalPlacementAndRapidStorageClass_setsCustomPlacementRapidAndHns(
      String storageClass) throws IOException {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsCreateBucketOptions options =
        GcsCreateBucketOptions.builder()
            .setHierarchicalNamespaceEnabled(true)
            .setLocation("us-central1")
            .setStorageClass(storageClass)
            .setZonalPlacement("us-central1-a")
            .build();

    clientWithMock.createBucket(TEST_BUCKET_NAME, options);

    BucketInfo expectedBucketInfo =
        BucketInfo.newBuilder(TEST_BUCKET_NAME)
            .setLocation("us-central1")
            .setStorageClass(StorageClass.valueOf(RAPID_STORAGE_CLASS))
            .setIamConfiguration(
                IamConfiguration.newBuilder().setIsUniformBucketLevelAccessEnabled(true).build())
            .setHierarchicalNamespace(HierarchicalNamespace.newBuilder().setEnabled(true).build())
            .setCustomPlacementConfig(
                CustomPlacementConfig.newBuilder()
                    .setDataLocations(ImmutableList.of("us-central1-a"))
                    .build())
            .build();
    verify(mockStorage).create(expectedBucketInfo);
  }

  @Test
  void createBucket_withZonalPlacementAndUnsetStorageClass_defaultsToRapid() throws IOException {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsCreateBucketOptions options =
        GcsCreateBucketOptions.builder()
            .setHierarchicalNamespaceEnabled(true)
            .setLocation("us-central1")
            .setZonalPlacement("us-central1-a")
            .build();

    clientWithMock.createBucket(TEST_BUCKET_NAME, options);

    BucketInfo expectedBucketInfo =
        BucketInfo.newBuilder(TEST_BUCKET_NAME)
            .setLocation("us-central1")
            .setStorageClass(StorageClass.valueOf(RAPID_STORAGE_CLASS))
            .setIamConfiguration(
                IamConfiguration.newBuilder().setIsUniformBucketLevelAccessEnabled(true).build())
            .setHierarchicalNamespace(HierarchicalNamespace.newBuilder().setEnabled(true).build())
            .setCustomPlacementConfig(
                CustomPlacementConfig.newBuilder()
                    .setDataLocations(ImmutableList.of("us-central1-a"))
                    .build())
            .build();
    verify(mockStorage).create(expectedBucketInfo);
  }

  @Test
  void createBucket_zonalPlacementWithoutHns_throwsUnsupportedOperationException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsCreateBucketOptions options =
        GcsCreateBucketOptions.builder()
            .setHierarchicalNamespaceEnabled(false)
            .setZonalPlacement("us-central1-a")
            .build();

    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> clientWithMock.createBucket(TEST_BUCKET_NAME, options));
    assertThat(exception).hasMessageThat().isEqualTo("Zonal buckets must have HNS Enabled.");
    verify(mockStorage, never()).create(any(BucketInfo.class));
  }

  @Test
  void createBucket_zonalPlacementWithNonRapidStorageClass_throwsUnsupportedOperationException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsCreateBucketOptions options =
        GcsCreateBucketOptions.builder()
            .setHierarchicalNamespaceEnabled(true)
            .setZonalPlacement("us-central1-a")
            .setStorageClass("STANDARD")
            .build();

    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> clientWithMock.createBucket(TEST_BUCKET_NAME, options));
    assertThat(exception).hasMessageThat().isEqualTo("Zonal bucket storage class must be RAPID");
    verify(mockStorage, never()).create(any(BucketInfo.class));
  }

  @Test
  void createBucket_zonalPlacementWithTtl_throwsUnsupportedOperationException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsCreateBucketOptions options =
        GcsCreateBucketOptions.builder()
            .setHierarchicalNamespaceEnabled(true)
            .setZonalPlacement("us-central1-a")
            .setTtl(Duration.ofDays(7))
            .build();

    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> clientWithMock.createBucket(TEST_BUCKET_NAME, options));
    assertThat(exception).hasMessageThat().isEqualTo("Zonal buckets do not support TTL");
    verify(mockStorage, never()).create(any(BucketInfo.class));
  }

  @Test
  void createBucket_nonZonalWithInvalidStorageClass_throwsIllegalArgumentException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsCreateBucketOptions options =
        GcsCreateBucketOptions.builder().setStorageClass("INVALID_STORAGE_CLASS").build();

    assertThrows(
        IllegalArgumentException.class,
        () -> clientWithMock.createBucket(TEST_BUCKET_NAME, options));
    verify(mockStorage, never()).create(any(BucketInfo.class));
  }

  @Test
  void createBucket_alreadyExists_throwsFileAlreadyExistsException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    when(mockStorage.create(any(BucketInfo.class)))
        .thenThrow(new StorageException(409, "Bucket already exists"));

    assertThrows(
        FileAlreadyExistsException.class, () -> clientWithMock.createBucket(TEST_BUCKET_NAME));
  }

  @Test
  void createBucket_storageException_throwsIOException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    when(mockStorage.create(any(BucketInfo.class)))
        .thenThrow(new StorageException(500, "Internal Server Error"));

    assertThrows(IOException.class, () -> clientWithMock.createBucket(TEST_BUCKET_NAME));
  }

  @Test
  void createBucket_nullBucketName_throwsNullPointerException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    assertThrows(NullPointerException.class, () -> clientWithMock.createBucket(null));
  }

  @Test
  void createBucket_emptyBucketName_throwsIllegalArgumentException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    assertThrows(IllegalArgumentException.class, () -> clientWithMock.createBucket(""));
  }

  @Test
  void createBucket_nullOptions_throwsNullPointerException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    assertThrows(
        NullPointerException.class, () -> clientWithMock.createBucket(TEST_BUCKET_NAME, null));
  }

  @Test
  void createEmptyObject_success() throws IOException {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("dir/").build();

    clientWithMock.createEmptyObject(itemId);

    verify(mockStorage)
        .create(
            eq(
                BlobInfo.newBuilder(BlobId.of(TEST_BUCKET_NAME, "dir/"))
                    .setContentType("application/octet-stream")
                    .build()),
            eq(Storage.BlobTargetOption.disableGzipContent()),
            eq(Storage.BlobTargetOption.doesNotExist()));
  }

  @Test
  void createEmptyObject_conflict_throwsFileAlreadyExistsException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId dirItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("dir/").build();
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(new StorageException(409, "Object already exists"));

    FileAlreadyExistsException e =
        assertThrows(
            FileAlreadyExistsException.class, () -> clientWithMock.createEmptyObject(dirItemId));
    assertThat(e.getFile()).isEqualTo("gs://" + TEST_BUCKET_NAME + "/dir/");
    verify(mockStorage, never()).get(any(BlobId.class), any(Storage.BlobGetOption[].class));
  }

  @Test
  void createEmptyObject_nonDirectoryPreconditionFailed_throwsFileAlreadyExistsException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId fileItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT_ID).build();
    GcsWriteOptions noOverwriteOptions =
        GcsWriteOptions.builder().setOverwriteExisting(false).build();
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(new StorageException(412, "Precondition Failed"));

    FileAlreadyExistsException e =
        assertThrows(
            FileAlreadyExistsException.class,
            () -> clientWithMock.createEmptyObject(fileItemId, noOverwriteOptions));
    assertThat(e.getFile()).isEqualTo("gs://" + TEST_BUCKET_NAME + "/" + TEST_OBJECT_ID);
    verify(mockStorage, never()).get(any(BlobId.class), any(Storage.BlobGetOption[].class));
  }

  @Test
  void createEmptyObject_filePreconditionFailedWhenOverwriteAllowed_throwsIOException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId fileItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT_ID).build();
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(new StorageException(412, "Precondition Failed"));

    IOException fileException =
        assertThrows(IOException.class, () -> clientWithMock.createEmptyObject(fileItemId));

    assertThat(fileException).isNotInstanceOf(FileAlreadyExistsException.class);
    verify(mockStorage, never()).get(any(BlobId.class), any(Storage.BlobGetOption[].class));
  }

  @Test
  void createEmptyObject_directoryWithGenerationPreconditionFailed_throwsIOException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId dirItemIdWithGen =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET_NAME)
            .setObjectName("dir/")
            .setContentGeneration(TEST_CONTENT_GENERATION)
            .build();
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(new StorageException(412, "Precondition Failed"));

    IOException dirGenException =
        assertThrows(IOException.class, () -> clientWithMock.createEmptyObject(dirItemIdWithGen));

    assertThat(dirGenException).isNotInstanceOf(FileAlreadyExistsException.class);
    verify(mockStorage, never()).get(any(BlobId.class), any(Storage.BlobGetOption[].class));
  }

  @Test
  void createEmptyObject_whenOverwriteFalseAndGenerationPresent_usesDoesNotExist()
      throws IOException {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId itemIdWithGen =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET_NAME)
            .setObjectName(TEST_DIR)
            .setContentGeneration(TEST_CONTENT_GENERATION)
            .build();
    GcsWriteOptions noOverwriteOptions =
        GcsWriteOptions.builder().setOverwriteExisting(false).build();

    clientWithMock.createEmptyObject(itemIdWithGen, noOverwriteOptions);

    verify(mockStorage)
        .create(
            eq(
                BlobInfo.newBuilder(BlobId.of(TEST_BUCKET_NAME, TEST_DIR))
                    .setContentType("application/octet-stream")
                    .build()),
            eq(Storage.BlobTargetOption.disableGzipContent()),
            eq(Storage.BlobTargetOption.doesNotExist()));
  }

  @Test
  void createEmptyObject_directoryPreconditionFailedWhenObjectDoesNotExist_throwsIOException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(new StorageException(412, "Precondition Failed (e.g. CMEK policy)"));
    when(mockStorage.get(
            eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)), any(Storage.BlobGetOption[].class)))
        .thenReturn(null);

    IOException e =
        assertThrows(IOException.class, () -> clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID));

    assertThat(e).isNotInstanceOf(FileAlreadyExistsException.class);
    assertThat(e).hasMessageThat().contains("Failed to create empty object");
    verify(mockStorage, times(1))
        .get(eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)), any(Storage.BlobGetOption[].class));
  }

  @Test
  void createEmptyObject_storageException_throwsIOException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("dir/").build();
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(new StorageException(500, "Internal Server Error"));

    assertThrows(IOException.class, () -> clientWithMock.createEmptyObject(itemId));
  }

  @Test
  void createEmptyObject_withOptionsAndGeneration_setsBlobInfoAndTargetOptions()
      throws IOException {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId itemIdWithGen =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET_NAME)
            .setObjectName(TEST_DIR)
            .setContentGeneration(TEST_CONTENT_GENERATION)
            .build();
    GcsWriteOptions options =
        GcsWriteOptions.builder()
            .setDisableGzipContent(true)
            .setOverwriteExisting(true)
            .setContentType(TEST_DIRECTORY_CONTENT_TYPE)
            .setContentEncoding(TEST_CONTENT_ENCODING)
            .setMetadata(ImmutableMap.of("key", "val".getBytes(StandardCharsets.UTF_8)))
            .setKmsKeyName("test-kms-key")
            .setEncryptionKey(TEST_ENCRYPTION_KEY)
            .setUserProject("test-user-project")
            .build();

    clientWithMock.createEmptyObject(itemIdWithGen, options);

    verify(mockStorage)
        .create(
            eq(
                BlobInfo.newBuilder(BlobId.of(TEST_BUCKET_NAME, TEST_DIR))
                    .setContentType(TEST_DIRECTORY_CONTENT_TYPE)
                    .setContentEncoding(TEST_CONTENT_ENCODING)
                    .setMetadata(ImmutableMap.of("key", "dmFs"))
                    .build()),
            eq(Storage.BlobTargetOption.disableGzipContent()),
            eq(Storage.BlobTargetOption.generationMatch(TEST_CONTENT_GENERATION)),
            eq(Storage.BlobTargetOption.kmsKeyName("test-kms-key")),
            eq(Storage.BlobTargetOption.encryptionKey(TEST_ENCRYPTION_KEY)),
            eq(Storage.BlobTargetOption.userProject("test-user-project")));
  }

  @Test
  void createEmptyObject_withOptionsWithoutGeneration_setsBlobInfoAndTargetOptions()
      throws IOException {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId itemIdObj =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT_ID).build();
    GcsWriteOptions options =
        GcsWriteOptions.builder()
            .setDisableGzipContent(true)
            .setOverwriteExisting(true)
            .setContentType(TEST_DIRECTORY_CONTENT_TYPE)
            .setContentEncoding(TEST_CONTENT_ENCODING)
            .setMetadata(ImmutableMap.of("key", "val".getBytes(StandardCharsets.UTF_8)))
            .setKmsKeyName("test-kms-key")
            .setEncryptionKey(TEST_ENCRYPTION_KEY)
            .setUserProject("test-user-project")
            .build();

    clientWithMock.createEmptyObject(itemIdObj, options);

    verify(mockStorage)
        .create(
            eq(
                BlobInfo.newBuilder(BlobId.of(TEST_BUCKET_NAME, TEST_OBJECT_ID))
                    .setContentType(TEST_DIRECTORY_CONTENT_TYPE)
                    .setContentEncoding(TEST_CONTENT_ENCODING)
                    .setMetadata(ImmutableMap.of("key", "dmFs"))
                    .build()),
            eq(Storage.BlobTargetOption.disableGzipContent()),
            eq(Storage.BlobTargetOption.kmsKeyName("test-kms-key")),
            eq(Storage.BlobTargetOption.encryptionKey(TEST_ENCRYPTION_KEY)),
            eq(Storage.BlobTargetOption.userProject("test-user-project")));
  }

  @Test
  void createEmptyObject_nullOptions_throwsNullPointerException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    assertThrows(
        NullPointerException.class, () -> clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID, null));
  }

  @ParameterizedTest
  @ValueSource(ints = {408, 412, 429, 500, 502, 503, 504})
  void createEmptyObject_ignoreExceptionWhenMarkerAlreadyExists_success(int statusCode)
      throws IOException {
    Storage mockStorage = mock(Storage.class);
    Blob mockBlob = mock(Blob.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    when(mockBlob.getSize()).thenReturn(0L);
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(new StorageException(statusCode, "Simulated error"));
    when(mockStorage.get(
            eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)),
            eq(Storage.BlobGetOption.fields(Storage.BlobField.SIZE))))
        .thenReturn(mockBlob);

    clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID);

    verify(mockStorage).create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class));
    verify(mockStorage, times(1))
        .get(
            eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)),
            eq(Storage.BlobGetOption.fields(Storage.BlobField.SIZE)));
  }

  private static Stream<Arguments> rapidCreateEmptyObjectPreconditionCases() {
    GcsItemId fileItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT_ID).build();
    GcsItemId fileWithGen =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET_NAME)
            .setObjectName(TEST_OBJECT_ID)
            .setContentGeneration(TEST_CONTENT_GENERATION)
            .build();
    GcsWriteOptions defaultOptions = GcsWriteOptions.builder().build();
    GcsWriteOptions noOverwriteOptions =
        GcsWriteOptions.builder().setOverwriteExisting(false).build();

    return Stream.of(
        // 1. Directory, no generation -> doesNotExist
        Arguments.of(
            TEST_DIR_ITEM_ID,
            defaultOptions,
            BlobId.of(TEST_BUCKET_NAME, TEST_DIR),
            new Storage.BlobWriteOption[] {
              Storage.BlobWriteOption.doesNotExist(), Storage.BlobWriteOption.disableGzipContent()
            }),
        // 2. File, no generation, overwriteExisting=false -> doesNotExist
        Arguments.of(
            fileItemId,
            noOverwriteOptions,
            BlobId.of(TEST_BUCKET_NAME, TEST_OBJECT_ID),
            new Storage.BlobWriteOption[] {
              Storage.BlobWriteOption.doesNotExist(), Storage.BlobWriteOption.disableGzipContent()
            }),
        // 3. File, no generation, overwriteExisting=true (default) -> no generation precondition
        Arguments.of(
            fileItemId,
            defaultOptions,
            BlobId.of(TEST_BUCKET_NAME, TEST_OBJECT_ID),
            new Storage.BlobWriteOption[] {Storage.BlobWriteOption.disableGzipContent()}),
        // 4. Generation set, overwriteExisting=true (default) -> generationMatch
        Arguments.of(
            fileWithGen,
            defaultOptions,
            BlobId.of(TEST_BUCKET_NAME, TEST_OBJECT_ID),
            new Storage.BlobWriteOption[] {
              Storage.BlobWriteOption.generationMatch(TEST_CONTENT_GENERATION),
              Storage.BlobWriteOption.disableGzipContent()
            }),
        // 5. Generation set, overwriteExisting=false -> doesNotExist
        Arguments.of(
            fileWithGen,
            noOverwriteOptions,
            BlobId.of(TEST_BUCKET_NAME, TEST_OBJECT_ID),
            new Storage.BlobWriteOption[] {
              Storage.BlobWriteOption.doesNotExist(), Storage.BlobWriteOption.disableGzipContent()
            }));
  }

  @ParameterizedTest
  @MethodSource("rapidCreateEmptyObjectPreconditionCases")
  void createEmptyObject_rapidStorageClass_usesAppendableUploadWithMatchingPreconditions(
      GcsItemId itemId,
      GcsWriteOptions writeOptions,
      BlobId expectedBlobId,
      Storage.BlobWriteOption[] expectedWriteOptions)
      throws IOException {
    Storage mockStorage = mock(Storage.class);
    Bucket mockBucket = mock(Bucket.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    BlobAppendableUpload mockUpload = mock(BlobAppendableUpload.class);
    AppendableUploadWriteableByteChannel mockChannel =
        mock(AppendableUploadWriteableByteChannel.class);
    when(mockBucket.getStorageClass()).thenReturn(StorageClass.valueOf(RAPID_STORAGE_CLASS));
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_BUCKET_NAME), any(BucketGetOption.class));
    when(mockStorage.blobAppendableUpload(
            any(BlobInfo.class),
            any(BlobAppendableUploadConfig.class),
            any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockUpload);
    when(mockUpload.open()).thenReturn(mockChannel);

    clientWithMock.createEmptyObject(itemId, writeOptions);

    ArgumentCaptor<Storage.BlobWriteOption[]> optionsCaptor =
        ArgumentCaptor.forClass(Storage.BlobWriteOption[].class);
    verify(mockStorage)
        .blobAppendableUpload(
            eq(
                BlobInfo.newBuilder(expectedBlobId)
                    .setContentType("application/octet-stream")
                    .build()),
            any(BlobAppendableUploadConfig.class),
            optionsCaptor.capture());
    assertThat(optionsCaptor.getValue()).asList().containsExactlyElementsIn(expectedWriteOptions);
    verify(mockChannel).write(any(java.nio.ByteBuffer.class));
    verify(mockChannel).finalizeAndClose();
  }

  @Test
  void createEmptyObject_withUserProject_forwardsUserProjectOnLookupAfterFailedCreate()
      throws IOException {
    Storage mockStorage = mock(Storage.class);
    Blob mockBlob = mock(Blob.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsWriteOptions options = GcsWriteOptions.builder().setUserProject("test-user-project").build();
    when(mockBlob.getSize()).thenReturn(0L);
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(new StorageException(412, "Precondition Failed"));
    when(mockStorage.get(
            eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)),
            eq(Storage.BlobGetOption.fields(Storage.BlobField.SIZE)),
            eq(Storage.BlobGetOption.userProject("test-user-project"))))
        .thenReturn(mockBlob);

    clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID, options);

    verify(mockStorage, times(1))
        .get(
            eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)),
            eq(Storage.BlobGetOption.fields(Storage.BlobField.SIZE)),
            eq(Storage.BlobGetOption.userProject("test-user-project")));
  }

  @Test
  void createEmptyObject_rapidStorageClass_ignores412WhenZeroByteMarkerExists() throws IOException {
    Storage mockStorage = mock(Storage.class);
    Bucket mockBucket = mock(Bucket.class);
    Blob mockBlob = mock(Blob.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    BlobAppendableUpload mockUpload = mock(BlobAppendableUpload.class);
    when(mockBucket.getStorageClass()).thenReturn(StorageClass.valueOf(RAPID_STORAGE_CLASS));
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_BUCKET_NAME), any(BucketGetOption.class));
    when(mockStorage.blobAppendableUpload(
            any(BlobInfo.class),
            any(BlobAppendableUploadConfig.class),
            any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockUpload);
    when(mockUpload.open())
        .thenThrow(new IOException(new StorageException(412, "Precondition Failed")));
    when(mockBlob.getSize()).thenReturn(0L);
    when(mockStorage.get(
            eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)), any(Storage.BlobGetOption[].class)))
        .thenReturn(mockBlob);

    clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID);

    verify(mockStorage, times(1))
        .get(eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)), any(Storage.BlobGetOption[].class));
  }

  @Test
  void createEmptyObject_nonEmptyObjectExistsDuringVerification_doesNotIgnoreException() {
    Storage mockStorage = mock(Storage.class);
    Blob mockBlob = mock(Blob.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    when(mockBlob.getSize()).thenReturn(10L);
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(new StorageException(412, "Precondition Failed"));
    when(mockStorage.get(
            eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)), any(Storage.BlobGetOption[].class)))
        .thenReturn(mockBlob);

    FileAlreadyExistsException e =
        assertThrows(
            FileAlreadyExistsException.class,
            () -> clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID));
    assertThat(e.getFile()).isEqualTo("gs://" + TEST_BUCKET_NAME + "/" + TEST_DIR);
  }

  @Test
  void createEmptyObject_readErrorDuringVerification_throwsVerificationErrorWithContext() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    StorageException originalException = new StorageException(429, "Rate limit exceeded");
    StorageException getException = new StorageException(403, "Permission Denied");
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(originalException);
    when(mockStorage.get(
            eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)), any(Storage.BlobGetOption[].class)))
        .thenThrow(getException);

    IOException e =
        assertThrows(IOException.class, () -> clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID));

    assertThat(e).hasMessageThat().contains("Failed to verify existence of 0-byte object");
    assertThat(e).hasCauseThat().isEqualTo(originalException);
    assertThat(e.getCause().getSuppressed()).asList().containsExactly(getException);
  }

  @Test
  void createEmptyObject_rapidStorageClass_unwrapsStorageExceptionFromUpload() throws IOException {
    Storage mockStorage = mock(Storage.class);
    Bucket mockBucket = mock(Bucket.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    when(mockBucket.getStorageClass()).thenReturn(StorageClass.valueOf(RAPID_STORAGE_CLASS));
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_BUCKET_NAME), any(BucketGetOption.class));
    BlobAppendableUpload mockUpload = mock(BlobAppendableUpload.class);
    when(mockStorage.blobAppendableUpload(
            any(BlobInfo.class),
            any(BlobAppendableUploadConfig.class),
            any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockUpload);
    StorageException storageEx = new StorageException(500, "Internal Error");
    when(mockUpload.open()).thenThrow(new IOException(storageEx));

    IOException e =
        assertThrows(IOException.class, () -> clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID));

    assertThat(e.getCause()).isEqualTo(storageEx);
  }

  @Test
  void createEmptyObject_rapidStorageClass_rethrowsIOExceptionFromUpload() throws IOException {
    Storage mockStorage = mock(Storage.class);
    Bucket mockBucket = mock(Bucket.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    when(mockBucket.getStorageClass()).thenReturn(StorageClass.valueOf(RAPID_STORAGE_CLASS));
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_BUCKET_NAME), any(BucketGetOption.class));
    BlobAppendableUpload mockUpload = mock(BlobAppendableUpload.class);
    when(mockStorage.blobAppendableUpload(
            any(BlobInfo.class),
            any(BlobAppendableUploadConfig.class),
            any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockUpload);
    IOException ioEx = new IOException("Socket closed");
    when(mockUpload.open()).thenThrow(ioEx);

    IOException e =
        assertThrows(IOException.class, () -> clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID));

    assertThat(e).isEqualTo(ioEx);
  }

  @Test
  void createEmptyObject_rapidStorageClass_finalizeAndCloseThrows412_ignoresWhenZeroByteExists()
      throws IOException {
    Storage mockStorage = mock(Storage.class);
    Bucket mockBucket = mock(Bucket.class);
    Blob mockBlob = mock(Blob.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    BlobAppendableUpload mockUpload = mock(BlobAppendableUpload.class);
    AppendableUploadWriteableByteChannel mockChannel =
        mock(AppendableUploadWriteableByteChannel.class);
    when(mockBucket.getStorageClass()).thenReturn(StorageClass.valueOf(RAPID_STORAGE_CLASS));
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_BUCKET_NAME), any(BucketGetOption.class));
    when(mockStorage.blobAppendableUpload(
            any(BlobInfo.class),
            any(BlobAppendableUploadConfig.class),
            any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockUpload);
    when(mockUpload.open()).thenReturn(mockChannel);
    doThrow(new IOException("Stream failed", new StorageException(412, "Precondition Failed")))
        .when(mockChannel)
        .finalizeAndClose();
    when(mockBlob.getSize()).thenReturn(0L);
    when(mockStorage.get(
            eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)), any(Storage.BlobGetOption[].class)))
        .thenReturn(mockBlob);

    clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID);

    verify(mockStorage, times(1))
        .get(eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)), any(Storage.BlobGetOption[].class));
  }

  @Test
  void createEmptyObject_rapidStorageClass_openThrowsUnchecked412_throwsFileAlreadyExists()
      throws IOException {
    Storage mockStorage = mock(Storage.class);
    Bucket mockBucket = mock(Bucket.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    BlobAppendableUpload mockUpload = mock(BlobAppendableUpload.class);
    GcsItemId fileItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT_ID).build();
    GcsWriteOptions noOverwriteOptions =
        GcsWriteOptions.builder().setOverwriteExisting(false).build();
    StorageException storageEx = new StorageException(412, "Precondition Failed");
    when(mockBucket.getStorageClass()).thenReturn(StorageClass.valueOf(RAPID_STORAGE_CLASS));
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_BUCKET_NAME), any(BucketGetOption.class));
    when(mockStorage.blobAppendableUpload(
            any(BlobInfo.class),
            any(BlobAppendableUploadConfig.class),
            any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockUpload);
    when(mockUpload.open()).thenThrow(storageEx);

    FileAlreadyExistsException e =
        assertThrows(
            FileAlreadyExistsException.class,
            () -> clientWithMock.createEmptyObject(fileItemId, noOverwriteOptions));

    assertThat(e.getFile()).isEqualTo("gs://" + TEST_BUCKET_NAME + "/" + TEST_OBJECT_ID);
    assertThat(e).hasCauseThat().isSameInstanceAs(storageEx);
    verify(mockStorage, never()).get(any(BlobId.class), any(Storage.BlobGetOption[].class));
  }

  @Test
  void createEmptyObject_rapidStorageClass_finalizeAndCloseThrowsNonStorageCause_rethrowsSame()
      throws IOException {
    Storage mockStorage = mock(Storage.class);
    Bucket mockBucket = mock(Bucket.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    BlobAppendableUpload mockUpload = mock(BlobAppendableUpload.class);
    AppendableUploadWriteableByteChannel mockChannel =
        mock(AppendableUploadWriteableByteChannel.class);
    IOException timeoutIoEx = new IOException("Timed out", new TimeoutException("Deadline"));
    when(mockBucket.getStorageClass()).thenReturn(StorageClass.valueOf(RAPID_STORAGE_CLASS));
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_BUCKET_NAME), any(BucketGetOption.class));
    when(mockStorage.blobAppendableUpload(
            any(BlobInfo.class),
            any(BlobAppendableUploadConfig.class),
            any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockUpload);
    when(mockUpload.open()).thenReturn(mockChannel);
    doThrow(timeoutIoEx).when(mockChannel).finalizeAndClose();

    IOException e =
        assertThrows(IOException.class, () -> clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID));

    assertThat(e).isSameInstanceAs(timeoutIoEx);
    verify(mockStorage, never()).get(any(BlobId.class), any(Storage.BlobGetOption[].class));
  }

  @Test
  void createFolder_success() throws IOException {
    StorageControlClient mockControlClient = mock(StorageControlClient.class);
    GcsClientImpl clientWithMockControl = createClientWithMockControl(mockControlClient);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("dir/").build();
    CreateFolderRequest expectedRequest =
        CreateFolderRequest.newBuilder()
            .setParent("projects/_/buckets/" + TEST_BUCKET_NAME)
            .setFolderId("dir")
            .setRecursive(true)
            .build();

    clientWithMockControl.createFolder(itemId, true);

    verify(mockControlClient).createFolder(expectedRequest);
  }

  @Test
  void createFolder_emptyFolderName_throwsIllegalArgumentException() {
    StorageControlClient mockControlClient = mock(StorageControlClient.class);
    GcsClientImpl clientWithMockControl = createClientWithMockControl(mockControlClient);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("/").build();

    assertThrows(
        IllegalArgumentException.class, () -> clientWithMockControl.createFolder(itemId, true));
  }

  @Test
  void createFolder_alreadyExists_throwsFileAlreadyExistsException() {
    StorageControlClient mockControlClient = mock(StorageControlClient.class);
    GcsClientImpl clientWithMockControl = createClientWithMockControl(mockControlClient);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("dir/").build();
    AlreadyExistsException alreadyExistsException = mock(AlreadyExistsException.class);
    when(mockControlClient.createFolder(any(CreateFolderRequest.class)))
        .thenThrow(alreadyExistsException);

    FileAlreadyExistsException e =
        assertThrows(
            FileAlreadyExistsException.class,
            () -> clientWithMockControl.createFolder(itemId, true));
    assertThat(e.getFile()).isEqualTo("gs://" + TEST_BUCKET_NAME + "/dir/");
  }

  @Test
  void createFolder_runtimeException_throwsIOException() {
    StorageControlClient mockControlClient = mock(StorageControlClient.class);
    GcsClientImpl clientWithMockControl = createClientWithMockControl(mockControlClient);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("dir/").build();
    when(mockControlClient.createFolder(any(CreateFolderRequest.class)))
        .thenThrow(new RuntimeException("RPC failure"));

    assertThrows(IOException.class, () -> clientWithMockControl.createFolder(itemId, true));
  }

  @Test
  void close_withGrpcStorageInitialized_closesGrpcStorageAndNullsReference() throws Exception {
    Storage mockStorage = mock(Storage.class);
    Storage mockGrpcStorage = mock(Storage.class);
    StorageControlClient mockControlClient = mock(StorageControlClient.class);
    GcsClientImpl localClient = createClientWithMockStorage(mockStorage);
    localClient.storageControlClient = mockControlClient;
    localClient.grpcStorage = mockGrpcStorage;

    localClient.close();

    verify(mockStorage).close();
    verify(mockControlClient).close();
    verify(mockGrpcStorage).close();
    assertThat(localClient.storageControlClient).isNull();
    assertThat(localClient.grpcStorage).isNull();
  }

  @Test
  void lazyGetGrpcStorage_initializesWhenNullAndReusesInstance() throws IOException {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    Storage createdInstance = clientWithMock.lazyGetGrpcStorage();
    Storage cachedInstance = clientWithMock.lazyGetGrpcStorage();

    assertThat(createdInstance).isSameInstanceAs(mockStorage);
    assertThat(cachedInstance).isSameInstanceAs(mockStorage);
    assertThat(clientWithMock.grpcStorage).isSameInstanceAs(mockStorage);
  }

  @Test
  void lazyGetGrpcStorage_grpcProtocolEnabled_returnsStorageDirectly() throws IOException {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl client = createClientWithMockStorage(mockStorage, bidiClientOptions());

    Storage actualStorage = client.lazyGetGrpcStorage();

    assertThat(actualStorage).isSameInstanceAs(mockStorage);
    assertThat(client.grpcStorage).isNull();
  }

  @Test
  void lazyGetGrpcStorage_afterClose_throwsIOException() {
    GcsClientImpl localClient = createClientWithMockStorage(mock(Storage.class));

    localClient.close();

    IOException e = assertThrows(IOException.class, localClient::lazyGetGrpcStorage);
    assertThat(e).hasMessageThat().contains("GrpcStorage is closed");
  }

  @Test
  void lazyGetGrpcStorage_closedWhileWaitingForLock_throwsIOException() throws Exception {
    GcsClientImpl client = createClientWithMockStorage(mock(Storage.class));
    CountDownLatch started = new CountDownLatch(1);
    AtomicReference<IOException> thrown = new AtomicReference<>();

    CompletableFuture<Void> future;
    synchronized (client) {
      future =
          CompletableFuture.runAsync(
              () -> {
                started.countDown();
                thrown.set(assertThrows(IOException.class, client::lazyGetGrpcStorage));
              });
      started.await(5, TimeUnit.SECONDS);
      Thread.sleep(50);
      client.close();
    }
    future.get(5, TimeUnit.SECONDS);

    assertThat(thrown.get()).hasMessageThat().contains("GrpcStorage is closed");
  }

  @Test
  void createFolder_afterClose_propagatesIOExceptionDirectly() {
    GcsClientImpl localClient = createClientWithMockStorage(mock(Storage.class));
    localClient.close();

    IOException e =
        assertThrows(IOException.class, () -> localClient.createFolder(TEST_DIR_ITEM_ID, true));
    assertThat(e).hasMessageThat().isEqualTo("StorageControlClient is closed");
    assertThat(e.getCause()).isNull();
  }

  @Test
  void lazyGetGrpcStorage_concurrentCalls_instantiatesExactlyOnce() throws Exception {
    Storage mockStorageInstance = mock(Storage.class);
    AtomicInteger factoryInvocations = new AtomicInteger();
    GcsClientImpl client =
        new GcsClientImpl(
            TEST_GCS_CLIENT_OPTIONS,
            executorServiceSupplier,
            telemetry,
            newBucketPropertiesCache()) {
          @Override
          protected Storage createGrpcStorage(Optional<Credentials> credentials) {
            factoryInvocations.incrementAndGet();
            return mockStorageInstance;
          }
        };
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    Callable<Storage> task =
        () -> {
          ready.countDown();
          start.await();
          return client.lazyGetGrpcStorage();
        };

    try {
      Future<Storage> f1 = executor.submit(task);
      Future<Storage> f2 = executor.submit(task);
      ready.await(5, TimeUnit.SECONDS);
      start.countDown();
      Storage result1 = f1.get(5, TimeUnit.SECONDS);
      Storage result2 = f2.get(5, TimeUnit.SECONDS);

      assertThat(result1).isSameInstanceAs(mockStorageInstance);
      assertThat(result2).isSameInstanceAs(mockStorageInstance);
      assertThat(factoryInvocations.get()).isEqualTo(1);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void createEmptyObject_repeatedCalls_cachesBucketProperties() throws IOException {
    Storage mockStorage = mock(Storage.class);
    Bucket mockBucket = mockBucketWithHns(false);
    when(mockBucket.getStorageClass()).thenReturn(StorageClass.STANDARD);
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_BUCKET_NAME), any(BucketGetOption.class));
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID);
    clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID);

    verify(mockStorage, times(1)).get(eq(TEST_BUCKET_NAME), any(BucketGetOption.class));
    verify(mockStorage, times(2))
        .create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class));
  }

  @Test
  void getBucketProperties_andCreateEmptyObject_shareBucketPropertiesCache() throws IOException {
    Storage mockStorage = mock(Storage.class);
    Bucket mockBucket = mockBucketWithHns(false);
    when(mockBucket.getStorageClass()).thenReturn(StorageClass.STANDARD);
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_BUCKET_NAME), any(BucketGetOption.class));
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    assertThat(clientWithMock.getBucketProperties(TEST_BUCKET_NAME).isHnsEnabled()).isFalse();
    clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID);

    verify(mockStorage, times(1)).get(eq(TEST_BUCKET_NAME), any(BucketGetOption.class));
  }

  @Test
  void createBucket_invalidatesCachedBucketPropertiesOnSuccess() throws IOException {
    Storage mockStorage = mock(Storage.class);
    Bucket hnsBucket = mockBucketWithHns(true);
    when(hnsBucket.getStorageClass()).thenReturn(StorageClass.STANDARD);
    when(mockStorage.get(eq(TEST_BUCKET_NAME), any(BucketGetOption.class)))
        .thenReturn(null)
        .thenReturn(hnsBucket);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    assertThat(clientWithMock.getBucketProperties(TEST_BUCKET_NAME).isHnsEnabled()).isFalse();
    clientWithMock.createBucket(
        TEST_BUCKET_NAME,
        GcsCreateBucketOptions.builder().setHierarchicalNamespaceEnabled(true).build());
    assertThat(clientWithMock.getBucketProperties(TEST_BUCKET_NAME).isHnsEnabled()).isTrue();

    verify(mockStorage, times(2)).get(eq(TEST_BUCKET_NAME), any(BucketGetOption.class));
  }

  @Test
  void createBucket_conflict409_invalidatesCachedBucketProperties() throws IOException {
    Storage mockStorage = mock(Storage.class);
    Bucket hnsBucket = mockBucketWithHns(true);
    when(hnsBucket.getStorageClass()).thenReturn(StorageClass.STANDARD);
    when(mockStorage.get(eq(TEST_BUCKET_NAME), any(BucketGetOption.class)))
        .thenReturn(null)
        .thenReturn(hnsBucket);
    when(mockStorage.create(any(BucketInfo.class)))
        .thenThrow(new StorageException(409, "Conflict"));
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    assertThat(clientWithMock.getBucketProperties(TEST_BUCKET_NAME).isHnsEnabled()).isFalse();
    assertThrows(
        FileAlreadyExistsException.class,
        () ->
            clientWithMock.createBucket(
                TEST_BUCKET_NAME,
                GcsCreateBucketOptions.builder().setHierarchicalNamespaceEnabled(true).build()));
    assertThat(clientWithMock.getBucketProperties(TEST_BUCKET_NAME).isHnsEnabled()).isTrue();

    verify(mockStorage, times(2)).get(eq(TEST_BUCKET_NAME), any(BucketGetOption.class));
  }

  @Test
  void getBucketProperties_unauthorized401_returnsDefaultProperties() throws IOException {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl localGcsClient = createClientWithMockStorage(mockStorage);
    doThrow(new StorageException(401, "Unauthorized"))
        .when(mockStorage)
        .get(eq(TEST_ERROR_BUCKET), any(BucketGetOption.class));

    BucketProperties properties = localGcsClient.getBucketProperties(TEST_ERROR_BUCKET);

    assertThat(properties).isEqualTo(BucketProperties.create(false, false));
  }

  @Test
  void createBucket_accessDenied_throwsAccessDeniedException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    when(mockStorage.create(any(BucketInfo.class)))
        .thenThrow(new StorageException(403, "Forbidden"));

    assertThrows(AccessDeniedException.class, () -> clientWithMock.createBucket(TEST_BUCKET_NAME));
  }

  @Test
  void createEmptyObject_notFound_throwsFileNotFoundException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(new StorageException(404, "Bucket not found"));

    assertThrows(
        FileNotFoundException.class, () -> clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID));
  }

  @Test
  void createEmptyObject_accessDenied_throwsAccessDeniedException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(new StorageException(403, "Forbidden"));

    assertThrows(
        AccessDeniedException.class, () -> clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID));
  }

  @Test
  void createEmptyObject_nonDirectoryTransientError_doesNotLookUpAndThrowsIOException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId fileItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT_ID).build();
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(new StorageException(503, "Service Unavailable"));

    assertThrows(IOException.class, () -> clientWithMock.createEmptyObject(fileItemId));
    verify(mockStorage, never()).get(any(BlobId.class), any(Storage.BlobGetOption[].class));
  }

  @Test
  void createFolder_notFound_throwsFileNotFoundException() {
    StorageControlClient mockControlClient = mock(StorageControlClient.class);
    GcsClientImpl clientWithMockControl = createClientWithMockControl(mockControlClient);
    when(mockControlClient.createFolder(any(CreateFolderRequest.class)))
        .thenThrow(mock(NotFoundException.class));

    assertThrows(
        FileNotFoundException.class,
        () -> clientWithMockControl.createFolder(TEST_DIR_ITEM_ID, true));
  }

  @Test
  void createFolder_permissionDenied_throwsAccessDeniedException() {
    StorageControlClient mockControlClient = mock(StorageControlClient.class);
    GcsClientImpl clientWithMockControl = createClientWithMockControl(mockControlClient);
    when(mockControlClient.createFolder(any(CreateFolderRequest.class)))
        .thenThrow(mock(PermissionDeniedException.class));

    assertThrows(
        AccessDeniedException.class,
        () -> clientWithMockControl.createFolder(TEST_DIR_ITEM_ID, true));
  }

  @Test
  void createEmptyObject_rapidBucketLookupAfterFailedCreate_usesGrpcStorage() throws IOException {
    Storage mockHttpStorage = mock(Storage.class);
    Storage mockGrpcStorage = mock(Storage.class);
    Bucket mockBucket = mock(Bucket.class);
    Blob mockBlob = mock(Blob.class);
    BlobAppendableUpload mockUpload = mock(BlobAppendableUpload.class);
    when(mockBucket.getStorageClass()).thenReturn(StorageClass.valueOf(RAPID_STORAGE_CLASS));
    doReturn(mockBucket)
        .when(mockHttpStorage)
        .get(eq(TEST_BUCKET_NAME), any(BucketGetOption.class));
    when(mockGrpcStorage.blobAppendableUpload(
            any(BlobInfo.class),
            any(BlobAppendableUploadConfig.class),
            any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockUpload);
    when(mockUpload.open())
        .thenThrow(new IOException(new StorageException(412, "Precondition Failed")));
    when(mockBlob.getSize()).thenReturn(0L);
    when(mockGrpcStorage.get(
            eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)), any(Storage.BlobGetOption[].class)))
        .thenReturn(mockBlob);
    GcsClientImpl client =
        new GcsClientImpl(
            TEST_GCS_CLIENT_OPTIONS,
            executorServiceSupplier,
            telemetry,
            newBucketPropertiesCache()) {
          @Override
          protected Storage createStorage(Optional<Credentials> credentials) {
            return mockHttpStorage;
          }

          @Override
          protected Storage createGrpcStorage(Optional<Credentials> credentials) {
            return mockGrpcStorage;
          }
        };

    client.createEmptyObject(TEST_DIR_ITEM_ID);

    verify(mockGrpcStorage, times(1))
        .get(eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)), any(Storage.BlobGetOption[].class));
    verify(mockHttpStorage, never()).get(any(BlobId.class), any(Storage.BlobGetOption[].class));
  }
}
