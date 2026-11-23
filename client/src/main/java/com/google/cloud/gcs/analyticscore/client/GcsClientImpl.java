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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.longrunning.OperationSnapshot;
import com.google.api.gax.paging.Page;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.retrying.RetryingFuture;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.api.gax.rpc.UnauthenticatedException;
import com.google.auth.Credentials;
import com.google.cloud.gcs.analyticscore.client.GcsClientOptions.Protocol;
import com.google.cloud.gcs.analyticscore.client.GcsReadChannel.ItemInfoProvider;
import com.google.cloud.gcs.analyticscore.client.auth.GcsAuthOptions;
import com.google.cloud.gcs.analyticscore.client.auth.GcsTransportOptionsFactory;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCache;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUpload.AppendableUploadWriteableByteChannel;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.BucketInfo.CustomPlacementConfig;
import com.google.cloud.storage.BucketInfo.HierarchicalNamespace;
import com.google.cloud.storage.BucketInfo.IamConfiguration;
import com.google.cloud.storage.BucketInfo.LifecycleRule;
import com.google.cloud.storage.BucketInfo.LifecycleRule.LifecycleAction;
import com.google.cloud.storage.BucketInfo.LifecycleRule.LifecycleCondition;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.Storage.BucketField;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.storage.control.v2.CreateFolderRequest;
import com.google.storage.control.v2.DeleteFolderRecursiveMetadata;
import com.google.storage.control.v2.DeleteFolderRecursiveRequest;
import com.google.storage.control.v2.DeleteFolderRequest;
import com.google.storage.control.v2.Folder;
import com.google.storage.control.v2.FolderName;
import com.google.storage.control.v2.GetFolderRequest;
import com.google.storage.control.v2.ListFoldersRequest;
import com.google.storage.control.v2.StorageControlClient;
import com.google.storage.control.v2.StorageControlSettings;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GcsClientImpl implements GcsClient {
  private static final Logger LOG = LoggerFactory.getLogger(GcsClientImpl.class);
  private static final BucketField[] BUCKET_METADATA_FIELDS_ARRAY =
      new BucketField[] {
        BucketField.NAME,
        BucketField.LOCATION,
        BucketField.METAGENERATION,
        BucketField.TIME_CREATED,
        BucketField.UPDATED,
        BucketField.STORAGE_CLASS
      };
  private static final String USER_AGENT_PREFIX = "gcs-analytics-core/";
  private static final String RAPID_STORAGE_CLASS = "RAPID";
  private static final ImmutableSet<Integer> IGNORABLE_EMPTY_OBJECT_CREATE_ERROR_CODES =
      ImmutableSet.of(408, 429, 500, 502, 503, 504);
  private static final String UNIVERSE_DOMAIN_ENV_VAR = "GOOGLE_CLOUD_UNIVERSE_DOMAIN";
  private static final GcsListOptions FIRST_OBJECT_LIST_OPTIONS =
      GcsListOptions.LIST_RECURSIVE.toBuilder().setIncludePrefixObject(true).setPageSize(1).build();
  private static final Map<GcsListOptions.ListField, BlobField> LIST_FIELD_TO_BLOB_FIELD =
      Maps.immutableEnumMap(
          ImmutableMap.<GcsListOptions.ListField, BlobField>builder()
              .put(GcsListOptions.ListField.NAME, BlobField.NAME)
              .put(GcsListOptions.ListField.BUCKET, BlobField.BUCKET)
              .put(GcsListOptions.ListField.GENERATION, BlobField.GENERATION)
              .put(GcsListOptions.ListField.METAGENERATION, BlobField.METAGENERATION)
              .put(GcsListOptions.ListField.SIZE, BlobField.SIZE)
              .put(GcsListOptions.ListField.CONTENT_TYPE, BlobField.CONTENT_TYPE)
              .put(GcsListOptions.ListField.CONTENT_ENCODING, BlobField.CONTENT_ENCODING)
              .put(GcsListOptions.ListField.TIME_CREATED, BlobField.TIME_CREATED)
              .put(GcsListOptions.ListField.UPDATED, BlobField.UPDATED)
              .put(GcsListOptions.ListField.STORAGE_CLASS, BlobField.STORAGE_CLASS)
              .put(GcsListOptions.ListField.CRC32C, BlobField.CRC32C)
              .put(GcsListOptions.ListField.MD5HASH, BlobField.MD5HASH)
              .put(GcsListOptions.ListField.METADATA, BlobField.METADATA)
              .buildOrThrow());

  /** Object metadata fields requested by single-object lookups; same as the list default. */
  private static final BlobField[] BLOB_METADATA_FIELDS_ARRAY =
      GcsListOptions.DEFAULT_FIELDS.stream()
          .map(GcsClientImpl::toBlobField)
          .toArray(BlobField[]::new);

  /** Every {@link GcsListOptions.ListField}, for converting blobs fetched without a projection. */
  private static final Set<GcsListOptions.ListField> ALL_LIST_FIELDS =
      Sets.immutableEnumSet(EnumSet.allOf(GcsListOptions.ListField.class));

  // Storage Control parent resource name prefix for a bucket; the bucket name follows it.
  private static final String STORAGE_CONTROL_BUCKET_PARENT_PREFIX = "projects/_/buckets/";
  // Requested ListFolders page size, as in hadoop-connectors ListFolderOptions.DEFAULT. GCS
  // currently caps each page at 1,000 folders, so listFolders fetches every page.
  private static final int LIST_FOLDERS_PAGE_SIZE = 5000;

  @VisibleForTesting Storage storage;
  @VisibleForTesting volatile Storage grpcStorage;
  private final GcsClientOptions clientOptions;
  private final Optional<Credentials> credentials;
  private Supplier<ExecutorService> executorServiceSupplier;
  private final Telemetry telemetry;
  private final AnalyticsCache<String, BucketProperties> bucketPropertiesCache;
  @VisibleForTesting volatile StorageControlClient storageControlClient;
  private volatile boolean isClosed = false;

  private final GcsAuthOptions authOptions;
  private final Optional<String> universeDomain;

  GcsClientImpl(
      Credentials credentials,
      GcsClientOptions clientOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry,
      AnalyticsCache<String, BucketProperties> bucketPropertiesCache,
      GcsAuthOptions authOptions) {
    this(
        Optional.of(credentials),
        clientOptions,
        executorServiceSupplier,
        telemetry,
        bucketPropertiesCache,
        authOptions);
  }

  GcsClientImpl(
      Credentials credentials,
      GcsClientOptions clientOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry,
      AnalyticsCache<String, BucketProperties> bucketPropertiesCache) {
    this(
        Optional.of(credentials),
        clientOptions,
        executorServiceSupplier,
        telemetry,
        bucketPropertiesCache,
        GcsAuthOptions.builder().build());
  }

  GcsClientImpl(
      GcsClientOptions clientOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry,
      AnalyticsCache<String, BucketProperties> bucketPropertiesCache) {
    this(
        Optional.empty(),
        clientOptions,
        executorServiceSupplier,
        telemetry,
        bucketPropertiesCache,
        GcsAuthOptions.builder().build());
  }

  private GcsClientImpl(
      Optional<Credentials> credentials,
      GcsClientOptions clientOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry,
      AnalyticsCache<String, BucketProperties> bucketPropertiesCache,
      GcsAuthOptions authOptions) {
    this.clientOptions = clientOptions;
    this.credentials = credentials;
    this.executorServiceSupplier = executorServiceSupplier;
    this.telemetry = telemetry;
    this.bucketPropertiesCache =
        checkNotNull(bucketPropertiesCache, "bucketPropertiesCache cannot be null");
    this.authOptions = authOptions;
    this.universeDomain =
        resolveUniverseDomain(
            clientOptions.getUniverseDomain(), System.getenv(UNIVERSE_DOMAIN_ENV_VAR));
    this.storage = createStorage(credentials);
  }

  /**
   * Returns the configured universe domain, or else the {@code GOOGLE_CLOUD_UNIVERSE_DOMAIN}
   * environment variable when it is set and not blank.
   */
  @VisibleForTesting
  static Optional<String> resolveUniverseDomain(
      Optional<String> configuredUniverseDomain, @Nullable String environmentUniverseDomain) {
    return configuredUniverseDomain.or(
        () ->
            Optional.ofNullable(environmentUniverseDomain)
                .map(String::trim)
                .filter(value -> !value.isEmpty()));
  }

  @Override
  public VectoredSeekableByteChannel openReadChannel(
      GcsItemInfo gcsItemInfo, GcsReadOptions readOptions) throws IOException {
    checkNotNull(gcsItemInfo, "itemInfo should not be null");
    checkNotNull(readOptions, "readOptions should not be null");
    checkArgument(
        gcsItemInfo.getItemId().isGcsObject(),
        "Expected GCS object to be provided. But got: %s",
        gcsItemInfo.getItemId());

    if (clientOptions.isBidiEnabled()) {
      return new GcsBidiReadChannel(
          storage, gcsItemInfo, readOptions, executorServiceSupplier, telemetry);
    }

    return new GcsReadChannel(
        storage, gcsItemInfo, readOptions, executorServiceSupplier, telemetry);
  }

  @Override
  public VectoredSeekableByteChannel openReadChannel(
      GcsItemId gcsItemId, GcsReadOptions readOptions) throws IOException {
    checkNotNull(gcsItemId, "gcsItemId should not be null");
    checkNotNull(readOptions, "readOptions should not be null");
    ItemInfoProvider itemInfoProvider = this::getGcsItemInfo;
    if (clientOptions.isBidiEnabled()) {
      return new GcsBidiReadChannel(
          storage, gcsItemId, readOptions, executorServiceSupplier, telemetry, itemInfoProvider);
    } else {
      return new GcsReadChannel(
          storage, gcsItemId, readOptions, executorServiceSupplier, telemetry, itemInfoProvider);
    }
  }

  @Override
  public WritableByteChannel createWriteChannel(GcsItemId itemId, GcsWriteOptions writeOptions)
      throws IOException {
    checkNotNull(itemId, "itemId should not be null");

    GcsWriteOptions resolvedWriteOptions =
        Optional.ofNullable(writeOptions).orElseGet(() -> GcsWriteOptions.builder().build());
    BlobInfo blobInfo = createBlobInfo(itemId, resolvedWriteOptions);

    try {
      BlobWriteOption[] sdkWriteOptions = resolvedWriteOptions.generateWriteOptions(itemId);

      if (resolvedWriteOptions.isBidiWriteEnabled()) {
        checkArgument(
            clientOptions.getProtocol() == Protocol.BIDI,
            "channel.write.bidi.enabled requires client.protocol=BIDI, but was %s",
            clientOptions.getProtocol());
        return new GcsBidiWriteChannel(storage, blobInfo, resolvedWriteOptions, sdkWriteOptions);
      }

      BlobWriteSession sdkWriteSession = storage.blobWriteSession(blobInfo, sdkWriteOptions);
      WritableByteChannel channel = sdkWriteSession.open();
      return new GcsWriteChannel(sdkWriteSession, channel, blobInfo, resolvedWriteOptions);
    } catch (StorageException | IOException e) {
      throw GcsExceptionUtil.translateWriteException(
          e, "initialization", blobInfo.getBlobId(), 0L, resolvedWriteOptions);
    }
  }

  @Override
  public void moveObject(GcsItemId srcId, GcsItemId dstId) throws IOException {
    checkMoveObjectArguments(srcId, dstId);
    List<Storage.BlobSourceOption> sourceOptions = new ArrayList<>();
    srcId
        .getContentGeneration()
        .map(Storage.BlobSourceOption::generationMatch)
        .ifPresent(sourceOptions::add);
    clientOptions
        .getGcsWriteOptions()
        .getUserProject()
        .or(() -> clientOptions.getGcsReadOptions().getUserProjectId())
        .map(Storage.BlobSourceOption::userProject)
        .ifPresent(sourceOptions::add);
    List<Storage.BlobTargetOption> targetOptions = new ArrayList<>();
    dstId
        .getContentGeneration()
        .map(Storage.BlobTargetOption::generationMatch)
        .ifPresent(targetOptions::add);

    // objects.move has no source generation parameter: generations are sent only as preconditions.
    // No encryption key is sent: a move keeps the object's encryption and never re-encrypts.
    Storage.MoveBlobRequest request =
        Storage.MoveBlobRequest.newBuilder()
            .setSource(BlobId.of(srcId.getBucketName(), srcId.getObjectName().get()))
            .setTarget(BlobId.of(dstId.getBucketName(), dstId.getObjectName().get()))
            .setSourceOptions(sourceOptions)
            .setTargetOptions(targetOptions)
            .build();
    try {
      storage.moveBlob(request);
    } catch (StorageException e) {
      // TODO: When GcsFileSystemImpl#rename uses moveObject, detect a retried move whose earlier
      // attempt already succeeded. It fails with 404, or with 412 if the source name was
      // re-created; telling it apart from a real failure needs the source's checksum from before
      // the move.
      throw GcsExceptionUtil.translateMoveObjectException(e, srcId, dstId);
    }
  }

  /**
   * Checks the arguments of {@link #moveObject(GcsItemId, GcsItemId)}. Package-private so that
   * {@code FakeGcsClientImpl} validates a move exactly like this class.
   */
  static void checkMoveObjectArguments(GcsItemId srcId, GcsItemId dstId) {
    checkNotNull(srcId, "srcId must not be null");
    checkNotNull(dstId, "dstId must not be null");
    checkArgument(srcId.isGcsObject(), "Expected a GCS object srcId but got: %s", srcId);
    checkArgument(dstId.isGcsObject(), "Expected a GCS object dstId but got: %s", dstId);
    // The GCS objects.move API only moves an object within its bucket: the request has a single
    // bucket, so the client library would ignore a different destination bucket.
    checkArgument(
        srcId.getBucketName().equals(dstId.getBucketName()),
        "Cannot move objects across buckets: %s to %s",
        srcId,
        dstId);
    checkArgument(
        !srcId.getObjectName().get().equals(dstId.getObjectName().get()),
        "Move destination must be different from source: %s",
        srcId);
    srcId
        .getContentGeneration()
        .ifPresent(
            generation ->
                checkArgument(
                    generation > 0, "Source generation must be positive but got: %s", generation));
    dstId
        .getContentGeneration()
        .ifPresent(
            generation ->
                checkArgument(
                    generation >= 0,
                    "Destination generation must not be negative but got: %s",
                    generation));
  }

  @Override
  public GcsItemInfo getGcsItemInfo(GcsItemId itemId) throws IOException {
    checkNotNull(itemId, "Item ID must not be null.");
    if (itemId.isGcsObject()) {
      return getGcsObjectInfo(itemId);
    }
    throw new UnsupportedOperationException(
        String.format("Expected gcs object but got %s", itemId));
  }

  @Override
  public GcsItemInfo getBucketInfo(GcsItemId itemId) throws IOException {
    checkNotNull(itemId, "Item ID must not be null.");
    checkArgument(itemId.isBucket(), "Expected a bucket itemId");
    BucketInfo bucketInfo;
    try {
      bucketInfo =
          storage.get(
              itemId.getBucketName(), Storage.BucketGetOption.fields(BUCKET_METADATA_FIELDS_ARRAY));
    } catch (StorageException e) {
      if (e.getCode() == 404) {
        bucketInfo = null;
      } else if (GcsExceptionUtil.getErrorType(e) == GcsExceptionUtil.ErrorType.ACCESS_DENIED) {
        throw GcsExceptionUtil.createAccessDeniedException(
            UriUtil.getStringPath(itemId), "bucket lookup", e);
      } else {
        throw new IOException("Unable to access bucket: " + itemId.getBucketName(), e);
      }
    }
    if (bucketInfo == null) {
      throw GcsExceptionUtil.createFileNotFoundException(itemId);
    }
    return fromBucketInfo(bucketInfo);
  }

  @Override
  public GcsItemInfo getFolderInfo(GcsItemId itemId) throws IOException {
    checkNotNull(itemId, "Item ID must not be null.");
    if (itemId.isRoot()) {
      return GcsItemInfo.ROOT_INFO;
    }
    if (itemId.isBucket()) {
      return getBucketInfo(itemId);
    }
    checkArgument(itemId.isGcsObject(), "Expected a folder itemId");
    String objectName = itemId.getObjectName().orElse("");
    String folderName = UriUtil.removeTrailingSlash(objectName);
    checkArgument(!folderName.isEmpty(), "Folder name cannot be empty");

    GetFolderRequest request =
        GetFolderRequest.newBuilder()
            .setName(FolderName.format("_", itemId.getBucketName(), folderName))
            .build();
    try {
      Folder folder = lazyGetStorageControlClient().getFolder(request);
      // itemId may come with or without a trailing slash; always return the directory form.
      // A not-found error below names itemId as the caller gave it.
      return fromFolder(folder, itemId.toDirectoryId());
    } catch (NotFoundException e) {
      throw GcsExceptionUtil.createFileNotFoundException(itemId);
    } catch (PermissionDeniedException | UnauthenticatedException e) {
      throw GcsExceptionUtil.createAccessDeniedException(
          UriUtil.getStringPath(itemId), "folder lookup", e);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Failed to get folder info for: " + itemId, e);
    }
  }

  @Override
  public List<GcsItemInfo> listFolders(GcsItemId prefixId) throws IOException {
    checkNotNull(prefixId, "prefixId must not be null");
    checkArgument(
        !prefixId.getBucketName().isEmpty(),
        "prefixId must have a non-empty bucket name but got: %s",
        prefixId);
    String bucketName = prefixId.getBucketName();
    String objectName = prefixId.getObjectName().orElse("");
    // Errors name the listed location without the content generation, which listing ignores.
    GcsItemId listedId =
        GcsItemId.builder().setBucketName(bucketName).setObjectName(objectName).build();
    String parent = STORAGE_CONTROL_BUCKET_PARENT_PREFIX + bucketName;
    ListFoldersRequest.Builder request =
        ListFoldersRequest.newBuilder().setParent(parent).setPageSize(LIST_FOLDERS_PAGE_SIZE);
    if (!objectName.isEmpty()) {
      // ListFolders only accepts prefixes that end with '/'.
      request.setPrefix(UriUtil.toDirectoryPath(objectName));
    }

    String folderResourcePrefix = parent + "/folders/";
    ImmutableList.Builder<GcsItemInfo> folders = ImmutableList.builder();
    try {
      // iterateAll() fetches the remaining pages lazily, so iteration errors are translated too.
      Iterable<Folder> allFolders =
          lazyGetStorageControlClient().listFolders(request.build()).iterateAll();
      for (Folder folder : allFolders) {
        GcsItemId folderId = toFolderItemId(folder, bucketName, folderResourcePrefix, listedId);
        folders.add(fromFolder(folder, folderId));
      }
    } catch (NotFoundException e) {
      // ListFolders returns NOT_FOUND only when the bucket itself does not exist.
      throw GcsExceptionUtil.createBucketNotFoundException(bucketName, e);
    } catch (Exception e) {
      throw GcsExceptionUtil.translateControlException(
          e, listedId, "Failed to list folders under: " + UriUtil.getStringPath(listedId));
    }
    return folders.build();
  }

  /**
   * Returns the directory-form item ID of a folder returned by ListFolders.
   *
   * <p>Takes the folder path verbatim after {@code folderResourcePrefix}, as hadoop-connectors
   * FolderInfo does. {@code FolderName.parse} would trim whitespace around every '/' and so misname
   * such folders.
   *
   * @param folder the folder returned by ListFolders
   * @param bucketName the listed bucket
   * @param folderResourcePrefix the {@code projects/_/buckets/<bucket>/folders/} resource prefix
   * @param listedId the listed location, used in the error message
   * @return the folder's item ID, whose object name ends with {@code /}
   * @throws IOException if the folder resource name is not a non-empty folder path under {@code
   *     folderResourcePrefix}
   */
  private static GcsItemId toFolderItemId(
      Folder folder, String bucketName, String folderResourcePrefix, GcsItemId listedId)
      throws IOException {
    String resourceName = folder.getName();
    String folderPath =
        resourceName.startsWith(folderResourcePrefix)
            ? resourceName.substring(folderResourcePrefix.length())
            : "";
    if (folderPath.isEmpty()) {
      throw new IOException(
          String.format(
              "Unexpected folder resource name '%s' when listing folders under: %s",
              resourceName, UriUtil.getStringPath(listedId)));
    }
    return GcsItemId.builder()
        .setBucketName(bucketName)
        .setObjectName(UriUtil.toDirectoryPath(folderPath))
        .build();
  }

  @VisibleForTesting
  StorageControlClient lazyGetStorageControlClient() throws IOException {
    if (isClosed) {
      throw new IOException("StorageControlClient is closed");
    }
    StorageControlClient result = this.storageControlClient;
    if (result == null) {
      synchronized (this) {
        if (isClosed) {
          throw new IOException("StorageControlClient is closed");
        }
        result = this.storageControlClient;
        if (result == null) {
          this.storageControlClient = result = createStorageControlClient(this.credentials);
        }
      }
    }
    return result;
  }

  @VisibleForTesting
  protected StorageControlClient createStorageControlClient(Optional<Credentials> credentials)
      throws IOException {
    StorageControlSettings.Builder builder = StorageControlSettings.newBuilder();
    builder.setHeaderProvider(
        FixedHeaderProvider.create(ImmutableMap.of("User-Agent", getUserAgent())));
    credentials.ifPresent(c -> builder.setCredentialsProvider(FixedCredentialsProvider.create(c)));
    universeDomain.ifPresent(builder::setUniverseDomain);
    return StorageControlClient.create(builder.build());
  }

  @Override
  public Optional<GcsItemInfo> listFirstObjectWithPrefix(GcsItemId prefixId) throws IOException {
    GcsListOptions options = FIRST_OBJECT_LIST_OPTIONS;
    while (true) {
      GcsListPage<GcsItemInfo> page = listObjectsPage(prefixId, options);
      if (!page.getItems().isEmpty()) {
        return Optional.of(page.getItems().get(0));
      }
      if (!page.hasNextPage()) {
        return Optional.empty();
      }
      options = options.toBuilder().setPageToken(page.getNextPageToken().get()).build();
    }
  }

  @Override
  public GcsListPage<GcsItemInfo> listObjectsPage(GcsItemId prefixId, GcsListOptions options)
      throws IOException {
    checkNotNull(prefixId, "prefixId must not be null");
    checkNotNull(options, "options must not be null");
    checkArgument(!prefixId.getBucketName().isEmpty(), "Empty bucket name: %s", prefixId);

    String bucketName = prefixId.getBucketName();
    String prefix = prefixId.getObjectName().orElse("");
    BlobListOption[] blobListOptions = buildBlobListOptions(prefix, options);
    try {
      return toListPage(bucketName, prefix, options, storage.list(bucketName, blobListOptions));
    } catch (StorageException e) {
      if (e.getCode() == 404) {
        throw GcsExceptionUtil.createBucketNotFoundException(bucketName, e);
      }
      if (GcsExceptionUtil.getErrorType(e) == GcsExceptionUtil.ErrorType.ACCESS_DENIED) {
        throw GcsExceptionUtil.createAccessDeniedException(
            UriUtil.getStringPath(prefixId), "list", e);
      }
      throw new IOException(
          "Failed to list objects for prefix: " + UriUtil.getStringPath(prefixId), e);
    }
  }

  /** Builds the {@code objects.list} request options for one page under {@code prefix}. */
  private BlobListOption[] buildBlobListOptions(String prefix, GcsListOptions options) {
    List<BlobListOption> blobListOptions = new ArrayList<>();
    blobListOptions.add(BlobListOption.prefix(prefix));
    options
        .getDelimiter()
        .ifPresent(
            d -> {
              blobListOptions.add(BlobListOption.delimiter(d));
              blobListOptions.add(BlobListOption.includeTrailingDelimiter());
            });
    if (options.isIncludeFoldersAsPrefixes()) {
      blobListOptions.add(BlobListOption.includeFolders(true));
    }
    blobListOptions.add(
        BlobListOption.pageSize(
            options.getPageSize().orElse(GcsListOptions.DEFAULT_MAX_LIST_ITEMS_PER_CALL)));
    options.getPageToken().map(BlobListOption::pageToken).ifPresent(blobListOptions::add);
    options.getStartOffset().map(BlobListOption::startOffset).ifPresent(blobListOptions::add);
    options.getEndOffset().map(BlobListOption::endOffset).ifPresent(blobListOptions::add);
    options.getMatchGlob().map(BlobListOption::matchGlob).ifPresent(blobListOptions::add);
    clientOptions
        .getGcsReadOptions()
        .getUserProjectId()
        .or(() -> clientOptions.getGcsWriteOptions().getUserProject())
        .map(BlobListOption::userProject)
        .ifPresent(blobListOptions::add);
    blobListOptions.add(BlobListOption.fields(resolveBlobFields(options)));
    return blobListOptions.toArray(new BlobListOption[0]);
  }

  /**
   * Sorts one {@code objects.list} response into items, collapsed prefixes, and the listed prefix's
   * own placeholder object.
   */
  private static GcsListPage<GcsItemInfo> toListPage(
      String bucketName, String prefix, GcsListOptions options, Page<Blob> page) {
    Optional<String> delimiter = options.getDelimiter();
    String dirSuffix = delimiter.orElse("/");
    boolean isDirectoryPrefix = !prefix.isEmpty() && prefix.endsWith(dirSuffix);
    List<GcsItemInfo> items = new ArrayList<>();
    Set<String> rawPrefixes = new LinkedHashSet<>();
    Set<String> seenObjectNames = new HashSet<>();
    GcsItemInfo prefixObject = null;
    for (Blob blob : page.getValues()) {
      if (blob == null) {
        continue;
      }
      if (blob.isDirectory()) {
        rawPrefixes.add(blob.getName());
        continue;
      }
      if (delimiter.isPresent() && blob.getName().endsWith(dirSuffix)) {
        seenObjectNames.add(blob.getName());
      }
      GcsItemInfo info = fromBlob(blob, options.getFields());
      if (isDirectoryPrefix && blob.getName().equals(prefix) && !options.isIncludePrefixObject()) {
        prefixObject = info;
      } else {
        items.add(info);
      }
    }
    List<GcsItemId> prefixes = new ArrayList<>(rawPrefixes.size());
    for (String p : rawPrefixes) {
      if (!(isDirectoryPrefix && p.equals(prefix)) && !seenObjectNames.contains(p)) {
        prefixes.add(GcsItemId.builder().setBucketName(bucketName).setObjectName(p).build());
      }
    }
    GcsListPage.Builder<GcsItemInfo> pageBuilder =
        GcsListPage.<GcsItemInfo>builder().setItems(items).setPrefixes(prefixes);
    if (prefixObject != null) {
      pageBuilder.setPrefixObject(prefixObject);
    }
    String nextPageToken = page.getNextPageToken();
    if (nextPageToken != null) {
      pageBuilder.setNextPageToken(nextPageToken);
    }
    return pageBuilder.build();
  }

  static BlobField[] resolveBlobFields(GcsListOptions options) {
    return options.getFields().stream().map(GcsClientImpl::toBlobField).toArray(BlobField[]::new);
  }

  static BlobField toBlobField(GcsListOptions.ListField field) {
    return checkNotNull(LIST_FIELD_TO_BLOB_FIELD.get(field), "Unsupported ListField: %s", field);
  }

  static GcsItemInfo fromBlob(Blob blob) {
    return fromBlob(blob, ALL_LIST_FIELDS);
  }

  /**
   * Converts {@code blob} to a {@link GcsItemInfo}, copying only the attributes in {@code fields}.
   *
   * <p>Over gRPC, the client library reports attributes that are unset or left out of the field
   * mask as proto defaults rather than {@code null} (for example generation {@code 0} and empty
   * content type and encoding). Copying only requested attributes, and treating a non-positive
   * generation and empty strings as absent, keeps gRPC results the same as JSON results.
   */
  static GcsItemInfo fromBlob(Blob blob, Set<GcsListOptions.ListField> fields) {
    Optional<Long> generation =
        fields.contains(GcsListOptions.ListField.GENERATION)
            ? Optional.ofNullable(blob.getGeneration()).filter(gen -> gen > 0L)
            : Optional.empty();
    GcsItemId.Builder idBuilder =
        GcsItemId.builder().setBucketName(blob.getBucket()).setObjectName(blob.getName());
    generation.ifPresent(idBuilder::setContentGeneration);
    GcsItemId id = idBuilder.build();

    GcsItemInfo.Builder infoBuilder =
        GcsItemInfo.builder()
            .setItemId(id)
            .setSize(
                fields.contains(GcsListOptions.ListField.SIZE) && blob.getSize() != null
                    ? blob.getSize()
                    : 0L)
            .setCreationTime(
                fields.contains(GcsListOptions.ListField.TIME_CREATED)
                    ? toEpochMilli(blob.getCreateTimeOffsetDateTime())
                    : 0L)
            .setModificationTime(
                fields.contains(GcsListOptions.ListField.UPDATED)
                    ? toEpochMilli(blob.getUpdateTimeOffsetDateTime())
                    : 0L);

    Optional<String> md5 =
        fields.contains(GcsListOptions.ListField.MD5HASH)
            ? nonEmpty(blob.getMd5())
            : Optional.empty();
    Optional<String> crc32c =
        fields.contains(GcsListOptions.ListField.CRC32C)
            ? nonEmpty(blob.getCrc32c())
            : Optional.empty();
    if (md5.isPresent() || crc32c.isPresent()) {
      infoBuilder.setVerificationAttributes(
          VerificationAttributes.create(
              md5.map(BaseEncoding.base64()::decode).orElse(null),
              crc32c.map(BaseEncoding.base64()::decode).orElse(null)));
    }

    generation.ifPresent(infoBuilder::setContentGeneration);
    if (fields.contains(GcsListOptions.ListField.CONTENT_TYPE)) {
      nonEmpty(blob.getContentType()).ifPresent(infoBuilder::setContentType);
    }
    if (fields.contains(GcsListOptions.ListField.CONTENT_ENCODING)) {
      nonEmpty(blob.getContentEncoding()).ifPresent(infoBuilder::setContentEncoding);
    }
    if (fields.contains(GcsListOptions.ListField.STORAGE_CLASS)) {
      Optional.ofNullable(blob.getStorageClass())
          .map(Object::toString)
          .ifPresent(infoBuilder::setStorageClass);
    }
    if (fields.contains(GcsListOptions.ListField.METAGENERATION)) {
      Optional.ofNullable(blob.getMetageneration()).ifPresent(infoBuilder::setMetaGeneration);
    }

    // A real 0-byte "dir/" object is not reported by Blob.isDirectory() (only delimiter prefixes
    // are), so also treat any object name ending with '/' as a placeholder directory.
    if (blob.isDirectory() || blob.getName().endsWith("/")) {
      infoBuilder.setItemType(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY);
    }

    infoBuilder.setExtendedAttributes(
        fields.contains(GcsListOptions.ListField.METADATA)
            ? GcsItemInfo.decodeMetadata(blob.getMetadata())
            : ImmutableMap.of());

    return infoBuilder.build();
  }

  private static Optional<String> nonEmpty(@Nullable String value) {
    return Optional.ofNullable(value).filter(v -> !v.isEmpty());
  }

  private static GcsItemInfo fromBucketInfo(BucketInfo bucketInfo) {
    GcsItemId itemId = GcsItemId.builder().setBucketName(bucketInfo.getName()).build();
    GcsItemInfo.Builder builder =
        GcsItemInfo.createBucket(itemId).toBuilder()
            .setCreationTime(toEpochMilli(bucketInfo.getCreateTimeOffsetDateTime()))
            .setModificationTime(toEpochMilli(bucketInfo.getUpdateTimeOffsetDateTime()));

    Optional.ofNullable(bucketInfo.getLocation()).ifPresent(builder::setLocation);
    Optional.ofNullable(bucketInfo.getStorageClass())
        .map(Object::toString)
        .ifPresent(builder::setStorageClass);
    Optional.ofNullable(bucketInfo.getMetageneration()).ifPresent(builder::setMetaGeneration);
    return builder.build();
  }

  private static GcsItemInfo fromFolder(Folder folder, GcsItemId itemId) {
    return GcsItemInfo.createFolder(
        itemId,
        toEpochMilli(folder.hasCreateTime() ? folder.getCreateTime() : null),
        toEpochMilli(folder.hasUpdateTime() ? folder.getUpdateTime() : null),
        folder.getMetageneration());
  }

  private static long toEpochMilli(OffsetDateTime dateTime) {
    return dateTime != null ? dateTime.toInstant().toEpochMilli() : 0L;
  }

  private static long toEpochMilli(Timestamp timestamp) {
    return timestamp != null
        ? Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()).toEpochMilli()
        : 0L;
  }

  @Override
  public BucketProperties getBucketProperties(String bucketName) throws IOException {
    checkNotNull(bucketName, "bucketName cannot be null");
    return bucketPropertiesCache.get(bucketName, this::fetchBucketProperties);
  }

  private BucketProperties fetchBucketProperties(String bucketName) throws IOException {
    try {
      BucketInfo bucketInfo =
          storage.get(
              bucketName,
              Storage.BucketGetOption.fields(
                  Storage.BucketField.HIERARCHICAL_NAMESPACE, Storage.BucketField.STORAGE_CLASS));
      if (bucketInfo == null) {
        throw GcsExceptionUtil.createBucketNotFoundException(bucketName);
      }
      boolean hnsEnabled =
          Optional.ofNullable(bucketInfo.getHierarchicalNamespace())
              .map(HierarchicalNamespace::getEnabled)
              .orElse(false);
      boolean isRapid =
          bucketInfo.getStorageClass() != null
              && RAPID_STORAGE_CLASS.equalsIgnoreCase(bucketInfo.getStorageClass().toString());
      return BucketProperties.create(hnsEnabled, isRapid);
    } catch (StorageException storageException) {
      if (storageException.getCode() == 404) {
        throw GcsExceptionUtil.createBucketNotFoundException(bucketName, storageException);
      }
      if (GcsExceptionUtil.getErrorType(storageException)
          == GcsExceptionUtil.ErrorType.ACCESS_DENIED) {
        LOG.warn(
            "Access to bucket {} is denied ({}), HNS and RAPID features will be disabled",
            bucketName,
            storageException.getCode());
        return BucketProperties.create(false, false);
      }
      throw GcsExceptionUtil.translateStorageException(
          storageException,
          GcsItemId.builder().setBucketName(bucketName).build(),
          "Unable to access bucket: " + bucketName);
    }
  }

  @Override
  public void close() {
    try {
      storage.close();
    } catch (Exception e) {
      LOG.debug("Exception while closing storage instance", e);
    }
    synchronized (this) {
      if (isClosed) {
        return;
      }
      isClosed = true;
      if (grpcStorage != null) {
        try {
          grpcStorage.close();
        } catch (Exception e) {
          LOG.debug("Exception while closing grpcStorage instance", e);
        }
        grpcStorage = null;
      }
      if (storageControlClient != null) {
        try {
          storageControlClient.close();
        } catch (Exception e) {
          LOG.debug("Exception while closing storageControlClient", e);
        }
        storageControlClient = null;
      }
    }
  }

  @Override
  public void createBucket(String bucketName) throws IOException {
    createBucket(bucketName, GcsCreateBucketOptions.DEFAULT);
  }

  @Override
  public void createBucket(String bucketName, GcsCreateBucketOptions options) throws IOException {
    checkNotNull(bucketName, "bucketName must not be null");
    checkArgument(!bucketName.isEmpty(), "bucketName must not be empty");
    checkNotNull(options, "options must not be null");
    BucketInfo.Builder bucketInfoBuilder = BucketInfo.newBuilder(bucketName);
    options.getLocation().ifPresent(bucketInfoBuilder::setLocation);
    if (options.getZonalPlacement().isPresent()) {
      if (!options.isHierarchicalNamespaceEnabled()) {
        throw new UnsupportedOperationException("Zonal buckets must have HNS Enabled.");
      }
      if (options.getStorageClass().isPresent()
          && !RAPID_STORAGE_CLASS.equalsIgnoreCase(options.getStorageClass().get())) {
        throw new UnsupportedOperationException("Zonal bucket storage class must be RAPID");
      }
      if (options.getTtl().isPresent()) {
        throw new UnsupportedOperationException("Zonal buckets do not support TTL");
      }
      bucketInfoBuilder
          .setCustomPlacementConfig(
              CustomPlacementConfig.newBuilder()
                  .setDataLocations(ImmutableList.of(options.getZonalPlacement().get()))
                  .build())
          .setStorageClass(StorageClass.valueOf(RAPID_STORAGE_CLASS));
      enableHns(bucketInfoBuilder);
    } else {
      options
          .getStorageClass()
          .ifPresent(
              sc ->
                  bucketInfoBuilder.setStorageClass(
                      StorageClass.valueOfStrict(sc.toUpperCase(Locale.ROOT))));
      if (options.isHierarchicalNamespaceEnabled()) {
        enableHns(bucketInfoBuilder);
      }
      options
          .getTtl()
          .ifPresent(
              ttl ->
                  bucketInfoBuilder.setLifecycleRules(
                      ImmutableList.of(
                          new LifecycleRule(
                              LifecycleAction.newDeleteAction(),
                              LifecycleCondition.newBuilder()
                                  .setAge(Math.toIntExact(ttl.toDays()))
                                  .build()))));
    }
    try {
      storage.create(bucketInfoBuilder.build());
    } catch (StorageException e) {
      throw GcsExceptionUtil.translateStorageException(
          e,
          GcsItemId.builder().setBucketName(bucketName).build(),
          "Failed to create bucket: " + bucketName);
    }
    // Clear only after a successful create, so that a 409 or 403 keeps the cached properties of the
    // existing bucket.
    bucketPropertiesCache.invalidate(bucketName);
  }

  private static void enableHns(BucketInfo.Builder bucketInfoBuilder) {
    bucketInfoBuilder
        .setIamConfiguration(
            IamConfiguration.newBuilder().setIsUniformBucketLevelAccessEnabled(true).build())
        .setHierarchicalNamespace(HierarchicalNamespace.newBuilder().setEnabled(true).build());
  }

  @Override
  public void createEmptyObject(GcsItemId itemId) throws IOException {
    GcsWriteOptions configuredWriteOptions = clientOptions.getGcsWriteOptions();
    GcsWriteOptions.Builder optionsBuilder = GcsWriteOptions.builder();
    configuredWriteOptions.getKmsKeyName().ifPresent(optionsBuilder::setKmsKeyName);
    configuredWriteOptions.getEncryptionKey().ifPresent(optionsBuilder::setEncryptionKey);
    configuredWriteOptions.getUserProject().ifPresent(optionsBuilder::setUserProject);
    createEmptyObject(itemId, optionsBuilder.build());
  }

  @Override
  public void createEmptyObject(GcsItemId itemId, GcsWriteOptions options) throws IOException {
    checkNotNull(itemId, "itemId must not be null");
    checkArgument(itemId.isGcsObject(), "Expected a GCS object itemId but got: " + itemId);
    checkNotNull(options, "options must not be null");
    try {
      createEmptyObjectInternal(itemId, options);
    } catch (StorageException e) {
      Optional<Blob> existing = lookUpAfterFailedCreate(e, itemId, options);
      if (existing.isPresent() && isZeroByte(existing.get())) {
        LOG.debug("Ignored exception while creating empty object {}", itemId, e);
        return;
      }
      if (e.getCode() == 412
          && shouldNotOverwrite(itemId, options)
          && (!itemId.isDirectory() || existing.isPresent())) {
        throw GcsExceptionUtil.createFileAlreadyExistsException(itemId, e);
      }
      throw GcsExceptionUtil.translateStorageException(
          e, itemId, "Failed to create empty object: " + itemId);
    }
  }

  private void createEmptyObjectInternal(GcsItemId itemId, GcsWriteOptions options)
      throws IOException {
    GcsWriteOptions effectiveOptions =
        options.toBuilder().setOverwriteExisting(!shouldNotOverwrite(itemId, options)).build();
    BlobInfo blobInfo =
        createBlobInfo(
            BlobId.of(itemId.getBucketName(), itemId.getObjectName().get()), effectiveOptions);
    if (isRapidBucket(itemId.getBucketName())) {
      createAppendableEmptyObject(itemId, blobInfo, effectiveOptions);
      return;
    }

    storage.create(blobInfo, effectiveOptions.generateTargetOptions(itemId));
  }

  private boolean isRapidBucket(String bucketName) throws IOException {
    return getBucketProperties(bucketName).isRapid();
  }

  private void createAppendableEmptyObject(
      GcsItemId itemId, BlobInfo blobInfo, GcsWriteOptions effectiveOptions) throws IOException {
    BlobWriteOption[] writeOptions = effectiveOptions.generateWriteOptions(itemId);
    try {
      BlobAppendableUpload upload =
          lazyGetGrpcStorage()
              .blobAppendableUpload(blobInfo, BlobAppendableUploadConfig.of(), writeOptions);
      try (AppendableUploadWriteableByteChannel channel = upload.open()) {
        channel.write(ByteBuffer.wrap(new byte[0]));
        channel.finalizeAndClose();
      }
    } catch (IOException e) {
      // In google-cloud-storage, open() throws StorageException directly and write() /
      // finalizeAndClose() coalesce any gRPC ApiException into a StorageException as e.getCause().
      if (e.getCause() instanceof StorageException) {
        throw (StorageException) e.getCause();
      }
      throw e;
    }
  }

  /**
   * Looks up the target object on GCS after a failed 0-byte object creation when the failure code
   * indicates the object may already exist.
   *
   * <p>When creating empty objects, concurrent requests (triggering 412 - Precondition Failed when
   * precondition {@code doesNotExist()} is set on a directory marker) or transient GCS errors (408,
   * 429, 500, 502, 503, 504) can cause object creation to fail even when the 0-byte object exists.
   * The SDK retries creation only when a generation precondition is present, which is always the
   * case for directories. Metadata lookups are always retried. A single lookup is intentional
   * (unlike hadoop-connectors' {@code canIgnoreExceptionForEmptyObject}): object metadata is
   * strongly consistent, and transient errors surface only after the SDK exhausts its retries of
   * the precondition-guarded create.
   */
  private Optional<Blob> lookUpAfterFailedCreate(
      StorageException exceptionOnCreate, GcsItemId itemId, GcsWriteOptions options)
      throws IOException {
    int code = exceptionOnCreate.getCode();
    if (itemId.isDirectory()
        && (IGNORABLE_EMPTY_OBJECT_CREATE_ERROR_CODES.contains(code)
            || (code == 412 && shouldNotOverwrite(itemId, options)))) {
      BlobId blobId = BlobId.of(itemId.getBucketName(), itemId.getObjectName().orElse(""));
      List<Storage.BlobGetOption> getOptions = new ArrayList<>();
      getOptions.add(Storage.BlobGetOption.fields(BlobField.SIZE));
      options.getUserProject().map(Storage.BlobGetOption::userProject).ifPresent(getOptions::add);
      try {
        Storage client =
            isRapidBucket(itemId.getBucketName()) ? lazyGetGrpcStorage() : this.storage;
        return Optional.ofNullable(
            client.get(blobId, getOptions.toArray(new Storage.BlobGetOption[0])));
      } catch (StorageException e) {
        exceptionOnCreate.addSuppressed(e);
        throw new IOException(
            "Failed to verify existence of 0-byte object " + itemId + " after creation failed",
            exceptionOnCreate);
      }
    }
    return Optional.empty();
  }

  private static boolean shouldNotOverwrite(GcsItemId itemId, GcsWriteOptions options) {
    return !options.isOverwriteExisting()
        || (itemId.isDirectory() && itemId.getContentGeneration().isEmpty());
  }

  private static boolean isZeroByte(Blob blob) {
    return blob.getSize() == null || blob.getSize() == 0L;
  }

  @Override
  public void deleteFolder(GcsItemId folderId) throws IOException {
    checkNotNull(folderId, "folderId must not be null");
    checkArgument(folderId.isGcsObject(), "Expected a folder itemId but got: %s", folderId);
    String objectName = folderId.getObjectName().orElse("");
    checkArgument(
        !UriUtil.removeTrailingSlash(objectName).isEmpty(), "Folder name cannot be empty");
    DeleteFolderRequest request =
        DeleteFolderRequest.newBuilder()
            .setName(folderResourceName(folderId.getBucketName(), objectName))
            .build();
    try {
      lazyGetStorageControlClient().deleteFolder(request);
    } catch (NotFoundException e) {
      // A missing folder is not an error: there is nothing to delete. This typically happens when
      // the client library retries a delete whose first attempt already removed the folder.
      LOG.debug("Folder to delete was not found: {}", folderId);
    } catch (Exception e) {
      // Errors name the folder without contentGeneration, which HNS folders do not have.
      GcsItemId errorId =
          GcsItemId.builder()
              .setBucketName(folderId.getBucketName())
              .setObjectName(objectName)
              .build();
      throw GcsExceptionUtil.translateDeleteFolderException(e, errorId);
    }
  }

  @Override
  public void deleteFolderRecursive(GcsItemId folderId) throws IOException {
    checkNotNull(folderId, "folderId must not be null");
    checkArgument(
        folderId.isGcsObject(),
        "folderId must have a bucket and a folder path, but got: %s",
        folderId);
    String objectName = folderId.getObjectName().orElse("");
    checkArgument(
        !UriUtil.removeTrailingSlash(objectName).isEmpty(), "Folder name cannot be empty");
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedIOException(
          "Interrupted before deleting folder recursively: " + UriUtil.getStringPath(folderId));
    }
    DeleteFolderRecursiveRequest request =
        DeleteFolderRecursiveRequest.newBuilder()
            .setName(folderResourceName(folderId.getBucketName(), objectName))
            .build();
    OperationFuture<Empty, DeleteFolderRecursiveMetadata> future;
    try {
      future = lazyGetStorageControlClient().deleteFolderRecursiveAsync(request);
    } catch (NotFoundException e) {
      // A missing folder is not an error, as when the operation reports it below.
      LOG.debug("Folder to delete recursively was not found: {}", folderId);
      return;
    } catch (Exception e) {
      throw GcsExceptionUtil.translateDeleteFolderRecursiveException(e, folderId);
    }
    try {
      // TODO: Set polling settings for this operation once the polling policy for long-running
      // folder operations is decided. Until then the client library defaults apply: the first poll
      // is sent up to 5 seconds after the request, and the polling schedule targets up to 5 minutes
      // (transient GetOperation retries can extend waiting, and defaults may change across client
      // library versions).
      future.get();
    } catch (InterruptedException e) {
      // Stops waiting: cancels local polling, or the initial request if it is still in flight.
      // GAX never calls CancelOperation, so a delete the server already started keeps running.
      // Restore the interrupt flag in finally after cancel(true): GAX can clear an already-set
      // interrupt flag inside cancel(true).
      try {
        future.cancel(true);
      } finally {
        Thread.currentThread().interrupt();
      }
      throw (InterruptedIOException)
          new InterruptedIOException(
                  String.format(
                      "Interrupted while waiting to delete folder recursively: %s; the delete may"
                          + " still complete on the server%s",
                      UriUtil.getStringPath(folderId),
                      GcsExceptionUtil.operationNameSuffix(acceptedOperationName(future))))
              .initCause(e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      if (cause instanceof InterruptedException) {
        Thread.currentThread().interrupt();
        throw (InterruptedIOException)
            new InterruptedIOException(
                    String.format(
                        "Interrupted while waiting to delete folder recursively: %s; the delete may"
                            + " still complete on the server%s",
                        UriUtil.getStringPath(folderId),
                        GcsExceptionUtil.operationNameSuffix(acceptedOperationName(future))))
                .initCause(cause);
      }
      if (isPollingFailure(future)) {
        // Once the server has started the operation, a GetOperation NOT_FOUND means the operation
        // resource (not folderId) was not found; actual operation errors arrive in Operation.error
        // on an OK poll, so polling NOT_FOUND must not be treated as a no-op.
        throw GcsExceptionUtil.translateDeleteFolderRecursivePollingException(
            cause, folderId, acceptedOperationName(future));
      }
      if (cause instanceof NotFoundException) {
        // A missing folder is not an error: there is nothing to delete. A call also ends this way
        // when an earlier delete of the same folder is still running or has just finished, for
        // example after a wait that was interrupted or timed out.
        LOG.debug("Folder to delete recursively was not found: {}", folderId);
        return;
      }
      throw GcsExceptionUtil.translateDeleteFolderRecursiveException(cause, folderId);
    } catch (CancellationException e) {
      // The client library cancels the future once its total polling timeout expires.
      throw new IOException(
          String.format(
              "Timed out waiting to delete folder recursively: %s; the delete may still complete on"
                  + " the server%s",
              UriUtil.getStringPath(folderId),
              GcsExceptionUtil.operationNameSuffix(acceptedOperationName(future))),
          e);
    }
  }

  /**
   * Builds the {@code projects/_/buckets/<bucket>/folders/<folder>/} resource name, keeping the
   * folder name verbatim and adding a trailing {@code /} only when it is missing.
   *
   * <p>{@code FolderName.format} is not used because its {@code PathTemplate} trims spaces around
   * each {@code /} segment, while HNS folder names may start or end with spaces. The trailing slash
   * is not stripped either: Storage Control appends one only when it is missing, so stripping it
   * from a name such as {@code a//} would address the parent folder {@code a/}.
   */
  private static String folderResourceName(String bucketName, String objectName) {
    return "projects/_/buckets/" + bucketName + "/folders/" + UriUtil.toDirectoryPath(objectName);
  }

  /**
   * Returns {@code true} if the server accepted the long-running operation ({@code initialFuture}
   * completed normally) and a subsequent {@code GetOperation} poll failed ({@code pollingFuture}
   * did not complete normally).
   *
   * <p>When a poll succeeds with a completed {@code Operation} carrying an {@code Operation.error},
   * {@code pollingFuture} itself completes normally with that snapshot and {@code
   * OperationFuture#get()} raises the operation error while transforming the response, so this
   * method returns {@code false}.
   */
  private static boolean isPollingFailure(OperationFuture<?, ?> future) {
    ApiFuture<OperationSnapshot> initialFuture;
    RetryingFuture<OperationSnapshot> pollingFuture;
    try {
      initialFuture = future.getInitialFuture();
      pollingFuture = future.getPollingFuture();
    } catch (UnsupportedOperationException e) {
      return false;
    }
    if (!isDone(initialFuture) || !isDone(pollingFuture)) {
      return false;
    }
    return completedNormally(initialFuture) && !completedNormally(pollingFuture);
  }

  private static boolean isDone(@Nullable Future<?> future) {
    return future != null && future.isDone();
  }

  private static boolean completedNormally(Future<?> future) {
    try {
      Uninterruptibles.getUninterruptibly(future);
      return true;
    } catch (CancellationException | ExecutionException e) {
      return false;
    }
  }

  /**
   * Returns the name of the long-running operation behind {@code future} if the server has accepted
   * it ({@code initialFuture} completed normally), so that error messages can name an operation
   * that may still be running. Returns {@code null} if the operation was not accepted, if the
   * request that starts it is still in flight, or if {@code future} does not expose its initial
   * future. Never blocks.
   */
  @Nullable
  private static String acceptedOperationName(OperationFuture<?, ?> future) {
    ApiFuture<OperationSnapshot> initialFuture;
    try {
      initialFuture = future.getInitialFuture();
    } catch (UnsupportedOperationException e) {
      return null;
    }
    if (!isDone(initialFuture)) {
      return null;
    }
    try {
      OperationSnapshot snapshot = Uninterruptibles.getUninterruptibly(initialFuture);
      return snapshot == null ? null : snapshot.getName();
    } catch (CancellationException | ExecutionException e) {
      return null;
    }
  }

  @Override
  public void createFolder(GcsItemId itemId, boolean recursive) throws IOException {
    checkNotNull(itemId, "itemId must not be null");
    checkArgument(itemId.isGcsObject(), "Expected a folder itemId but got: " + itemId);
    String objectName = itemId.getObjectName().orElse("");
    String folderName = UriUtil.removeTrailingSlash(objectName);
    checkArgument(!folderName.isEmpty(), "Folder name cannot be empty");
    CreateFolderRequest request =
        CreateFolderRequest.newBuilder()
            .setParent(STORAGE_CONTROL_BUCKET_PARENT_PREFIX + itemId.getBucketName())
            .setFolderId(folderName)
            .setRecursive(recursive)
            .build();
    try {
      lazyGetStorageControlClient().createFolder(request);
    } catch (Exception e) {
      throw GcsExceptionUtil.translateControlException(
          e, itemId, "Failed to create folder: " + itemId);
    }
  }

  @Override
  public void deleteObject(GcsItemId itemId) throws IOException {
    checkNotNull(itemId, "itemId must not be null");
    checkArgument(itemId.isGcsObject(), "Expected a GCS object itemId, but got: %s", itemId);
    List<Storage.BlobSourceOption> sourceOptions = new ArrayList<>();
    if (itemId.getContentGeneration().isPresent()) {
      long generation = itemId.getContentGeneration().get();
      checkArgument(
          generation > 0L,
          "Expected positive contentGeneration for deleteObject, but got: %s",
          generation);
      sourceOptions.add(Storage.BlobSourceOption.generationMatch(generation));
    }
    resolveUserProject().map(Storage.BlobSourceOption::userProject).ifPresent(sourceOptions::add);
    BlobId blobId = BlobId.of(itemId.getBucketName(), itemId.getObjectName().get());
    boolean deleted;
    try {
      deleted = storage.delete(blobId, sourceOptions.toArray(new Storage.BlobSourceOption[0]));
    } catch (StorageException e) {
      if (GcsExceptionUtil.getErrorType(e) != GcsExceptionUtil.ErrorType.NOT_FOUND) {
        throw GcsExceptionUtil.translateDeleteObjectException(e, itemId);
      }
      deleted = false;
    }
    // A missing object is not an error: there is nothing to delete. This typically happens when
    // the client library retries a delete whose first attempt already removed the object.
    if (!deleted) {
      LOG.debug("Object to delete was not found: {}", itemId);
    }
  }

  @Override
  public void deleteBucket(GcsItemId itemId) throws IOException {
    checkNotNull(itemId, "itemId must not be null");
    checkArgument(itemId.isBucket(), "Expected a bucket itemId, but got: %s", itemId);
    String bucketName = itemId.getBucketName();
    List<Storage.BucketSourceOption> sourceOptions = new ArrayList<>();
    resolveUserProject().map(Storage.BucketSourceOption::userProject).ifPresent(sourceOptions::add);
    boolean deleted;
    try {
      deleted =
          storage.delete(bucketName, sourceOptions.toArray(new Storage.BucketSourceOption[0]));
    } catch (StorageException e) {
      // Keep the cached properties when the failure shows the bucket may still exist (for example
      // 403 or 409), as createBucket does; drop them only once the bucket is known to be gone.
      if (GcsExceptionUtil.getErrorType(e) == GcsExceptionUtil.ErrorType.NOT_FOUND) {
        bucketPropertiesCache.invalidate(bucketName);
      }
      throw GcsExceptionUtil.translateDeleteBucketException(e, bucketName);
    }
    // The bucket was deleted, or did not exist (false), so its cached properties are stale.
    bucketPropertiesCache.invalidate(bucketName);
    if (!deleted) {
      throw GcsExceptionUtil.createBucketNotFoundException(bucketName);
    }
  }

  // TODO: Use the client-level user project (GcsClientOptions#getUserProject) once it is
  // available, so deletes are billed like every other request.
  private Optional<String> resolveUserProject() {
    return clientOptions
        .getGcsWriteOptions()
        .getUserProject()
        .or(() -> clientOptions.getGcsReadOptions().getUserProjectId());
  }

  @VisibleForTesting
  Storage lazyGetGrpcStorage() throws IOException {
    if (isClosed) {
      throw new IOException("GrpcStorage is closed");
    }
    if (clientOptions.isGrpcEnabled()) {
      return this.storage;
    }
    Storage result = this.grpcStorage;
    if (result == null) {
      synchronized (this) {
        if (isClosed) {
          throw new IOException("GrpcStorage is closed");
        }
        result = this.grpcStorage;
        if (result == null) {
          this.grpcStorage = result = createGrpcStorage(this.credentials);
        }
      }
    }
    return result;
  }

  @VisibleForTesting
  protected Storage createGrpcStorage(Optional<Credentials> credentials) {
    return buildStorage(StorageOptions.grpc(), credentials);
  }

  @VisibleForTesting
  protected Storage createStorage(Optional<Credentials> credentials) {
    StorageOptions.Builder builder;
    if (clientOptions.isGrpcEnabled()) {
      if (authOptions.getProxyAddress().isPresent()) {
        LOG.warn(
            "analytics-core.auth.proxy.* applies only to authentication requests when"
                + " client.protocol is {}; data requests use the JVM proxy settings.",
            clientOptions.getProtocol());
      }
      builder = StorageOptions.grpc();
    } else {
      builder =
          StorageOptions.newBuilder()
              .setTransportOptions(
                  GcsTransportOptionsFactory.createHttpTransportOptions(
                      authOptions,
                      clientOptions.getServiceHost().orElse(null),
                      universeDomain.orElse(null)));
    }
    return buildStorage(builder, credentials);
  }

  private Storage buildStorage(StorageOptions.Builder builder, Optional<Credentials> credentials) {
    String userAgent = getUserAgent();
    builder.setHeaderProvider(FixedHeaderProvider.create(ImmutableMap.of("User-Agent", userAgent)));
    clientOptions.getProjectId().ifPresent(builder::setProjectId);
    clientOptions.getClientLibToken().ifPresent(builder::setClientLibToken);
    clientOptions.getServiceHost().ifPresent(builder::setHost);
    universeDomain.ifPresent(builder::setUniverseDomain);
    credentials.ifPresent(builder::setCredentials);
    builder.setBlobWriteSessionConfig(clientOptions.generateSessionConfig());
    GcsRetryOptions retryOptions = clientOptions.getGcsRetryOptions();
    if (retryOptions.hasOverrides()) {
      RetrySettings retrySettings = retryOptions.applyTo(StorageOptions.getDefaultRetrySettings());
      LOG.info(
          "Using custom retry settings for {} protocol: maxAttempts={}, initialRetryDelay={},"
              + " retryDelayMultiplier={}, maxRetryDelay={}, totalTimeout={}",
          clientOptions.getProtocol(),
          retrySettings.getMaxAttempts(),
          retrySettings.getInitialRetryDelayDuration(),
          retrySettings.getRetryDelayMultiplier(),
          retrySettings.getMaxRetryDelayDuration(),
          retrySettings.getTotalTimeoutDuration());
      builder.setRetrySettings(retrySettings);
    }

    return builder.build().getService();
  }

  private String getVersion() {
    return VersionHelper.VERSION;
  }

  @VisibleForTesting
  String getUserAgent() {
    return USER_AGENT_PREFIX
        + getVersion()
        + clientOptions.getUserAgent().map(agent -> " " + agent).orElse("");
  }

  private GcsItemInfo getGcsObjectInfo(GcsItemId itemId) throws IOException {
    checkArgument(itemId.isGcsObject(), String.format("Expected gcs object got %s", itemId));
    Blob blob = getBlob(itemId.getBucketName(), itemId.getObjectName().get());
    if (blob == null) {
      throw GcsExceptionUtil.createFileNotFoundException(itemId);
    }
    return fromBlob(blob);
  }

  private Blob getBlob(String bucketName, String objectName) throws IOException {
    checkNotNull(bucketName);
    checkNotNull(objectName);
    BlobId blobId = BlobId.of(bucketName, objectName);
    try {
      return storage.get(blobId, Storage.BlobGetOption.fields(BLOB_METADATA_FIELDS_ARRAY));
    } catch (StorageException storageException) {
      throw GcsExceptionUtil.translateException(storageException, "metadata lookup", blobId, 0L);
    }
  }

  private static BlobInfo createBlobInfo(GcsItemId itemId, GcsWriteOptions writeOptions) {
    checkNotNull(itemId, "itemId should not be null");
    String objectName =
        itemId
            .getObjectName()
            .orElseThrow(() -> new IllegalArgumentException("Object name must be present"));
    BlobId blobId =
        itemId
            .getContentGeneration()
            .map(generation -> BlobId.of(itemId.getBucketName(), objectName, generation))
            .orElseGet(() -> BlobId.of(itemId.getBucketName(), objectName));
    return createBlobInfo(blobId, writeOptions);
  }

  private static BlobInfo createBlobInfo(BlobId blobId, GcsWriteOptions writeOptions) {
    BlobInfo.Builder blobInfoBuilder = BlobInfo.newBuilder(blobId);

    Optional.ofNullable(writeOptions)
        .ifPresent(
            options -> {
              options.getContentType().ifPresent(blobInfoBuilder::setContentType);
              options.getContentEncoding().ifPresent(blobInfoBuilder::setContentEncoding);
              ImmutableMap<String, byte[]> metadata = options.getMetadata();
              if (!metadata.isEmpty()) {
                blobInfoBuilder.setMetadata(GcsItemInfo.encodeMetadata(metadata));
              }
            });

    return blobInfoBuilder.build();
  }
}
