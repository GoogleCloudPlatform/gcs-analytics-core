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

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auth.Credentials;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCache;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.base.Supplier;
import com.google.storage.control.v2.StorageControlClient;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;

public class FakeGcsClientImpl extends GcsClientImpl {

  public static Storage storage = LocalStorageHelper.getOptions().getService();

  private static int openReadChannelCount = 0;
  private static int closeCount = 0;

  FakeGcsClientImpl(
      Credentials credentials,
      GcsClientOptions clientOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry,
      AnalyticsCache<String, BucketProperties> bucketPropertiesCache) {
    super(credentials, clientOptions, executorServiceSupplier, telemetry, bucketPropertiesCache);
  }

  FakeGcsClientImpl(
      GcsClientOptions clientOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry,
      AnalyticsCache<String, BucketProperties> bucketPropertiesCache) {
    super(clientOptions, executorServiceSupplier, telemetry, bucketPropertiesCache);
  }

  @Override
  protected Storage createStorage(Optional<Credentials> credentials) {
    return storage;
  }

  @Override
  protected Storage createGrpcStorage(Optional<Credentials> credentials) {
    return storage;
  }

  @Override
  protected StorageControlClient createStorageControlClient(Optional<Credentials> credentials) {
    return StorageControlClient.create(new FakeStorageControlStub(storage));
  }

  @Override
  public BucketProperties getBucketProperties(String bucketName) {
    // LocalStorageHelper does not support bucket operations
    return BucketProperties.create(false, false);
  }

  /**
   * Moves the object as a copy followed by a delete of the source, enforcing the generation
   * preconditions given by {@link GcsItemId#getContentGeneration()}.
   *
   * <p>{@link Storage#moveBlob} is not used: in {@link LocalStorageHelper} it ignores the
   * preconditions, keeps the source name in the moved object's metadata, and fails for an object
   * without a generation.
   */
  @Override
  public void moveObject(GcsItemId srcId, GcsItemId dstId) throws IOException {
    GcsClientImpl.checkMoveObjectArguments(srcId, dstId);
    BlobId srcBlobId = BlobId.of(srcId.getBucketName(), srcId.getObjectName().get());
    BlobId dstBlobId = BlobId.of(dstId.getBucketName(), dstId.getObjectName().get());
    Blob src = storage.get(srcBlobId);
    // GCS reports a missing source before it checks any precondition.
    if (src == null) {
      throw GcsExceptionUtil.translateMoveObjectException(
          new StorageException(404, "Not Found"), srcId, dstId);
    }
    Blob dst = storage.get(dstBlobId);
    boolean sourceChanged =
        srcId.getContentGeneration().filter(gen -> !gen.equals(src.getGeneration())).isPresent();
    boolean destinationChanged =
        dstId
            .getContentGeneration()
            .filter(
                gen -> gen == 0L ? dst != null : dst == null || !gen.equals(dst.getGeneration()))
            .isPresent();
    if (sourceChanged || destinationChanged) {
      throw GcsExceptionUtil.translateMoveObjectException(
          new StorageException(412, "Precondition Failed"), srcId, dstId);
    }
    storage
        .copy(
            Storage.CopyRequest.newBuilder()
                .setSource(srcBlobId)
                .setTarget(src.toBuilder().setBlobId(dstBlobId).build())
                .build())
        .getResult();
    storage.delete(srcBlobId);
  }

  /**
   * Deletes the object, enforcing {@link GcsItemId#getContentGeneration()} as a generation-match
   * precondition, which {@link LocalStorageHelper} ignores.
   */
  @Override
  public void deleteObject(GcsItemId itemId) throws IOException {
    if (itemId != null && itemId.isGcsObject()) {
      Optional<Long> generation = itemId.getContentGeneration().filter(gen -> gen > 0L);
      if (generation.isPresent()) {
        Blob blob = storage.get(BlobId.of(itemId.getBucketName(), itemId.getObjectName().get()));
        if (blob != null && !generation.get().equals(blob.getGeneration())) {
          throw GcsExceptionUtil.translateDeleteObjectException(
              new StorageException(412, "Precondition Failed"), itemId);
        }
      }
    }
    super.deleteObject(itemId);
  }

  /**
   * Deletes an empty bucket.
   *
   * <p>{@link LocalStorageHelper} does not model buckets, so every bucket is treated as existing:
   * deletion succeeds when the bucket holds no objects and fails with {@link
   * java.nio.file.DirectoryNotEmptyException} otherwise. Deleting a bucket never removes it from
   * later lookups.
   */
  @Override
  public void deleteBucket(GcsItemId itemId) throws IOException {
    checkNotNull(itemId, "itemId must not be null");
    checkArgument(itemId.isBucket(), "Expected a bucket itemId, but got: %s", itemId);
    String bucketName = itemId.getBucketName();
    if (storage.list(bucketName).getValues().iterator().hasNext()) {
      throw GcsExceptionUtil.translateDeleteBucketException(
          new StorageException(409, "The bucket you tried to delete is not empty"), bucketName);
    }
  }

  @Override
  public VectoredSeekableByteChannel openReadChannel(
      GcsItemInfo itemInfo, GcsReadOptions readOptions) throws IOException {
    openReadChannelCount++;
    return super.openReadChannel(itemInfo, readOptions);
  }

  /**
   * Lists a single simulated page from {@link LocalStorageHelper}.
   *
   * <p>Limitations inherited from {@code LocalStorageHelper}: because the in-memory store has no
   * bucket registry or HNS metadata service, listing an unknown bucket returns an empty {@link
   * GcsListPage} instead of throwing {@link java.io.FileNotFoundException}, and {@link
   * GcsListOptions#isIncludeFoldersAsPrefixes()} is ignored.
   */
  @Override
  public GcsListPage<GcsItemInfo> listObjectsPage(GcsItemId prefixId, GcsListOptions options)
      throws IOException {
    checkNotNull(prefixId, "prefixId must not be null");
    checkNotNull(options, "options must not be null");
    checkArgument(!prefixId.getBucketName().isEmpty(), "Empty bucket name: %s", prefixId);
    Optional<String> afterName = FakePageToken.decode(options.getPageToken());
    String bucketName = prefixId.getBucketName();
    String prefix = prefixId.getObjectName().orElse("");
    // Pass only bucketName to LocalStorageHelper: FakeStorageRpc.list strips a leading '/' from
    // Option.PREFIX and ignores Option.FIELDS; collectMatchedNames handles prefix filtering below.
    Iterable<Blob> rawBlobs = storage.list(bucketName).iterateAll();

    Map<String, Blob> emittedObjects = new HashMap<>();
    List<String> matched = collectMatchedNames(rawBlobs, prefix, options, emittedObjects);
    int startIndex = findStartIndex(matched, afterName);
    int pageSize =
        Math.min(
            options.getPageSize().orElse(GcsListOptions.DEFAULT_MAX_LIST_ITEMS_PER_CALL),
            GcsListOptions.DEFAULT_MAX_LIST_ITEMS_PER_CALL);
    int end = (int) Math.min(matched.size(), (long) startIndex + pageSize);
    List<String> slice =
        startIndex >= matched.size() ? List.of() : matched.subList(startIndex, end);

    String dirSuffix = options.getDelimiter().orElse("/");
    boolean isDirectoryPrefix = !prefix.isEmpty() && prefix.endsWith(dirSuffix);
    Set<GcsListOptions.ListField> fields = options.getFields();
    List<GcsItemInfo> items = new ArrayList<>();
    List<GcsItemId> prefixes = new ArrayList<>();
    GcsItemInfo prefixObject = null;
    for (String name : slice) {
      Blob blob = emittedObjects.get(name);
      if (blob == null) {
        prefixes.add(GcsItemId.builder().setBucketName(bucketName).setObjectName(name).build());
      } else {
        GcsItemInfo info = fromBlob(blob, fields);
        if (isDirectoryPrefix && name.equals(prefix) && !options.isIncludePrefixObject()) {
          prefixObject = info;
        } else {
          items.add(info);
        }
      }
    }
    GcsListPage.Builder<GcsItemInfo> pageBuilder =
        GcsListPage.<GcsItemInfo>builder().setItems(items).setPrefixes(prefixes);
    if (prefixObject != null) {
      pageBuilder.setPrefixObject(prefixObject);
    }
    if (end < matched.size()) {
      pageBuilder.setNextPageToken(FakePageToken.encode(slice.get(slice.size() - 1)));
    }
    return pageBuilder.build();
  }

  private static List<String> collectMatchedNames(
      Iterable<Blob> rawBlobs,
      String prefix,
      GcsListOptions options,
      Map<String, Blob> emittedObjects)
      throws IOException {
    byte[] startBytes = options.getStartOffset().orElse("").getBytes(StandardCharsets.UTF_8);
    byte[] endBytes = options.getEndOffset().orElse("").getBytes(StandardCharsets.UTF_8);
    Optional<String> delimiter = options.getDelimiter();
    Set<String> entryNames = new TreeSet<>(FakeGcsClientImpl::compareUtf8);
    Set<String> collapsedPrefixes = new HashSet<>();
    for (Blob blob : rawBlobs) {
      if (blob == null || blob.isDirectory() || !blob.getName().startsWith(prefix)) {
        continue;
      }
      String name = blob.getName();
      byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
      if (Arrays.compareUnsigned(nameBytes, startBytes) < 0
          || (endBytes.length > 0 && Arrays.compareUnsigned(nameBytes, endBytes) >= 0)) {
        continue;
      }
      int delimIdx = delimiter.map(d -> name.indexOf(d, prefix.length())).orElse(-1);
      if (delimIdx < 0 || delimIdx + delimiter.get().length() == name.length()) {
        emittedObjects.put(name, blob);
        entryNames.add(name);
      } else {
        String collapsed = name.substring(0, delimIdx + delimiter.get().length());
        if (!collapsed.equals(prefix)) {
          collapsedPrefixes.add(collapsed);
        }
      }
    }
    for (String collapsed : collapsedPrefixes) {
      if (!emittedObjects.containsKey(collapsed)) {
        entryNames.add(collapsed);
      }
    }
    String globExpr = options.getMatchGlob().orElse("");
    Pattern glob = globExpr.isEmpty() ? null : FakeMatchGlob.compile(globExpr);
    List<String> matched = new ArrayList<>(entryNames.size());
    for (String name : entryNames) {
      if (glob == null || glob.matcher(name).matches()) {
        matched.add(name);
      }
    }
    return matched;
  }

  private static int findStartIndex(List<String> sortedNames, Optional<String> afterName) {
    if (afterName.isEmpty()) {
      return 0;
    }
    int idx =
        Collections.binarySearch(sortedNames, afterName.get(), FakeGcsClientImpl::compareUtf8);
    return idx >= 0 ? idx + 1 : -(idx + 1);
  }

  private static int compareUtf8(String a, String b) {
    return Arrays.compareUnsigned(
        a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void close() {
    closeCount++;
    super.close();
  }

  public static int getOpenReadChannelCount() {
    return openReadChannelCount;
  }

  public static int getCloseCount() {
    return closeCount;
  }

  public static void resetCounts() {
    openReadChannelCount = 0;
    closeCount = 0;
  }
}
