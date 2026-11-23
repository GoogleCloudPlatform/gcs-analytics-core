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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.Callables;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.storage.control.v2.CreateFolderRequest;
import com.google.storage.control.v2.DeleteFolderRequest;
import com.google.storage.control.v2.Folder;
import com.google.storage.control.v2.GetFolderRequest;
import com.google.storage.control.v2.ListFoldersRequest;
import com.google.storage.control.v2.ListFoldersResponse;
import com.google.storage.control.v2.StorageControlClient.ListFoldersPagedResponse;
import com.google.storage.control.v2.stub.StorageControlStub;
import com.google.storage.control.v2.stub.StorageControlStubSettings;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * In-memory {@link StorageControlStub} for hermetic {@link FakeGcsClientImpl} folder operations.
 *
 * <p>Supports {@code CreateFolder}, {@code GetFolder}, {@code DeleteFolder} and {@code
 * ListFolders}; every other call throws {@link UnsupportedOperationException}. There is no bucket
 * registry, so every bucket is treated as an existing bucket with hierarchical namespace.
 *
 * <p>State scope: objects live in {@link FakeGcsClientImpl#storage}, a static singleton shared by
 * every client in the JVM, while folders live in this stub, so each client instance starts with no
 * folders. Writing an object does not create its parent folders; create them with {@code
 * createFolder}.
 *
 * <p>Folder names follow GCS: they are kept verbatim (leading and trailing spaces, {@code //}, and
 * a leading {@code /} are all valid) with a trailing {@code /} added when missing, and only an
 * empty name is rejected with {@code INVALID_ARGUMENT}. Parents are cut at every {@code /}, a
 * non-recursive create requires its parent, and a delete fails while child folders or objects
 * exist.
 *
 * <p>{@code ListFolders} follows GCS too: it returns the folder named by the prefix and every
 * folder below it, ordered by the UTF-8 bytes of their names, at most 1,000 per page. {@code
 * delimiter}, {@code lexicographic_start} and {@code lexicographic_end} are not supported and are
 * rejected with {@code UNIMPLEMENTED}.
 */
final class FakeStorageControlStub extends StorageControlStub {
  private static final String BUCKET_PARENT_PREFIX = "projects/_/buckets/";
  private static final String FOLDERS_SEGMENT = "/folders/";

  /** GCS returns at most this many folders per ListFolders page, whatever page size is asked. */
  private static final int MAX_LIST_FOLDERS_PAGE_SIZE = 1000;

  private final Storage storage;
  private final Map<String, Folder> folders = new ConcurrentHashMap<>();
  private volatile boolean closed = false;

  FakeStorageControlStub(Storage storage) {
    this.storage = checkNotNull(storage, "storage must not be null");
  }

  @Override
  public UnaryCallable<CreateFolderRequest, Folder> createFolderCallable() {
    return unaryCallable(this::createFolder);
  }

  @Override
  public UnaryCallable<GetFolderRequest, Folder> getFolderCallable() {
    return unaryCallable(this::getFolder);
  }

  @Override
  public UnaryCallable<DeleteFolderRequest, Empty> deleteFolderCallable() {
    return unaryCallable(this::deleteFolder);
  }

  @Override
  public UnaryCallable<ListFoldersRequest, ListFoldersResponse> listFoldersCallable() {
    return unaryCallable(this::listFolders);
  }

  @Override
  public UnaryCallable<ListFoldersRequest, ListFoldersPagedResponse> listFoldersPagedCallable() {
    // The client library's own paging settings, so that iterating the response fetches the next
    // pages through listFoldersCallable() exactly as it does against GCS. Paging requires a call
    // context, which a real stub also supplies as its default.
    return Callables.paged(
            listFoldersCallable(),
            StorageControlStubSettings.newBuilder().listFoldersSettings().build())
        .withDefaultCallContext(GrpcCallContext.createDefault());
  }

  private Folder createFolder(CreateFolderRequest request) {
    String bucket = extractBucketName(request.getParent());
    String folderPath = toFolderPath(request.getFolderId());
    String key = folderResourceName(bucket, folderPath);
    Timestamp now = currentTimestamp();
    synchronized (folders) {
      if (folders.containsKey(key)) {
        throw apiException(StatusCode.Code.ALREADY_EXISTS, "Folder already exists: " + key);
      }
      ensureParentFolders(bucket, folderPath, request.getRecursive(), now);
      Folder folder = newFolder(key, now);
      folders.put(key, folder);
      return folder;
    }
  }

  private Folder getFolder(GetFolderRequest request) {
    String key = parseFolderResourceName(request.getName());
    Folder folder = folders.get(key);
    if (folder == null) {
      throw apiException(StatusCode.Code.NOT_FOUND, "Folder not found: " + key);
    }
    return folder;
  }

  private Empty deleteFolder(DeleteFolderRequest request) {
    String key = parseFolderResourceName(request.getName());
    synchronized (folders) {
      if (!folders.containsKey(key)) {
        throw apiException(StatusCode.Code.NOT_FOUND, "Folder not found: " + key);
      }
      if (hasChildFolder(key) || hasChildObject(key)) {
        throw apiException(
            StatusCode.Code.FAILED_PRECONDITION, "The folder you tried to delete is not empty.");
      }
      folders.remove(key);
      return Empty.getDefaultInstance();
    }
  }

  /**
   * Returns one page of the folders of the bucket whose names start with the request prefix, in the
   * order GCS lists them. The next page token names the last folder of the page.
   */
  private ListFoldersResponse listFolders(ListFoldersRequest request) {
    String bucket = extractBucketName(request.getParent());
    String prefix = request.getPrefix();
    if (!prefix.isEmpty() && !prefix.endsWith("/")) {
      throw apiException(StatusCode.Code.INVALID_ARGUMENT, "Prefix must end with '/'.");
    }
    if (request.getPageSize() < 0) {
      throw apiException(
          StatusCode.Code.INVALID_ARGUMENT,
          "Page size must be an integer that is greater than or equal to zero.");
    }
    if (!request.getDelimiter().isEmpty()
        || !request.getLexicographicStart().isEmpty()
        || !request.getLexicographicEnd().isEmpty()) {
      throw apiException(
          StatusCode.Code.UNIMPLEMENTED,
          "FakeStorageControlStub does not support delimiter, lexicographic_start or"
              + " lexicographic_end");
    }
    Optional<String> previousName = decodePageToken(request.getPageToken());
    String namePrefix = folderResourceName(bucket, prefix);
    List<Folder> matches = new ArrayList<>();
    synchronized (folders) {
      for (Folder folder : folders.values()) {
        String name = folder.getName();
        if (name.startsWith(namePrefix)
            && (previousName.isEmpty() || compareUtf8(name, previousName.get()) > 0)) {
          matches.add(folder);
        }
      }
    }
    matches.sort((a, b) -> compareUtf8(a.getName(), b.getName()));
    int pageSize =
        request.getPageSize() == 0
            ? MAX_LIST_FOLDERS_PAGE_SIZE
            : Math.min(request.getPageSize(), MAX_LIST_FOLDERS_PAGE_SIZE);
    ListFoldersResponse.Builder response = ListFoldersResponse.newBuilder();
    if (matches.size() > pageSize) {
      matches = matches.subList(0, pageSize);
      response.setNextPageToken(FakePageToken.encode(matches.get(pageSize - 1).getName()));
    }
    return response.addAllFolders(matches).build();
  }

  /**
   * Creates the missing parents of {@code folderPath} when {@code recursive}, or requires the
   * immediate parent to exist otherwise. Parents are cut at every {@code /}, as GCS does, so {@code
   * a//b/} has the parents {@code a//} and {@code a/}.
   */
  private void ensureParentFolders(
      String bucket, String folderPath, boolean recursive, Timestamp now) {
    List<String> parentPaths = parentFolderPaths(folderPath);
    if (parentPaths.isEmpty()) {
      return;
    }
    if (!recursive) {
      String parentKey = folderResourceName(bucket, parentPaths.get(0));
      if (!folders.containsKey(parentKey)) {
        throw apiException(
            StatusCode.Code.FAILED_PRECONDITION, "The parent folder does not exist: " + parentKey);
      }
      return;
    }
    for (String parentPath : parentPaths) {
      folders.computeIfAbsent(folderResourceName(bucket, parentPath), k -> newFolder(k, now));
    }
  }

  /** Returns the parent folder paths of {@code folderPath}, nearest first. */
  private static List<String> parentFolderPaths(String folderPath) {
    List<String> parentPaths = new ArrayList<>();
    String current = folderPath;
    while (true) {
      current = current.substring(0, current.length() - 1);
      int lastSlash = current.lastIndexOf('/');
      if (lastSlash < 0) {
        return parentPaths;
      }
      current = current.substring(0, lastSlash + 1);
      parentPaths.add(current);
    }
  }

  private boolean hasChildFolder(String key) {
    for (String existingKey : folders.keySet()) {
      if (existingKey.startsWith(key) && !existingKey.equals(key)) {
        return true;
      }
    }
    return false;
  }

  private boolean hasChildObject(String key) {
    String bucket = key.substring(BUCKET_PARENT_PREFIX.length(), key.indexOf(FOLDERS_SEGMENT));
    String prefix = key.substring(key.indexOf(FOLDERS_SEGMENT) + FOLDERS_SEGMENT.length());
    // List the whole bucket and filter here: LocalStorageHelper's FakeStorageRpc strips a leading
    // '/' from the PREFIX option, so it would miss the children of a folder whose name starts with
    // '/'.
    for (Blob blob : storage.list(bucket).iterateAll()) {
      if (blob != null && !blob.isDirectory() && blob.getName().startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  private static String extractBucketName(String parent) {
    if (parent == null || !parent.startsWith(BUCKET_PARENT_PREFIX)) {
      throw apiException(StatusCode.Code.INVALID_ARGUMENT, "Invalid parent: " + parent);
    }
    String bucket = parent.substring(BUCKET_PARENT_PREFIX.length());
    if (bucket.isEmpty()) {
      throw apiException(StatusCode.Code.INVALID_ARGUMENT, "Empty bucket in parent: " + parent);
    }
    return bucket;
  }

  /**
   * Parses {@code projects/_/buckets/<bucket>/folders/<folder>} without {@code FolderName}, whose
   * {@code PathTemplate} trims spaces around each segment, and returns the canonical key.
   */
  private static String parseFolderResourceName(String resourceName) {
    if (resourceName == null || !resourceName.startsWith(BUCKET_PARENT_PREFIX)) {
      throw apiException(
          StatusCode.Code.INVALID_ARGUMENT, "Invalid folder resource name: " + resourceName);
    }
    int foldersIndex = resourceName.indexOf(FOLDERS_SEGMENT, BUCKET_PARENT_PREFIX.length());
    if (foldersIndex <= BUCKET_PARENT_PREFIX.length()) {
      throw apiException(
          StatusCode.Code.INVALID_ARGUMENT, "Invalid folder resource name: " + resourceName);
    }
    String bucket = resourceName.substring(BUCKET_PARENT_PREFIX.length(), foldersIndex);
    String folderId = resourceName.substring(foldersIndex + FOLDERS_SEGMENT.length());
    return folderResourceName(bucket, toFolderPath(folderId));
  }

  /**
   * Returns {@code folderId} verbatim with a trailing {@code /}, as GCS stores folder names.
   * Leading, trailing, and repeated slashes and spaces are all valid; only an empty name is not.
   */
  private static String toFolderPath(String folderId) {
    if (folderId == null || folderId.isEmpty()) {
      throw apiException(StatusCode.Code.INVALID_ARGUMENT, "Folder name must not be empty");
    }
    return UriUtil.toDirectoryPath(folderId);
  }

  private static String folderResourceName(String bucket, String folderPath) {
    return BUCKET_PARENT_PREFIX + bucket + FOLDERS_SEGMENT + folderPath;
  }

  /** Returns the folder name that {@code pageToken} resumes after, or empty for the first page. */
  private static Optional<String> decodePageToken(String pageToken) {
    try {
      return FakePageToken.decode(pageToken.isEmpty() ? Optional.empty() : Optional.of(pageToken));
    } catch (IOException e) {
      throw ApiExceptionFactory.createException(
          "Invalid page token.", e, new FakeStatusCode(StatusCode.Code.INVALID_ARGUMENT), false);
    }
  }

  /** Compares names by their UTF-8 bytes, the order in which GCS lists folders. */
  private static int compareUtf8(String a, String b) {
    return Arrays.compareUnsigned(
        a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
  }

  private static Folder newFolder(String resourceName, Timestamp now) {
    return Folder.newBuilder()
        .setName(resourceName)
        .setCreateTime(now)
        .setUpdateTime(now)
        .setMetageneration(1L)
        .build();
  }

  private static Timestamp currentTimestamp() {
    Instant now = Instant.now();
    return Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
  }

  private <Req, Resp> UnaryCallable<Req, Resp> unaryCallable(Function<Req, Resp> handler) {
    return new UnaryCallable<Req, Resp>() {
      @Override
      public ApiFuture<Resp> futureCall(Req request, ApiCallContext context) {
        if (closed) {
          return ApiFutures.immediateFailedFuture(
              apiException(StatusCode.Code.UNAVAILABLE, "StorageControlStub is closed"));
        }
        try {
          return ApiFutures.immediateFuture(handler.apply(request));
        } catch (RuntimeException e) {
          return ApiFutures.immediateFailedFuture(e);
        }
      }
    };
  }

  private static ApiException apiException(StatusCode.Code code, String message) {
    return ApiExceptionFactory.createException(message, null, new FakeStatusCode(code), false);
  }

  private static final class FakeStatusCode implements StatusCode {
    private final StatusCode.Code code;

    private FakeStatusCode(StatusCode.Code code) {
      this.code = code;
    }

    @Override
    public Code getCode() {
      return code;
    }

    @Override
    public Object getTransportCode() {
      return code;
    }
  }

  @Override
  public void close() {
    closed = true;
  }

  @Override
  public void shutdown() {
    closed = true;
  }

  @Override
  public boolean isShutdown() {
    return closed;
  }

  @Override
  public boolean isTerminated() {
    return closed;
  }

  @Override
  public void shutdownNow() {
    closed = true;
  }

  @Override
  public boolean awaitTermination(long duration, TimeUnit unit) {
    return closed;
  }
}
