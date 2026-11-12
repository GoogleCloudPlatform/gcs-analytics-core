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
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.storage.control.v2.CreateFolderRequest;
import com.google.storage.control.v2.DeleteFolderRequest;
import com.google.storage.control.v2.Folder;
import com.google.storage.control.v2.FolderName;
import com.google.storage.control.v2.GetFolderRequest;
import com.google.storage.control.v2.stub.StorageControlStub;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * In-memory {@link StorageControlStub} for hermetic {@link FakeGcsClientImpl} folder operations.
 */
final class FakeStorageControlStub extends StorageControlStub {
  private static final String BUCKET_PARENT_PREFIX = "projects/_/buckets/";

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

  private Folder createFolder(CreateFolderRequest request) {
    String bucket = extractBucketName(request.getParent());
    String folderPath = UriUtil.removeTrailingSlash(request.getFolderId());
    if (folderPath.isEmpty()) {
      throw apiException(StatusCode.Code.INVALID_ARGUMENT, "Folder ID must not be empty");
    }
    String key = FolderName.format("_", bucket, folderPath);
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
    FolderName parsed = parseFolderName(request.getName());
    String key =
        FolderName.format("_", parsed.getBucket(), UriUtil.removeTrailingSlash(parsed.getFolder()));
    Folder folder = folders.get(key);
    if (folder == null) {
      throw apiException(StatusCode.Code.NOT_FOUND, "Folder not found: " + key);
    }
    return folder;
  }

  private Empty deleteFolder(DeleteFolderRequest request) {
    FolderName parsed = parseFolderName(request.getName());
    String bucket = parsed.getBucket();
    String folderPath = UriUtil.removeTrailingSlash(parsed.getFolder());
    String key = FolderName.format("_", bucket, folderPath);
    synchronized (folders) {
      if (!folders.containsKey(key)) {
        throw apiException(StatusCode.Code.NOT_FOUND, "Folder not found: " + key);
      }
      if (hasChildFolder(bucket, folderPath) || hasChildObject(bucket, folderPath)) {
        throw apiException(
            StatusCode.Code.FAILED_PRECONDITION, "The folder you tried to delete is not empty.");
      }
      folders.remove(key);
      return Empty.getDefaultInstance();
    }
  }

  private void ensureParentFolders(
      String bucket, String folderPath, boolean recursive, Timestamp now) {
    int lastSlash = folderPath.lastIndexOf('/');
    if (lastSlash < 0) {
      return;
    }
    if (!recursive) {
      String immediateParent = folderPath.substring(0, lastSlash);
      String parentKey = FolderName.format("_", bucket, immediateParent);
      if (!folders.containsKey(parentKey)) {
        throw apiException(
            StatusCode.Code.FAILED_PRECONDITION, "Parent folder does not exist: " + parentKey);
      }
      return;
    }
    int idx = folderPath.indexOf('/');
    while (idx >= 0) {
      String ancestorPath = folderPath.substring(0, idx);
      if (!ancestorPath.isEmpty()) {
        String ancestorKey = FolderName.format("_", bucket, ancestorPath);
        folders.computeIfAbsent(ancestorKey, k -> newFolder(k, now));
      }
      idx = folderPath.indexOf('/', idx + 1);
    }
  }

  private boolean hasChildFolder(String bucket, String folderPath) {
    String prefix = FolderName.format("_", bucket, folderPath) + "/";
    for (String existingKey : folders.keySet()) {
      if (existingKey.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  private boolean hasChildObject(String bucket, String folderPath) {
    String prefix = folderPath + "/";
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

  private static FolderName parseFolderName(String resourceName) {
    if (resourceName == null || !FolderName.isParsableFrom(resourceName)) {
      throw apiException(
          StatusCode.Code.INVALID_ARGUMENT, "Invalid folder resource name: " + resourceName);
    }
    return FolderName.parse(resourceName);
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
