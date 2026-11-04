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
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.base.Supplier;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
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
  public BucketProperties getBucketProperties(String bucketName) {
    // LocalStorageHelper does not support bucket operations
    return BucketProperties.create(false, false);
  }

  @Override
  public VectoredSeekableByteChannel openReadChannel(
      GcsItemInfo itemInfo, GcsReadOptions readOptions) throws IOException {
    openReadChannelCount++;
    return super.openReadChannel(itemInfo, readOptions);
  }

  @Override
  public GcsListPage<GcsItemInfo> listObjectsPage(GcsItemId prefixId, GcsListOptions options)
      throws IOException {
    checkNotNull(prefixId, "prefixId must not be null");
    checkNotNull(options, "options must not be null");
    checkArgument(!prefixId.getBucketName().isEmpty(), "Empty bucket name: %s", prefixId);
    int offset = parsePageToken(options.getPageToken());
    String bucketName = prefixId.getBucketName();
    String prefix = prefixId.getObjectName().orElse("");
    Iterable<Blob> rawBlobs =
        storage
            .list(
                bucketName,
                BlobListOption.prefix(prefix),
                BlobListOption.fields(resolveBlobFields(options)))
            .iterateAll();

    Map<String, Blob> emittedObjects = new HashMap<>();
    List<String> matched = collectMatchedNames(rawBlobs, prefix, options, emittedObjects);
    int end =
        options.getPageSize().isPresent()
            ? (int) Math.min(matched.size(), (long) offset + options.getPageSize().getAsInt())
            : matched.size();
    List<String> slice =
        offset >= matched.size() ? List.of() : matched.subList(offset, Math.max(offset, end));

    String dirSuffix = options.getDelimiter().orElse("/");
    boolean isDirectoryPrefix = !prefix.isEmpty() && prefix.endsWith(dirSuffix);
    List<GcsItemInfo> items = new ArrayList<>();
    List<GcsItemId> prefixes = new ArrayList<>();
    GcsItemInfo prefixObject = null;
    for (String name : slice) {
      Blob blob = emittedObjects.get(name);
      if (blob == null) {
        prefixes.add(GcsItemId.builder().setBucketName(bucketName).setObjectName(name).build());
      } else {
        GcsItemInfo info = fromBlob(blob);
        if (isDirectoryPrefix && name.equals(prefix) && !options.isIncludePrefixObject()) {
          prefixObject = info;
        } else {
          items.add(info);
        }
      }
    }
    Optional<String> nextToken =
        end < matched.size() ? Optional.of(Integer.toString(end)) : Optional.empty();
    return GcsListPage.<GcsItemInfo>builder()
        .setItems(items)
        .setPrefixes(prefixes)
        .setPrefixObject(Optional.ofNullable(prefixObject))
        .setNextPageToken(nextToken)
        .build();
  }

  private static List<String> collectMatchedNames(
      Iterable<Blob> rawBlobs,
      String prefix,
      GcsListOptions options,
      Map<String, Blob> emittedObjects) {
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
      if (delimIdx < 0
          || (delimIdx + delimiter.get().length() == name.length()
              && options.isIncludeTrailingDelimiter())) {
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
    Pattern glob = globExpr.isEmpty() ? null : compileGlob(globExpr);
    List<String> matched = new ArrayList<>(entryNames.size());
    for (String name : entryNames) {
      if (glob == null || glob.matcher(name).matches()) {
        matched.add(name);
      }
    }
    return matched;
  }

  private static int parsePageToken(Optional<String> pageToken) throws IOException {
    if (pageToken.isEmpty()) {
      return 0;
    }
    try {
      int parsed = Integer.parseInt(pageToken.get());
      if (parsed >= 0) {
        return parsed;
      }
    } catch (NumberFormatException e) {
      throw new IOException("Invalid pageToken: " + pageToken.get(), e);
    }
    throw new IOException("Invalid negative pageToken: " + pageToken.get());
  }

  private static Pattern compileGlob(String glob) {
    StringBuilder regex = new StringBuilder();
    for (int i = 0; i < glob.length(); ) {
      if (glob.startsWith("**/", i)) {
        regex.append(i == 0 || glob.charAt(i - 1) == '/' ? "(?:.*/)?" : ".*/");
        i += 3;
      } else if (glob.startsWith("**", i)) {
        regex.append(".*");
        i += 2;
      } else {
        char c = glob.charAt(i++);
        String token =
            c == '*'
                ? "[^/]*"
                : c == '?' ? "[^/]" : ("\\.[]{}()+-^$|".indexOf(c) >= 0 ? "\\" + c : "" + c);
        regex.append(token);
      }
    }
    return Pattern.compile(regex.toString(), Pattern.DOTALL);
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
