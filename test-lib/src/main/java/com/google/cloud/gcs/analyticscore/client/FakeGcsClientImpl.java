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
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.base.Supplier;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
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
import java.util.regex.PatternSyntaxException;

public class FakeGcsClientImpl extends GcsClientImpl {
  private static final String PAGE_TOKEN_PREFIX = "name:";
  private static final String REGEX_META_CHARS = "\\.*+?[]{}()|^$-";

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
    Optional<String> afterName = decodePageToken(options.getPageToken());
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
        GcsItemInfo info = projectFields(fromBlob(blob), fields);
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
      pageBuilder.setNextPageToken(encodePageToken(slice.get(slice.size() - 1)));
    }
    return pageBuilder.build();
  }

  private static GcsItemInfo projectFields(GcsItemInfo full, Set<GcsListOptions.ListField> fields) {
    GcsItemId fullId = full.getItemId();
    GcsItemId.Builder idBuilder =
        GcsItemId.builder()
            .setBucketName(fullId.getBucketName())
            .setObjectName(fullId.getObjectName().orElse(""));
    if (fields.contains(GcsListOptions.ListField.GENERATION)) {
      fullId.getContentGeneration().ifPresent(idBuilder::setContentGeneration);
    }
    GcsItemInfo.Builder infoBuilder =
        GcsItemInfo.builder()
            .setItemId(idBuilder.build())
            .setItemType(full.getItemType())
            .setSize(fields.contains(GcsListOptions.ListField.SIZE) ? full.getSize() : 0L)
            .setCreationTime(
                fields.contains(GcsListOptions.ListField.TIME_CREATED)
                    ? full.getCreationTime()
                    : 0L)
            .setModificationTime(
                fields.contains(GcsListOptions.ListField.UPDATED) ? full.getModificationTime() : 0L)
            .setMetaGeneration(
                fields.contains(GcsListOptions.ListField.METAGENERATION)
                    ? full.getMetaGeneration()
                    : 0L);
    if (fields.contains(GcsListOptions.ListField.GENERATION)) {
      full.getContentGeneration().ifPresent(infoBuilder::setContentGeneration);
    }
    if (fields.contains(GcsListOptions.ListField.CONTENT_TYPE)) {
      full.getContentType().ifPresent(infoBuilder::setContentType);
    }
    if (fields.contains(GcsListOptions.ListField.CONTENT_ENCODING)) {
      full.getContentEncoding().ifPresent(infoBuilder::setContentEncoding);
    }
    if (fields.contains(GcsListOptions.ListField.STORAGE_CLASS)) {
      full.getStorageClass().ifPresent(infoBuilder::setStorageClass);
    }
    if (fields.contains(GcsListOptions.ListField.METADATA)) {
      infoBuilder.setExtendedAttributes(full.getExtendedAttributes());
    }
    full.getVerificationAttributes()
        .ifPresent(
            va -> {
              byte[] md5 =
                  fields.contains(GcsListOptions.ListField.MD5HASH) ? va.getMd5hash() : null;
              byte[] crc32c =
                  fields.contains(GcsListOptions.ListField.CRC32C) ? va.getCrc32c() : null;
              if (md5 != null || crc32c != null) {
                infoBuilder.setVerificationAttributes(VerificationAttributes.create(md5, crc32c));
              }
            });
    return infoBuilder.build();
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
    Pattern glob = globExpr.isEmpty() ? null : compileGlob(globExpr);
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

  private static String encodePageToken(String lastEntryName) {
    return PAGE_TOKEN_PREFIX
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(lastEntryName.getBytes(StandardCharsets.UTF_8));
  }

  private static Optional<String> decodePageToken(Optional<String> pageToken) throws IOException {
    if (pageToken.isEmpty()) {
      return Optional.empty();
    }
    String token = pageToken.get();
    if (!token.startsWith(PAGE_TOKEN_PREFIX)) {
      throw new IOException("Invalid pageToken: " + token);
    }
    try {
      byte[] decoded = Base64.getUrlDecoder().decode(token.substring(PAGE_TOKEN_PREFIX.length()));
      String cursor =
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .decode(ByteBuffer.wrap(decoded))
              .toString();
      if (cursor.isEmpty()) {
        throw new IOException("Invalid empty pageToken cursor");
      }
      return Optional.of(cursor);
    } catch (IllegalArgumentException | CharacterCodingException e) {
      throw new IOException("Invalid pageToken: " + token, e);
    }
  }

  private static Pattern compileGlob(String glob) throws IOException {
    StringBuilder regex = new StringBuilder();
    int braceDepth = 0;
    for (int i = 0; i < glob.length(); ) {
      char c = glob.charAt(i);
      if (c == '\\') {
        i = appendBackslashEscape(glob, i, braceDepth, regex);
      } else if (glob.startsWith("**/", i)) {
        if (braceDepth > 0) {
          throw new IOException(
              "Invalid matchGlob (cannot contain '/' or '**' in braces): " + glob);
        }
        regex.append(i == 0 || glob.charAt(i - 1) == '/' ? "(?:.*/)?" : ".*/");
        i += 3;
      } else if (glob.startsWith("**", i)) {
        if (braceDepth > 0) {
          throw new IOException("Invalid matchGlob (cannot contain '**' in braces): " + glob);
        }
        regex.append(".*");
        i += 2;
      } else if (c == '[') {
        i = appendCharacterClass(glob, i + 1, regex);
      } else if (c == ']') {
        throw new IOException("Invalid matchGlob (unmatched ']'): " + glob);
      } else if (c == '{') {
        braceDepth++;
        regex.append("(?:");
        i++;
      } else if (c == ',') {
        regex.append(braceDepth > 0 ? "|" : ",");
        i++;
      } else if (c == '}') {
        if (braceDepth == 0) {
          throw new IOException("Invalid matchGlob (unmatched '}'): " + glob);
        }
        braceDepth--;
        regex.append(')');
        i++;
      } else if (c == '/') {
        if (braceDepth > 0) {
          throw new IOException("Invalid matchGlob (cannot contain '/' in braces): " + glob);
        }
        regex.append('/');
        i++;
      } else if (c == '*') {
        regex.append("[^/]*");
        i++;
      } else if (c == '?') {
        regex.append("[^/]");
        i++;
      } else {
        appendEscapedLiteral(regex, c);
        i++;
      }
    }
    if (braceDepth > 0) {
      throw new IOException("Invalid matchGlob (unclosed '{'): " + glob);
    }
    try {
      return Pattern.compile(regex.toString(), Pattern.DOTALL);
    } catch (PatternSyntaxException e) {
      throw new IOException("Invalid matchGlob: " + glob, e);
    }
  }

  private static int appendBackslashEscape(
      String glob, int backslashIdx, int braceDepth, StringBuilder regex) throws IOException {
    if (backslashIdx + 1 >= glob.length()) {
      regex.append("\\\\");
      return backslashIdx + 1;
    }
    char escaped = glob.charAt(backslashIdx + 1);
    if (braceDepth > 0 && escaped == '/') {
      throw new IOException("Invalid matchGlob (cannot contain '/' in braces): " + glob);
    }
    appendEscapedLiteral(regex, escaped);
    return backslashIdx + 2;
  }

  private static int appendCharacterClass(String glob, int startIdx, StringBuilder regex)
      throws IOException {
    StringBuilder body = new StringBuilder();
    boolean negated = false;
    int i = startIdx;
    if (i < glob.length() && (glob.charAt(i) == '!' || glob.charAt(i) == '^')) {
      negated = true;
      i++;
    }
    if (i < glob.length() && glob.charAt(i) == ']') {
      throw new IOException("Invalid matchGlob (empty character class): " + glob);
    }
    while (i < glob.length()) {
      char ch = glob.charAt(i);
      if (ch == '\\') {
        if (i + 1 >= glob.length()) {
          throw new IOException("Invalid matchGlob (unclosed '['): " + glob);
        }
        char esc = glob.charAt(i + 1);
        if ("\\[]-^&".indexOf(esc) >= 0) {
          body.append('\\');
        }
        body.append(esc);
        i += 2;
      } else if (ch == '[') {
        throw new IOException("Invalid matchGlob (unescaped '[' in character class): " + glob);
      } else if (ch == ']') {
        regex.append(negated ? "(?!/)[^" : "(?!/)[");
        regex.append(body).append(']');
        return i + 1;
      } else if (ch == '&') {
        body.append("\\&");
        i++;
      } else {
        body.append(ch);
        i++;
      }
    }
    throw new IOException("Invalid matchGlob (unclosed '['): " + glob);
  }

  private static void appendEscapedLiteral(StringBuilder regex, char c) {
    if (REGEX_META_CHARS.indexOf(c) >= 0) {
      regex.append('\\');
    }
    regex.append(c);
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
