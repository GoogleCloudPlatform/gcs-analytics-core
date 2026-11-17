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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.common.collect.Sets;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/** Options controlling single-page object and prefix listing in {@link GcsClient}. */
@AutoValue
public abstract class GcsListOptions {

  /**
   * Object metadata fields that can be projected when listing GCS objects.
   *
   * <p>By default, {@link #builder()} selects the standard object metadata fields (all values
   * except {@link #STORAGE_CLASS}, which can be requested explicitly when needed). {@link #NAME}
   * and {@link #BUCKET} are always included on {@link Builder#build()}, so passing no fields or
   * only {@link #NAME} (for example, {@code setFields()} or {@code setFields(ListField.NAME)})
   * selects a minimal {@code bucket,name}-only projection.
   */
  public enum ListField {
    /** Object name within the bucket. */
    NAME,
    /** Bucket name containing the object. */
    BUCKET,
    /** Object content generation number. */
    GENERATION,
    /** Object metadata generation number. */
    METAGENERATION,
    /**
     * Object size in bytes. Omitting this field causes listed items to report {@link
     * GcsItemInfo#getSize()} {@code == 0L} (with {@link GcsItemInfo#exists()} {@code true}); do not
     * pass items listed without {@code SIZE} to read channels or file-sizing paths.
     */
    SIZE,
    /** Object MIME content type. */
    CONTENT_TYPE,
    /** Object content encoding. */
    CONTENT_ENCODING,
    /** Object creation timestamp. */
    TIME_CREATED,
    /** Object last modification timestamp. */
    UPDATED,
    /** Object storage class. */
    STORAGE_CLASS,
    /** Object CRC32C checksum. */
    CRC32C,
    /** Object MD5 hash. */
    MD5HASH,
    /** User-defined custom object metadata. */
    METADATA
  }

  /** Fields selected by {@link #builder()}; also requested by single-object metadata lookups. */
  static final Set<ListField> DEFAULT_FIELDS =
      Sets.immutableEnumSet(
          ListField.NAME,
          ListField.BUCKET,
          ListField.GENERATION,
          ListField.METAGENERATION,
          ListField.SIZE,
          ListField.CONTENT_TYPE,
          ListField.CONTENT_ENCODING,
          ListField.TIME_CREATED,
          ListField.UPDATED,
          ListField.CRC32C,
          ListField.MD5HASH,
          ListField.METADATA);

  /**
   * Default per-RPC upper-bound hint on {@code items + prefixes} ({@code 5,000}) used by {@link
   * GcsClient} when {@link #getPageSize()} is empty, matching {@code hadoop-connectors} ({@code
   * fs.gs.list.max.items.per.call} / {@code GoogleCloudStorage.LIST_MAX_RESULTS}) and GCS's
   * server-side ceiling for non-ACL {@code objects.list} requests.
   */
  public static final int DEFAULT_MAX_LIST_ITEMS_PER_CALL = 5000;

  /**
   * Preset for listing immediate children of a directory in a flat-namespace bucket (equals {@code
   * builder().build()}, matching {@code hadoop-connectors} {@code ListObjectOptions.DEFAULT}).
   */
  public static final GcsListOptions LIST_DIRECTORY = builder().build();

  /**
   * Preset for listing immediate children of a directory in a Hierarchical Namespace (HNS) bucket,
   * returning HNS folders (and managed folders) in {@link GcsListPage#getPrefixes()}.
   */
  public static final GcsListOptions LIST_DIRECTORY_HNS =
      builder().setIncludeFoldersAsPrefixes(true).build();

  /**
   * Preset for recursively listing all objects under a prefix without delimiter collapsing
   * (matching {@code hadoop-connectors} {@code ListObjectOptions.DEFAULT_FLAT_LIST}: {@code
   * includePrefixObject = false}, so a real {@code "dir/"} marker matching the listed prefix is
   * excluded from {@link GcsListPage#getItems()} and surfaced via {@link
   * GcsListPage#getPrefixObject()}). Delete or rename callers needing only IDs and generations can
   * project {@code LIST_RECURSIVE.toBuilder().setFields(ListField.NAME,
   * ListField.GENERATION).build()}.
   */
  public static final GcsListOptions LIST_RECURSIVE = builder().clearDelimiter().build();

  /**
   * Package-private preset for flat-namespace directory emptiness checks: {@code pageSize = 2}
   * accommodates a leading {@code "dir/"} placeholder plus at least one child in a single RPC, and
   * {@code fields = {NAME, BUCKET}} projects minimal {@code bucket,name} fields. Empty iff {@code
   * page.getItems().isEmpty() && page.getPrefixes().isEmpty() && !page.hasNextPage()} (keep paging
   * while {@code page.hasNextPage()} is {@code true}).
   */
  static final GcsListOptions EMPTINESS_CHECK_FLAT =
      LIST_DIRECTORY.toBuilder().setPageSize(2).setFields(ListField.NAME).build();

  /**
   * Package-private preset for HNS directory emptiness checks (including HNS folders): when listing
   * {@code prefix = "dir/"}, GCS excludes {@code "dir/"} itself from {@code prefixes[]}, so {@code
   * pageSize = 2} accommodates an optional 0-byte {@code "dir/"} placeholder in {@code items[]}
   * plus at least one child or folder in one RPC, and {@code fields = {NAME, BUCKET}} projects
   * minimal {@code bucket,name} fields. Empty iff {@code page.getItems().isEmpty() &&
   * page.getPrefixes().isEmpty() && !page.hasNextPage()} (keep paging while {@code
   * page.hasNextPage()} is {@code true}).
   */
  static final GcsListOptions EMPTINESS_CHECK_HNS =
      LIST_DIRECTORY_HNS.toBuilder().setPageSize(2).setFields(ListField.NAME).build();

  /**
   * Returns the delimiter used to collapse object names into prefixes, or empty for flat list. When
   * present, {@link GcsClient} enables {@code includeTrailingDelimiter} on GCS {@code objects.list}
   * requests so child placeholder objects (for example, {@code "dir/sub/"}) are surfaced in {@link
   * GcsListPage#getItems()} with full metadata.
   */
  public abstract Optional<String> getDelimiter();

  /**
   * Returns whether HNS folders (and managed folders) are returned in prefixes. Requires {@link
   * #getDelimiter()} to be {@code "/"}.
   */
  public abstract boolean isIncludeFoldersAsPrefixes();

  /**
   * Returns whether a real object matching the listed directory prefix (e.g. {@code "dir/"}) is
   * included in {@link GcsListPage#getItems()}.
   *
   * <p>When {@code false}, a real object matching the listed directory prefix is excluded from
   * {@link GcsListPage#getItems()} and surfaced via {@link GcsListPage#getPrefixObject()}. No
   * synthetic or inferred directory entry is ever fabricated for the listed prefix.
   */
  public abstract boolean isIncludePrefixObject();

  /**
   * Returns the per-RPC upper-bound hint on {@code items + prefixes}, or empty to defer to the
   * client's default per-RPC limit ({@link #DEFAULT_MAX_LIST_ITEMS_PER_CALL}, {@code 5,000}). When
   * present, the value is passed unchanged as the per-RPC {@code pageSize}; GCS silently caps each
   * returned page at {@code 5,000} entries on non-ACL list requests when {@code pageSize > 5,000}
   * (vs. GCS's {@code 1,000} server default when {@code pageSize} is omitted on the wire). Pages
   * may contain fewer or {@code 0} entries while {@link GcsListPage#hasNextPage()} is {@code true}
   * due to server behavior, self-prefix filtering, or placeholder-prefix deduplication.
   */
  public abstract OptionalInt getPageSize();

  /** Returns the non-empty continuation token from a previous {@link GcsListPage}, if set. */
  public abstract Optional<String> getPageToken();

  /**
   * Returns GCS's raw inclusive UTF-8 byte-order lower bound ({@code objectName >= startOffset}).
   * Does not automatically disable delimiter collapsing or skip directory marker objects (use
   * {@link #LIST_RECURSIVE}{@code .toBuilder().setStartOffset(...)} for flat range scans).
   */
  public abstract Optional<String> getStartOffset();

  /** Returns GCS's raw exclusive UTF-8 byte-order upper bound ({@code objectName < endOffset}). */
  public abstract Optional<String> getEndOffset();

  /**
   * Returns the glob pattern filtering object and prefix names. Requires {@link #getDelimiter()} to
   * be absent or {@code "/"}.
   */
  public abstract Optional<String> getMatchGlob();

  /**
   * Returns an unmodifiable set of {@link ListField}s to project (never empty).
   *
   * <p>{@link ListField#NAME} and {@link ListField#BUCKET} are always included. Defaults to the
   * standard object metadata fields (all {@link ListField} values except {@link
   * ListField#STORAGE_CLASS}).
   */
  // Keep JDK collection types in public signatures for Guava-excluded consumers (#456).
  @SuppressWarnings("AutoValueImmutableFields")
  public abstract Set<ListField> getFields();

  /** Returns a builder initialized with the values of this {@link GcsListOptions} instance. */
  public abstract Builder toBuilder();

  /**
   * Returns a new {@link Builder} initialized with defaults matching {@link #LIST_DIRECTORY}:
   * {@code delimiter = "/"}, {@code includeFoldersAsPrefixes = false}, {@code includePrefixObject =
   * false}, empty optional filters, and the standard object metadata {@code fields} projection.
   */
  public static Builder builder() {
    return new AutoValue_GcsListOptions.Builder()
        .setDelimiter("/")
        .setIncludeFoldersAsPrefixes(false)
        .setIncludePrefixObject(false)
        .setFields(DEFAULT_FIELDS);
  }

  /** Builder for {@link GcsListOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Sets the non-empty delimiter string used to collapse object names into prefixes. */
    public abstract Builder setDelimiter(String delimiter);

    abstract Builder setDelimiter(Optional<String> delimiter);

    /** Clears the delimiter so objects are listed flat without prefix collapsing. */
    public Builder clearDelimiter() {
      return setDelimiter(Optional.empty());
    }

    /** Sets whether HNS folders (and managed folders) are returned in prefixes. */
    public abstract Builder setIncludeFoldersAsPrefixes(boolean includeFoldersAsPrefixes);

    /** Sets whether a real object matching the listed directory prefix is included in items. */
    public abstract Builder setIncludePrefixObject(boolean includePrefixObject);

    /** Sets the positive per-RPC upper-bound hint on {@code items + prefixes}. */
    public abstract Builder setPageSize(int pageSize);

    /**
     * Sets the pagination continuation token. An empty string normalizes to absent because gRPC
     * proto3 {@code ListObjectsResponse.next_page_token} defaults to {@code ""} when unset.
     */
    public abstract Builder setPageToken(String pageToken);

    abstract Builder setPageToken(Optional<String> pageToken);

    abstract Optional<String> getPageToken();

    /** Sets the non-empty inclusive UTF-8 byte-order lower bound filter. */
    public abstract Builder setStartOffset(String startOffset);

    /** Sets the non-empty exclusive UTF-8 byte-order upper bound filter. */
    public abstract Builder setEndOffset(String endOffset);

    /** Sets the non-empty glob pattern filter. */
    public abstract Builder setMatchGlob(String matchGlob);

    abstract Builder setFields(Set<ListField> fields);

    abstract Set<ListField> getFields();

    /** Sets the projection of object metadata fields from an {@link Iterable}. */
    public Builder setFields(Iterable<ListField> fields) {
      return setFields(Sets.immutableEnumSet(fields));
    }

    /** Sets the projection of object metadata fields. */
    public Builder setFields(ListField... fields) {
      return setFields(Arrays.asList(fields));
    }

    abstract GcsListOptions autoBuild();

    /**
     * Normalizes and validates options, returning an immutable {@link GcsListOptions} instance.
     *
     * @throws IllegalArgumentException if {@code delimiter}, {@code startOffset}, {@code
     *     endOffset}, or {@code matchGlob} is empty, if {@code includeFoldersAsPrefixes} or {@code
     *     matchGlob} violates GCS delimiter constraints, if {@code pageSize <= 0}, or if {@code
     *     startOffset >= endOffset} in UTF-8 byte order
     */
    public GcsListOptions build() {
      EnumSet<ListField> normalizedFields = EnumSet.of(ListField.NAME, ListField.BUCKET);
      normalizedFields.addAll(getFields());
      setFields(Sets.immutableEnumSet(normalizedFields));
      // gRPC proto3 ListObjectsResponse.next_page_token defaults to "" when absent.
      if (getPageToken().map(String::isEmpty).orElse(false)) {
        setPageToken(Optional.empty());
      }
      GcsListOptions options = autoBuild();
      Optional<String> delimiter = options.getDelimiter();
      if (delimiter.isPresent()) {
        checkArgument(
            !delimiter.get().isEmpty(),
            "delimiter must not be empty; use clearDelimiter() for flat listing");
      }
      boolean slashDelimiter = delimiter.map("/"::equals).orElse(false);
      checkArgument(
          !options.isIncludeFoldersAsPrefixes() || slashDelimiter,
          "includeFoldersAsPrefixes requires delimiter \"/\", but got: %s",
          delimiter);
      if (options.getMatchGlob().isPresent()) {
        checkArgument(!options.getMatchGlob().get().isEmpty(), "matchGlob must not be empty");
        checkArgument(
            delimiter.isEmpty() || slashDelimiter,
            "matchGlob requires delimiter to be absent or \"/\", but got: %s",
            delimiter);
      }
      if (options.getPageSize().isPresent()) {
        int pageSize = options.getPageSize().getAsInt();
        checkArgument(pageSize > 0, "pageSize must be positive, but got: %s", pageSize);
      }
      if (options.getStartOffset().isPresent()) {
        checkArgument(!options.getStartOffset().get().isEmpty(), "startOffset must not be empty");
      }
      if (options.getEndOffset().isPresent()) {
        checkArgument(!options.getEndOffset().get().isEmpty(), "endOffset must not be empty");
      }
      if (options.getStartOffset().isPresent() && options.getEndOffset().isPresent()) {
        String startOffset = options.getStartOffset().get();
        String endOffset = options.getEndOffset().get();
        checkArgument(
            Arrays.compareUnsigned(
                    startOffset.getBytes(StandardCharsets.UTF_8),
                    endOffset.getBytes(StandardCharsets.UTF_8))
                < 0,
            "startOffset (%s) must be < endOffset (%s) in UTF-8 byte order",
            startOffset,
            endOffset);
      }
      return options;
    }
  }
}
