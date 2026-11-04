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
import com.google.common.collect.ImmutableSet;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/** Options controlling single-page object and prefix listing in {@link GcsClient}. */
@AutoValue
public abstract class GcsListOptions {

  /**
   * Object metadata fields that can be projected when listing GCS objects.
   *
   * <p>When {@link #getFields()} is empty, the client requests its default 12-field object metadata
   * projection ({@code NAME}, {@code BUCKET}, {@code GENERATION}, {@code METAGENERATION}, {@code
   * SIZE}, {@code CONTENT_TYPE}, {@code CONTENT_ENCODING}, {@code TIME_CREATED}, {@code UPDATED},
   * {@code MD5HASH}, {@code CRC32C}, and {@code METADATA}, i.e. {@code
   * GcsClientImpl.BLOB_METADATA_FIELDS_ARRAY}). When {@link #getFields()} is non-empty, {@code
   * google-cloud-storage} always includes {@code BUCKET} and {@code NAME} ({@code
   * BlobField.REQUIRED_FIELDS}), so {@code Set.of(ListField.NAME)} requests a minimal {@code
   * bucket,name}-only projection.
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
    /** Object size in bytes. */
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
   * Preset for recursively listing all objects under a prefix without delimiter collapsing. Delete
   * or rename callers needing only IDs and generations can project {@code
   * LIST_RECURSIVE.toBuilder().setFields(List.of(ListField.NAME, ListField.GENERATION)).build()}.
   */
  public static final GcsListOptions LIST_RECURSIVE =
      builder()
          .setDelimiter(Optional.empty())
          .setIncludeFoldersAsPrefixes(false)
          .setIncludeTrailingDelimiter(false)
          .setIncludePrefixObject(true)
          .build();

  /**
   * Preset for flat-namespace directory emptiness checks: {@code pageSize = 2} accommodates a
   * leading {@code "dir/"} placeholder plus at least one child in a single RPC, and {@code fields =
   * {NAME}} projects minimal {@code bucket,name} fields. Empty iff {@code page.getItems().isEmpty()
   * && page.getPrefixes().isEmpty() && !page.hasNextPage()} (keep paging while {@code
   * page.hasNextPage()} is {@code true}).
   */
  public static final GcsListOptions EMPTINESS_CHECK_FLAT =
      LIST_DIRECTORY.toBuilder().setPageSize(2).setFields(Set.of(ListField.NAME)).build();

  /**
   * Preset for HNS directory emptiness checks (including HNS folders): {@code pageSize = 2}
   * accommodates a leading {@code "dir/"} placeholder plus at least one child or folder in one RPC,
   * and {@code fields = {NAME}} projects minimal {@code bucket,name} fields. Empty iff {@code
   * page.getItems().isEmpty() && page.getPrefixes().isEmpty() && !page.hasNextPage()} (keep paging
   * while {@code page.hasNextPage()} is {@code true}).
   */
  public static final GcsListOptions EMPTINESS_CHECK_HNS =
      LIST_DIRECTORY_HNS.toBuilder().setPageSize(2).setFields(Set.of(ListField.NAME)).build();

  /** Returns the delimiter used to collapse object names into prefixes, or empty for flat list. */
  public abstract Optional<String> getDelimiter();

  /**
   * Returns whether HNS folders (and managed folders) are returned in prefixes. Requires {@link
   * #getDelimiter()} to be {@code "/"}.
   */
  public abstract boolean isIncludeFoldersAsPrefixes();

  /**
   * Returns whether child objects ending with {@link #getDelimiter()} are also surfaced as items.
   * Ignored when {@link #getDelimiter()} is empty.
   */
  public abstract boolean isIncludeTrailingDelimiter();

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
   * present, the value is passed unchanged as the per-RPC {@code pageSize} (GCS enforces a {@code
   * 5,000} ceiling on non-ACL list requests, vs. GCS's {@code 1,000} server default when {@code
   * pageSize} is omitted on the wire). Pages may contain fewer or {@code 0} entries while {@link
   * GcsListPage#hasNextPage()} is {@code true} due to server behavior, self-prefix filtering, or
   * placeholder-prefix deduplication.
   */
  public abstract OptionalInt getPageSize();

  /** Returns the non-empty continuation token from a previous {@link GcsListPage}, if set. */
  public abstract Optional<String> getPageToken();

  /**
   * Returns GCS's raw inclusive UTF-8 byte-order lower bound ({@code objectName >= startOffset}).
   * Does not automatically disable delimiter collapsing or skip directory marker objects.
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
   * Returns an unmodifiable set of {@link ListField}s to project.
   *
   * <p>An empty set selects the client's default 12-field metadata projection ({@code
   * GcsClientImpl.BLOB_METADATA_FIELDS_ARRAY}). For any non-empty set, {@code google-cloud-storage}
   * always includes {@code BUCKET} and {@code NAME} ({@code BlobField.REQUIRED_FIELDS}), so {@code
   * Set.of(ListField.NAME)} requests a minimal {@code bucket,name}-only projection.
   */
  public abstract Set<ListField> getFields();

  /** Returns a builder initialized with the values of this {@link GcsListOptions} instance. */
  public abstract Builder toBuilder();

  /**
   * Returns a new {@link Builder} initialized with defaults matching {@link #LIST_DIRECTORY}:
   * {@code delimiter = "/"}, {@code includeFoldersAsPrefixes = false}, {@code
   * includeTrailingDelimiter = true}, {@code includePrefixObject = false}, empty optional filters,
   * and an empty {@code fields} set.
   */
  public static Builder builder() {
    return new AutoValue_GcsListOptions.Builder()
        .setDelimiter("/")
        .setIncludeFoldersAsPrefixes(false)
        .setIncludeTrailingDelimiter(true)
        .setIncludePrefixObject(false)
        .setFields(ImmutableSet.of());
  }

  /** Builder for {@link GcsListOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Sets the non-empty delimiter string used to collapse object names into prefixes. */
    public abstract Builder setDelimiter(String delimiter);

    /** Sets or clears the optional delimiter (use {@link Optional#empty()} for flat listing). */
    public abstract Builder setDelimiter(Optional<String> delimiter);

    /** Sets whether HNS folders (and managed folders) are returned in prefixes. */
    public abstract Builder setIncludeFoldersAsPrefixes(boolean includeFoldersAsPrefixes);

    /** Sets whether child objects ending with the delimiter are included in items. */
    public abstract Builder setIncludeTrailingDelimiter(boolean includeTrailingDelimiter);

    /** Sets whether a real object matching the listed directory prefix is included in items. */
    public abstract Builder setIncludePrefixObject(boolean includePrefixObject);

    /** Sets the positive per-RPC upper-bound hint on {@code items + prefixes}. */
    public abstract Builder setPageSize(int pageSize);

    /** Sets or clears the optional per-RPC upper-bound hint on {@code items + prefixes}. */
    public abstract Builder setPageSize(OptionalInt pageSize);

    /** Sets the pagination continuation token (an empty string normalizes to absent). */
    public abstract Builder setPageToken(String pageToken);

    /** Sets or clears the optional continuation token (an empty string normalizes to absent). */
    public abstract Builder setPageToken(Optional<String> pageToken);

    abstract Optional<String> getPageToken();

    /** Sets the inclusive UTF-8 byte-order lower bound filter. */
    public abstract Builder setStartOffset(String startOffset);

    /** Sets or clears the optional inclusive UTF-8 byte-order lower bound filter. */
    public abstract Builder setStartOffset(Optional<String> startOffset);

    /** Sets the exclusive UTF-8 byte-order upper bound filter. */
    public abstract Builder setEndOffset(String endOffset);

    /** Sets or clears the optional exclusive UTF-8 byte-order upper bound filter. */
    public abstract Builder setEndOffset(Optional<String> endOffset);

    /** Sets the glob pattern filter. */
    public abstract Builder setMatchGlob(String matchGlob);

    /** Sets or clears the optional glob pattern filter. */
    public abstract Builder setMatchGlob(Optional<String> matchGlob);

    abstract Builder setFields(Set<ListField> fields);

    abstract Set<ListField> getFields();

    /** Sets the projection of object metadata fields from an {@link Iterable}. */
    public Builder setFields(Iterable<ListField> fields) {
      return setFields(ImmutableSet.copyOf(fields));
    }

    abstract GcsListOptions autoBuild();

    /**
     * Normalizes and validates options, returning an immutable {@link GcsListOptions} instance.
     *
     * @throws IllegalArgumentException if {@code delimiter} is empty, if {@code
     *     includeFoldersAsPrefixes} or {@code matchGlob} violates GCS delimiter constraints, if
     *     {@code pageSize <= 0}, or if {@code startOffset > endOffset} in UTF-8 byte order
     */
    public GcsListOptions build() {
      setFields(ImmutableSet.copyOf(getFields()));
      if (getPageToken().map(String::isEmpty).orElse(false)) {
        setPageToken(Optional.empty());
      }
      GcsListOptions options = autoBuild();
      Optional<String> delimiter = options.getDelimiter();
      if (delimiter.isPresent()) {
        checkArgument(
            !delimiter.get().isEmpty(),
            "delimiter must not be empty; use Optional.empty() for flat listing");
      }
      boolean slashDelimiter = delimiter.map("/"::equals).orElse(false);
      checkArgument(
          !options.isIncludeFoldersAsPrefixes() || slashDelimiter,
          "includeFoldersAsPrefixes requires delimiter \"/\", but got: %s",
          delimiter);
      checkArgument(
          options.getMatchGlob().isEmpty() || delimiter.isEmpty() || slashDelimiter,
          "matchGlob requires delimiter to be absent or \"/\", but got: %s",
          delimiter);
      if (options.getPageSize().isPresent()) {
        int pageSize = options.getPageSize().getAsInt();
        checkArgument(pageSize > 0, "pageSize must be positive, but got: %s", pageSize);
      }
      if (options.getStartOffset().isPresent() && options.getEndOffset().isPresent()) {
        String startOffset = options.getStartOffset().get();
        String endOffset = options.getEndOffset().get();
        checkArgument(
            Arrays.compareUnsigned(
                    startOffset.getBytes(StandardCharsets.UTF_8),
                    endOffset.getBytes(StandardCharsets.UTF_8))
                <= 0,
            "startOffset (%s) must be <= endOffset (%s) in UTF-8 byte order",
            startOffset,
            endOffset);
      }
      return options;
    }
  }
}
