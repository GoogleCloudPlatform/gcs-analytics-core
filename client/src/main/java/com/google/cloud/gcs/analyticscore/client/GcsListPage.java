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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;

/**
 * Immutable single page of results from a GCS list operation.
 *
 * <p>Both {@link #getItems()} and {@link #getPrefixes()} preserve GCS's unsigned UTF-8
 * lexicographic ordering. When a child placeholder object (such as {@code "dir/sub/"}) is present
 * in {@link #getItems()}, the corresponding collapsed prefix is omitted from {@link
 * #getPrefixes()}, and the listed directory prefix itself is never included in {@link
 * #getPrefixes()}. No synthetic or inferred directory entry is ever fabricated for the listed
 * prefix; a real object matching the listed directory prefix appears either in {@link
 * #getPrefixObject()} (when {@link GcsListOptions#isIncludePrefixObject()} is {@code false}) or in
 * {@link #getItems()} (when {@code true}), never both.
 *
 * @param <T> the element type of listed items (for example, {@link GcsItemInfo})
 */
@AutoValue
public abstract class GcsListPage<T> {

  /**
   * Returns an unmodifiable list of object items in this page in unsigned UTF-8 byte order,
   * disjoint from {@link #getPrefixObject()}.
   */
  public abstract List<T> getItems();

  /**
   * Returns an unmodifiable, deduplicated list of collapsed directory prefixes in this page in
   * unsigned UTF-8 byte order, excluding prefixes present as placeholder objects in {@link
   * #getItems()} and excluding the listed directory prefix itself.
   */
  public abstract List<GcsItemId> getPrefixes();

  /**
   * Returns the real object matching the listed directory prefix when {@link
   * GcsListOptions#isIncludePrefixObject()} is {@code false}, or empty if no such object was
   * returned on this page (never synthesized).
   */
  public abstract Optional<T> getPrefixObject();

  /** Returns the non-empty continuation token for fetching the next page, if any. */
  public abstract Optional<String> getNextPageToken();

  /** Returns {@code true} if {@link #getNextPageToken()} is present. */
  public boolean hasNextPage() {
    return getNextPageToken().isPresent();
  }

  /** Returns a builder initialized with the values of this {@link GcsListPage} instance. */
  public abstract Builder<T> toBuilder();

  /** Returns a new {@link Builder} with empty {@code items} and {@code prefixes} lists. */
  public static <T> Builder<T> builder() {
    return new AutoValue_GcsListPage.Builder<T>()
        .setItems(ImmutableList.of())
        .setPrefixes(ImmutableList.of());
  }

  /**
   * Builder for {@link GcsListPage}.
   *
   * @param <T> the element type of listed items
   */
  @AutoValue.Builder
  public abstract static class Builder<T> {

    abstract Builder<T> setItems(List<T> items);

    abstract List<T> getItems();

    /** Sets the items in this page from an {@link Iterable}. */
    public Builder<T> setItems(Iterable<? extends T> items) {
      return setItems(ImmutableList.copyOf(items));
    }

    abstract Builder<T> setPrefixes(List<GcsItemId> prefixes);

    abstract List<GcsItemId> getPrefixes();

    /** Sets the collapsed directory prefixes in this page from an {@link Iterable}. */
    public Builder<T> setPrefixes(Iterable<GcsItemId> prefixes) {
      return setPrefixes(ImmutableList.copyOf(prefixes));
    }

    /** Sets the real object matching the listed directory prefix. */
    public abstract Builder<T> setPrefixObject(T prefixObject);

    /** Sets or clears the optional real object matching the listed directory prefix. */
    public abstract Builder<T> setPrefixObject(Optional<T> prefixObject);

    /** Sets the continuation token for fetching the next page (empty normalizes to absent). */
    public abstract Builder<T> setNextPageToken(String nextPageToken);

    /** Sets or clears the optional continuation token (empty normalizes to absent). */
    public abstract Builder<T> setNextPageToken(Optional<String> nextPageToken);

    abstract Optional<String> getNextPageToken();

    abstract GcsListPage<T> autoBuild();

    /**
     * Builds the {@link GcsListPage} with unmodifiable lists and a normalized continuation token.
     */
    public GcsListPage<T> build() {
      setItems(ImmutableList.copyOf(getItems()));
      setPrefixes(ImmutableList.copyOf(getPrefixes()));
      if (getNextPageToken().map(String::isEmpty).orElse(false)) {
        setNextPageToken(Optional.empty());
      }
      return autoBuild();
    }
  }
}
