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

package com.google.cloud.gcs.analyticscore.core.prefetch;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.util.List;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;

/**
 * Computes a fingerprint of the columns that a Parquet schema defines.
 *
 * <p>Two schemas share a fingerprint when they describe the same tree of fields, with the same
 * names and physical types, and therefore the same column paths. Each field contributes its name. A
 * group also contributes its number of children, which fixes the shape of the tree, so {@code a{b},
 * c} and {@code a{b, c}} differ even though they list the same names in the same order. A leaf
 * contributes its physical type and, for a fixed-length byte array, its width.
 *
 * <p>The name of the root, which depends on the writer, and attributes that leave column paths
 * unchanged, such as repetition, logical types, and field ids, are left out on purpose. So are a
 * leaf's child count and the width of any other type, which some writers set and others omit. Files
 * of one table written by different engines or writer versions therefore share a fingerprint.
 *
 * <p>Fingerprints are meant for comparisons within one process. Do not store them or share them
 * across library versions, since a change to what this class hashes changes every fingerprint.
 */
final class ParquetSchemaFingerprint {

  private static final int ABSENT = -1;

  private ParquetSchemaFingerprint() {}

  /**
   * Returns the fingerprint of {@code schema}, the depth-first list of schema elements stored in a
   * footer, whose first element is the root.
   *
   * @throws IllegalArgumentException if {@code schema} is empty
   */
  static long of(List<SchemaElement> schema) {
    checkArgument(!schema.isEmpty(), "Schema must contain a root element");
    Hasher hasher = Hashing.farmHashFingerprint64().newHasher();
    for (int index = 0; index < schema.size(); index++) {
      SchemaElement element = schema.get(index);
      if (index > 0) {
        String name = element.getName();
        // The length prefix keeps adjacent names apart, so "ab" then "c" differs from "a" then
        // "bc".
        hasher.putInt(name.length()).putUnencodedChars(name);
      }
      putShape(hasher, element);
    }
    return hasher.hash().asLong();
  }

  /**
   * Adds what makes {@code element} a group or a leaf. Physical type values are never negative, so
   * a leading {@link #ABSENT} tells a group apart from a leaf.
   */
  private static void putShape(Hasher hasher, SchemaElement element) {
    if (!element.isSetType()) {
      hasher.putInt(ABSENT);
      hasher.putInt(element.isSetNum_children() ? element.getNum_children() : ABSENT);
      return;
    }
    hasher.putInt(element.getType().getValue());
    if (element.getType() == Type.FIXED_LEN_BYTE_ARRAY) {
      hasher.putInt(element.isSetType_length() ? element.getType_length() : ABSENT);
    }
  }
}
