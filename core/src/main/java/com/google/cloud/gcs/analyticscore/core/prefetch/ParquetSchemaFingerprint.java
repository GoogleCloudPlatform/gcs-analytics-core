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

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.util.List;
import org.apache.parquet.format.SchemaElement;

/**
 * Computes a fingerprint of the columns that a Parquet schema defines.
 *
 * <p>Two schemas share a fingerprint when they describe the same tree of fields, with the same
 * names and physical types, and therefore the same column paths. Each field contributes its name,
 * its number of children, and its physical type, which together fix the shape of the tree, so
 * {@code a{b}, c} and {@code a{b, c}} differ even though they list the same names in the same
 * order.
 *
 * <p>The name of the root, which depends on the writer, and attributes that leave column paths
 * unchanged, such as repetition, logical types, and field ids, are left out on purpose. Files of
 * one table written by different engines or writer versions therefore share a fingerprint.
 */
final class ParquetSchemaFingerprint {

  private static final int ABSENT = -1;

  private ParquetSchemaFingerprint() {}

  /**
   * Returns the fingerprint of {@code schema}, the depth-first list of schema elements stored in a
   * footer, whose first element is the root.
   */
  static long of(List<SchemaElement> schema) {
    Hasher hasher = Hashing.farmHashFingerprint64().newHasher();
    for (int index = 0; index < schema.size(); index++) {
      SchemaElement element = schema.get(index);
      if (index > 0) {
        String name = element.getName();
        // The length prefix keeps adjacent names apart, so "ab" then "c" differs from "a" then
        // "bc".
        hasher.putInt(name.length()).putUnencodedChars(name);
      }
      hasher.putInt(element.isSetNum_children() ? element.getNum_children() : ABSENT);
      hasher.putInt(element.isSetType() ? element.getType().getValue() : ABSENT);
    }
    return hasher.hash().asLong();
  }
}
