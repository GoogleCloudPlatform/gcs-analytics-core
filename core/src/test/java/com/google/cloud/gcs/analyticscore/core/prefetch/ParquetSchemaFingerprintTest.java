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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;

class ParquetSchemaFingerprintTest {

  @Test
  void of_equalSchemas_returnsEqualFingerprints() {
    long first = ParquetSchemaFingerprint.of(ImmutableList.of(root(1), leaf("id", Type.INT64)));

    long second = ParquetSchemaFingerprint.of(ImmutableList.of(root(1), leaf("id", Type.INT64)));

    assertThat(first).isEqualTo(second);
  }

  @Test
  void of_sameNamesNestedDifferently_returnsDifferentFingerprints() {
    // Columns a.b and c, against columns a.b and a.c.
    long siblings =
        ParquetSchemaFingerprint.of(
            ImmutableList.of(root(2), group("a", 1), leaf("b", Type.INT32), leaf("c", Type.INT32)));

    long nested =
        ParquetSchemaFingerprint.of(
            ImmutableList.of(root(1), group("a", 2), leaf("b", Type.INT32), leaf("c", Type.INT32)));

    assertThat(siblings).isNotEqualTo(nested);
  }

  @Test
  void of_differentColumnNames_returnsDifferentFingerprints() {
    long id = ParquetSchemaFingerprint.of(ImmutableList.of(root(1), leaf("id", Type.INT64)));

    long key = ParquetSchemaFingerprint.of(ImmutableList.of(root(1), leaf("key", Type.INT64)));

    assertThat(id).isNotEqualTo(key);
  }

  @Test
  void of_namesSplitDifferently_returnsDifferentFingerprints() {
    long abThenC =
        ParquetSchemaFingerprint.of(
            ImmutableList.of(root(2), leaf("ab", Type.INT32), leaf("c", Type.INT32)));

    long aThenBc =
        ParquetSchemaFingerprint.of(
            ImmutableList.of(root(2), leaf("a", Type.INT32), leaf("bc", Type.INT32)));

    assertThat(abThenC).isNotEqualTo(aThenBc);
  }

  @Test
  void of_differentPhysicalTypes_returnsDifferentFingerprints() {
    long int32 = ParquetSchemaFingerprint.of(ImmutableList.of(root(1), leaf("id", Type.INT32)));

    long int64 = ParquetSchemaFingerprint.of(ImmutableList.of(root(1), leaf("id", Type.INT64)));

    assertThat(int32).isNotEqualTo(int64);
  }

  @Test
  void of_differentRootNames_returnsEqualFingerprints() {
    long spark =
        ParquetSchemaFingerprint.of(
            ImmutableList.of(
                new SchemaElement("spark_schema").setNum_children(1), leaf("id", Type.INT64)));

    long hive =
        ParquetSchemaFingerprint.of(
            ImmutableList.of(
                new SchemaElement("hive_schema").setNum_children(1), leaf("id", Type.INT64)));

    assertThat(spark).isEqualTo(hive);
  }

  @Test
  void of_differentRepetition_returnsEqualFingerprints() {
    long required =
        ParquetSchemaFingerprint.of(
            ImmutableList.of(
                root(1), leaf("id", Type.INT64).setRepetition_type(FieldRepetitionType.REQUIRED)));

    long optional =
        ParquetSchemaFingerprint.of(
            ImmutableList.of(
                root(1), leaf("id", Type.INT64).setRepetition_type(FieldRepetitionType.OPTIONAL)));

    assertThat(required).isEqualTo(optional);
  }

  private static SchemaElement root(int childCount) {
    return group("schema", childCount);
  }

  private static SchemaElement group(String name, int childCount) {
    return new SchemaElement(name).setNum_children(childCount);
  }

  private static SchemaElement leaf(String name, Type type) {
    return new SchemaElement(name).setType(type);
  }
}
