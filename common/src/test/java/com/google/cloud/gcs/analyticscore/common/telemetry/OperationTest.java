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
package com.google.cloud.gcs.analyticscore.common.telemetry;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OperationTest {

  @Test
  void builder_generatedOperationId_isAType4Uuid() {
    Operation operation = Operation.builder().setName("READ").build();

    UUID parsed = UUID.fromString(operation.getOperationId());

    assertThat(parsed.version()).isEqualTo(4);
  }

  @Test
  void builder_generatedOperationId_usesIetfVariant() {
    Operation operation = Operation.builder().setName("READ").build();

    UUID parsed = UUID.fromString(operation.getOperationId());

    assertThat(parsed.variant()).isEqualTo(2);
  }

  @Test
  void builder_generatedOperationId_hasCanonicalUuidLength() {
    Operation operation = Operation.builder().setName("READ").build();

    assertThat(operation.getOperationId()).hasLength(36);
  }

  @Test
  void builder_repeatedCalls_generateDistinctOperationIds() {
    int operationCount = 10_000;
    Set<String> operationIds = new HashSet<>();

    for (int i = 0; i < operationCount; i++) {
      operationIds.add(Operation.builder().setName("READ").build().getOperationId());
    }

    assertThat(operationIds).hasSize(operationCount);
  }

  @Test
  void builder_explicitOperationId_overridesGeneratedValue() {
    Operation operation = Operation.builder().setName("READ").setOperationId("explicit-id").build();

    assertThat(operation.getOperationId()).isEqualTo("explicit-id");
  }

  @Test
  void builder_withRequiredNameOnly_setsNameAndEmptyDefaults() {
    Operation operation = Operation.builder().setName("READ").build();

    assertThat(operation.getName()).isEqualTo("READ");
    assertThat(operation.getAttributes()).isEmpty();
    assertThat(operation.getDurationMetric()).isEmpty();
  }

  @Test
  void builder_setAttributes_copiesEntriesIntoImmutableMap() {
    Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("bucket", "test-bucket");

    Operation operation =
        Operation.builder().setName("READ").setAttributes(sourceAttributes).build();
    sourceAttributes.put("bucket", "mutated");

    assertThat(operation.getAttributes()).containsExactly("bucket", "test-bucket");
  }

  @Test
  void builder_setDurationMetric_setsPresentMetric() {
    Metric durationMetric = TestMetric.of("read_duration", Metric.MetricType.DURATION);

    Operation operation =
        Operation.builder().setName("READ").setDurationMetric(durationMetric).build();

    assertThat(operation.getDurationMetric()).hasValue(durationMetric);
  }

  @Test
  void builder_missingName_throwsIllegalStateException() {
    Operation.Builder builder = Operation.builder();

    IllegalStateException exception = assertThrows(IllegalStateException.class, builder::build);

    assertThat(exception).hasMessageThat().contains("name");
  }

  @Test
  void builder_nullName_throwsNullPointerException() {
    Operation.Builder builder = Operation.builder();

    assertThrows(NullPointerException.class, () -> builder.setName(null));
  }

  @Test
  void builder_nullAttributes_throwsNullPointerException() {
    Operation.Builder builder = Operation.builder();

    assertThrows(NullPointerException.class, () -> builder.setAttributes(null));
  }

  @Test
  void builder_nullOperationId_throwsNullPointerException() {
    Operation.Builder builder = Operation.builder();

    assertThrows(NullPointerException.class, () -> builder.setOperationId(null));
  }

  @Test
  void builder_nullDurationMetric_throwsNullPointerException() {
    Operation.Builder builder = Operation.builder();

    assertThrows(NullPointerException.class, () -> builder.setDurationMetric(null));
  }

  @Test
  void equals_sameFields_returnsTrueAndConsistentHashCode() {
    Metric durationMetric = TestMetric.of("read_duration", Metric.MetricType.DURATION);
    Operation first =
        createOperation("READ", ImmutableMap.of("bucket", "b1"), "op-1", durationMetric);
    Operation second =
        createOperation("READ", ImmutableMap.of("bucket", "b1"), "op-1", durationMetric);
    Operation sameInstance = first;
    Set<Operation> operations = new HashSet<>(Set.of(first));

    assertThat(first.equals(sameInstance)).isTrue();
    assertThat(first).isEqualTo(second);
    assertThat(first.hashCode()).isEqualTo(second.hashCode());
    assertThat(operations).contains(second);
  }

  @Test
  void equals_differentFieldsOrType_returnsFalse() {
    Metric durationMetric = TestMetric.of("read_duration", Metric.MetricType.DURATION);
    Metric differentMetric = TestMetric.of("other_duration", Metric.MetricType.DURATION);
    Operation base =
        createOperation("READ", ImmutableMap.of("bucket", "b1"), "op-1", durationMetric);
    Operation differentName =
        createOperation("WRITE", ImmutableMap.of("bucket", "b1"), "op-1", durationMetric);
    Operation differentAttributes =
        createOperation("READ", ImmutableMap.of("bucket", "b2"), "op-1", durationMetric);
    Operation differentOperationId =
        createOperation("READ", ImmutableMap.of("bucket", "b1"), "op-2", durationMetric);
    Operation differentDurationMetric =
        createOperation("READ", ImmutableMap.of("bucket", "b1"), "op-1", differentMetric);

    assertThat(base).isNotEqualTo(differentName);
    assertThat(base).isNotEqualTo(differentAttributes);
    assertThat(base).isNotEqualTo(differentOperationId);
    assertThat(base).isNotEqualTo(differentDurationMetric);
    assertThat(base).isNotEqualTo(new Object());
  }

  @Test
  void toString_includesAllConfiguredFields() {
    Metric durationMetric = TestMetric.of("read_duration", Metric.MetricType.DURATION);
    Operation operation =
        createOperation("READ", ImmutableMap.of("bucket", "test-bucket"), "op-1", durationMetric);

    String description = operation.toString();

    assertThat(description).contains("name=READ");
    assertThat(description).contains("bucket=test-bucket");
    assertThat(description).contains("operationId=op-1");
    assertThat(description).contains("read_duration");
  }

  private static Operation createOperation(
      String name, Map<String, String> attributes, String operationId, Metric durationMetric) {
    return Operation.builder()
        .setName(name)
        .setAttributes(attributes)
        .setOperationId(operationId)
        .setDurationMetric(durationMetric)
        .build();
  }
}
