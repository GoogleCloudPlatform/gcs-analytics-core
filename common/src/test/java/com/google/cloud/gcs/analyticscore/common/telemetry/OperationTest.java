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

import java.util.HashSet;
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
}
