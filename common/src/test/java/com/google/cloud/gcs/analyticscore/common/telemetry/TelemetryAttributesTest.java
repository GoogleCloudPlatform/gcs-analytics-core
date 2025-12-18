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
package com.google.cloud.gcs.analyticscore.common.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class TelemetryAttributesTest {

  @Test
  void builder_validValues_setsValuesCorrectly() {
    String className = "TestClass";
    Long readOffset = 0L;
    Long readLength = 100L;
    String threadId = "thread-1";

    TelemetryAttributes attributes =
        TelemetryAttributes.builder()
            .setClassName(className)
            .setReadOffset(readOffset)
            .setReadLength(readLength)
            .setThreadId(threadId)
            .build();

    assertEquals(className, attributes.className().get());
    assertEquals(readOffset, attributes.readOffset().get());
    assertEquals(readLength, attributes.readLength().get());
    assertEquals(threadId, attributes.threadId().get());
  }

  @Test
  void equals_sameValues_returnsTrue() {
    TelemetryAttributes attr1 =
        TelemetryAttributes.builder().setClassName("ClassA").setReadOffset(10L).build();
    TelemetryAttributes attr2 =
        TelemetryAttributes.builder().setClassName("ClassA").setReadOffset(10L).build();

    assertEquals(attr1, attr2);
    assertEquals(attr1.hashCode(), attr2.hashCode());
  }

  @Test
  void equals_differentValues_returnsFalse() {
    TelemetryAttributes attr1 =
        TelemetryAttributes.builder().setClassName("ClassA").setReadOffset(10L).build();
    TelemetryAttributes attr2 =
        TelemetryAttributes.builder().setClassName("ClassB").setReadOffset(10L).build();

    assertNotEquals(attr1, attr2);
  }
}
