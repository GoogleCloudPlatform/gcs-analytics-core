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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class TestDataGeneratorTest {

  @Test
  void generateSeededRandomBytes_sameSeedSameSize_returnsSameBytes() {
    byte[] bytes1 = TestDataGenerator.generateSeededRandomBytes(100, 12345L);
    byte[] bytes2 = TestDataGenerator.generateSeededRandomBytes(100, 12345L);
    assertArrayEquals(bytes1, bytes2);
  }

  @Test
  void generateSeededRandomBytes_differentSeedSameSize_returnsDifferentBytes() {
    byte[] bytes1 = TestDataGenerator.generateSeededRandomBytes(100, 12345L);
    byte[] bytes2 = TestDataGenerator.generateSeededRandomBytes(100, 54321L);
    assertFalse(java.util.Arrays.equals(bytes1, bytes2));
  }

  @Test
  void generateSeededRandomBytes_sameSeedDifferentSize_returnsDifferentBytes() {
    byte[] bytes1 = TestDataGenerator.generateSeededRandomBytes(100, 12345L);
    byte[] bytes2 = TestDataGenerator.generateSeededRandomBytes(200, 12345L);
    assertNotEquals(bytes1.length, bytes2.length);
  }

  @Test
  void generateSeededRandomBytes_sizeZero_returnsEmptyArray() {
    byte[] bytes = TestDataGenerator.generateSeededRandomBytes(0, 12345L);
    assertEquals(0, bytes.length);
  }

  @Test
  void generateSeededRandomBytes_negativeSize_throwsException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          TestDataGenerator.generateSeededRandomBytes(-1, 12345L);
        });
  }
}
