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

import java.util.Random;

public class TestDataGenerator {

  /**
   * Generates a predictable, pseudo-random byte array for testing purposes. Using the same seed
   * will always result in the same byte array for a given size.
   *
   * @param size The desired size of the byte array.
   * @param seed The seed for the Random number generator to ensure deterministic results.
   * @return A new byte array of the specified size filled with predictable "random" bytes.
   */
  public static byte[] generateSeededRandomBytes(int size, long seed) {
    if (size < 0) {
      throw new IllegalArgumentException("Size cannot be negative.");
    }
    Random random = new Random(seed);
    byte[] bytes = new byte[size];
    random.nextBytes(bytes);
    return bytes;
  }
}
