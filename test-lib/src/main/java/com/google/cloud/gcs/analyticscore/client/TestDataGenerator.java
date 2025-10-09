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

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
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

  /**
   * Creates a GCS object in the local in-memory storage with the specified size and predictable
   * content. The content is generated using {@link #generateSeededRandomBytes(int, long)} with a
   * fixed seed of 1. This method is useful for setting up test data in a mock GCS environment.
   *
   * @param itemId The GCS item ID, including bucket and object name.
   * @param size The size of the object to create in bytes.
   * @return The byte array of the data written to the local storage.
   */
  @Deprecated
  public static byte[] createGcsData(GcsItemId itemId, int size) {
    Storage storage = FakeGcsClientImpl.storage;
    byte[] data = TestDataGenerator.generateSeededRandomBytes(size, 1);
    storage.create(
        BlobInfo.newBuilder(itemId.getBucketName(), itemId.getObjectName().get(), 1L).build(),
        data);
    return data;
  }
}
