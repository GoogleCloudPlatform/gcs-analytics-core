/*
 * Copyright 2025 Google LLC
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
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/** Represents a byte range from a GCS object range. */
@AutoValue
public abstract class GcsObjectRange {

  // The future that will be completed with the contents of the byte range.
  public abstract CompletableFuture<ByteBuffer> getByteBuffer();

  // The starting offset of the byte range.
  public abstract long getOffset();

  // The length of the byte range.
  public abstract int getLength();

  public static Builder builder() {
    return new AutoValue_GcsObjectRange.Builder();
  }

  /** Builder for {@link GcsObjectRange}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setByteBuffer(CompletableFuture<ByteBuffer> byteBuffer);

    public abstract Builder setOffset(long offset);

    public abstract Builder setLength(int length);

    public abstract GcsObjectRange build();
  }
}
