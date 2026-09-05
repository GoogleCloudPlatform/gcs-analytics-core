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

package com.google.cloud.gcs.analyticscore.client;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/** GCS provided validation attributes for a single object. */
@AutoValue
public abstract class VerificationAttributes {
  @Nullable
  @SuppressWarnings("mutable")
  public abstract byte[] getMd5hash();

  @Nullable
  @SuppressWarnings("mutable")
  public abstract byte[] getCrc32c();

  public static VerificationAttributes create(@Nullable byte[] md5hash, @Nullable byte[] crc32c) {
    return new AutoValue_VerificationAttributes(md5hash, crc32c);
  }
}
