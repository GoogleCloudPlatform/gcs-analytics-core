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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@AutoValue
public abstract class Operation {

  public abstract String getName();

  public abstract ImmutableMap<String, String> getAttributes();

  public abstract String getOperationId();

  public abstract Optional<Metric> getDurationMetric();

  public static Builder builder() {
    return new AutoValue_Operation.Builder()
        .setAttributes(Collections.emptyMap())
        .setOperationId(newOperationId());
  }

  /**
   * Generates a random type 4 UUID, identical in form to {@link UUID#randomUUID()} but drawn from
   * {@link ThreadLocalRandom} rather than a shared {@code SecureRandom}.
   *
   * <p>An operation id only has to be unique enough to correlate log lines, so the cryptographic
   * strength of {@code SecureRandom} buys nothing here while its shared instance becomes a point of
   * contention: an id is generated for every operation, and operations are created per read.
   */
  private static String newOperationId() {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    long mostSignificantBits = random.nextLong();
    long leastSignificantBits = random.nextLong();
    // Apply the version (4) and IETF variant bits, exactly as UUID.randomUUID() does.
    mostSignificantBits &= 0xffffffffffff0fffL;
    mostSignificantBits |= 0x0000000000004000L;
    leastSignificantBits &= 0x3fffffffffffffffL;
    leastSignificantBits |= 0x8000000000000000L;
    return new UUID(mostSignificantBits, leastSignificantBits).toString();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setName(String name);

    public abstract Builder setAttributes(Map<String, String> attributes);

    public abstract Builder setOperationId(String operationId);

    public abstract Builder setDurationMetric(Metric durationMetric);

    public abstract Operation build();
  }
}
