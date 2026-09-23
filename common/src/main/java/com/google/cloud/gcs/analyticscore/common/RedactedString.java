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

package com.google.cloud.gcs.analyticscore.common;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;

/**
 * Holder for a string that must never be logged, such as a credential or an encryption key.
 *
 * <p>{@code toString} returns a fixed placeholder instead of the wrapped string, so a value held by
 * this type stays redacted in log statements and in the generated {@code toString} of any value
 * class that holds it. Call {@link #value()} to obtain the wrapped string at the point where it is
 * actually needed.
 *
 * <p>The protection lasts only as long as the value stays in this type. Assigning {@link #value()}
 * to a {@code String} field, passing it as a log format argument, or concatenating it into a
 * message all defeat the redaction, so unwrap it at the point of use and do not retain the result.
 */
@AutoValue
public abstract class RedactedString {

  private static final String REDACTED_PLACEHOLDER = "<redacted>";

  /**
   * Wraps a secret so that it stays redacted when logged.
   *
   * <p>A blank secret is rejected rather than wrapped, so that a misconfigured credential is
   * reported against the configuration that caused it rather than as an opaque authentication
   * failure later. Represent an unset secret by not calling this method at all.
   *
   * @param value The secret to wrap.
   * @return The wrapped secret.
   * @throws NullPointerException If {@code value} is null.
   * @throws IllegalArgumentException If {@code value} is empty or contains only whitespace.
   */
  public static RedactedString create(String value) {
    checkNotNull(value, "value cannot be null");
    checkArgument(!value.trim().isEmpty(), "value cannot be blank");
    return new AutoValue_RedactedString(value);
  }

  public abstract String value();

  @Override
  public final String toString() {
    return REDACTED_PLACEHOLDER;
  }
}
