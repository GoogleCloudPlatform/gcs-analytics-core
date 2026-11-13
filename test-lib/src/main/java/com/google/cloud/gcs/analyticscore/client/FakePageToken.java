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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

/**
 * Encodes and decodes the page tokens issued by {@link FakeGcsClientImpl}.
 *
 * <p>A token records the name of the last entry on the previous page, so listing resumes strictly
 * after that name, as GCS does. Tokens are {@code "name:"} followed by the URL-safe Base64 (no
 * padding) encoding of the UTF-8 name.
 */
final class FakePageToken {

  private static final String PAGE_TOKEN_PREFIX = "name:";

  private FakePageToken() {}

  /** Returns the token that resumes listing after {@code lastEntryName}. */
  static String encode(String lastEntryName) {
    return PAGE_TOKEN_PREFIX
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(lastEntryName.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Returns the entry name to resume after, or {@link Optional#empty()} when {@code pageToken} is
   * empty (first page).
   *
   * @throws IOException if {@code pageToken} was not produced by {@link #encode(String)}
   */
  static Optional<String> decode(Optional<String> pageToken) throws IOException {
    if (pageToken.isEmpty()) {
      return Optional.empty();
    }
    String token = pageToken.get();
    if (!token.startsWith(PAGE_TOKEN_PREFIX)) {
      throw new IOException("Invalid pageToken: " + token);
    }
    try {
      byte[] decoded = Base64.getUrlDecoder().decode(token.substring(PAGE_TOKEN_PREFIX.length()));
      String cursor =
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .decode(ByteBuffer.wrap(decoded))
              .toString();
      if (cursor.isEmpty()) {
        throw new IOException("Invalid empty pageToken cursor");
      }
      return Optional.of(cursor);
    } catch (IllegalArgumentException | CharacterCodingException e) {
      throw new IOException("Invalid pageToken: " + token, e);
    }
  }
}
