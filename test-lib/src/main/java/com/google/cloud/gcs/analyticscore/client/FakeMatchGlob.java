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
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Translates a GCS {@code matchGlob} expression into a {@link Pattern} for {@link
 * FakeGcsClientImpl}.
 *
 * <p>Supports {@code *} (any characters except {@code /}), {@code **} (any characters), {@code **}
 * followed by {@code /} (zero or more directories), {@code ?}, character classes ({@code [...]},
 * {@code [!...]}), brace alternation ({@code {a,b}}) and backslash escapes. Rejects the malformed
 * expressions that GCS rejects, such as unmatched brackets or braces and {@code /} or {@code **}
 * inside braces.
 */
final class FakeMatchGlob {

  private static final String REGEX_META_CHARS = "\\.*+?[]{}()|^$-";

  private FakeMatchGlob() {}

  /**
   * Compiles {@code glob} into a full-match {@link Pattern}.
   *
   * @throws IOException if {@code glob} is not a valid GCS {@code matchGlob} expression
   */
  static Pattern compile(String glob) throws IOException {
    StringBuilder regex = new StringBuilder();
    int braceDepth = 0;
    for (int i = 0; i < glob.length(); ) {
      char c = glob.charAt(i);
      if (c == '\\') {
        i = appendBackslashEscape(glob, i, braceDepth, regex);
      } else if (glob.startsWith("**/", i)) {
        if (braceDepth > 0) {
          throw new IOException(
              "Invalid matchGlob (cannot contain '/' or '**' in braces): " + glob);
        }
        regex.append(i == 0 || glob.charAt(i - 1) == '/' ? "(?:.*/)?" : ".*/");
        i += 3;
      } else if (glob.startsWith("**", i)) {
        if (braceDepth > 0) {
          throw new IOException("Invalid matchGlob (cannot contain '**' in braces): " + glob);
        }
        regex.append(".*");
        i += 2;
      } else if (c == '[') {
        i = appendCharacterClass(glob, i + 1, regex);
      } else if (c == ']') {
        throw new IOException("Invalid matchGlob (unmatched ']'): " + glob);
      } else if (c == '{') {
        braceDepth++;
        regex.append("(?:");
        i++;
      } else if (c == ',') {
        regex.append(braceDepth > 0 ? "|" : ",");
        i++;
      } else if (c == '}') {
        if (braceDepth == 0) {
          throw new IOException("Invalid matchGlob (unmatched '}'): " + glob);
        }
        braceDepth--;
        regex.append(')');
        i++;
      } else if (c == '/') {
        if (braceDepth > 0) {
          throw new IOException("Invalid matchGlob (cannot contain '/' in braces): " + glob);
        }
        regex.append('/');
        i++;
      } else if (c == '*') {
        regex.append("[^/]*");
        i++;
      } else if (c == '?') {
        regex.append("[^/]");
        i++;
      } else {
        appendEscapedLiteral(regex, c);
        i++;
      }
    }
    if (braceDepth > 0) {
      throw new IOException("Invalid matchGlob (unclosed '{'): " + glob);
    }
    try {
      return Pattern.compile(regex.toString(), Pattern.DOTALL);
    } catch (PatternSyntaxException e) {
      throw new IOException("Invalid matchGlob: " + glob, e);
    }
  }

  private static int appendBackslashEscape(
      String glob, int backslashIdx, int braceDepth, StringBuilder regex) throws IOException {
    if (backslashIdx + 1 >= glob.length()) {
      regex.append("\\\\");
      return backslashIdx + 1;
    }
    char escaped = glob.charAt(backslashIdx + 1);
    if (braceDepth > 0 && escaped == '/') {
      throw new IOException("Invalid matchGlob (cannot contain '/' in braces): " + glob);
    }
    appendEscapedLiteral(regex, escaped);
    return backslashIdx + 2;
  }

  private static int appendCharacterClass(String glob, int startIdx, StringBuilder regex)
      throws IOException {
    StringBuilder body = new StringBuilder();
    boolean negated = false;
    int i = startIdx;
    if (i < glob.length() && (glob.charAt(i) == '!' || glob.charAt(i) == '^')) {
      negated = true;
      i++;
    }
    if (i < glob.length() && glob.charAt(i) == ']') {
      throw new IOException("Invalid matchGlob (empty character class): " + glob);
    }
    while (i < glob.length()) {
      char ch = glob.charAt(i);
      if (ch == '\\') {
        if (i + 1 >= glob.length()) {
          throw new IOException("Invalid matchGlob (unclosed '['): " + glob);
        }
        char esc = glob.charAt(i + 1);
        if ("\\[]-^&".indexOf(esc) >= 0) {
          body.append('\\');
        }
        body.append(esc);
        i += 2;
      } else if (ch == '[') {
        throw new IOException("Invalid matchGlob (unescaped '[' in character class): " + glob);
      } else if (ch == ']') {
        regex.append(negated ? "(?!/)[^" : "(?!/)[");
        regex.append(body).append(']');
        return i + 1;
      } else if (ch == '&') {
        body.append("\\&");
        i++;
      } else {
        body.append(ch);
        i++;
      }
    }
    throw new IOException("Invalid matchGlob (unclosed '['): " + glob);
  }

  private static void appendEscapedLiteral(StringBuilder regex, char c) {
    if (REGEX_META_CHARS.indexOf(c) >= 0) {
      regex.append('\\');
    }
    regex.append(c);
  }
}
