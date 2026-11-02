/*
 * Copyright 2026 Google LLC
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

package com.google.cloud.gcs.analyticscore.client.auth;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.VerifyException;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;

/** Parses strict JSON and rejects duplicate field names. */
final class StrictJsonParser {

  private StrictJsonParser() {}

  /** Parses {@code json}, throwing {@link IllegalArgumentException} if it is invalid. */
  static JsonElement parse(String json) {
    checkNotNull(json, "json cannot be null");
    try (JsonReader reader = new JsonReader(new StringReader(json))) {
      JsonElement document = read(reader);
      checkArgument(
          reader.peek() == JsonToken.END_DOCUMENT, "Invalid JSON: content follows the document");
      return document;
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid JSON: " + e.getMessage(), e);
    }
  }

  private static JsonElement read(JsonReader reader) throws IOException {
    JsonToken token = reader.peek();
    switch (token) {
      case BEGIN_OBJECT:
        return readObject(reader);
      case BEGIN_ARRAY:
        return readArray(reader);
      case STRING:
        return new JsonPrimitive(reader.nextString());
      case NUMBER:
        return new JsonPrimitive(new BigDecimal(reader.nextString()));
      case BOOLEAN:
        return new JsonPrimitive(reader.nextBoolean());
      case NULL:
        reader.nextNull();
        return JsonNull.INSTANCE;
      default:
        throw new VerifyException("Expected a JSON value but found " + token);
    }
  }

  private static JsonObject readObject(JsonReader reader) throws IOException {
    String path = reader.getPath();
    JsonObject object = new JsonObject();
    reader.beginObject();
    while (reader.hasNext()) {
      String field = reader.nextName();
      checkArgument(!object.has(field), "%s has a duplicate field '%s'", path, field);
      object.add(field, read(reader));
    }
    reader.endObject();
    return object;
  }

  private static JsonArray readArray(JsonReader reader) throws IOException {
    JsonArray array = new JsonArray();
    reader.beginArray();
    while (reader.hasNext()) {
      array.add(read(reader));
    }
    reader.endArray();
    return array;
  }
}
