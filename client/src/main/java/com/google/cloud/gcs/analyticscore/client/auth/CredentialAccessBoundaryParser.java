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

import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Optional;
import java.util.function.Supplier;

/** Parses Credential Access Boundary JSON, rejecting unknown, repeated, and null fields. */
final class CredentialAccessBoundaryParser {

  private static final String ACCESS_BOUNDARY = "accessBoundary";
  private static final String ACCESS_BOUNDARY_RULES = "accessBoundaryRules";
  private static final String AVAILABLE_RESOURCE = "availableResource";
  private static final String AVAILABLE_PERMISSIONS = "availablePermissions";
  private static final String AVAILABILITY_CONDITION = "availabilityCondition";
  private static final String EXPRESSION = "expression";
  private static final String TITLE = "title";
  private static final String DESCRIPTION = "description";

  private static final ImmutableSet<String> DOCUMENT_FIELDS = ImmutableSet.of(ACCESS_BOUNDARY);
  private static final ImmutableSet<String> ACCESS_BOUNDARY_FIELDS =
      ImmutableSet.of(ACCESS_BOUNDARY_RULES);
  private static final ImmutableSet<String> RULE_FIELDS =
      ImmutableSet.of(AVAILABLE_RESOURCE, AVAILABLE_PERMISSIONS, AVAILABILITY_CONDITION);
  private static final ImmutableSet<String> CONDITION_FIELDS =
      ImmutableSet.of(EXPRESSION, TITLE, DESCRIPTION);

  private static final String DOCUMENT_PATH = "$";
  private static final String ACCESS_BOUNDARY_PATH = DOCUMENT_PATH + "." + ACCESS_BOUNDARY;
  private static final String RULES_PATH = ACCESS_BOUNDARY_PATH + "." + ACCESS_BOUNDARY_RULES;

  private CredentialAccessBoundaryParser() {}

  /** Parses {@code json}, throwing {@link IllegalArgumentException} if it is invalid. */
  static CredentialAccessBoundary parse(String json) {
    checkNotNull(json, "json cannot be null");
    JsonObject document = asObject(StrictJsonParser.parse(json), DOCUMENT_PATH, DOCUMENT_FIELDS);
    JsonObject accessBoundary =
        asObject(
            getRequired(document, DOCUMENT_PATH, ACCESS_BOUNDARY),
            ACCESS_BOUNDARY_PATH,
            ACCESS_BOUNDARY_FIELDS);
    JsonArray rules =
        asArray(
            getRequired(accessBoundary, ACCESS_BOUNDARY_PATH, ACCESS_BOUNDARY_RULES), RULES_PATH);
    ImmutableList.Builder<CredentialAccessBoundary.AccessBoundaryRule> parsedRules =
        ImmutableList.builder();
    for (int i = 0; i < rules.size(); i++) {
      parsedRules.add(parseRule(rules.get(i), RULES_PATH + "[" + i + "]"));
    }
    CredentialAccessBoundary.Builder boundary =
        CredentialAccessBoundary.newBuilder().setRules(parsedRules.build());
    return build(boundary::build, RULES_PATH);
  }

  private static CredentialAccessBoundary.AccessBoundaryRule parseRule(
      JsonElement element, String path) {
    JsonObject rule = asObject(element, path, RULE_FIELDS);
    CredentialAccessBoundary.AccessBoundaryRule.Builder builder =
        CredentialAccessBoundary.AccessBoundaryRule.newBuilder()
            .setAvailableResource(getRequiredString(rule, path, AVAILABLE_RESOURCE))
            .setAvailablePermissions(getPermissions(rule, path));
    getOptional(rule, AVAILABILITY_CONDITION)
        .map(condition -> parseCondition(condition, path + "." + AVAILABILITY_CONDITION))
        .ifPresent(builder::setAvailabilityCondition);
    return build(builder::build, path);
  }

  private static ImmutableList<String> getPermissions(JsonObject rule, String rulePath) {
    String path = rulePath + "." + AVAILABLE_PERMISSIONS;
    JsonArray permissions = asArray(getRequired(rule, rulePath, AVAILABLE_PERMISSIONS), path);
    ImmutableList.Builder<String> values = ImmutableList.builder();
    for (int i = 0; i < permissions.size(); i++) {
      values.add(asNonBlankString(permissions.get(i), path + "[" + i + "]"));
    }
    return values.build();
  }

  private static CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition parseCondition(
      JsonElement element, String path) {
    JsonObject condition = asObject(element, path, CONDITION_FIELDS);
    CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition.Builder builder =
        CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition.newBuilder()
            .setExpression(getRequiredString(condition, path, EXPRESSION));
    getOptionalString(condition, path, TITLE).ifPresent(builder::setTitle);
    getOptionalString(condition, path, DESCRIPTION).ifPresent(builder::setDescription);
    return build(builder::build, path);
  }

  /** Runs an auth library builder, prefixing its errors with {@code path}. */
  private static <T> T build(Supplier<T> builder, String path) {
    try {
      return builder.get();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(path + ": " + e.getMessage(), e);
    }
  }

  private static JsonObject asObject(
      JsonElement element, String path, ImmutableSet<String> knownFields) {
    checkArgument(element.isJsonObject(), "%s must be a JSON object", path);
    JsonObject object = element.getAsJsonObject();
    for (String field : object.keySet()) {
      checkArgument(
          knownFields.contains(field),
          "%s has an unknown field '%s', expected one of %s",
          path,
          field,
          knownFields);
    }
    return object;
  }

  private static JsonArray asArray(JsonElement element, String path) {
    checkArgument(element.isJsonArray(), "%s must be a JSON array", path);
    return element.getAsJsonArray();
  }

  private static String asString(JsonElement element, String path) {
    checkArgument(
        element.isJsonPrimitive() && element.getAsJsonPrimitive().isString(),
        "%s must be a JSON string",
        path);
    return element.getAsString();
  }

  private static String asNonBlankString(JsonElement element, String path) {
    String value = asString(element, path);
    checkArgument(!value.isBlank(), "%s must not be blank", path);
    return value;
  }

  private static JsonElement getRequired(JsonObject object, String path, String field) {
    return getOptional(object, field)
        .orElseThrow(() -> new IllegalArgumentException(path + "." + field + " is missing"));
  }

  /** Returns the field's value, keeping JSON null so that the type checks reject it. */
  private static Optional<JsonElement> getOptional(JsonObject object, String field) {
    return Optional.ofNullable(object.get(field));
  }

  private static String getRequiredString(JsonObject object, String path, String field) {
    return asNonBlankString(getRequired(object, path, field), path + "." + field);
  }

  private static Optional<String> getOptionalString(JsonObject object, String path, String field) {
    return getOptional(object, field).map(value -> asString(value, path + "." + field));
  }
}
