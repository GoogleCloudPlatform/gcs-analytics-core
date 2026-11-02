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

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.io.IOException;
import java.util.Collections;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CredentialAccessBoundaryParserTest {

  private static final String RESOURCE =
      "'availableResource': '//storage.googleapis.com/projects/_/buckets/example-bucket'";
  private static final String PERMISSIONS =
      "'availablePermissions': ['inRole:roles/storage.objectViewer']";
  private static final String VALID_RULE = rule(RESOURCE, PERMISSIONS);

  @Test
  void parse_documentedExample_returnsRulesInOrder() throws IOException {
    String json = readDocumentedExample();

    CredentialAccessBoundary boundary = CredentialAccessBoundaryParser.parse(json);

    assertThat(availableResources(boundary))
        .containsExactly(
            "//storage.googleapis.com/projects/_/buckets/example-bucket-1",
            "//storage.googleapis.com/projects/_/buckets/example-bucket-2")
        .inOrder();
  }

  @Test
  void parse_documentedExample_returnsAvailablePermissions() throws IOException {
    String json = readDocumentedExample();

    CredentialAccessBoundary boundary = CredentialAccessBoundaryParser.parse(json);

    assertThat(boundary.getAccessBoundaryRules().get(1).getAvailablePermissions())
        .containsExactly("inRole:roles/storage.objectViewer", "inRole:roles/storage.objectCreator")
        .inOrder();
  }

  @Test
  void parse_ruleWithCondition_returnsExpression() throws IOException {
    String json = readDocumentedExample();

    CredentialAccessBoundary boundary = CredentialAccessBoundaryParser.parse(json);

    assertThat(availabilityCondition(boundary, 0).getExpression())
        .isEqualTo(
            "resource.name.startsWith('projects/_/buckets/example-bucket-1/objects/eu/')"
                + " || api.getAttribute('storage.googleapis.com/objectListPrefix', '')"
                + ".startsWith('eu/')");
  }

  @Test
  void parse_ruleWithCondition_returnsTitle() throws IOException {
    String json = readDocumentedExample();

    CredentialAccessBoundary boundary = CredentialAccessBoundaryParser.parse(json);

    assertThat(availabilityCondition(boundary, 0).getTitle()).isEqualTo("EU objects");
  }

  @Test
  void parse_ruleWithCondition_returnsDescription() throws IOException {
    String json = readDocumentedExample();

    CredentialAccessBoundary boundary = CredentialAccessBoundaryParser.parse(json);

    assertThat(availabilityCondition(boundary, 0).getDescription())
        .isEqualTo("Objects whose names start with eu/");
  }

  @Test
  void parse_ruleWithoutCondition_hasNoAvailabilityCondition() throws IOException {
    String json = readDocumentedExample();

    CredentialAccessBoundary boundary = CredentialAccessBoundaryParser.parse(json);

    assertThat(availabilityCondition(boundary, 1)).isNull();
  }

  @Test
  void parse_nullCondition_reportsTheField() {
    String json = boundary(rule(RESOURCE, PERMISSIONS, "'availabilityCondition': null"));

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> CredentialAccessBoundaryParser.parse(json));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(
            "$.accessBoundary.accessBoundaryRules[0].availabilityCondition must be a JSON object");
  }

  @Test
  void parse_blankPermission_reportsThePermission() {
    String json = boundary(rule(RESOURCE, "'availablePermissions': ['   ']"));

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> CredentialAccessBoundaryParser.parse(json));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(
            "$.accessBoundary.accessBoundaryRules[0].availablePermissions[0] must not be blank");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidBoundaries")
  void parse_invalidBoundary_throwsIllegalArgumentException(String description, String json) {
    assertThrows(IllegalArgumentException.class, () -> CredentialAccessBoundaryParser.parse(json));
  }

  @Test
  void parse_nineRules_returnsAllRules() {
    String json = boundary(Collections.nCopies(9, VALID_RULE).toArray(new String[0]));

    CredentialAccessBoundary boundary = CredentialAccessBoundaryParser.parse(json);

    assertThat(boundary.getAccessBoundaryRules()).hasSize(9);
  }

  @Test
  void parse_moreRulesThanCloudIamAllows_reportsTheRuleList() {
    String json = boundary(Collections.nCopies(11, VALID_RULE).toArray(new String[0]));

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> CredentialAccessBoundaryParser.parse(json));

    assertThat(e).hasMessageThat().startsWith("$.accessBoundary.accessBoundaryRules: ");
  }

  @Test
  void parse_misspelledField_reportsTheField() {
    String json =
        boundary(rule(RESOURCE, PERMISSIONS, "'availabilityConditon': {'expression': 'true'}"));

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> CredentialAccessBoundaryParser.parse(json));

    assertThat(e)
        .hasMessageThat()
        .startsWith(
            "$.accessBoundary.accessBoundaryRules[0] has an unknown field 'availabilityConditon'");
  }

  @Test
  void parse_duplicateCondition_reportsTheRule() {
    String json =
        boundary(
            rule(
                RESOURCE,
                PERMISSIONS,
                "'availabilityCondition': {'expression': 'false'}",
                "'availabilityCondition': {'expression': 'true'}"));

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> CredentialAccessBoundaryParser.parse(json));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(
            "$.accessBoundary.accessBoundaryRules[0] has a duplicate field 'availabilityCondition'");
  }

  @Test
  void parse_missingField_reportsItsPath() {
    String json = boundary(VALID_RULE, rule(PERMISSIONS));

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> CredentialAccessBoundaryParser.parse(json));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo("$.accessBoundary.accessBoundaryRules[1].availableResource is missing");
  }

  @Test
  void parse_ruleRejectedByAuthLibrary_reportsTheRule() {
    String json = boundary(rule(RESOURCE, "'availablePermissions': []"));

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> CredentialAccessBoundaryParser.parse(json));

    assertThat(e).hasMessageThat().startsWith("$.accessBoundary.accessBoundaryRules[0]: ");
  }

  @Test
  void parse_nullJson_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> CredentialAccessBoundaryParser.parse(null));
  }

  private static Stream<Arguments> invalidBoundaries() {
    return Stream.of(
        Arguments.of("invalid JSON", json("{'accessBoundary': ")),
        Arguments.of(
            "comment",
            json("{'accessBoundary': /* comment */ {'accessBoundaryRules': [")
                + VALID_RULE
                + "]}}"),
        Arguments.of("document is not an object", json("[]")),
        Arguments.of("accessBoundary is missing", json("{}")),
        Arguments.of(
            "unknown top-level field",
            json("{'version': 1, 'accessBoundary': {'accessBoundaryRules': [")
                + VALID_RULE
                + "]}}"),
        Arguments.of("accessBoundary is not an object", json("{'accessBoundary': []}")),
        Arguments.of("accessBoundary is null", json("{'accessBoundary': null}")),
        Arguments.of("accessBoundaryRules is missing", json("{'accessBoundary': {}}")),
        Arguments.of(
            "accessBoundaryRules is not an array",
            json("{'accessBoundary': {'accessBoundaryRules': {}}}")),
        Arguments.of("no rules", boundary()),
        Arguments.of("rule is not an object", boundary(json("'rule'"))),
        Arguments.of(
            "availableResource is a number", boundary(rule("'availableResource': 7", PERMISSIONS))),
        Arguments.of(
            "availableResource is an object",
            boundary(rule("'availableResource': {}", PERMISSIONS))),
        Arguments.of(
            "availableResource is blank",
            boundary(rule("'availableResource': '   '", PERMISSIONS))),
        Arguments.of("availablePermissions is missing", boundary(rule(RESOURCE))),
        Arguments.of(
            "availablePermissions is not an array",
            boundary(
                rule(RESOURCE, "'availablePermissions': 'inRole:roles/storage.objectViewer'"))),
        Arguments.of(
            "permission is not a string", boundary(rule(RESOURCE, "'availablePermissions': [7]"))),
        Arguments.of(
            "availablePermissions is empty",
            boundary(rule(RESOURCE, "'availablePermissions': []"))),
        Arguments.of(
            "availabilityCondition is not an object",
            boundary(rule(RESOURCE, PERMISSIONS, "'availabilityCondition': 'true'"))),
        Arguments.of(
            "expression is missing",
            boundary(rule(RESOURCE, PERMISSIONS, "'availabilityCondition': {'title': 'EU'}"))),
        Arguments.of(
            "expression is empty",
            boundary(rule(RESOURCE, PERMISSIONS, "'availabilityCondition': {'expression': ''}"))),
        Arguments.of(
            "expression is blank",
            boundary(
                rule(RESOURCE, PERMISSIONS, "'availabilityCondition': {'expression': '   '}"))),
        Arguments.of(
            "title is null",
            boundary(
                rule(
                    RESOURCE,
                    PERMISSIONS,
                    "'availabilityCondition': {'expression': 'true', 'title': null}"))),
        Arguments.of(
            "description is null",
            boundary(
                rule(
                    RESOURCE,
                    PERMISSIONS,
                    "'availabilityCondition': {'expression': 'true', 'description': null}"))),
        Arguments.of(
            "title is not a string",
            boundary(
                rule(
                    RESOURCE,
                    PERMISSIONS,
                    "'availabilityCondition': {'expression': 'true', 'title': 1}"))),
        Arguments.of(
            "unknown condition field",
            boundary(
                rule(
                    RESOURCE,
                    PERMISSIONS,
                    "'availabilityCondition': {'expression': 'true', 'note': 'EU'}"))));
  }

  private static String boundary(String... rules) {
    return json("{'accessBoundary': {'accessBoundaryRules': [") + String.join(", ", rules) + "]}}";
  }

  private static String rule(String... fields) {
    return json("{" + String.join(", ", fields) + "}");
  }

  private static String json(String singleQuotedJson) {
    return singleQuotedJson.replace('\'', '"');
  }

  private static ImmutableList<String> availableResources(CredentialAccessBoundary boundary) {
    return boundary.getAccessBoundaryRules().stream()
        .map(CredentialAccessBoundary.AccessBoundaryRule::getAvailableResource)
        .collect(ImmutableList.toImmutableList());
  }

  private static CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition
      availabilityCondition(CredentialAccessBoundary boundary, int ruleIndex) {
    return boundary.getAccessBoundaryRules().get(ruleIndex).getAvailabilityCondition();
  }

  private static String readDocumentedExample() throws IOException {
    return Resources.toString(Resources.getResource("credential-access-boundary.json"), UTF_8);
  }
}
