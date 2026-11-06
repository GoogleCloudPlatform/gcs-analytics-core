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
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A local OAuth 2.0 token endpoint for tests.
 *
 * <p>It hands out the access token only to requests whose form body holds every required parameter,
 * and answers other requests with {@code 400}. This lets a test check which grant the client sent,
 * and keeps the token away from other local callers. The endpoint path holds a random ID for the
 * same reason. Point {@code analytics-core.auth.token-server-uri} at {@link #tokenUri()} to send
 * token requests here.
 */
final class FakeTokenServer implements AutoCloseable {

  private static final String HOST = "127.0.0.1";
  private static final byte[] INVALID_REQUEST_BODY =
      ("{\"error\":\"invalid_request\","
              + "\"error_description\":\"The token request is missing a required parameter.\"}")
          .getBytes(UTF_8);

  private final HttpServer server;
  private final String tokenPath = "/token/" + UUID.randomUUID();
  private final byte[] grantedResponseBody;
  private final ImmutableMap<String, String> requiredParameters;
  private final AtomicInteger grantedRequestCount = new AtomicInteger();

  private FakeTokenServer(String accessToken, ImmutableMap<String, String> requiredParameters)
      throws IOException {
    checkNotNull(accessToken, "accessToken cannot be null");
    checkArgument(
        !accessToken.contains("\"") && !accessToken.contains("\\"),
        "accessToken must not contain quotes or backslashes");
    checkArgument(!requiredParameters.isEmpty(), "requiredParameters cannot be empty");
    this.requiredParameters = requiredParameters;
    this.grantedResponseBody =
        String.format(
                "{\"access_token\":\"%s\",\"expires_in\":3600,\"token_type\":\"Bearer\"}",
                accessToken)
            .getBytes(UTF_8);
    this.server = HttpServer.create(new InetSocketAddress(HOST, 0), 0);
    server.createContext(tokenPath, this::handle);
    server.start();
  }

  /**
   * Starts a server that answers token requests holding every entry of {@code requiredParameters}
   * with {@code accessToken}, which must not contain quotes or backslashes.
   */
  static FakeTokenServer grantingAccessToken(
      String accessToken, ImmutableMap<String, String> requiredParameters) throws IOException {
    return new FakeTokenServer(accessToken, requiredParameters);
  }

  /** Returns the URI of the token endpoint. */
  URI tokenUri() {
    return URI.create("http://" + HOST + ":" + server.getAddress().getPort() + tokenPath);
  }

  /** Returns the number of token requests that held every required parameter. */
  int grantedRequestCount() {
    return grantedRequestCount.get();
  }

  @Override
  public void close() {
    server.stop(0);
  }

  private void handle(HttpExchange exchange) throws IOException {
    Map<String, String> parameters;
    try (InputStream requestBody = exchange.getRequestBody()) {
      parameters = parseForm(new String(requestBody.readAllBytes(), UTF_8));
    }
    boolean granted = parameters.entrySet().containsAll(requiredParameters.entrySet());
    if (granted) {
      grantedRequestCount.incrementAndGet();
    }
    respond(
        exchange,
        granted ? HttpURLConnection.HTTP_OK : HttpURLConnection.HTTP_BAD_REQUEST,
        granted ? grantedResponseBody : INVALID_REQUEST_BODY);
  }

  /**
   * Parses an {@code application/x-www-form-urlencoded} body. A repeated name keeps its last value.
   */
  private static Map<String, String> parseForm(String body) {
    Map<String, String> parameters = new HashMap<>();
    for (String pair : Splitter.on('&').omitEmptyStrings().split(body)) {
      int equals = pair.indexOf('=');
      String name = equals < 0 ? pair : pair.substring(0, equals);
      String value = equals < 0 ? "" : pair.substring(equals + 1);
      parameters.put(URLDecoder.decode(name, UTF_8), URLDecoder.decode(value, UTF_8));
    }
    return parameters;
  }

  private static void respond(HttpExchange exchange, int status, byte[] body) throws IOException {
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, body.length);
    try (OutputStream responseStream = exchange.getResponseBody()) {
      responseStream.write(body);
    }
  }
}
