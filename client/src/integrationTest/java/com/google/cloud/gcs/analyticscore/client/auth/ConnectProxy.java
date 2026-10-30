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

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Ascii;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A local HTTP proxy for tests that records {@code CONNECT} requests without opening a tunnel.
 *
 * <p>Basic proxy authentication takes two requests. The client first sends a request without
 * credentials, which the proxy records in {@link #challengedTargets()} and answers with {@code
 * 407}. The client then sends the request again with credentials. The proxy records a target with
 * wrong credentials in {@link #rejectedTargets()} and answers it with {@code 407}. It records a
 * target with matching credentials in {@link #authenticatedTargets()} and answers it with {@code
 * 502}, so the client's request fails right after the proxy sees it.
 */
final class ConnectProxy implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectProxy.class);

  private static final String HOST = "127.0.0.1";
  private static final int SOCKET_READ_TIMEOUT_MS = 5_000;
  private static final String PROXY_AUTHENTICATION_REQUIRED =
      "HTTP/1.1 407 Proxy Authentication Required\r\n"
          + "Proxy-Authenticate: Basic realm=\"connect-proxy\"\r\n"
          + "Content-Length: 0\r\n"
          + "Connection: close\r\n\r\n";
  private static final String BAD_GATEWAY =
      "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

  private final ServerSocket serverSocket;
  private final String expectedCredentials;
  private final List<String> challengedTargets = new CopyOnWriteArrayList<>();
  private final List<String> authenticatedTargets = new CopyOnWriteArrayList<>();
  private final List<String> rejectedTargets = new CopyOnWriteArrayList<>();

  private ConnectProxy(String expectedCredentials) throws IOException {
    this.expectedCredentials = expectedCredentials;
    this.serverSocket = new ServerSocket(0, 50, InetAddress.getByName(HOST));
  }

  /** Starts a proxy that accepts only requests with these Basic credentials. */
  static ConnectProxy requiringPassword(String username, String password) throws IOException {
    String credentials = username + ":" + password;
    ConnectProxy proxy =
        new ConnectProxy(
            "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(UTF_8)));
    Thread acceptThread = new Thread(proxy::serve, "connect-proxy");
    acceptThread.setDaemon(true);
    acceptThread.start();
    return proxy;
  }

  /** Returns the proxy address in the {@code host:port} form of the proxy address setting. */
  String address() {
    return HOST + ":" + serverSocket.getLocalPort();
  }

  /**
   * Returns the {@code host:port} of each request without a {@code Proxy-Authorization} header,
   * oldest first.
   */
  ImmutableList<String> challengedTargets() {
    return ImmutableList.copyOf(challengedTargets);
  }

  /** Returns the {@code host:port} of each request with the expected credentials, oldest first. */
  ImmutableList<String> authenticatedTargets() {
    return ImmutableList.copyOf(authenticatedTargets);
  }

  /**
   * Returns the {@code host:port} of each request that sent wrong {@code Proxy-Authorization}
   * credentials, oldest first.
   */
  ImmutableList<String> rejectedTargets() {
    return ImmutableList.copyOf(rejectedTargets);
  }

  @Override
  public void close() throws IOException {
    serverSocket.close();
  }

  /** Answers one connection at a time until the proxy is closed. */
  private void serve() {
    while (!serverSocket.isClosed()) {
      try (Socket client = serverSocket.accept()) {
        answer(client);
      } catch (IOException e) {
        LOG.debug("Proxy connection failed", e);
      }
    }
  }

  private void answer(Socket client) throws IOException {
    client.setSoTimeout(SOCKET_READ_TIMEOUT_MS);
    InputStream input = client.getInputStream();
    String target = readConnectTarget(input);
    boolean sawCredentials = false;
    boolean authenticated = false;
    for (String header = readLine(input); !header.isEmpty(); header = readLine(input)) {
      if (isProxyAuthorizationHeader(header)) {
        sawCredentials = true;
        authenticated |= hasExpectedCredentials(header);
      }
    }
    if (authenticated) {
      authenticatedTargets.add(target);
    } else if (sawCredentials) {
      rejectedTargets.add(target);
    } else {
      challengedTargets.add(target);
    }
    String response = authenticated ? BAD_GATEWAY : PROXY_AUTHENTICATION_REQUIRED;
    client.getOutputStream().write(response.getBytes(US_ASCII));
  }

  private static String readConnectTarget(InputStream input) throws IOException {
    String requestLine = readLine(input);
    List<String> parts = Splitter.on(' ').splitToList(requestLine);
    if (parts.size() < 2 || !parts.get(0).equals("CONNECT")) {
      throw new IOException("Expected a CONNECT request line, got: " + requestLine);
    }
    return parts.get(1);
  }

  /**
   * Returns whether {@code header} is a {@code Proxy-Authorization} header. The name is compared
   * without case because the JDK sends {@code Proxy-authorization}.
   */
  private static boolean isProxyAuthorizationHeader(String header) {
    int colon = header.indexOf(':');
    return colon > 0 && Ascii.equalsIgnoreCase(header.substring(0, colon), "Proxy-Authorization");
  }

  private boolean hasExpectedCredentials(String header) {
    int colon = header.indexOf(':');
    return colon > 0 && header.substring(colon + 1).trim().equals(expectedCredentials);
  }

  /** Reads one line of the request head, without its line break. */
  private static String readLine(InputStream input) throws IOException {
    StringBuilder line = new StringBuilder();
    for (int next = input.read(); next != '\n'; next = input.read()) {
      if (next < 0) {
        throw new EOFException("Connection closed before the request head ended");
      }
      if (next != '\r') {
        line.append((char) next);
      }
    }
    return line.toString();
  }
}
