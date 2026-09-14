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
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNullElseGet;

import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory for creating {@link HttpTransport} instances configured with proxies and timeouts. */
class GcsHttpTransportFactory {

  private static final Logger LOG = LoggerFactory.getLogger(GcsHttpTransportFactory.class);

  private GcsHttpTransportFactory() {}

  /**
   * Creates an {@link HttpTransport} configured according to the provided {@link GcsAuthOptions}.
   *
   * @param options The authentication options containing proxy and timeout configurations.
   * @return A configured {@link HttpTransport} instance.
   * @throws IOException If there is an issue establishing SSL trust or creating the transport.
   */
  public static HttpTransport createHttpTransport(GcsAuthOptions options) throws IOException {
    return createHttpTransport(
        options.getProxyAddress().orElse(null),
        options.getProxyUsername().orElse(null),
        options.getProxyPassword().orElse(null),
        options.getHttpReadTimeout());
  }

  /**
   * Creates an {@link HttpTransport} based on optional HTTP proxy and socket read timeout.
   *
   * @param proxyAddress The HTTP proxy address of the form {@code [https?://]hostname:port}.
   * @param proxyUsername The HTTP proxy username.
   * @param proxyPassword The HTTP proxy password.
   * @param readTimeout The socket read timeout to apply to HTTP requests.
   * @return The resulting {@link HttpTransport}.
   * @throws IllegalArgumentException If proxy configuration parameters are invalid.
   * @throws IOException If an error occurs initializing the certificate trust store or transport.
   */
  public static HttpTransport createHttpTransport(
      @Nullable String proxyAddress,
      @Nullable String proxyUsername,
      @Nullable String proxyPassword,
      @Nullable Duration readTimeout)
      throws IOException {
    LOG.debug(
        "createHttpTransport(proxyAddress={}, proxyUsername={}, readTimeout={})",
        proxyAddress,
        proxyUsername,
        readTimeout);
    checkArgument(
        proxyAddress != null || (proxyUsername == null && proxyPassword == null),
        "if proxyAddress is null then proxyUsername and proxyPassword should be null too");
    checkArgument(
        (proxyUsername == null) == (proxyPassword == null),
        "both proxyUsername and proxyPassword should be null or not null together");

    URI proxyUri = parseProxyAddress(proxyAddress);
    try {
      PasswordAuthentication proxyAuth =
          proxyUsername != null
              ? new PasswordAuthentication(proxyUsername, proxyPassword.toCharArray())
              : null;
      return createNetHttpTransport(proxyUri, proxyAuth, readTimeout);
    } catch (GeneralSecurityException e) {
      throw new IOException("Failed to create NetHttpTransport with SSL trust store", e);
    }
  }

  @VisibleForTesting
  static NetHttpTransport createNetHttpTransport(
      @Nullable URI proxyUri,
      @Nullable PasswordAuthentication proxyAuth,
      @Nullable Duration readTimeout)
      throws IOException, GeneralSecurityException {
    checkArgument(
        proxyUri != null || proxyAuth == null,
        "if proxyUri is null then proxyAuth should be null too");

    if (proxyAuth != null) {
      // Enable "Basic" authentication on JDK 8+
      System.setProperty("jdk.http.auth.tunneling.disabledSchemes", "");
      Authenticator.setDefault(
          new Authenticator() {
            @Nullable
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
              if (getRequestorType() == RequestorType.PROXY
                  && getRequestingHost().equalsIgnoreCase(proxyUri.getHost())
                  && getRequestingPort() == proxyUri.getPort()) {
                return proxyAuth;
              }
              return null;
            }
          });
    }
    return createNetHttpTransportBuilder(proxyUri, readTimeout).build();
  }

  @VisibleForTesting
  static NetHttpTransport.Builder createNetHttpTransportBuilder(
      @Nullable URI proxyUri, @Nullable Duration readTimeout)
      throws IOException, GeneralSecurityException {
    NetHttpTransport.Builder builder =
        new NetHttpTransport.Builder().trustCertificates(GoogleUtils.getCertificateTrustStore());
    SSLSocketFactory wrappedSslSocketFactory =
        requireNonNullElseGet(
            builder.getSslSocketFactory(), HttpsURLConnection::getDefaultSSLSocketFactory);
    return builder
        .setSslSocketFactory(new CustomSslSocketFactory(wrappedSslSocketFactory, readTimeout))
        .setProxy(
            proxyUri == null
                ? null
                : new Proxy(
                    Proxy.Type.HTTP,
                    new InetSocketAddress(proxyUri.getHost(), proxyUri.getPort())));
  }

  /**
   * Parses an HTTP proxy address string into a {@link URI}.
   *
   * @param proxyAddress The address of the form {@code [https?://]HOST:PORT}.
   * @return The URI representation of the proxy, or {@code null} if proxyAddress is null or empty.
   * @throws IllegalArgumentException If the proxy address is malformed.
   */
  @VisibleForTesting
  static URI parseProxyAddress(@Nullable String proxyAddress) {
    if (isNullOrEmpty(proxyAddress)) {
      return null;
    }
    String uriString = (proxyAddress.contains("//") ? "" : "//") + proxyAddress;
    try {
      URI uri = new URI(uriString);
      String scheme = uri.getScheme();
      String host = uri.getHost();
      int port = uri.getPort();
      checkArgument(
          isNullOrEmpty(scheme) || scheme.matches("https?"),
          "HTTP proxy address '%s' has invalid scheme '%s'.",
          proxyAddress,
          scheme);
      checkArgument(!isNullOrEmpty(host), "Proxy address '%s' has no host.", proxyAddress);
      checkArgument(port != -1, "Proxy address '%s' has no port.", proxyAddress);
      checkArgument(
          uri.equals(new URI(scheme, null, host, port, null, null, null)),
          "Invalid proxy address '%s'.",
          proxyAddress);
      return uri;
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Invalid proxy address '%s'.", proxyAddress), e);
    }
  }

  /** SSLSocketFactory wrapper to apply keep-alive and socket read timeout during TLS handshake. */
  @VisibleForTesting
  static final class CustomSslSocketFactory extends SSLSocketFactory {

    private final SSLSocketFactory wrappedSocketFactory;
    private final Integer readTimeoutMillis;

    public CustomSslSocketFactory(
        SSLSocketFactory wrappedSocketFactory, @Nullable Duration readTimeout) {
      this.wrappedSocketFactory = wrappedSocketFactory;
      this.readTimeoutMillis = readTimeout != null ? Math.toIntExact(readTimeout.toMillis()) : null;
    }

    @Override
    public String[] getDefaultCipherSuites() {
      return wrappedSocketFactory.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return wrappedSocketFactory.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket() throws IOException {
      return customizeSocket(wrappedSocketFactory.createSocket());
    }

    @Override
    public Socket createSocket(Socket s, InputStream consumed, boolean autoClose)
        throws IOException {
      return customizeSocket(wrappedSocketFactory.createSocket(s, consumed, autoClose));
    }

    @Override
    public Socket createSocket(Socket s, String host, int port, boolean autoClose)
        throws IOException {
      return customizeSocket(wrappedSocketFactory.createSocket(s, host, port, autoClose));
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
      return customizeSocket(wrappedSocketFactory.createSocket(host, port));
    }

    @Override
    public Socket createSocket(InetAddress address, int port) throws IOException {
      return customizeSocket(wrappedSocketFactory.createSocket(address, port));
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress clientAddress, int clientPort)
        throws IOException {
      return customizeSocket(
          wrappedSocketFactory.createSocket(host, port, clientAddress, clientPort));
    }

    @Override
    public Socket createSocket(
        InetAddress address, int port, InetAddress clientAddress, int clientPort)
        throws IOException {
      return customizeSocket(
          wrappedSocketFactory.createSocket(address, port, clientAddress, clientPort));
    }

    private Socket customizeSocket(Socket socket) throws SocketException {
      // Enable TCP keep-alive.
      socket.setKeepAlive(true);

      // Set socket read timeout. This shouldn't be necessary, because we generally set the timeout
      // through other layers, such as com.google.api.client.http.HttpRequest#setReadTimeout(int).
      // However, setting it here guarantees that the timeout is enforced during TLS handshake when
      // using Conscrypt as the security provider. (See discussion in
      // https://github.com/google/conscrypt/issues/864 .)
      if (readTimeoutMillis != null) {
        socket.setSoTimeout(readTimeoutMillis);
      }

      return socket;
    }
  }
}
