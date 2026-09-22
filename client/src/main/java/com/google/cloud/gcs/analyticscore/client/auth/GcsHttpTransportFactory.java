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

import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.cloud.gcs.analyticscore.common.RedactedString;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.Socket;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.time.Duration;
import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating {@link HttpTransport} instances configured with proxies and timeouts.
 *
 * <p>A transport created here carries the socket read timeout only. The connect timeout is a
 * per-request setting in the Google HTTP client rather than a property of the transport, so callers
 * apply {@link GcsAuthOptions#getHttpConnectTimeout()} at the request layer instead, converting it
 * with {@link #toTimeoutMillis}.
 */
final class GcsHttpTransportFactory {

  private static final String TUNNELING_DISABLED_SCHEMES_PROPERTY =
      "jdk.http.auth.tunneling.disabledSchemes";

  private static final Logger LOG = LoggerFactory.getLogger(GcsHttpTransportFactory.class);

  private GcsHttpTransportFactory() {}

  /**
   * Creates an {@link HttpTransport} configured according to the provided {@link GcsAuthOptions}.
   *
   * <p><b>Side effects:</b> supplying proxy credentials mutates process-wide JDK state that
   * outlives the returned transport; see {@link #installProxyAuthentication} for the blast radius
   * in a shared JVM.
   *
   * @param options The authentication options containing proxy and timeout configurations.
   * @param trustStoreSource The certificate trust store to validate TLS certificates against.
   * @return A configured {@link HttpTransport} instance.
   * @throws IOException If there is an issue establishing SSL trust or creating the transport.
   */
  static HttpTransport createHttpTransport(
      GcsAuthOptions options, TrustStoreSource trustStoreSource) throws IOException {
    return createHttpTransport(
        options.getProxyAddress().orElse(null),
        options.getProxyUsername().orElse(null),
        options.getProxyPassword().orElse(null),
        options.getHttpReadTimeout(),
        trustStoreSource);
  }

  /**
   * Creates an {@link HttpTransport} based on optional HTTP proxy and socket read timeout.
   *
   * <p>The proxy is always contacted as an HTTP proxy: a scheme in {@code proxyAddress} is
   * validated but otherwise ignored, so {@code https://} does not imply TLS to the proxy itself.
   *
   * <p><b>Side effects:</b> supplying proxy credentials mutates process-wide JDK state that
   * outlives the returned transport; see {@link #installProxyAuthentication} for the blast radius
   * in a shared JVM.
   *
   * @param proxyAddress The HTTP proxy address of the form {@code [https?://]hostname:port}.
   * @param proxyUsername The HTTP proxy username.
   * @param proxyPassword The HTTP proxy password.
   * @param readTimeout The socket read timeout to apply to HTTP requests, which must be positive.
   * @param trustStoreSource The certificate trust store to validate TLS certificates against.
   * @return The resulting {@link HttpTransport}.
   * @throws IllegalArgumentException If proxy or timeout configuration parameters are invalid.
   * @throws IOException If an error occurs initializing the certificate trust store or transport.
   */
  static HttpTransport createHttpTransport(
      @Nullable String proxyAddress,
      @Nullable RedactedString proxyUsername,
      @Nullable RedactedString proxyPassword,
      Duration readTimeout,
      TrustStoreSource trustStoreSource)
      throws IOException {
    LOG.debug(
        "createHttpTransport(proxyAddress={}, proxyAuthenticated={}, readTimeout={},"
            + " trustStoreSource={})",
        proxyAddress,
        proxyUsername != null,
        readTimeout,
        trustStoreSource);
    URI proxyUri = GcsAuthOptions.parseProxyAddress(proxyAddress);
    checkArgument(
        proxyUri != null || (proxyUsername == null && proxyPassword == null),
        "if proxyAddress is null or empty then proxyUsername and proxyPassword should be null too");
    checkArgument(
        (proxyUsername == null) == (proxyPassword == null),
        "both proxyUsername and proxyPassword should be null or not null together");

    try {
      PasswordAuthentication proxyAuth =
          proxyUsername == null
              ? null
              : new PasswordAuthentication(
                  proxyUsername.value(), proxyPassword.value().toCharArray());
      return createNetHttpTransport(proxyUri, proxyAuth, readTimeout, trustStoreSource);
    } catch (GeneralSecurityException e) {
      throw new IOException("Failed to create NetHttpTransport with SSL trust store", e);
    }
  }

  /**
   * Creates a {@link NetHttpTransport} routed through the given proxy.
   *
   * <p><b>Side effects:</b> when {@code proxyAuth} is non-null, this method calls {@link
   * #installProxyAuthentication}, which overwrites the JVM default {@link Authenticator} (last
   * writer wins across the JVM) and permanently clears {@code
   * jdk.http.auth.tunneling.disabledSchemes} for the life of the process.
   *
   * @param proxyUri The parsed proxy address, or {@code null} to connect directly.
   * @param proxyAuth The proxy credentials, or {@code null} if the proxy needs no authentication.
   * @param readTimeout The socket read timeout, which must be positive.
   * @param trustStoreSource The certificate trust store to validate TLS certificates against.
   * @return The resulting {@link NetHttpTransport}.
   * @throws IllegalArgumentException If proxyAuth is set without a proxyUri, or if readTimeout is
   *     not positive.
   * @throws IOException If an error occurs initializing the certificate trust store.
   * @throws GeneralSecurityException If the certificate trust store cannot be loaded.
   */
  @VisibleForTesting
  static NetHttpTransport createNetHttpTransport(
      @Nullable URI proxyUri,
      @Nullable PasswordAuthentication proxyAuth,
      Duration readTimeout,
      TrustStoreSource trustStoreSource)
      throws IOException, GeneralSecurityException {
    checkArgument(
        proxyUri != null || proxyAuth == null,
        "if proxyUri is null then proxyAuth should be null too");

    NetHttpTransport transport =
        createNetHttpTransportBuilder(proxyUri, readTimeout, trustStoreSource).build();
    if (proxyAuth != null) {
      installProxyAuthentication(proxyUri, proxyAuth);
    }
    return transport;
  }

  /**
   * Installs the JVM-wide state that lets the JDK answer a proxy authentication challenge.
   *
   * <p>The JDK exposes no per-connection hook for proxy credentials when tunneling HTTPS through a
   * {@code CONNECT} proxy, so the credentials must be published through process-wide state with two
   * consequences that outlive the transport:
   *
   * <ol>
   *   <li><b>Last writer wins:</b> {@link Authenticator#setDefault} replaces any existing default
   *       authenticator without delegation. If two {@code GcsFileSystem} instances (for example,
   *       two Spark catalogs in the same executor JVM) configure different authenticated proxies,
   *       the second installation silently overwrites the first, causing the first proxy to fail
   *       with HTTP 407.
   *   <li><b>Process-wide tunneling policy change:</b> {@code
   *       jdk.http.auth.tunneling.disabledSchemes} is cleared process-wide and never restored,
   *       re-enabling HTTP {@code Basic} authentication over {@code CONNECT} for every HTTPS tunnel
   *       in the JVM for the life of the process.
   * </ol>
   *
   * @param proxyUri The parsed proxy address the credentials belong to.
   * @param proxyAuth The proxy credentials to serve for that address.
   */
  private static void installProxyAuthentication(URI proxyUri, PasswordAuthentication proxyAuth) {
    // Re-enable "Basic" authentication for HTTPS proxy tunnels (disabled by default in JDK 8u111+).
    System.setProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY, "");
    Authenticator.setDefault(
        new Authenticator() {
          @Nullable
          @Override
          protected PasswordAuthentication getPasswordAuthentication() {
            // getRequestingHost() is null when the JDK only knows the proxy by address, so the
            // known proxy host is the receiver of the comparison.
            if (getRequestorType() == RequestorType.PROXY
                && proxyUri.getHost().equalsIgnoreCase(getRequestingHost())
                && getRequestingPort() == proxyUri.getPort()) {
              return proxyAuth;
            }
            return null;
          }
        });
  }

  /**
   * Creates the {@link NetHttpTransport.Builder} configured from {@link GcsAuthOptions}.
   *
   * @param options The authentication options containing proxy and timeout configurations.
   * @param trustStoreSource The certificate trust store to validate TLS certificates against.
   * @return The configured builder.
   * @throws IOException If an error occurs initializing the certificate trust store.
   * @throws GeneralSecurityException If the certificate trust store cannot be loaded.
   */
  @VisibleForTesting
  static NetHttpTransport.Builder createNetHttpTransportBuilder(
      GcsAuthOptions options, TrustStoreSource trustStoreSource)
      throws IOException, GeneralSecurityException {
    URI proxyUri = GcsAuthOptions.parseProxyAddress(options.getProxyAddress().orElse(null));
    return createNetHttpTransportBuilder(proxyUri, options.getHttpReadTimeout(), trustStoreSource);
  }

  /**
   * Creates the {@link NetHttpTransport.Builder} that backs every transport this factory returns.
   *
   * @param proxyUri The parsed proxy address, or {@code null} to connect directly.
   * @param readTimeout The socket read timeout, which must be positive.
   * @param trustStoreSource The certificate trust store to validate TLS certificates against.
   * @return The configured builder.
   * @throws IOException If an error occurs initializing the certificate trust store.
   * @throws GeneralSecurityException If the certificate trust store cannot be loaded.
   */
  @VisibleForTesting
  static NetHttpTransport.Builder createNetHttpTransportBuilder(
      @Nullable URI proxyUri, Duration readTimeout, TrustStoreSource trustStoreSource)
      throws IOException, GeneralSecurityException {
    NetHttpTransport.Builder builder = new NetHttpTransport.Builder();
    if (!trustStoreSource.isSystemDefault()) {
      builder.trustCertificates(GoogleUtils.getCertificateTrustStore());
    }
    SSLSocketFactory wrappedSslSocketFactory = builder.getSslSocketFactory();
    if (wrappedSslSocketFactory == null) {
      wrappedSslSocketFactory = HttpsURLConnection.getDefaultSSLSocketFactory();
    }
    return builder
        .setSslSocketFactory(
            new KeepAliveSslSocketFactory(
                wrappedSslSocketFactory, toReadTimeoutMillis(readTimeout)))
        .setProxy(createProxy(proxyUri));
  }

  /**
   * Creates the HTTP {@link Proxy} to route requests through.
   *
   * @param proxyUri The parsed proxy address, or {@code null} to connect directly.
   * @return The HTTP proxy, or {@code null} if {@code proxyUri} is null.
   */
  @VisibleForTesting
  @Nullable
  static Proxy createProxy(@Nullable URI proxyUri) {
    return proxyUri == null
        ? null
        : new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyUri.getHost(), proxyUri.getPort()));
  }

  /**
   * Converts a socket read timeout into the positive milliseconds accepted by {@link
   * Socket#setSoTimeout}.
   *
   * @param readTimeout The socket read timeout, which must be positive.
   * @return The positive timeout in milliseconds.
   * @throws IllegalArgumentException If the timeout is zero, negative, shorter than a millisecond,
   *     or overflows an int.
   */
  static int toReadTimeoutMillis(Duration readTimeout) {
    return toTimeoutMillis(readTimeout, "readTimeout");
  }

  /**
   * Converts a timeout duration into positive milliseconds.
   *
   * <p>Consistent with {@link GcsAuthOptions}, timeouts must be strictly positive (at least 1
   * millisecond and at most {@link Integer#MAX_VALUE} milliseconds). Zero and sub-millisecond
   * durations are rejected rather than rounded down to zero.
   *
   * @param timeout The timeout duration, which must be positive.
   * @param timeoutName The name of the timeout parameter for error reporting.
   * @return The positive timeout in milliseconds.
   * @throws IllegalArgumentException If the timeout is zero, negative, shorter than a millisecond,
   *     or overflows an int.
   */
  static int toTimeoutMillis(Duration timeout, String timeoutName) {
    checkNotNull(timeout, "%s cannot be null", timeoutName);
    checkArgument(
        !timeout.isNegative() && !timeout.isZero(),
        "%s must be positive, but was %s",
        timeoutName,
        timeout);
    long timeoutMillis = timeout.toMillis();
    checkArgument(
        timeoutMillis > 0, "%s must be at least 1 millisecond, but was %s", timeoutName, timeout);
    checkArgument(
        timeoutMillis <= Integer.MAX_VALUE,
        "%s must not exceed %s milliseconds, but was %s",
        timeoutName,
        Integer.MAX_VALUE,
        timeout);
    return (int) timeoutMillis;
  }

  /** SSLSocketFactory wrapper to apply keep-alive and socket read timeout during TLS handshake. */
  @VisibleForTesting
  static final class KeepAliveSslSocketFactory extends SSLSocketFactory {

    private final SSLSocketFactory wrappedSocketFactory;
    private final int readTimeoutMillis;

    /**
     * @param wrappedSocketFactory The socket factory to delegate socket creation to.
     * @param readTimeoutMillis The positive socket read timeout in milliseconds.
     */
    KeepAliveSslSocketFactory(SSLSocketFactory wrappedSocketFactory, int readTimeoutMillis) {
      checkArgument(
          readTimeoutMillis > 0,
          "readTimeoutMillis must be positive, but was %s",
          readTimeoutMillis);
      this.wrappedSocketFactory = wrappedSocketFactory;
      this.readTimeoutMillis = readTimeoutMillis;
    }

    /** Returns the socket factory that this factory delegates socket creation to. */
    @VisibleForTesting
    SSLSocketFactory getWrappedSocketFactory() {
      return wrappedSocketFactory;
    }

    /** Returns the positive read timeout in milliseconds applied to created sockets. */
    @VisibleForTesting
    int getReadTimeoutMillis() {
      return readTimeoutMillis;
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
      return customizeSocket(wrappedSocketFactory.createSocket(), /* closeOnFailure= */ true);
    }

    @Override
    public Socket createSocket(Socket s, InputStream consumed, boolean autoClose)
        throws IOException {
      return customizeSocket(
          wrappedSocketFactory.createSocket(s, consumed, autoClose),
          /* closeOnFailure= */ autoClose);
    }

    @Override
    public Socket createSocket(Socket s, String host, int port, boolean autoClose)
        throws IOException {
      return customizeSocket(
          wrappedSocketFactory.createSocket(s, host, port, autoClose),
          /* closeOnFailure= */ autoClose);
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
      return customizeSocket(
          wrappedSocketFactory.createSocket(host, port), /* closeOnFailure= */ true);
    }

    @Override
    public Socket createSocket(InetAddress address, int port) throws IOException {
      return customizeSocket(
          wrappedSocketFactory.createSocket(address, port), /* closeOnFailure= */ true);
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress clientAddress, int clientPort)
        throws IOException {
      return customizeSocket(
          wrappedSocketFactory.createSocket(host, port, clientAddress, clientPort),
          /* closeOnFailure= */ true);
    }

    @Override
    public Socket createSocket(
        InetAddress address, int port, InetAddress clientAddress, int clientPort)
        throws IOException {
      return customizeSocket(
          wrappedSocketFactory.createSocket(address, port, clientAddress, clientPort),
          /* closeOnFailure= */ true);
    }

    @Nullable
    private Socket customizeSocket(@Nullable Socket socket, boolean closeOnFailure)
        throws IOException {
      if (socket == null) {
        return null;
      }
      try {
        // Enable TCP keep-alive.
        socket.setKeepAlive(true);

        // Set socket read timeout. This shouldn't be necessary, because we generally set the
        // timeout through other layers, such as
        // com.google.api.client.http.HttpRequest#setReadTimeout(int). However, setting it here
        // guarantees that the timeout is enforced during TLS handshake when using Conscrypt as the
        // security provider. (See discussion in https://github.com/google/conscrypt/issues/864 .)
        socket.setSoTimeout(readTimeoutMillis);
      } catch (IOException | RuntimeException e) {
        // When this factory owns the socket (newly created sockets, or layered sockets with
        // autoClose=true), a half-configured socket must not outlive the failure that abandoned it.
        // When autoClose=false, the caller explicitly retains ownership of the underlying socket.
        if (closeOnFailure) {
          try {
            socket.close();
          } catch (IOException closeFailure) {
            e.addSuppressed(closeFailure);
          }
        }
        throw e;
      }
      return socket;
    }
  }
}
