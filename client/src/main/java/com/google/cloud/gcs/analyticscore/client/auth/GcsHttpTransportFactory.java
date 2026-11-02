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
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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
 * <p>A transport created here carries the socket read timeout only. Request timeouts are set per
 * request in the Google HTTP client: {@link GcsTransportOptionsFactory} applies both timeouts to
 * GCS data requests, while token requests keep the HTTP client's defaults.
 */
final class GcsHttpTransportFactory {

  private static final Logger LOG = LoggerFactory.getLogger(GcsHttpTransportFactory.class);

  private GcsHttpTransportFactory() {}

  /**
   * Creates an {@link HttpTransport} configured according to the provided {@link GcsAuthOptions}.
   *
   * <p>The proxy is always reached over plain HTTP. An {@code https://} scheme in the proxy address
   * is checked but otherwise ignored.
   *
   * <p>Does not set the proxy username and password; see {@link
   * GcsCredentialsFactory#installProxyCredentialsIfConfigured(GcsAuthOptions)}.
   *
   * @param options The authentication options containing proxy and timeout configurations.
   * @param trustStoreSource The certificate trust store to validate TLS certificates against.
   * @return A configured {@link HttpTransport} instance.
   * @throws IOException If there is an issue establishing SSL trust or creating the transport.
   */
  static HttpTransport createHttpTransport(
      GcsAuthOptions options, TrustStoreSource trustStoreSource) throws IOException {
    checkNotNull(options, "options cannot be null");
    checkNotNull(trustStoreSource, "trustStoreSource cannot be null");
    LOG.debug(
        "createHttpTransport(proxyAddress={}, readTimeout={}, trustStoreSource={})",
        options.getProxyAddress().orElse(null),
        options.getHttpReadTimeout(),
        trustStoreSource);
    try {
      return createNetHttpTransportBuilder(options, trustStoreSource).build();
    } catch (GeneralSecurityException e) {
      throw new IOException("Failed to create NetHttpTransport with SSL trust store", e);
    }
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
   * @param readTimeout The socket read timeout, where zero means no timeout.
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
   * Converts a socket read timeout into the milliseconds accepted by {@link Socket#setSoTimeout},
   * where zero means no timeout.
   *
   * @param readTimeout The socket read timeout, where zero means no timeout.
   * @return The timeout in milliseconds.
   * @throws IllegalArgumentException If the timeout is negative, shorter than a millisecond but not
   *     zero, or overflows an int.
   */
  static int toReadTimeoutMillis(Duration readTimeout) {
    return toTimeoutMillis(readTimeout, "readTimeout");
  }

  /**
   * Converts a timeout duration into milliseconds, where zero means no timeout.
   *
   * <p>Consistent with {@link GcsAuthOptions}, timeouts must not be negative. A positive duration
   * shorter than a millisecond is rejected rather than rounded down to zero, which would mean no
   * timeout.
   *
   * @param timeout The timeout duration, where zero means no timeout.
   * @param timeoutName The name of the timeout parameter for error reporting.
   * @return The timeout in milliseconds.
   * @throws IllegalArgumentException If the timeout is negative, shorter than a millisecond but not
   *     zero, or overflows an int.
   */
  static int toTimeoutMillis(Duration timeout, String timeoutName) {
    checkNotNull(timeout, "%s cannot be null", timeoutName);
    checkArgument(
        !timeout.isNegative(), "%s must not be negative, but was %s", timeoutName, timeout);
    long timeoutMillis = timeout.toMillis();
    checkArgument(
        timeout.isZero() || timeoutMillis > 0,
        "%s must be zero or at least 1 millisecond, but was %s",
        timeoutName,
        timeout);
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
     * @param readTimeoutMillis The socket read timeout in milliseconds, where zero means no
     *     timeout.
     */
    KeepAliveSslSocketFactory(SSLSocketFactory wrappedSocketFactory, int readTimeoutMillis) {
      checkArgument(
          readTimeoutMillis >= 0,
          "readTimeoutMillis must not be negative, but was %s",
          readTimeoutMillis);
      this.wrappedSocketFactory = wrappedSocketFactory;
      this.readTimeoutMillis = readTimeoutMillis;
    }

    /** Returns the socket factory that this factory delegates socket creation to. */
    @VisibleForTesting
    SSLSocketFactory getWrappedSocketFactory() {
      return wrappedSocketFactory;
    }

    /** Returns the read timeout in milliseconds applied to created sockets. */
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
