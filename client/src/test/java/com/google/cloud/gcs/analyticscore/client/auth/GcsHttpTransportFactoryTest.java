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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mockStatic;

import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.cloud.gcs.analyticscore.client.auth.GcsHttpTransportFactory.KeepAliveSslSocketFactory;
import com.google.cloud.gcs.analyticscore.common.RedactedString;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.Authenticator.RequestorType;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

class GcsHttpTransportFactoryTest {

  private static final String TUNNELING_DISABLED_SCHEMES_PROPERTY =
      "jdk.http.auth.tunneling.disabledSchemes";
  private static final String PROXY_HOST = "proxy.example.com";
  private static final int PROXY_PORT = 8080;
  private static final String PROXY_ADDRESS = PROXY_HOST + ":" + PROXY_PORT;
  private static final RedactedString PROXY_USERNAME = RedactedString.create("user");
  private static final RedactedString PROXY_PASSWORD = RedactedString.create("pass");
  private static final Duration READ_TIMEOUT = Duration.ofSeconds(5);

  private Authenticator originalAuthenticator;
  private String originalTunnelingDisabledSchemes;

  /**
   * Captures the JVM wide state that configuring an authenticated proxy overwrites, so that these
   * tests cannot leak a default {@link Authenticator} into tests that run after them.
   */
  @BeforeEach
  void captureGlobalState() {
    originalAuthenticator = Authenticator.getDefault();
    originalTunnelingDisabledSchemes = System.getProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY);
  }

  @AfterEach
  void restoreGlobalState() {
    Authenticator.setDefault(originalAuthenticator);
    if (originalTunnelingDisabledSchemes == null) {
      System.clearProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY);
    } else {
      System.setProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY, originalTunnelingDisabledSchemes);
    }
  }

  @Test
  void createProxy_nullUri_returnsNull() {
    Proxy proxy = GcsHttpTransportFactory.createProxy(null);

    assertThat(proxy).isNull();
  }

  @Test
  void createProxy_proxyUri_returnsHttpProxy() {
    Proxy proxy = GcsHttpTransportFactory.createProxy(URI.create("//127.0.0.1:3128"));

    assertThat(proxy.type()).isEqualTo(Proxy.Type.HTTP);
  }

  @Test
  void createProxy_proxyUri_returnsProxyWithConfiguredAddress() {
    Proxy proxy = GcsHttpTransportFactory.createProxy(URI.create("//127.0.0.1:3128"));

    assertThat(proxy.address()).isEqualTo(new InetSocketAddress("127.0.0.1", 3128));
  }

  @Test
  void createNetHttpTransportBuilder_installsKeepAliveSslSocketFactory()
      throws IOException, GeneralSecurityException {
    NetHttpTransport.Builder builder =
        GcsHttpTransportFactory.createNetHttpTransportBuilder(
            null, READ_TIMEOUT, TrustStoreSource.GOOGLE_BUNDLED);

    assertThat(builder.getSslSocketFactory()).isInstanceOf(KeepAliveSslSocketFactory.class);
  }

  @Test
  void createNetHttpTransportBuilder_googleBundledTrustStore_doesNotUseSystemDefaultSocketFactory()
      throws IOException, GeneralSecurityException {
    NetHttpTransport.Builder builder =
        GcsHttpTransportFactory.createNetHttpTransportBuilder(
            null, READ_TIMEOUT, TrustStoreSource.GOOGLE_BUNDLED);

    // Google's bundled trust store yields a dedicated SSLSocketFactory, not the JVM default.
    assertThat(
            ((KeepAliveSslSocketFactory) builder.getSslSocketFactory()).getWrappedSocketFactory())
        .isNotSameInstanceAs(HttpsURLConnection.getDefaultSSLSocketFactory());
  }

  @Test
  void createNetHttpTransportBuilder_systemDefaultTrustStore_usesSystemDefaultSocketFactory()
      throws IOException, GeneralSecurityException {
    NetHttpTransport.Builder builder =
        GcsHttpTransportFactory.createNetHttpTransportBuilder(
            null, READ_TIMEOUT, TrustStoreSource.SYSTEM_DEFAULT);

    // Custom endpoints, such as Trusted Partner Cloud, validate against the JVM's trust store.
    assertThat(
            ((KeepAliveSslSocketFactory) builder.getSslSocketFactory()).getWrappedSocketFactory())
        .isSameInstanceAs(HttpsURLConnection.getDefaultSSLSocketFactory());
  }

  @Test
  void createHttpTransport_withProxyCredentials_installsDefaultAuthenticator() throws IOException {
    installProxyAuthenticator();

    assertThat(Authenticator.getDefault()).isNotNull();
  }

  @Test
  void createHttpTransport_withProxyCredentials_enablesBasicAuthenticationForTunnels()
      throws IOException {
    installProxyAuthenticator();

    assertThat(System.getProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY)).isEmpty();
  }

  @Test
  void proxyAuthenticator_matchingProxy_returnsProxyUsername() throws IOException {
    installProxyAuthenticator();

    PasswordAuthentication credentials =
        requestProxyCredentials(PROXY_HOST, PROXY_PORT, RequestorType.PROXY);

    assertThat(credentials.getUserName()).isEqualTo(PROXY_USERNAME.value());
  }

  @Test
  void proxyAuthenticator_matchingProxy_returnsProxyPassword() throws IOException {
    installProxyAuthenticator();

    PasswordAuthentication credentials =
        requestProxyCredentials(PROXY_HOST, PROXY_PORT, RequestorType.PROXY);

    assertThat(credentials.getPassword()).isEqualTo(PROXY_PASSWORD.value().toCharArray());
  }

  @Test
  void proxyAuthenticator_differentHost_returnsNull() throws IOException {
    installProxyAuthenticator();

    PasswordAuthentication credentials =
        requestProxyCredentials("other.example.com", PROXY_PORT, RequestorType.PROXY);

    assertThat(credentials).isNull();
  }

  @Test
  void proxyAuthenticator_differentPort_returnsNull() throws IOException {
    installProxyAuthenticator();

    PasswordAuthentication credentials =
        requestProxyCredentials(PROXY_HOST, PROXY_PORT + 1, RequestorType.PROXY);

    assertThat(credentials).isNull();
  }

  @Test
  void proxyAuthenticator_serverRequestor_returnsNull() throws IOException {
    installProxyAuthenticator();

    PasswordAuthentication credentials =
        requestProxyCredentials(PROXY_HOST, PROXY_PORT, RequestorType.SERVER);

    assertThat(credentials).isNull();
  }

  @Test
  void proxyAuthenticator_unknownRequestingHost_returnsNull() throws IOException {
    installProxyAuthenticator();

    // The JDK reports a null host when it only knows the requestor by address.
    PasswordAuthentication credentials =
        requestProxyCredentials(/* host= */ null, PROXY_PORT, RequestorType.PROXY);

    assertThat(credentials).isNull();
  }

  /** Installs the JVM wide {@link Authenticator} that serves the configured proxy credentials. */
  private static void installProxyAuthenticator() throws IOException {
    GcsHttpTransportFactory.createHttpTransport(
        PROXY_ADDRESS,
        PROXY_USERNAME,
        PROXY_PASSWORD,
        Duration.ofSeconds(10),
        TrustStoreSource.GOOGLE_BUNDLED);
  }

  /** Asks the installed {@link Authenticator} for credentials the way the JDK's HTTP stack does. */
  @Nullable
  private static PasswordAuthentication requestProxyCredentials(
      @Nullable String host, int port, RequestorType requestorType) {
    return Authenticator.requestPasswordAuthentication(
        host,
        /* addr= */ null,
        port,
        /* protocol= */ "http",
        /* prompt= */ "",
        /* scheme= */ "basic",
        /* url= */ null,
        requestorType);
  }

  @Test
  void createNetHttpTransportBuilder_fromGcsAuthOptionsWithProxy_appliesConfiguredReadTimeout()
      throws IOException, GeneralSecurityException {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setProxyAddress(PROXY_ADDRESS)
            .setProxyUsername(PROXY_USERNAME)
            .setProxyPassword(PROXY_PASSWORD)
            .setHttpReadTimeout(Duration.ofSeconds(10))
            .build();

    NetHttpTransport.Builder builder =
        GcsHttpTransportFactory.createNetHttpTransportBuilder(
            options, TrustStoreSource.GOOGLE_BUNDLED);

    assertThat(((KeepAliveSslSocketFactory) builder.getSslSocketFactory()).getReadTimeoutMillis())
        .isEqualTo(10_000);
  }

  @Test
  void createNetHttpTransportBuilder_fromGcsAuthOptionsWithoutProxy_appliesDefaultReadTimeout()
      throws IOException, GeneralSecurityException {
    GcsAuthOptions options = GcsAuthOptions.builder().build();

    NetHttpTransport.Builder builder =
        GcsHttpTransportFactory.createNetHttpTransportBuilder(
            options, TrustStoreSource.GOOGLE_BUNDLED);

    assertThat(((KeepAliveSslSocketFactory) builder.getSslSocketFactory()).getReadTimeoutMillis())
        .isEqualTo(5_000);
  }

  @Test
  void createHttpTransport_fromGcsAuthOptionsWithProxy_installsConfiguredProxyCredentials()
      throws IOException {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setProxyAddress(PROXY_ADDRESS)
            .setProxyUsername(PROXY_USERNAME)
            .setProxyPassword(PROXY_PASSWORD)
            .setHttpReadTimeout(Duration.ofSeconds(10))
            .build();

    GcsHttpTransportFactory.createHttpTransport(options, TrustStoreSource.GOOGLE_BUNDLED);
    PasswordAuthentication credentials =
        requestProxyCredentials(PROXY_HOST, PROXY_PORT, RequestorType.PROXY);

    assertThat(credentials.getUserName()).isEqualTo(PROXY_USERNAME.value());
  }

  @Test
  void createHttpTransport_fromGcsAuthOptionsWithoutProxy_leavesDefaultAuthenticatorUnchanged()
      throws IOException {
    Authenticator sentinelAuthenticator = new Authenticator() {};
    Authenticator.setDefault(sentinelAuthenticator);
    GcsAuthOptions options = GcsAuthOptions.builder().build();

    GcsHttpTransportFactory.createHttpTransport(options, TrustStoreSource.GOOGLE_BUNDLED);

    assertThat(Authenticator.getDefault()).isSameInstanceAs(sentinelAuthenticator);
  }

  @Test
  void createHttpTransport_proxyAuthWithoutProxyAddress_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GcsHttpTransportFactory.createHttpTransport(
                null,
                PROXY_USERNAME,
                PROXY_PASSWORD,
                READ_TIMEOUT,
                TrustStoreSource.GOOGLE_BUNDLED));
  }

  @Test
  void createHttpTransport_proxyAuthWithEmptyProxyAddress_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GcsHttpTransportFactory.createHttpTransport(
                "", PROXY_USERNAME, PROXY_PASSWORD, READ_TIMEOUT, TrustStoreSource.GOOGLE_BUNDLED));
  }

  @Test
  void createHttpTransport_proxyUsernameWithoutPassword_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GcsHttpTransportFactory.createHttpTransport(
                PROXY_ADDRESS,
                PROXY_USERNAME,
                null,
                READ_TIMEOUT,
                TrustStoreSource.GOOGLE_BUNDLED));
  }

  @Test
  void createHttpTransport_proxyPasswordWithoutUsername_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GcsHttpTransportFactory.createHttpTransport(
                PROXY_ADDRESS,
                null,
                PROXY_PASSWORD,
                READ_TIMEOUT,
                TrustStoreSource.GOOGLE_BUNDLED));
  }

  @Test
  void createHttpTransport_proxyPasswordWithoutAddressAndUsername_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GcsHttpTransportFactory.createHttpTransport(
                null, null, PROXY_PASSWORD, READ_TIMEOUT, TrustStoreSource.GOOGLE_BUNDLED));
  }

  @Test
  void createHttpTransport_trustStoreFailure_throwsIOExceptionWithCause() {
    GeneralSecurityException trustStoreFailure =
        new GeneralSecurityException("Simulated trust store load failure");

    try (MockedStatic<GoogleUtils> mockedGoogleUtils = mockStatic(GoogleUtils.class)) {
      mockedGoogleUtils.when(GoogleUtils::getCertificateTrustStore).thenThrow(trustStoreFailure);

      IOException thrown =
          assertThrows(
              IOException.class,
              () ->
                  GcsHttpTransportFactory.createHttpTransport(
                      null, null, null, READ_TIMEOUT, TrustStoreSource.GOOGLE_BUNDLED));

      assertThat(thrown).hasCauseThat().isSameInstanceAs(trustStoreFailure);
    }
  }

  @Test
  void createHttpTransport_trustStoreFailure_leavesDefaultAuthenticatorUnchanged() {
    Authenticator sentinelAuthenticator = new Authenticator() {};
    Authenticator.setDefault(sentinelAuthenticator);

    try (MockedStatic<GoogleUtils> mockedGoogleUtils = mockStatic(GoogleUtils.class)) {
      mockedGoogleUtils
          .when(GoogleUtils::getCertificateTrustStore)
          .thenThrow(new GeneralSecurityException("Simulated trust store load failure"));

      assertThrows(IOException.class, GcsHttpTransportFactoryTest::installProxyAuthenticator);

      // A transport that could not be created must not leave its credentials behind for the rest
      // of the process.
      assertThat(Authenticator.getDefault()).isSameInstanceAs(sentinelAuthenticator);
    }
  }

  @Test
  void createHttpTransport_trustStoreFailure_leavesTunnelingSchemesPropertyUnchanged() {
    System.setProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY, "Basic");

    try (MockedStatic<GoogleUtils> mockedGoogleUtils = mockStatic(GoogleUtils.class)) {
      mockedGoogleUtils
          .when(GoogleUtils::getCertificateTrustStore)
          .thenThrow(new GeneralSecurityException("Simulated trust store load failure"));

      assertThrows(IOException.class, GcsHttpTransportFactoryTest::installProxyAuthenticator);

      // Failing to build a transport must not relax the JVM's proxy tunneling restrictions.
      assertThat(System.getProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY)).isEqualTo("Basic");
    }
  }

  @Test
  void createHttpTransport_nullReadTimeout_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () ->
            GcsHttpTransportFactory.createHttpTransport(
                null, null, null, null, TrustStoreSource.GOOGLE_BUNDLED));
  }

  @Test
  void createHttpTransport_zeroReadTimeout_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GcsHttpTransportFactory.createHttpTransport(
                null, null, null, Duration.ZERO, TrustStoreSource.GOOGLE_BUNDLED));
  }

  @Test
  void createHttpTransport_negativeReadTimeout_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GcsHttpTransportFactory.createHttpTransport(
                null, null, null, Duration.ofMillis(-1), TrustStoreSource.GOOGLE_BUNDLED));
  }

  @Test
  void createHttpTransport_subMillisecondReadTimeout_throwsIllegalArgumentException() {
    // Rounding this down to zero would silently leave reads unbounded.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GcsHttpTransportFactory.createHttpTransport(
                null, null, null, Duration.ofNanos(500), TrustStoreSource.GOOGLE_BUNDLED));
  }

  @Test
  void createNetHttpTransportBuilder_readTimeout_appliesTimeoutToSocketFactory()
      throws IOException, GeneralSecurityException {
    NetHttpTransport.Builder builder =
        GcsHttpTransportFactory.createNetHttpTransportBuilder(
            null, Duration.ofSeconds(7), TrustStoreSource.GOOGLE_BUNDLED);

    assertThat(((KeepAliveSslSocketFactory) builder.getSslSocketFactory()).getReadTimeoutMillis())
        .isEqualTo(7000);
  }

  @Test
  void createHttpTransport_readTimeoutOverflowingInt_throwsIllegalArgumentException() {
    Duration readTimeout = Duration.ofMillis(Integer.MAX_VALUE + 1L);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            GcsHttpTransportFactory.createHttpTransport(
                null, null, null, readTimeout, TrustStoreSource.GOOGLE_BUNDLED));
  }

  @Test
  void createNetHttpTransport_proxyAuthWithoutProxyUri_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GcsHttpTransportFactory.createNetHttpTransport(
                null,
                new PasswordAuthentication("u", "p".toCharArray()),
                READ_TIMEOUT,
                TrustStoreSource.GOOGLE_BUNDLED));
  }

  @Test
  void keepAliveSslSocketFactory_nonPositiveTimeout_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new KeepAliveSslSocketFactory(new FakeSslSocketFactory(), 0));
  }

  @ParameterizedTest(name = "createSocket overload: {0}")
  @MethodSource("createSocketOverloads")
  void customSslSocketFactory_anyCreateSocketOverload_enablesKeepAlive(
      String overload, CreateSocket createSocket) throws IOException {
    KeepAliveSslSocketFactory socketFactory =
        new KeepAliveSslSocketFactory(new FakeSslSocketFactory(), 1234);

    try (Socket socket = createSocket.apply(socketFactory)) {
      assertThat(socket.getKeepAlive()).isTrue();
    }
  }

  @ParameterizedTest(name = "createSocket overload: {0}")
  @MethodSource("createSocketOverloads")
  void customSslSocketFactory_anyCreateSocketOverload_appliesReadTimeout(
      String overload, CreateSocket createSocket) throws IOException {
    KeepAliveSslSocketFactory socketFactory =
        new KeepAliveSslSocketFactory(new FakeSslSocketFactory(), 1234);

    try (Socket socket = createSocket.apply(socketFactory)) {
      assertThat(socket.getSoTimeout()).isEqualTo(1234);
    }
  }

  /**
   * Supplies every {@code createSocket} overload of {@link SSLSocketFactory}, so that socket
   * customization cannot silently regress on an overload that no caller happens to exercise today.
   */
  private static Stream<Arguments> createSocketOverloads() throws IOException {
    InetAddress address = InetAddress.getByName("10.0.0.1");
    return Stream.of(
        Arguments.of("()", (CreateSocket) SSLSocketFactory::createSocket),
        Arguments.of(
            "(String, int)",
            (CreateSocket) factory -> factory.createSocket("host.example.com", 443)),
        Arguments.of(
            "(String, int, InetAddress, int)",
            (CreateSocket) factory -> factory.createSocket("host.example.com", 443, address, 0)),
        Arguments.of(
            "(InetAddress, int)", (CreateSocket) factory -> factory.createSocket(address, 443)),
        Arguments.of(
            "(InetAddress, int, InetAddress, int)",
            (CreateSocket) factory -> factory.createSocket(address, 443, address, 0)),
        Arguments.of(
            "(Socket, String, int, boolean)",
            (CreateSocket)
                factory -> factory.createSocket(new Socket(), "host.example.com", 443, false)),
        Arguments.of(
            "(Socket, InputStream, boolean)",
            (CreateSocket)
                factory ->
                    factory.createSocket(
                        new Socket(), new ByteArrayInputStream(new byte[0]), false)));
  }

  /** Invokes one of the {@code createSocket} overloads of an {@link SSLSocketFactory}. */
  @FunctionalInterface
  private interface CreateSocket {

    Socket apply(SSLSocketFactory socketFactory) throws IOException;
  }

  @Test
  void customSslSocketFactory_nullSocketFromWrappedFactory_returnsNull() throws IOException {
    FakeSslSocketFactory wrapped =
        new FakeSslSocketFactory() {
          @Override
          public Socket createSocket() {
            return null;
          }
        };
    KeepAliveSslSocketFactory socketFactory = new KeepAliveSslSocketFactory(wrapped, 1234);

    assertThat(socketFactory.createSocket()).isNull();
  }

  @Test
  void customSslSocketFactory_customizationFails_propagatesFailure() {
    KeepAliveSslSocketFactory socketFactory =
        new KeepAliveSslSocketFactory(new FailingSslSocketFactory(), 1234);

    assertThrows(SocketException.class, socketFactory::createSocket);
  }

  @Test
  void customSslSocketFactory_customizationFails_closesCreatedSocket() {
    FailingSslSocketFactory wrapped = new FailingSslSocketFactory();
    KeepAliveSslSocketFactory socketFactory = new KeepAliveSslSocketFactory(wrapped, 1234);

    assertThrows(SocketException.class, socketFactory::createSocket);

    assertThat(wrapped.lastCreatedSocket.isClosed()).isTrue();
  }

  @Test
  void
      customSslSocketFactory_wrappingSocketWithAutoCloseFalseAndCustomizationFails_keepsSocketOpen()
          throws IOException {
    KeepAliveSslSocketFactory socketFactory =
        new KeepAliveSslSocketFactory(new FakeSslSocketFactory(), 1234);
    try (Socket callerOwnedSocket =
        new Socket() {
          @Override
          public synchronized void setSoTimeout(int timeout) throws SocketException {
            throw new SocketException("Socket is no longer usable");
          }
        }) {
      assertThrows(
          SocketException.class,
          () ->
              socketFactory.createSocket(
                  callerOwnedSocket, "host.example.com", 443, /* autoClose= */ false));

      assertThat(callerOwnedSocket.isClosed()).isFalse();
    }
  }

  @Test
  void customSslSocketFactory_wrappingSocketWithAutoCloseTrueAndCustomizationFails_closesSocket()
      throws IOException {
    KeepAliveSslSocketFactory socketFactory =
        new KeepAliveSslSocketFactory(new FakeSslSocketFactory(), 1234);
    try (Socket wrappedSocket =
        new Socket() {
          @Override
          public synchronized void setSoTimeout(int timeout) throws SocketException {
            throw new SocketException("Socket is no longer usable");
          }
        }) {
      assertThrows(
          SocketException.class,
          () ->
              socketFactory.createSocket(
                  wrappedSocket, "host.example.com", 443, /* autoClose= */ true));

      assertThat(wrappedSocket.isClosed()).isTrue();
    }
  }

  @Test
  void customSslSocketFactory_customizationAndCloseBothFail_addsCloseFailureAsSuppressed() {
    IOException closeFailure = new IOException("Failed to close socket");
    FakeSslSocketFactory wrapped =
        new FakeSslSocketFactory() {
          @Override
          public Socket createSocket() {
            return new Socket() {
              @Override
              public synchronized void setSoTimeout(int timeout) throws SocketException {
                throw new SocketException("Socket is no longer usable");
              }

              @Override
              public synchronized void close() throws IOException {
                throw closeFailure;
              }
            };
          }
        };
    KeepAliveSslSocketFactory socketFactory = new KeepAliveSslSocketFactory(wrapped, 1234);

    SocketException thrown = assertThrows(SocketException.class, socketFactory::createSocket);

    assertThat(thrown.getSuppressed()).asList().containsExactly(closeFailure);
  }

  @Test
  void customSslSocketFactory_getDefaultCipherSuites_delegatesToWrappedFactory() {
    FakeSslSocketFactory wrapped = new FakeSslSocketFactory();
    KeepAliveSslSocketFactory socketFactory = new KeepAliveSslSocketFactory(wrapped, 1234);

    assertThat(socketFactory.getDefaultCipherSuites()).isEqualTo(wrapped.getDefaultCipherSuites());
  }

  @Test
  void customSslSocketFactory_getSupportedCipherSuites_delegatesToWrappedFactory() {
    FakeSslSocketFactory wrapped = new FakeSslSocketFactory();
    KeepAliveSslSocketFactory socketFactory = new KeepAliveSslSocketFactory(wrapped, 1234);

    assertThat(socketFactory.getSupportedCipherSuites())
        .isEqualTo(wrapped.getSupportedCipherSuites());
  }

  /**
   * An {@link SSLSocketFactory} that hands out unconnected plain sockets, so that socket
   * customization can be asserted without opening a TLS connection.
   */
  private static class FakeSslSocketFactory extends SSLSocketFactory {

    private static final String[] DEFAULT_CIPHER_SUITES = {"FAKE_DEFAULT_CIPHER_SUITE"};
    private static final String[] SUPPORTED_CIPHER_SUITES = {"FAKE_SUPPORTED_CIPHER_SUITE"};

    @Override
    public String[] getDefaultCipherSuites() {
      return DEFAULT_CIPHER_SUITES.clone();
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return SUPPORTED_CIPHER_SUITES.clone();
    }

    @Override
    public Socket createSocket() {
      return new Socket();
    }

    @Override
    public Socket createSocket(Socket s, InputStream consumed, boolean autoClose) {
      return s;
    }

    @Override
    public Socket createSocket(Socket s, String host, int port, boolean autoClose) {
      return s;
    }

    @Override
    public Socket createSocket(String host, int port) {
      return new Socket();
    }

    @Override
    public Socket createSocket(InetAddress address, int port) {
      return new Socket();
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress clientAddress, int clientPort) {
      return new Socket();
    }

    @Override
    public Socket createSocket(
        InetAddress address, int port, InetAddress clientAddress, int clientPort) {
      return new Socket();
    }
  }

  /**
   * An {@link SSLSocketFactory} whose sockets reject customization, standing in for a socket that
   * dies between creation and configuration.
   */
  private static final class FailingSslSocketFactory extends FakeSslSocketFactory {

    private Socket lastCreatedSocket;

    @Override
    public Socket createSocket() {
      lastCreatedSocket =
          new Socket() {
            @Override
            public synchronized void setSoTimeout(int timeout) throws SocketException {
              throw new SocketException("Socket is no longer usable");
            }
          };
      return lastCreatedSocket;
    }
  }
}
