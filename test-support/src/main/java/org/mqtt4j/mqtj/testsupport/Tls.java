package org.mqtt4j.mqtj.testsupport;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.TrustManagerFactory;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// Repository-private TLS identities. All bundled keys and their password are public test data.
public final class Tls {

    private static final String PASSWORD = "mqtj-test-only";

    private Tls() {}

    /// Trusts only the test CA, without presenting a client certificate.
    public static SSLContext clientContext() throws IOException, GeneralSecurityException {
        return context(null);
    }

    /// Trusts the test CA and presents its signed client certificate when requested.
    public static SSLContext clientContextWithCertificate()
            throws IOException, GeneralSecurityException {
        return context("client");
    }

    /// Presents a certificate for `localhost` and `test.mqtj.invalid`, with no IP address SAN.
    public static SSLContext serverContext() throws IOException, GeneralSecurityException {
        return context("server");
    }

    /// Creates a loopback peer whose TLS settings and accepted sockets tests can inspect directly.
    public static SSLServerSocket serverSocket() throws IOException, GeneralSecurityException {
        var socket = (SSLServerSocket) serverContext().getServerSocketFactory().createServerSocket(
                0,
                50,
                InetAddress.getLoopbackAddress()
        );
        socket.setSoTimeout(5000);
        return socket;
    }

    private static SSLContext context(@Nullable String identity)
            throws IOException, GeneralSecurityException {
        KeyManager @Nullable [] keyManagers = null;
        if (identity != null) {
            KeyManagerFactory keys = KeyManagerFactory.getInstance(
                    KeyManagerFactory.getDefaultAlgorithm()
            );
            keys.init(keyStore(identity), PASSWORD.toCharArray());
            keyManagers = keys.getKeyManagers();
        }
        TrustManagerFactory trust = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm()
        );
        trust.init(keyStore("ca"));
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keyManagers, trust.getTrustManagers(), null);
        return context;
    }

    private static KeyStore keyStore(String identity) throws IOException, GeneralSecurityException {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream input = requireNonNull(
                Tls.class.getResourceAsStream("/tls/" + identity + ".p12")
        )) {
            store.load(input, PASSWORD.toCharArray());
        }
        return store;
    }
}
