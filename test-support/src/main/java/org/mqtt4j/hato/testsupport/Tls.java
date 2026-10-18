package org.mqtt4j.hato.testsupport;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Base64;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.TrustManagerFactory;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// Repository-private TLS identities. All bundled keys and their password are public test data.
public final class Tls {

    private static final String PASSWORD = "hato-test-only";

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

    /// Presents a certificate for `localhost` and `test.hato.invalid`, with no IP address SAN.
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

    /// The PEM form of an identity's certificate, for a broker that cannot read PKCS12.
    static String certificatePem(String identity) throws IOException, GeneralSecurityException {
        Certificate certificate = requireNonNull(keyStore(identity).getCertificate(identity));
        return pem("CERTIFICATE", certificate.getEncoded());
    }

    /// The unencrypted PKCS #8 PEM form of an identity's private key. It is exported only at run
    /// time, so the repository keeps no PEM keys.
    static String privateKeyPem(String identity) throws IOException, GeneralSecurityException {
        Key key = requireNonNull(keyStore(identity).getKey(identity, PASSWORD.toCharArray()));
        return pem("PRIVATE KEY", key.getEncoded());
    }

    private static String pem(String label, byte[] encoded) {
        String body = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(encoded);
        return "-----BEGIN " + label + "-----\n" + body + "\n-----END " + label + "-----\n";
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
