package org.mqtt4j.hato.examples;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.MqttBroker;

import static java.nio.charset.StandardCharsets.UTF_8;

/// Renews the key and certificate of an embedded broker's TLS listener while the broker runs.
/// The listener's `SSLContext` holds a key manager of the application's own, which presents the
/// key and certificate of the key store that the application gave it last: new connections get
/// the renewed certificate, and the connections that are already open go on.
public final class RenewBrokerCertificate {

    private static final String ALIAS = "broker";
    private static final String PASSWORD = "example-only";

    void main() throws IOException, GeneralSecurityException, InterruptedException {
        // Two key stores, each with a new key and a certificate for localhost: the first one, and
        // the one that renews it. An application gets them from its CA or ACME client, and reads
        // its key store file again when the file changes.
        KeyStore first = newKeyStore();
        KeyStore renewed = newKeyStore();

        var keys = new RenewableKeyManager(first, PASSWORD.toCharArray());
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(new KeyManager[] {keys}, null, null);
        var address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        try (
                MqttBroker broker = MqttBroker.builder().listenTls(address, context).start();
                // Each client trusts only one of the two certificates, so it can connect only
                // while the broker presents that one. The clients of a real broker trust the CA
                // that issues the broker's certificates instead.
                MqttClient sensor = client(broker, first);
                MqttClient monitor = client(broker, renewed)
        ) {
            sensor.connect().join();
            System.out.println("sensor connected with the broker's first certificate");

            keys.renew(renewed, PASSWORD.toCharArray());
            System.out.println("Renewed the broker's key and certificate");
            monitor.connect().join();
            System.out.println("monitor connected with the renewed certificate");

            // The renewal changed nothing for the connection that was open: the sensor publishes
            // over the connection that it made with the first certificate.
            var received = new CountDownLatch(1);
            Subscription readings = monitor.subscribe(
                    Subscribe.of(new TopicFilter("sensors/+/temperature"), Qos.AT_LEAST_ONCE),
                    delivery -> {
                        MqttMessage message = delivery.message();
                        System.out.println("monitor received " + message.topic() + " = "
                                + new String(message.payload(), UTF_8)
                                + " from the sensor's first connection");
                        received.countDown();
                    }
            );
            readings.established().join();
            sensor.publish(MqttMessage.of(
                    new Topic("sensors/kitchen/temperature"),
                    "21.5".getBytes(UTF_8),
                    Qos.AT_LEAST_ONCE,
                    false
            )).join();
            received.await();

            sensor.disconnect().join();
            monitor.disconnect().join();
        }
    }

    /// A client of the broker's TLS listener that trusts only the certificate in `store`.
    private static MqttClient client(MqttBroker broker, KeyStore store)
            throws IOException, GeneralSecurityException {
        KeyStore trusted = KeyStore.getInstance("PKCS12");
        trusted.load(null, null);
        trusted.setCertificateEntry(ALIAS, store.getCertificate(ALIAS));
        TrustManagerFactory trust = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm()
        );
        trust.init(trusted);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, trust.getTrustManagers(), null);
        return MqttClient.builder()
                .server(URI.create("mqtts://localhost:" + broker.tlsPort()))
                .sslContext(context)
                .build();
    }

    /// Stands in for the CA or ACME client that delivers the broker's keys: the JDK's `keytool`
    /// creates a new key and a self-signed certificate for localhost in a key store file, which
    /// this method reads and deletes.
    private static KeyStore newKeyStore()
            throws IOException, GeneralSecurityException, InterruptedException {
        Path directory = Files.createTempDirectory("broker-keys");
        Path file = directory.resolve("broker.p12");
        try {
            Process keytool = new ProcessBuilder(
                    Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                    "-genkeypair",
                    "-alias",
                    ALIAS,
                    "-keyalg",
                    "EC",
                    "-dname",
                    "CN=localhost",
                    "-ext",
                    "san=dns:localhost",
                    "-keystore",
                    file.toString(),
                    "-storepass",
                    PASSWORD
            ).inheritIO().start();
            int exitCode = keytool.waitFor();
            if (exitCode != 0) {
                throw new IOException("keytool failed with exit code " + exitCode);
            }
            return KeyStore.getInstance(file.toFile(), PASSWORD.toCharArray());
        } finally {
            Files.deleteIfExists(file);
            Files.delete(directory);
        }
    }

    /// A key manager whose key store the application replaces while the broker runs. It passes
    /// the requests for the broker's key to the JDK's key manager for the latest key store, so
    /// the JDK still chooses the key as it does for any server.
    ///
    /// The broker takes a TLS engine for each connection, and a handshake on an engine asks
    /// [#chooseEngineServerAlias(String, Principal[], SSLEngine)] which key to present. So this
    /// manager extends [X509ExtendedKeyManager] and implements that method: with a key manager
    /// that leaves it out, or one that implements only [javax.net.ssl.X509KeyManager], every
    /// handshake fails.
    private static final class RenewableKeyManager extends X509ExtendedKeyManager {

        // A handshake chooses an alias, and then asks for the key and the certificate chain of
        // that alias. So that a renewal in between cannot pair the key of one key store with the
        // chain of another, each alias that this manager gives out starts with the number of its
        // key store, and the manager of the key store before the latest one is kept for the
        // handshakes that chose from it. Only that one is kept: a handshake that two renewals
        // overtake finds no key, and fails.
        private final AtomicReference<Managers> managers;

        RenewableKeyManager(KeyStore store, char[] password) throws GeneralSecurityException {
            managers = new AtomicReference<>(new Managers(0, jdkKeyManager(store, password), null));
        }

        /// Presents the key and certificate chain of `store` in the handshakes that begin from
        /// now on. A connection that is open goes on as it is, and a client that resumes the TLS
        /// session of an earlier connection gets no certificate in its handshake.
        void renew(KeyStore store, char[] password) throws GeneralSecurityException {
            X509ExtendedKeyManager renewed = jdkKeyManager(store, password);
            managers.updateAndGet(current -> current.renewedWith(renewed));
        }

        @Override
        public @Nullable String chooseEngineServerAlias(
                String keyType,
                Principal @Nullable [] issuers,
                @Nullable SSLEngine engine
        ) {
            Managers current = managers.get();
            String alias = current.latest().chooseEngineServerAlias(keyType, issuers, engine);
            return alias == null ? null : current.numbered(alias);
        }

        @Override
        public @Nullable String chooseServerAlias(
                String keyType,
                Principal @Nullable [] issuers,
                @Nullable Socket socket
        ) {
            Managers current = managers.get();
            String alias = current.latest().chooseServerAlias(keyType, issuers, socket);
            return alias == null ? null : current.numbered(alias);
        }

        @Override
        public String @Nullable [] getServerAliases(
                String keyType,
                Principal @Nullable [] issuers
        ) {
            Managers current = managers.get();
            String[] aliases = current.latest().getServerAliases(keyType, issuers);
            if (aliases == null) {
                return null;
            }
            return Arrays.stream(aliases).map(current::numbered).toArray(String[]::new);
        }

        @Override
        public @Nullable PrivateKey getPrivateKey(String alias) {
            X509ExtendedKeyManager owner = managers.get().managerOf(alias);
            return owner == null ? null : owner.getPrivateKey(unnumbered(alias));
        }

        @Override
        public X509Certificate @Nullable [] getCertificateChain(String alias) {
            X509ExtendedKeyManager owner = managers.get().managerOf(alias);
            return owner == null ? null : owner.getCertificateChain(unnumbered(alias));
        }

        // The broker never acts as a TLS client, so this manager offers no client keys.
        @Override
        public String @Nullable [] getClientAliases(
                String keyType,
                Principal @Nullable [] issuers
        ) {
            return null;
        }

        @Override
        public @Nullable String chooseClientAlias(
                String[] keyTypes,
                Principal @Nullable [] issuers,
                @Nullable Socket socket
        ) {
            return null;
        }

        private static X509ExtendedKeyManager jdkKeyManager(KeyStore store, char[] password)
                throws GeneralSecurityException {
            KeyManagerFactory factory = KeyManagerFactory.getInstance(
                    KeyManagerFactory.getDefaultAlgorithm()
            );
            factory.init(store, password);
            return (X509ExtendedKeyManager) factory.getKeyManagers()[0];
        }

        private static String unnumbered(String alias) {
            return alias.substring(alias.indexOf(':') + 1);
        }

        /// The JDK's key managers of the latest key store, whose number is `latestNumber`, and of
        /// the one before it.
        private record Managers(
                long latestNumber,
                X509ExtendedKeyManager latest,
                @Nullable X509ExtendedKeyManager previous
        ) {

            Managers renewedWith(X509ExtendedKeyManager renewed) {
                return new Managers(latestNumber + 1, renewed, latest);
            }

            String numbered(String alias) {
                return latestNumber + ":" + alias;
            }

            /// The manager that gave out a numbered alias, while it is kept.
            @Nullable X509ExtendedKeyManager managerOf(String alias) {
                if (alias.startsWith(latestNumber + ":")) {
                    return latest;
                }
                return alias.startsWith((latestNumber - 1) + ":") ? previous : null;
            }
        }
    }
}
