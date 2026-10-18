package org.mqtt4j.hato.nativesmoke;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.mqtt4j.hato.AckMode;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttClientBuilder;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReceiveResult;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.MqttBroker;

import static java.util.concurrent.TimeUnit.SECONDS;

/// Verifies that the packaged client and broker exchange messages at every QoS level in a native
/// image, both through a handler and through receiving on the caller's thread, over TCP and over
/// TLS. For TLS, a relay in front of the broker presents the repository's test server identity.
/// The image reads the identities at run time from the directory given as the only argument,
/// `test-support/src/main/resources/tls` in this repository: embedding them would take
/// native-image resource metadata, which Hato itself does not need.
public final class NativeSmoke {

    // Like the identities themselves, their password is public test data.
    private static final String PASSWORD = "hato-test-only";

    private NativeSmoke() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "Pass the directory that holds the test identities ca.p12 and server.p12,"
                            + " test-support/src/main/resources/tls in this repository"
            );
        }
        Path identities = Path.of(args[0]);
        try (MqttBroker broker = MqttBroker.start();
                var relay = new TlsRelay(broker.port(), serverContext(identities))) {
            roundTrip(
                    "TCP",
                    MqttClient.builder()
                            .server(URI.create("mqtt://localhost:" + broker.port()))
            );
            roundTrip(
                    "TLS",
                    MqttClient.builder()
                            .server(relay.uri())
                            .sslContext(clientContext(identities))
            );
        }
        System.out.println(
                "hato native smoke: QoS 0/1/2 through handler and receive over TCP and TLS OK"
        );
    }

    private static void roundTrip(String transport, MqttClientBuilder builder) throws Exception {
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient client = builder
                .version(MqttVersion.V3_1_1)
                .connectionListener(ended::add)
                .build()) {
            if (client.connect().get(5, SECONDS).sessionPresent()) {
                throw new AssertionError(transport + " clean connection resumed a session");
            }
            var handled = new LinkedBlockingQueue<MqttMessage>();
            Subscription push = client.subscribe(
                    Subscribe.of(new TopicFilter("smoke/push"), Qos.EXACTLY_ONCE),
                    delivery -> handled.add(delivery.message())
            );
            Subscription pull = client.subscribe(
                    Subscribe.of(new TopicFilter("smoke/pull"), Qos.EXACTLY_ONCE)
                            .withAckMode(AckMode.MANUAL)
            );
            push.established().get(5, SECONDS);
            pull.established().get(5, SECONDS);

            // The subscriptions request QoS 2, so every level arrives with its own QoS, which
            // the comparison of messages includes.
            for (Qos qos : Qos.values()) {
                var pushed = MqttMessage.of(new Topic("smoke/push"), new byte[] {42}, qos, false);
                client.publish(pushed).get(5, SECONDS);
                if (!pushed.equals(handled.poll(5, SECONDS))) {
                    throw new AssertionError(
                            transport + " " + qos + " delivery to a handler changed the message"
                    );
                }

                var pulled = MqttMessage.of(new Topic("smoke/pull"), new byte[] {7}, qos, false);
                client.publish(pulled).get(5, SECONDS);
                if (!(pull.receive(Duration.ofSeconds(5))
                        instanceof ReceiveResult.Received(var delivery))
                        || !pulled.equals(delivery.message())) {
                    throw new AssertionError(
                            transport + " " + qos + " delivery to a receiver changed the message"
                    );
                }
                delivery.ack();
            }

            push.unsubscribe().get(5, SECONDS);
            pull.unsubscribe().get(5, SECONDS);
            if (!(pull.receive(Duration.ZERO) instanceof ReceiveResult.Ended)) {
                throw new AssertionError(transport + " removed subscription kept receiving");
            }
            client.disconnect().get(5, SECONDS);
            // Removal and disconnect also complete without a connection, so only the listener
            // shows that the connection lasted until the requested disconnect.
            if (!Disconnection.of(null, true, 1).equals(ended.poll(5, SECONDS))) {
                throw new AssertionError(transport + " connection ended before the disconnect");
            }
        }
    }

    private static SSLContext serverContext(Path identities)
            throws IOException, GeneralSecurityException {
        KeyManagerFactory keys = KeyManagerFactory.getInstance(
                KeyManagerFactory.getDefaultAlgorithm()
        );
        keys.init(keyStore(identities.resolve("server.p12")), PASSWORD.toCharArray());
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keys.getKeyManagers(), null, null);
        return context;
    }

    // Trusts only the test CA. The builder's default hostname verification also checks that the
    // relay's certificate names localhost.
    private static SSLContext clientContext(Path identities)
            throws IOException, GeneralSecurityException {
        TrustManagerFactory trust = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm()
        );
        trust.init(keyStore(identities.resolve("ca.p12")));
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, trust.getTrustManagers(), null);
        return context;
    }

    private static KeyStore keyStore(Path file) throws IOException, GeneralSecurityException {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(file)) {
            store.load(input, PASSWORD.toCharArray());
        }
        return store;
    }
}
