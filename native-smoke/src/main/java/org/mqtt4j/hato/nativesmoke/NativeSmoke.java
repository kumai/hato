package org.mqtt4j.hato.nativesmoke;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.mqtt4j.hato.AckMode;
import org.mqtt4j.hato.DeliveryOptions;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttClientBuilder;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.PullSubscription;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReceiveResult;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.UserProperty;
import org.mqtt4j.hato.broker.MqttBroker;

import static java.util.concurrent.TimeUnit.SECONDS;

/// Verifies that the packaged client and broker exchange messages at every QoS level in a native
/// image, through a handler, through a striped handler, and through receiving on the caller's
/// thread, in MQTT 3.1.1 and in MQTT 5.0 with message properties, over TCP, TLS, WebSocket, and
/// WebSocket over TLS. A WebSocket relay in front of the broker serves WebSocket, and a TLS relay
/// in front of the broker or of the WebSocket relay presents the repository's test server
/// identity.
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
                var tls = new TlsRelay(broker.port(), serverContext(identities));
                var webSocket = new WebSocketRelay(broker.port());
                var secureWebSocket = new TlsRelay(webSocket.port(), serverContext(identities))) {
            for (MqttVersion version : MqttVersion.values()) {
                roundTrip(
                        "TCP " + version,
                        version,
                        MqttClient.builder()
                                .server(URI.create("mqtt://localhost:" + broker.port()))
                                .version(version)
                );
                roundTrip(
                        "TLS " + version,
                        version,
                        MqttClient.builder()
                                .server(URI.create("mqtts://localhost:" + tls.port()))
                                .version(version)
                                .sslContext(clientContext(identities))
                );
                roundTrip(
                        "WS " + version,
                        version,
                        MqttClient.builder()
                                .server(URI.create("ws://localhost:" + webSocket.port() + "/mqtt"))
                                .version(version)
                );
                roundTrip(
                        "WSS " + version,
                        version,
                        MqttClient.builder()
                                .server(URI.create(
                                        "wss://localhost:" + secureWebSocket.port() + "/mqtt"
                                ))
                                .version(version)
                                .sslContext(clientContext(identities))
                );
            }
        }
        System.out.println(
                "hato native smoke: QoS 0/1/2 through handler, striped handler, and receive"
                        + " in MQTT 3.1.1, and in MQTT 5.0 with properties, over TCP, TLS, WS,"
                        + " and WSS OK"
        );
    }

    private static void roundTrip(
            String route,
            MqttVersion version,
            MqttClientBuilder builder
    ) throws Exception {
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient client = builder
                .connectionListener(ended::add)
                .build()) {
            if (client.connect().get(5, SECONDS).sessionPresent()) {
                throw new AssertionError(route + " clean connection resumed a session");
            }
            var handled = new LinkedBlockingQueue<MqttMessage>();
            Subscription push = client.subscribe(
                    Subscribe.of(new TopicFilter("smoke/push"), Qos.EXACTLY_ONCE),
                    delivery -> handled.add(delivery.message())
            );
            PullSubscription pull = client.subscribe(
                    Subscribe.of(new TopicFilter("smoke/pull"), Qos.EXACTLY_ONCE)
                            .withAckMode(AckMode.MANUAL)
            );
            var striped = new LinkedBlockingQueue<MqttMessage>();
            Subscription stripes = client.subscribe(
                    Subscribe.of(new TopicFilter("smoke/striped"), Qos.EXACTLY_ONCE),
                    DeliveryOptions.striped(2),
                    delivery -> striped.add(delivery.message())
            );
            push.established().get(5, SECONDS);
            pull.established().get(5, SECONDS);
            stripes.established().get(5, SECONDS);

            // The subscriptions request QoS 2, so every level arrives with its own QoS, which
            // the comparison of messages includes.
            for (Qos qos : Qos.values()) {
                MqttMessage pushed = message(version, "smoke/push", (byte) 42, qos);
                client.publish(pushed).get(5, SECONDS);
                if (!pushed.equals(handled.poll(5, SECONDS))) {
                    throw new AssertionError(
                            route + " " + qos + " delivery to a handler changed the message"
                    );
                }

                MqttMessage pulled = message(version, "smoke/pull", (byte) 7, qos);
                client.publish(pulled).get(5, SECONDS);
                if (!(pull.receive(Duration.ofSeconds(5))
                        instanceof ReceiveResult.Received(var delivery))
                        || !pulled.equals(delivery.message())) {
                    throw new AssertionError(
                            route + " " + qos + " delivery to a receiver changed the message"
                    );
                }
                delivery.ack();

                MqttMessage stripedMessage = message(version, "smoke/striped", (byte) 9, qos);
                client.publish(stripedMessage).get(5, SECONDS);
                if (!stripedMessage.equals(striped.poll(5, SECONDS))) {
                    throw new AssertionError(
                            route + " " + qos + " delivery to a striped handler changed the"
                                    + " message"
                    );
                }
            }

            push.unsubscribe().get(5, SECONDS);
            pull.unsubscribe().get(5, SECONDS);
            stripes.unsubscribe().get(5, SECONDS);
            if (!(pull.receive(Duration.ZERO) instanceof ReceiveResult.Ended)) {
                throw new AssertionError(route + " removed subscription kept receiving");
            }
            client.disconnect().get(5, SECONDS);
            // Removal and disconnect also complete without a connection, so only the listener
            // shows that the connection lasted until the requested disconnect.
            if (!Disconnection.of(null, true, 1).equals(ended.poll(5, SECONDS))) {
                throw new AssertionError(route + " connection ended before the disconnect");
            }
        }
    }

    // A message to publish on `topic`. An MQTT 5.0 message also carries each property that the
    // broker forwards unchanged, so the comparison on arrival covers them, User Properties and
    // their order included. The message expiry is left out, because the broker forwards only what
    // is left of it.
    private static MqttMessage message(MqttVersion version, String topic, byte payload, Qos qos) {
        var message = MqttMessage.of(new Topic(topic), new byte[] {payload}, qos, false);
        return switch (version) {
            case V3_1_1 -> message;
            case V5 -> message
                    .withPayloadUtf8(true)
                    .withContentType("text/plain")
                    .withResponseTopic(new Topic(topic + "/replies"))
                    .withCorrelationData(new byte[] {1, 2, 3})
                    .withUserProperties(List.of(
                            new UserProperty("route", "native"),
                            new UserProperty("hop", "1"),
                            new UserProperty("route", "smoke")
                    ));
        };
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
