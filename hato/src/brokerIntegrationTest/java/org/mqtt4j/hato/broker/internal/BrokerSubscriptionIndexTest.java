package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.Authenticator;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.Properties;
import org.mqtt4j.hato.internal.protocol.Property;
import org.mqtt4j.hato.internal.protocol.PropertyId;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.internal.protocol.Properties.EMPTY;

/// The index of subscriptions that routing looks a publication up in: that a publication finds
/// the sessions whose filters select it, offline ones included, and no other, and that a
/// subscription leaves the index however it ends.
class BrokerSubscriptionIndexTest {

    private final BrokerDeadlineTest.ManualTimer timer = new BrokerDeadlineTest.ManualTimer();

    @Test
    void a_publication_reaches_the_offline_sessions_whose_filters_select_it_and_no_other()
            throws Exception {
        try (BrokerServer sut = broker(BrokerServer.builder());
                RawSession fleet = RawSession.connected(sut.port(), "fleet");
                RawSession publisher = RawSession.connected(sut.port(), "publisher")) {
            // Arrange: a hundred devices that subscribed to their own commands and went away,
            // and a fleet controller that watches them all.
            for (int i = 0; i < 100; i++) {
                try (RawSession device = lasting(sut, "device-" + i, true, false)) {
                    device.subscribe("devices/" + i + "/command", AT_LEAST_ONCE);
                }
            }
            fleet.subscribe("devices/+/command", AT_LEAST_ONCE);

            // Act
            publisher.publish("devices/7/command", "go", AT_LEAST_ONCE, 1);

            // Assert
            assertThat(fleet.expect(Packet.Publish.class).topic())
                    .isEqualTo(new Topic("devices/7/command"));
            assertThat(sut.indexedFor(new Topic("devices/7/command")))
                    .containsExactlyInAnyOrder("device-7", "fleet");
            try (RawSession device = lasting(sut, "device-7", false, true)) {
                assertThat(device.expect(Packet.Publish.class).topic())
                        .isEqualTo(new Topic("devices/7/command"));
            }
            try (RawSession device = lasting(sut, "device-8", false, true)) {
                device.ping();
            }
        }
    }

    @Test
    void a_subscribe_of_filters_whose_hash_codes_collide_is_answered_at_once() throws Exception {
        try (BrokerServer sut = broker(BrokerServer.builder());
                RawSession subscriber = RawSession.connected(sut.port(), "subscriber")) {
            // Arrange: 2,048 filters with one hash code, each of eleven pairs of "Aa" or "BB",
            // which hash alike.
            var subscriptions = new ArrayList<Packet.Subscription>();
            for (int bits = 0; bits < 2048; bits++) {
                var pattern = new StringBuilder();
                for (int pair = 0; pair < 11; pair++) {
                    pattern.append(((bits >> pair) & 1) == 0 ? "Aa" : "BB");
                }
                subscriptions.add(RawSession.subscription(pattern.toString(), AT_MOST_ONCE));
            }

            // Act
            subscriber.send(new Packet.Subscribe(1, subscriptions, EMPTY));

            // Assert: the SUBACK comes within the time that the session waits for a packet,
            // where subscriptions kept by their filters' records took ten seconds for these.
            assertThat(subscriber.expect(Packet.SubAck.class).reasonCodes())
                    .hasSize(2048)
                    .containsOnly(ReasonCode.SUCCESS);
        }
    }

    @Test
    void unsubscribing_takes_the_subscription_out_of_the_index() throws Exception {
        try (BrokerServer sut = broker(BrokerServer.builder());
                RawSession client = RawSession.connected(sut.port(), "client")) {
            // Arrange
            client.subscribe("sensors/+", AT_LEAST_ONCE);
            client.subscribe("sensors/#", AT_LEAST_ONCE);
            var topic = new Topic("sensors/temperature");
            assertThat(sut.indexedFor(topic)).containsExactly("client");

            // Act
            client.send(new Packet.Unsubscribe(
                    2,
                    List.of(new TopicFilter("sensors/+"), new TopicFilter("sensors/#")),
                    EMPTY
            ));

            // Assert
            assertThat(client.expect(Packet.UnsubAck.class).packetId()).isEqualTo(2);
            await().until(() -> sut.indexedFor(topic).isEmpty());
        }
    }

    @Test
    void a_session_that_ends_with_its_connection_leaves_the_index() throws Exception {
        try (BrokerServer sut = broker(BrokerServer.builder())) {
            // Arrange
            var topic = new Topic("sensors/temperature");
            RawSession client = RawSession.connected(sut.port(), "client");
            client.subscribe("sensors/#", AT_LEAST_ONCE);
            assertThat(sut.indexedFor(topic)).containsExactly("client");

            // Act
            client.close();

            // Assert
            await().until(() -> sut.indexedFor(topic).isEmpty());
        }
    }

    @Test
    @SuppressWarnings("try") // The explicit close is what the test is about.
    void a_closed_broker_keeps_no_session_in_the_index() throws Exception {
        try (BrokerServer sut = broker(BrokerServer.builder());
                RawSession client = RawSession.connected(sut.port(), "client")) {
            // Arrange: a session that the broker's end closes with its subscription.
            var topic = new Topic("sensors/temperature");
            client.subscribe("sensors/#", AT_LEAST_ONCE);
            assertThat(sut.indexedFor(topic)).containsExactly("client");

            // Act
            sut.close();

            // Assert
            assertThat(sut.indexedFor(topic)).isEmpty();
        }
    }

    @Test
    void a_session_that_expires_leaves_the_index() throws Exception {
        try (BrokerServer sut = broker(BrokerServer.builder().scheduler(timer));
                RawSession watcher = RawSession.connected(sut.port(), "watcher")) {
            // Arrange: a session that outlives its connection by ten seconds. Its Will tells
            // that the connection has left the session, whose expiry then counts.
            watcher.subscribe("left/#", AT_MOST_ONCE);
            var topic = new Topic("sensors/temperature");
            try (RawSession client = RawSession.open(sut.port(), MqttVersion.V5)) {
                client.send(connectWithWill("client", 10));
                client.expect(Packet.ConnAck.class);
                client.subscribe("sensors/#", AT_LEAST_ONCE);
            }
            assertThat(watcher.expect(Packet.Publish.class).topic())
                    .isEqualTo(new Topic("left/client"));
            assertThat(sut.indexedFor(topic)).containsExactly("client");

            // Act
            timer.clock.advance(Duration.ofSeconds(10));

            // Assert
            await().until(() -> sut.indexedFor(topic).isEmpty());
        }
    }

    @Test
    void a_client_that_starts_clean_leaves_none_of_its_former_subscriptions_in_the_index()
            throws Exception {
        try (BrokerServer sut = broker(BrokerServer.builder())) {
            // Arrange
            var former = new Topic("former/topic");
            try (RawSession client = lasting(sut, "client", true, false)) {
                client.subscribe("former/#", AT_LEAST_ONCE);
            }
            assertThat(sut.indexedFor(former)).containsExactly("client");

            // Act
            try (RawSession client = lasting(sut, "client", true, false)) {
                client.subscribe("latter/#", AT_LEAST_ONCE);

                // Assert
                assertThat(sut.indexedFor(former)).isEmpty();
                assertThat(sut.indexedFor(new Topic("latter/topic"))).containsExactly("client");
            }
        }
    }

    @Test
    void a_resumed_session_takes_its_subscriptions_outside_the_new_scope_out_of_the_index()
            throws Exception {
        // Arrange: a first connection that may subscribe to anything, and a second that may
        // subscribe only to its own commands.
        var scope = new AtomicReference<>(Authenticator.Scope.all());
        try (BrokerServer sut = broker(BrokerServer.builder()
                .authenticator(_ -> Authenticator.Decision.accept(scope.get())))) {
            try (RawSession client = lasting(sut, "client", true, false)) {
                client.subscribe("commands/client", AT_LEAST_ONCE);
                client.subscribe("others/#", AT_LEAST_ONCE);
            }
            scope.set(Authenticator.Scope.of(
                    List.of(new TopicFilter("#")),
                    List.of(new TopicFilter("commands/client"))
            ));

            // Act
            try (RawSession client = lasting(sut, "client", false, true)) {
                client.ping();

                // Assert
                assertThat(sut.indexedFor(new Topic("others/x"))).isEmpty();
                assertThat(sut.indexedFor(new Topic("commands/client"))).containsExactly("client");
            }
        }
    }

    private static BrokerServer broker(BrokerServer.Builder builder) throws IOException {
        return builder.connectTimeout(Duration.ofSeconds(5)).start(loopback());
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }

    /// An MQTT 5.0 connection of `clientId` whose session outlives it by ten minutes, which the
    /// broker accepted with a session of that client or without one, as `present` says.
    private static RawSession lasting(
            BrokerServer broker,
            String clientId,
            boolean cleanStart,
            boolean present
    ) throws Exception {
        return RawSession.connectedV5(broker.port(), clientId, cleanStart, 600, present);
    }

    /// An MQTT 5.0 CONNECT of `clientId` whose session outlives the connection by `expiry`
    /// seconds, with a Will to left/`clientId`, which goes out as the connection leaves the
    /// session.
    private static Packet.Connect connectWithWill(String clientId, long expiry) {
        var interval = new Property.FourByteValue(PropertyId.SESSION_EXPIRY_INTERVAL, expiry);
        return new Packet.Connect(
                clientId,
                MqttVersion.V5,
                true,
                0,
                new Packet.Will(
                        new Topic("left/" + clientId),
                        "gone".getBytes(UTF_8),
                        AT_MOST_ONCE,
                        false,
                        EMPTY
                ),
                null,
                null,
                new Properties(List.of(interval))
        );
    }
}
