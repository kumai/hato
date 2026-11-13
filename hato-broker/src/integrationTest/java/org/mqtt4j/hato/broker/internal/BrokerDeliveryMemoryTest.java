package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.Topic;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.broker.internal.RawSession.codec;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.EXACTLY_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

/// The memory that the deliveries that wait for the sessions of a broker may take together. In
/// these tests the broker's threads keep none of it to themselves, so each test knows to the byte
/// what fits.
class BrokerDeliveryMemoryTest {

    // The sessions whose dropped QoS 1 and 2 deliveries the application has heard of, one
    // entry for each delivery.
    private final List<String> reported = new CopyOnWriteArrayList<>();

    @Test
    void a_session_that_is_away_loses_its_oldest_delivery_when_the_memory_is_used_up()
            throws Exception {
        // Arrange: memory for two deliveries, and a session that waits for what comes while
        // its client is away.
        try (BrokerServer sut = broker(2 * roomOf("kept", "0001"));
                RawSession publisher = connected(sut.port(), "publisher")) {
            leaveSubscribed(sut, "away", "kept", AT_LEAST_ONCE);

            // Act
            for (String payload : List.of("0001", "0002", "0003")) {
                publisher.publish("kept", payload, AT_LEAST_ONCE, 7);
            }

            // Assert: the oldest made room for the newest, and the application heard of it.
            await().untilAsserted(() -> assertThat(reported).containsExactly("away"));
            try (RawSession back = connected(sut.port(), "away", false, true)) {
                assertThat(received(back, 2)).containsExactly("0002", "0003");
                back.ping();
            }
        }
    }

    @Test
    void a_delivery_that_the_waiting_ones_could_not_make_room_for_leaves_them_alone()
            throws Exception {
        // Arrange: memory for two small deliveries, which wait for a session that is away.
        try (BrokerServer sut = broker(2 * roomOf("kept", "0001"));
                RawSession publisher = connected(sut.port(), "publisher")) {
            leaveSubscribed(sut, "away", "kept", AT_LEAST_ONCE);
            publisher.publish("kept", "0001", AT_LEAST_ONCE, 7);
            publisher.publish("kept", "0002", AT_LEAST_ONCE, 7);

            // Act: a publication larger than the two together.
            publisher.publish("kept", "x".repeat(4096), AT_LEAST_ONCE, 7);

            // Assert: the session goes without it, and keeps what waited.
            await().untilAsserted(() -> assertThat(reported).containsExactly("away"));
            try (RawSession back = connected(sut.port(), "away", false, true)) {
                assertThat(received(back, 2)).containsExactly("0001", "0002");
                back.ping();
            }
        }
    }

    @Test
    void a_delivery_without_room_in_the_memory_does_not_wait_for_its_session() throws Exception {
        // Arrange: memory for less than one delivery.
        try (BrokerServer sut = broker(MemoryBudget.DELIVERY_BYTES);
                RawSession publisher = connected(sut.port(), "publisher")) {
            leaveSubscribed(sut, "away", "kept", AT_LEAST_ONCE);

            // Act
            publisher.publish("kept", "0001", AT_LEAST_ONCE, 7);

            // Assert: the application hears of it, and the session goes on without it.
            await().untilAsserted(() -> assertThat(reported).containsExactly("away"));
            try (RawSession back = connected(sut.port(), "away", false, true)) {
                back.ping();
            }
        }
    }

    @Test
    void a_session_that_keeps_up_needs_no_room() throws Exception {
        // Arrange: memory for less than one delivery.
        try (BrokerServer sut = broker(MemoryBudget.DELIVERY_BYTES);
                RawSession subscriber = connected(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("live", AT_LEAST_ONCE);

            // Act: deliveries of both kinds go straight out, and the client answers.
            var payloads = new ArrayList<String>();
            for (Qos qos : List.of(AT_MOST_ONCE, AT_LEAST_ONCE, AT_MOST_ONCE, AT_LEAST_ONCE)) {
                publisher.publish("live", "0001", qos, (qos == AT_MOST_ONCE) ? 0 : 7);
                payloads.addAll(received(subscriber, 1));
            }

            // Assert
            assertThat(payloads).hasSize(4);
            assertThat(reported).isEmpty();
        }
    }

    @Test
    void a_session_that_is_behind_takes_room_for_what_waits_for_its_window() throws Exception {
        // Arrange: memory for one delivery, and a window of one. A client takes a QoS 1
        // delivery, which needs no room while it is connected, and does not answer it yet.
        try (BrokerServer sut = settings(roomOf("slow", "0001")).maxInflight(1).start(loopback());
                RawSession subscriber = connected(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("slow", AT_LEAST_ONCE);
            publisher.publish("slow", "0001", AT_LEAST_ONCE, 7);

            // Act: three more come to wait behind it. The first takes the room, the QoS 0 one
            // finds none and no older one of its kind to go, and the last has the first make
            // room for it.
            publisher.publish("slow", "0002", AT_LEAST_ONCE, 7);
            publisher.publish("slow", "0003", AT_MOST_ONCE, 0);
            publisher.publish("slow", "0004", AT_LEAST_ONCE, 7);

            // Assert: the client gets the one that it had and the newest, and nothing else.
            await().untilAsserted(() -> assertThat(reported).containsExactly("subscriber"));
            assertThat(received(subscriber, 2)).containsExactly("0001", "0004");
            subscriber.ping();
        }
    }

    @Test
    void a_publication_for_many_sessions_takes_its_room_once() throws Exception {
        // Arrange: memory for one publication of four kilobytes and a delivery of it for each
        // of eight sessions, which is far less than eight times the publication.
        String payload = "x".repeat(4096);
        List<String> clients = List.of("a", "b", "c", "d", "e", "f", "g", "h");
        long room = wireSize("shared", payload)
                + (long) clients.size() * MemoryBudget.DELIVERY_BYTES;
        try (BrokerServer sut = broker(room);
                RawSession publisher = connected(sut.port(), "publisher")) {
            for (String client : clients) {
                leaveSubscribed(sut, client, "shared", AT_LEAST_ONCE);
            }

            // Act
            publisher.publish("shared", payload, AT_LEAST_ONCE, 7);

            // Assert: every session kept its delivery.
            for (String client : clients) {
                try (RawSession back = connected(sut.port(), client, false, true)) {
                    assertThat(received(back, 1)).containsExactly(payload);
                }
            }
            assertThat(reported).isEmpty();
        }
    }

    @Test
    void an_answered_delivery_gives_its_room_back() throws Exception {
        // Arrange: memory for one delivery, which waits for a session that is away.
        try (BrokerServer sut = broker(roomOf("one", "0001"));
                RawSession publisher = connected(sut.port(), "publisher")) {
            leaveSubscribed(sut, "first", "one", AT_LEAST_ONCE);
            leaveSubscribed(sut, "second", "two", AT_LEAST_ONCE);
            publisher.publish("one", "0001", AT_LEAST_ONCE, 7);

            // Act: the client comes back and answers the delivery, and the ping tells that the
            // broker has taken the answer.
            try (RawSession back = connected(sut.port(), "first", false, true)) {
                assertThat(received(back, 1)).containsExactly("0001");
                back.ping();
            }

            // Assert: a delivery that waits for the other session finds room.
            publisher.publish("two", "0002", AT_LEAST_ONCE, 7);
            try (RawSession back = connected(sut.port(), "second", false, true)) {
                assertThat(received(back, 1)).containsExactly("0002");
            }
            assertThat(reported).isEmpty();
        }
    }

    @Test
    void a_qos_two_delivery_gives_its_room_back_as_its_receiver_takes_it() throws Exception {
        // Arrange: memory for one delivery, which waits for a session that is away.
        try (BrokerServer sut = broker(roomOf("one", "0001"));
                RawSession publisher = connected(sut.port(), "publisher")) {
            leaveSubscribed(sut, "first", "one", EXACTLY_ONCE);
            leaveSubscribed(sut, "second", "two", AT_LEAST_ONCE);
            publisher.publish("one", "0001", EXACTLY_ONCE, 7);

            try (RawSession back = connected(sut.port(), "first", false, true)) {
                // Act: the receiver has the message, and its exchange goes on with PUBREL.
                Packet.Publish delivery = back.expect(Packet.Publish.class);
                back.send(new Packet.PubRec(delivery.packetId(), SUCCESS, EMPTY));
                assertThat(back.expect(Packet.PubRel.class).packetId())
                        .isEqualTo(delivery.packetId());

                // Assert: a delivery that waits for the other session finds room, although the
                // first exchange is not complete.
                publisher.publish("two", "0002", AT_LEAST_ONCE, 7);
            }
            try (RawSession back = connected(sut.port(), "second", false, true)) {
                assertThat(received(back, 1)).containsExactly("0002");
            }
            assertThat(reported).isEmpty();
        }
    }

    @Test
    void a_session_that_loses_its_connection_takes_room_for_what_it_has_yet_to_answer()
            throws Exception {
        // Arrange: memory for two deliveries. A client takes two, which need no room while it
        // is connected, answers neither, and goes; its Will tells that its session has let go
        // of the connection.
        try (BrokerServer sut = broker(2 * roomOf("one", "0001"));
                RawSession watcher = connected(sut.port(), "watcher");
                RawSession publisher = connected(sut.port(), "publisher")) {
            watcher.subscribe("gone", AT_MOST_ONCE);
            leaveSubscribed(sut, "second", "two", AT_LEAST_ONCE);
            try (RawSession leaving = RawSession.open(sut.port())) {
                leaving.send(new Packet.Connect(
                        "first",
                        MqttVersion.V3_1_1,
                        false,
                        0,
                        new Packet.Will(new Topic("gone"), new byte[0], AT_MOST_ONCE, false, EMPTY),
                        null,
                        null,
                        EMPTY
                ));
                leaving.expect(Packet.ConnAck.class);
                leaving.subscribe("one", AT_LEAST_ONCE);
                publisher.publish("one", "0001", AT_LEAST_ONCE, 7);
                publisher.publish("one", "0002", AT_LEAST_ONCE, 7);
                leaving.expect(Packet.Publish.class);
                leaving.expect(Packet.Publish.class);
            }
            watcher.expect(Packet.Publish.class);

            // Act: a delivery comes to wait for the other session.
            publisher.publish("two", "0003", AT_LEAST_ONCE, 7);

            // Assert: the memory has no room for it, until the first client has come back and
            // answered what it had.
            await().untilAsserted(() -> assertThat(reported).containsExactly("second"));
            try (RawSession back = connected(sut.port(), "first", false, true)) {
                assertThat(received(back, 2)).containsExactly("0001", "0002");
                back.ping();
            }
            publisher.publish("two", "0004", AT_LEAST_ONCE, 7);
            try (RawSession back = connected(sut.port(), "second", false, true)) {
                assertThat(received(back, 1)).containsExactly("0004");
            }
            assertThat(reported).containsExactly("second");
        }
    }

    @Test
    void the_deliveries_that_end_with_their_session_give_their_room_back() throws Exception {
        // Arrange: memory for two deliveries, which wait for a session whose client is away.
        try (BrokerServer sut = broker(2 * roomOf("kept", "0001"));
                RawSession publisher = connected(sut.port(), "publisher")) {
            leaveSubscribed(sut, "first", "kept", AT_LEAST_ONCE);
            publisher.publish("kept", "0001", AT_LEAST_ONCE, 7);
            publisher.publish("kept", "0002", AT_LEAST_ONCE, 7);

            // Act: the client comes back for a new session, which ends the old one.
            connected(sut.port(), "first").close();

            // Assert: another session has the room for two deliveries of its own.
            leaveSubscribed(sut, "second", "kept", AT_LEAST_ONCE);
            publisher.publish("kept", "0003", AT_LEAST_ONCE, 7);
            publisher.publish("kept", "0004", AT_LEAST_ONCE, 7);
            try (RawSession back = connected(sut.port(), "second", false, true)) {
                assertThat(received(back, 2)).containsExactly("0003", "0004");
            }
            assertThat(reported).isEmpty();
        }
    }

    /// The payloads of the next `count` deliveries to `receiver`, each of which it answers if
    /// it is a QoS 1 delivery.
    private static List<String> received(RawSession receiver, int count) throws Exception {
        var payloads = new ArrayList<String>();
        for (int number = 0; number < count; number++) {
            Packet.Publish delivery = receiver.expect(Packet.Publish.class);
            payloads.add(new String(delivery.payload(), UTF_8));
            if (delivery.qos() == AT_LEAST_ONCE) {
                receiver.send(new Packet.PubAck(delivery.packetId(), SUCCESS, EMPTY));
            }
        }
        return payloads;
    }

    /// Makes a session for `clientId` that subscribes to `filter` at `qos` and outlives its
    /// connection, which ends here.
    private static void leaveSubscribed(
            BrokerServer broker,
            String clientId,
            String filter,
            Qos qos
    ) throws Exception {
        try (RawSession client = connected(broker.port(), clientId, false, false)) {
            client.subscribe(filter, qos);
            client.disconnect();
        }
    }

    /// The room that a delivery of a QoS 1 or 2 publication to `topic` with `payload` takes
    /// while no other delivery shares the publication.
    private static long roomOf(String topic, String payload) {
        return MemoryBudget.DELIVERY_BYTES + wireSize(topic, payload);
    }

    /// The size of the packet that brings a QoS 1 or 2 publication, which is the room that the
    /// publication takes.
    private static int wireSize(String topic, String payload) {
        return codec.encode(publication(topic, payload, AT_LEAST_ONCE, 7)).length;
    }

    /// A broker whose waiting deliveries may take `bytes` of memory together, and whose
    /// threads fetch exactly what each delivery needs.
    private BrokerServer broker(long bytes) throws IOException {
        return settings(bytes).start(loopback());
    }

    /// The settings of such a broker, for a test that adds its own.
    private BrokerServer.Builder settings(long bytes) {
        return BrokerServer.builder()
                .maxDeliveryMemory(bytes)
                .deliveryMemoryUnit(0)
                .dropListener((clientId, count) -> {
                    for (long each = 0; each < count; each++) {
                        reported.add(clientId);
                    }
                });
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }
}
