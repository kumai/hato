package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.Topic;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.deliverySize;
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
    void sessions_that_share_what_waits_keep_it_when_dropping_it_would_make_no_room()
            throws Exception {
        // Arrange: memory for one publication and a delivery of it for each of two sessions,
        // which wait for it while their clients are away.
        long room = PublicationMemory.sizeOf(publication("shared", "0001", AT_LEAST_ONCE, 7))
                + 2L * MemoryBudget.DELIVERY_BYTES;
        try (BrokerServer sut = broker(room);
                RawSession publisher = connected(sut.port(), "publisher")) {
            leaveSubscribed(sut, "first", "shared", AT_LEAST_ONCE);
            leaveSubscribed(sut, "second", "shared", AT_LEAST_ONCE);
            publisher.publish("shared", "0001", AT_LEAST_ONCE, 7);

            // Act: a second publication of the same size. A session that dropped its delivery
            // of the first would give back that delivery alone, because the other session
            // still holds the publication.
            publisher.publish("shared", "0002", AT_LEAST_ONCE, 7);

            // Assert: each session goes without the new one, and keeps what waited.
            await().untilAsserted(
                    () -> assertThat(reported).containsExactlyInAnyOrder("first", "second")
            );
            for (String client : List.of("first", "second")) {
                try (RawSession back = connected(sut.port(), client, false, true)) {
                    assertThat(received(back, 1)).containsExactly("0001");
                    back.ping();
                }
            }
        }
    }

    @Test
    void a_delivery_of_a_publication_that_has_its_room_needs_room_for_itself_alone()
            throws Exception {
        // Arrange: two sessions that wait while their clients are away, each with three small
        // deliveries that it shares with nobody. The memory has room for them, for a large
        // publication with one delivery, and for a little less than a second delivery of it.
        String large = "x".repeat(4096);
        long room = 6 * roomOf("own/a", "0001")
                + roomOf("both", large)
                + MemoryBudget.DELIVERY_BYTES - 1;
        try (BrokerServer sut = broker(room);
                RawSession publisher = connected(sut.port(), "publisher")) {
            for (String client : List.of("a", "b")) {
                try (RawSession away = connected(sut.port(), client, false, false)) {
                    away.subscribe("own/" + client, AT_LEAST_ONCE);
                    away.subscribe("both", AT_LEAST_ONCE);
                    away.disconnect();
                }
                for (String payload : List.of("0001", "0002", "0003")) {
                    publisher.publish("own/" + client, payload, AT_LEAST_ONCE, 7);
                }
            }

            // Act: the session that the large publication reaches first takes its room. The
            // other one needs room for its delivery alone, which its oldest delivery makes.
            publisher.publish("both", large, AT_LEAST_ONCE, 7);

            // Assert: both sessions keep the large one, and one of them lost its oldest.
            await().untilAsserted(() -> assertThat(reported).hasSize(1));
            for (String client : List.of("a", "b")) {
                List<String> kept = reported.contains(client)
                        ? List.of("0002", "0003", large)
                        : List.of("0001", "0002", "0003", large);
                try (RawSession back = connected(sut.port(), client, false, true)) {
                    assertThat(received(back, kept.size())).isEqualTo(kept);
                    back.ping();
                }
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
        long room = PublicationMemory.sizeOf(publication("shared", payload, AT_LEAST_ONCE, 7))
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

    @Test
    void deliveries_take_room_for_the_properties_that_their_publications_hold() throws Exception {
        // Arrange: memory that the packets of the ten publications below would fill by a fifth,
        // and a session that waits for them while its client is away.
        try (BrokerServer sut = broker(64 * 1024);
                RawSession publisher = connectedV5(sut.port(), "publisher")) {
            leaveSubscribed(sut, "away", "kept", AT_LEAST_ONCE);

            // Act: each carries two hundred User Properties, which take 1.4 kilobytes in its
            // packet and at least 24 on the heap.
            for (int number = 0; number < 10; number++) {
                publisher.publish(withUserProperties("kept", 200));
            }

            // Assert: the memory holds fewer than half of them, so the session lost the rest.
            await().untilAsserted(() -> assertThat(reported).hasSizeGreaterThan(5));
            assertThat(reported).containsOnly("away");
        }
    }

    @Test
    void retained_messages_that_wait_for_a_session_take_room() throws Exception {
        // Arrange: three retained messages, memory for one of them, and a window of one.
        long room = PublicationMemory.sizeOf(retained("kept/1", "0001"));
        try (BrokerServer sut = settings(room).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            for (int number = 1; number <= 3; number++) {
                publisher.publish(retained("kept/" + number, "000" + number));
            }

            // Act: the subscription matches all three. The first takes the window, which needs
            // no room while the client is connected. The second waits and takes the room, and
            // then makes room for the third.
            subscriber.subscribe("kept/#", AT_LEAST_ONCE);

            // Assert: the client gets the first and the newest, and nothing else.
            await().untilAsserted(() -> assertThat(reported).containsExactly("subscriber"));
            assertThat(received(subscriber, 2)).containsExactly("0001", "0003");
            subscriber.ping();
        }
    }

    @Test
    void a_retained_message_that_the_waiting_ones_could_not_make_room_for_leaves_them_alone()
            throws Exception {
        // Arrange: memory for two small retained messages, a window of one, and a fourth
        // retained message that is larger than the two together.
        long room = 2 * PublicationMemory.sizeOf(retained("kept/1", "0001"));
        try (BrokerServer sut = settings(room).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            for (int number = 1; number <= 3; number++) {
                publisher.publish(retained("kept/" + number, "000" + number));
            }
            publisher.publish(retained("kept/4", "x".repeat(4096)));

            // Act: the first takes the window, and the second and the third wait with room.
            subscriber.subscribe("kept/#", AT_LEAST_ONCE);

            // Assert: the session goes without the large one, and keeps what waited.
            await().untilAsserted(() -> assertThat(reported).containsExactly("subscriber"));
            assertThat(received(subscriber, 3)).containsExactly("0001", "0002", "0003");
            subscriber.ping();
        }
    }

    @Test
    void retained_messages_that_wait_more_than_once_stay_when_dropping_them_makes_no_room()
            throws Exception {
        // Arrange: a client whose window of one holds a delivery that it does not answer, and
        // two retained messages, of which the memory has room for the smaller one alone.
        long room = PublicationMemory.sizeOf(retained("kept/1", "x".repeat(4096))) + 100;
        try (BrokerServer sut = settings(room).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            publisher.publish(retained("held/1", "held"));
            subscriber.subscribe("held/#", AT_LEAST_ONCE);
            Packet.Publish held = subscriber.expect(Packet.Publish.class);
            publisher.publish(retained("kept/1", "x".repeat(4096)));
            publisher.publish(retained("kept/2", "y".repeat(8192)));

            // Act: three subscriptions wait for both. Each time the smaller message takes or
            // shares its room, and the larger one finds none: the smaller ones that wait would
            // not give theirs back, because the other subscriptions hold the same message.
            for (int number = 0; number < 3; number++) {
                subscriber.subscribe("kept/#", AT_LEAST_ONCE);
            }

            // Assert: the client goes without the larger one each time, and gets the smaller
            // one for each subscription.
            await().untilAsserted(() -> assertThat(reported).hasSize(3));
            subscriber.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(received(subscriber, 3)).containsOnly("x".repeat(4096));
            subscriber.ping();
        }
    }

    @Test
    void retained_messages_give_their_room_back_as_their_subscription_ends() throws Exception {
        // Arrange: memory for one delivery, and a retained message that waits with room for a
        // client behind another that holds its window.
        try (BrokerServer sut = settings(roomOf("kept/1", "0001")).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            leaveRetainedWaiting(publisher, subscriber);
            leaveSubscribed(sut, "second", "next/1", AT_LEAST_ONCE);

            // Act
            subscriber.send(new Packet.Unsubscribe(2, List.of(new TopicFilter("kept/#")), EMPTY));
            subscriber.expect(Packet.UnsubAck.class);

            // Assert: a delivery that waits for another session finds the room.
            publisher.publish("next/1", "0003", AT_LEAST_ONCE, 7);
            try (RawSession back = connected(sut.port(), "second", false, true)) {
                assertThat(received(back, 1)).containsExactly("0003");
            }
            assertThat(reported).isEmpty();
        }
    }

    @Test
    void retained_messages_give_their_room_back_as_their_session_ends() throws Exception {
        // Arrange: memory for one delivery, and a retained message that waits with room for a
        // client behind another that holds its window.
        try (BrokerServer sut = settings(roomOf("kept/1", "0001")).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher")) {
            try (RawSession subscriber = connected(sut.port(), "subscriber")) {
                leaveRetainedWaiting(publisher, subscriber);
                leaveSubscribed(sut, "second", "next/1", AT_LEAST_ONCE);

                // Act: the client's session ends with its connection.
                subscriber.disconnect();
            }

            // Assert: a delivery that waits for another session finds the room.
            publisher.publish("next/1", "0003", AT_LEAST_ONCE, 7);
            try (RawSession back = connected(sut.port(), "second", false, true)) {
                assertThat(received(back, 1)).containsExactly("0003");
            }
            assertThat(reported).isEmpty();
        }
    }

    /// Has `publisher` retain two messages and `subscriber` subscribe to them: the first takes
    /// the subscriber's window of one and stays unanswered, and the second waits behind it,
    /// which it does by the time the broker handles the subscriber's next packet.
    private static void leaveRetainedWaiting(RawSession publisher, RawSession subscriber)
            throws Exception {
        publisher.publish(retained("kept/1", "0001"));
        publisher.publish(retained("kept/2", "0002"));
        subscriber.subscribe("kept/#", AT_LEAST_ONCE);
        subscriber.expect(Packet.Publish.class);
    }

    /// A retained QoS 1 publication to `topic` with `payload`.
    private static Packet.Publish retained(String topic, String payload) {
        return new Packet.Publish(
                new Topic(topic),
                payload.getBytes(UTF_8),
                AT_LEAST_ONCE,
                true,
                false,
                7,
                EMPTY
        );
    }

    /// A QoS 1 publication to `topic` that carries `count` User Properties of two strings of
    /// one character.
    private static Packet.Publish withUserProperties(String topic, int count) {
        var entries = new ArrayList<Property>();
        for (int number = 0; number < count; number++) {
            entries.add(new Property.UserProperty("a", "b"));
        }
        return new Packet.Publish(
                new Topic(topic),
                "0001".getBytes(UTF_8),
                AT_LEAST_ONCE,
                false,
                false,
                7,
                new Properties(entries)
        );
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

    /// The room that a delivery of a publication to `topic` with `payload` takes while no other
    /// delivery shares the publication.
    private static long roomOf(String topic, String payload) {
        return deliverySize(publication(topic, payload, AT_LEAST_ONCE, 7));
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
                .dropListener(new BrokerSettings.DropListener() {
                    @Override
                    public void dropped(String clientId, long count) {
                        for (long each = 0; each < count; each++) {
                            reported.add(clientId);
                        }
                    }

                    @Override
                    public void droppedUnattributed(long count) {
                        // No test here has more clients than the broker reports by identifier,
                        // so a delivery reported without one shows as an unexpected client.
                        dropped("no client", count);
                    }
                });
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }
}
