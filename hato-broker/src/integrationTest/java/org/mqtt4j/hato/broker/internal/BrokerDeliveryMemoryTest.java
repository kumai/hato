package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.Topic;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.cursorSize;
import static org.mqtt4j.hato.broker.internal.RawSession.deliverySize;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.RawSession.subscription;
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

    // What a test has other sessions do while one session is between counting which of its
    // oldest make room for a new one and their handing the room over (see interferingWith).
    private volatile Interference interference = () -> { };

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
        // Arrange: two sessions that wait while their clients are away, with deliveries of
        // three small publications each. The memory has room for them, for a large
        // publication with one delivery, and for a little less than a second delivery of it.
        String large = "x".repeat(4096);
        long room = 3 * (roomOf("kept", "0001") + MemoryBudget.DELIVERY_BYTES)
                + roomOf("kept", large)
                + MemoryBudget.DELIVERY_BYTES - 1;
        try (BrokerServer sut = broker(room);
                RawSession publisher = connected(sut.port(), "publisher")) {
            leaveSubscribed(sut, "a", "kept", AT_LEAST_ONCE);
            leaveSubscribed(sut, "b", "kept", AT_LEAST_ONCE);
            for (String payload : List.of("0001", "0002", "0003")) {
                publisher.publish("kept", payload, AT_LEAST_ONCE, 7);
            }

            // Act: the session that the large publication reaches first takes its room. The
            // other one needs room for its delivery alone. Its oldest delivery gives that
            // much back, though the publication that it shares with the first session stays.
            publisher.publish("kept", large, AT_LEAST_ONCE, 7);

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
    void a_new_delivery_does_not_push_out_older_ones_that_count_for_more_than_it_does()
            throws Exception {
        // Arrange: two sessions that wait while their clients are away. Both hold two large
        // publications, and the second one a third of its own behind them, which fills the
        // memory.
        long shared = roomOf("shared", "1" + "x".repeat(4095)) + MemoryBudget.DELIVERY_BYTES;
        long room = 2 * shared + roomOf("own", "3" + "x".repeat(4095));
        try (BrokerServer sut = broker(room);
                RawSession publisher = connected(sut.port(), "publisher")) {
            leaveSubscribed(sut, "a", "shared", AT_LEAST_ONCE);
            try (RawSession away = connected(sut.port(), "b", false, false)) {
                away.subscribe("shared", AT_LEAST_ONCE);
                away.subscribe("own", AT_LEAST_ONCE);
                away.disconnect();
            }
            publisher.publish("shared", "1" + "x".repeat(4095), AT_LEAST_ONCE, 7);
            publisher.publish("shared", "2" + "x".repeat(4095), AT_LEAST_ONCE, 7);
            publisher.publish("own", "3" + "x".repeat(4095), AT_LEAST_ONCE, 7);

            // Act: one more for the second session alone. Its two oldest deliveries would give
            // little back, because the first session holds their publications too, and only
            // the third would make the room.
            publisher.publish("own", "4" + "x".repeat(4095), AT_LEAST_ONCE, 7);

            // Assert: the session does not give up all three for one. It goes without the
            // new one, and keeps what waited.
            await().untilAsserted(() -> assertThat(reported).containsExactly("b"));
            try (RawSession back = connected(sut.port(), "b", false, true)) {
                assertThat(received(back, 3))
                        .extracting(payload -> payload.substring(0, 1))
                        .containsExactly("1", "2", "3");
                back.ping();
            }
        }
    }

    @Test
    void a_delivery_that_gives_little_back_goes_with_the_one_behind_it_that_makes_the_room()
            throws Exception {
        // Arrange: two sessions that wait while their clients are away. Both hold a
        // publication of 200 bytes, and the second one a publication of 300 bytes of its own
        // behind it. Besides, the memory has room for 150 bytes.
        String shared = "s".repeat(200);
        long room = roomOf("shared", shared)
                + MemoryBudget.DELIVERY_BYTES
                + roomOf("own", "1" + "o".repeat(299))
                + 150;
        try (BrokerServer sut = broker(room);
                RawSession publisher = connected(sut.port(), "publisher")) {
            leaveSubscribed(sut, "a", "shared", AT_LEAST_ONCE);
            try (RawSession away = connected(sut.port(), "b", false, false)) {
                away.subscribe("shared", AT_LEAST_ONCE);
                away.subscribe("own", AT_LEAST_ONCE);
                away.disconnect();
            }
            publisher.publish("shared", shared, AT_LEAST_ONCE, 7);
            publisher.publish("own", "1" + "o".repeat(299), AT_LEAST_ONCE, 7);

            // Act: one more of 300 bytes for the second session alone. Its delivery of the
            // shared publication would give back only what it holds itself, which is too
            // little, and the one behind it makes the room. The two count for less than the
            // new one before the second goes.
            publisher.publish("own", "2" + "o".repeat(299), AT_LEAST_ONCE, 7);

            // Assert: the session lost both and keeps the new one, and the other session
            // keeps the publication that they shared.
            await().untilAsserted(() -> assertThat(reported).containsExactly("b", "b"));
            try (RawSession back = connected(sut.port(), "b", false, true)) {
                assertThat(received(back, 1))
                        .extracting(payload -> payload.substring(0, 1))
                        .containsExactly("2");
                back.ping();
            }
            try (RawSession back = connected(sut.port(), "a", false, true)) {
                assertThat(received(back, 1)).containsExactly(shared);
            }
        }
    }

    @Test
    void a_second_delivery_of_a_publication_without_room_leaves_the_first_one_waiting()
            throws Exception {
        // Arrange: the session of an MQTT 5.0 client that is away, with two subscriptions to
        // one topic: one that keeps the RETAIN flag, and one with a Subscription Identifier
        // that does not. A retained publication so has two deliveries for it, of which the
        // second holds a sequence of properties of its own for the identifier. The memory
        // has room for the first, and for a byte less than the second holds more than it.
        Packet.Publish kept = retained("kept", "0001");
        long more = Delivery.ownSize(kept, List.of(7), MqttVersion.V5)
                - MemoryBudget.DELIVERY_BYTES;
        try (BrokerServer sut = broker(deliverySize(kept) + more - 1);
                RawSession publisher = connected(sut.port(), "publisher")) {
            var identifier = new Property.VariableByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER, 7);
            try (RawSession away = connectedV5(sut.port(), "away", true, 3600, false)) {
                away.subscribe(subscription("kept", AT_LEAST_ONCE, false, true));
                away.send(new Packet.Subscribe(
                        2,
                        List.of(subscription("#", AT_LEAST_ONCE)),
                        new Properties(List.of(identifier))
                ));
                away.expect(Packet.SubAck.class);
                away.disconnect();
            }

            // Act: the first delivery takes its room. The second finds the publication held
            // and needs room for itself alone, which the first would not give back in full.
            publisher.publish(kept);

            // Assert: the session goes without the second alone, and its client gets the
            // first, which keeps the RETAIN flag.
            await().untilAsserted(() -> assertThat(reported).containsExactly("away"));
            try (RawSession back = connectedV5(sut.port(), "away", false, 3600, true)) {
                Packet.Publish delivery = back.expect(Packet.Publish.class);
                assertThat(delivery.retain()).isTrue();
                back.send(new Packet.PubAck(delivery.packetId(), SUCCESS, EMPTY));
                back.ping();
            }
        }
    }

    @Test
    void a_session_keeps_its_oldest_delivery_when_another_session_takes_the_room_first()
            throws Exception {
        // Arrange: a small delivery waits for a session that is away. Besides, the memory has
        // room for one byte less than a large one, which so needs the room of the small one.
        // Another session that is away waits for a topic of its own.
        String large = "x".repeat(400);
        long room = roomOf("kept", "0001") + roomOf("kept", large) - 1;
        BrokerServer.Builder interfered = settings(room)
                .actors(2)
                .beforeHandOver(interferingWith("away"));
        try (BrokerServer sut = interfered.start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession second = connected(sut.port(), "second")) {
            leaveSubscribed(sut, "away", "kept", AT_LEAST_ONCE);
            leaveSubscribed(sut, "other", "else", AT_LEAST_ONCE);
            publisher.publish("kept", "0001", AT_LEAST_ONCE, 7);

            // Act: the large one comes. Once the session has counted that its small delivery
            // makes the room, and before that one goes, a delivery for the other session
            // takes part of the room that the count relied on.
            interference = () -> second.publish("else", "0009", AT_LEAST_ONCE, 7);
            publisher.publish("kept", large, AT_LEAST_ONCE, 7);

            // Assert: the session goes without the large one alone, and keeps what waited.
            await().untilAsserted(() -> assertThat(reported).containsExactly("away"));
            try (RawSession back = connected(sut.port(), "away", false, true)) {
                assertThat(received(back, 1)).containsExactly("0001");
                back.ping();
            }
            try (RawSession back = connected(sut.port(), "other", false, true)) {
                assertThat(received(back, 1)).containsExactly("0009");
            }
        }
    }

    @Test
    void a_session_keeps_its_oldest_delivery_when_another_session_comes_to_hold_its_publication()
            throws Exception {
        // Arrange: a delivery of a retained publication waits for a session that is away, and
        // a client whose window of one holds a delivery that it does not answer. Besides that
        // delivery, the memory has room for the cursor of one subscription.
        long room = roomOf("kept/1", "0001") + cursorSize("kept/1", 1);
        BrokerServer.Builder interfered = settings(room)
                .actors(2)
                .maxInflight(1)
                .beforeHandOver(interferingWith("away"));
        try (BrokerServer sut = interfered.start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession behind = connected(sut.port(), "behind")) {
            publisher.publish(retained("held/1", "held"));
            behind.subscribe("held/#", AT_LEAST_ONCE);
            Packet.Publish held = behind.expect(Packet.Publish.class);
            leaveSubscribed(sut, "away", "kept/#", AT_LEAST_ONCE);
            publisher.publish(retained("kept/1", "0001"));

            // Act: a publication to another topic whose delivery takes all of the memory.
            // Once the session has counted that its delivery of the retained one makes the
            // room, and before that one goes, the other client subscribes to the retained
            // message, which then waits for it as well: the delivery would give back no more
            // than its own room.
            interference = () -> {
                behind.subscribe("kept/1", AT_LEAST_ONCE);
                behind.ping();
            };
            publisher.publish("kept/2", fillingPayload("kept/2", room), AT_LEAST_ONCE, 7);

            // Assert: the session goes without the new one alone, and keeps what waited, as
            // the other client does.
            await().untilAsserted(() -> assertThat(reported).containsExactly("away"));
            try (RawSession back = connected(sut.port(), "away", false, true)) {
                assertThat(received(back, 1)).containsExactly("0001");
                back.ping();
            }
            behind.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(received(behind, 1)).containsExactly("0001");
            behind.ping();
        }
    }

    @Test
    void a_full_queue_keeps_its_oldest_delivery_when_the_memory_has_no_room_for_a_new_one()
            throws Exception {
        // Arrange: a session that is away, for which two deliveries may wait at the most. Two
        // small ones wait for it and fill the memory.
        BrokerServer.Builder limited = settings(2 * roomOf("kept", "0001")).maxQueued(2);
        try (BrokerServer sut = limited.start(loopback());
                RawSession publisher = connected(sut.port(), "publisher")) {
            leaveSubscribed(sut, "away", "kept", AT_LEAST_ONCE);
            publisher.publish("kept", "0001", AT_LEAST_ONCE, 7);
            publisher.publish("kept", "0002", AT_LEAST_ONCE, 7);

            // Act: a publication larger than the two together, for which the memory has no
            // room whatever the session drops.
            publisher.publish("kept", "x".repeat(4096), AT_LEAST_ONCE, 7);

            // Assert: the session goes without it alone. Its queue, which the new delivery
            // would have overflowed, gave up nothing for it.
            await().untilAsserted(() -> assertThat(reported).containsExactly("away"));
            try (RawSession back = connected(sut.port(), "away", false, true)) {
                assertThat(received(back, 2)).containsExactly("0001", "0002");
                back.ping();
            }
        }
    }

    @Test
    void a_busy_session_with_a_full_queue_keeps_its_oldest_delivery_when_the_memory_has_no_room()
            throws Exception {
        // Arrange: a client for which two deliveries may wait, with a window of one. One
        // delivery is unanswered in its window, and two more wait and fill the memory. Then
        // its session stalls on a packet of its own, so that another thread delivers to it.
        var reached = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        BrokerServer.Builder stalling = settings(2 * roomOf("kept", "0002"))
                .actors(2)
                .maxInflight(1)
                .maxQueued(2)
                .beforeHandle(packet -> {
                    if (packet instanceof Packet.Publish publish
                            && publish.topic().name().equals("stall")) {
                        reached.countDown();
                        awaitUninterruptibly(release);
                    }
                });
        try (BrokerServer sut = stalling.start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            subscriber.subscribe("kept", AT_LEAST_ONCE);
            for (String payload : List.of("0001", "0002", "0003")) {
                publisher.publish("kept", payload, AT_LEAST_ONCE, 7);
            }
            Packet.Publish held = subscriber.expect(Packet.Publish.class);
            subscriber.send(publication("stall", "stop here", AT_MOST_ONCE, 0));
            assertThat(reached.await(5, SECONDS)).isTrue();

            // Act: a publication larger than the memory, which comes to the full queue while
            // the session stalls.
            publisher.publish("kept", "x".repeat(4096), AT_LEAST_ONCE, 7);

            // Assert: the session goes without it alone, and its queue gave up nothing for it.
            await().untilAsserted(() -> assertThat(reported).containsExactly("subscriber"));
            release.countDown();
            subscriber.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(received(subscriber, 2)).containsExactly("0002", "0003");
            subscriber.ping();
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_small_delivery_takes_the_place_of_a_larger_one_and_leaves_the_rest_of_its_room()
            throws Exception {
        // Arrange: memory for one large delivery, which waits for a session that is away.
        String large = "x".repeat(4096);
        try (BrokerServer sut = broker(roomOf("kept", large));
                RawSession publisher = connected(sut.port(), "publisher")) {
            leaveSubscribed(sut, "away", "kept", AT_LEAST_ONCE);
            publisher.publish("kept", large, AT_LEAST_ONCE, 7);

            // Act: two small ones. The first needs a little of the room of the large one,
            // and the second finds its room in what the large one gave back besides.
            publisher.publish("kept", "0002", AT_LEAST_ONCE, 7);
            publisher.publish("kept", "0003", AT_LEAST_ONCE, 7);

            // Assert: the session lost the large one alone.
            await().untilAsserted(() -> assertThat(reported).containsExactly("away"));
            try (RawSession back = connected(sut.port(), "away", false, true)) {
                assertThat(received(back, 2)).containsExactly("0002", "0003");
                back.ping();
            }
            assertThat(reported).containsExactly("away");
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
        // Arrange: three retained messages, memory for one of them with the cursor of their
        // subscription, and a window of one.
        long room = cursorSize("kept/#", 3) + PublicationMemory.sizeOf(retained("kept/1", "0001"));
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
        // Arrange: memory for two small retained messages with the cursor of their
        // subscription, a window of one, and a fourth retained message that is larger than
        // all of that together.
        long room = cursorSize("kept/#", 4)
                + 2 * PublicationMemory.sizeOf(retained("kept/1", "0001"));
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
        // two retained messages. The memory has room for the smaller one and for the cursors
        // of three subscriptions to both, but not for the larger one.
        long room = PublicationMemory.sizeOf(retained("kept/1", "x".repeat(4096)))
                + 3 * cursorSize("kept/#", 2)
                + 100;
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
    void a_new_retained_message_does_not_push_out_waiting_ones_that_count_for_more_than_it_does()
            throws Exception {
        // Arrange: three large retained messages wait for a client whose window of one holds a
        // delivery that it does not answer. A session that is away holds deliveries of the
        // first two of them, so those would give no room back. The memory has room for all of
        // that and for the cursor of one more subscription, and no more.
        String large = "x".repeat(4096);
        long room = roomOf("kept/a/1", "1" + large)
                + roomOf("kept/a/2", "2" + large)
                + cursorSize("kept/#", 3)
                + PublicationMemory.sizeOf(retained("kept/b", "3" + large))
                + cursorSize("more/#", 1);
        try (BrokerServer sut = settings(room).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            publisher.publish(retained("held/1", "held"));
            subscriber.subscribe("held/#", AT_LEAST_ONCE);
            Packet.Publish held = subscriber.expect(Packet.Publish.class);
            leaveSubscribed(sut, "away", "kept/a/#", AT_LEAST_ONCE);
            publisher.publish(retained("kept/a/1", "1" + large));
            publisher.publish(retained("kept/a/2", "2" + large));
            publisher.publish(retained("kept/b", "3" + large));
            subscriber.subscribe("kept/#", AT_LEAST_ONCE);
            publisher.publish(retained("more/z", "4" + large));

            // Act: a subscription to a fourth message of the same size, which finds no room
            // with its cursor. Only the third of those that wait would make it, and the two
            // before it count for more than the new one and its cursor.
            subscriber.subscribe("more/#", AT_LEAST_ONCE);

            // Assert: the client does not lose all three for it. It goes without the new one,
            // and gets what waited.
            await().untilAsserted(() -> assertThat(reported).containsExactly("subscriber"));
            subscriber.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(received(subscriber, 3))
                    .extracting(payload -> payload.substring(0, 1))
                    .containsExactly("1", "2", "3");
            subscriber.ping();
        }
    }

    @Test
    void a_later_subscription_makes_room_at_the_cost_of_the_retained_message_of_an_earlier_one()
            throws Exception {
        // Arrange: a client whose window of one holds a delivery that it does not answer, and
        // one retained message that waits for it. The memory has room for that message and
        // the cursor of its subscription, and no more.
        long room = cursorSize("a/#", 1) + PublicationMemory.sizeOf(retained("a/1", "0001"));
        try (BrokerServer sut = settings(room).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            publisher.publish(retained("held/1", "held"));
            subscriber.subscribe("held/#", AT_LEAST_ONCE);
            Packet.Publish held = subscriber.expect(Packet.Publish.class);
            publisher.publish(retained("a/1", "0001"));
            publisher.publish(retained("b/1", "0002"));
            subscriber.subscribe("a/#", AT_LEAST_ONCE);

            // Act: a second subscription with a retained message of the same size. The message
            // and its cursor find no room, and the message that waits gives too little back
            // alone: its cursor, which then holds nothing, gives the rest.
            subscriber.subscribe("b/#", AT_LEAST_ONCE);

            // Assert: the client lost the older message and gets the newer one.
            await().untilAsserted(() -> assertThat(reported).containsExactly("subscriber"));
            subscriber.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(received(subscriber, 1)).containsExactly("0002");
            subscriber.ping();
        }
    }

    @Test
    void a_later_subscription_whose_retained_message_finds_no_room_leaves_the_earlier_one_waiting()
            throws Exception {
        // Arrange: a client whose window of one holds a delivery that it does not answer, and
        // one retained message that waits for it. The memory has room for that message and
        // the cursor of its subscription, and no more.
        long room = cursorSize("a/#", 1) + PublicationMemory.sizeOf(retained("a/1", "0001"));
        try (BrokerServer sut = settings(room).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            publisher.publish(retained("held/1", "held"));
            subscriber.subscribe("held/#", AT_LEAST_ONCE);
            Packet.Publish held = subscriber.expect(Packet.Publish.class);
            publisher.publish(retained("a/1", "0001"));
            publisher.publish(retained("b/1", "x".repeat(4096)));
            subscriber.subscribe("a/#", AT_LEAST_ONCE);

            // Act: a second subscription with a retained message that is larger than the
            // memory. What waits would give back enough for the cursor of the subscription,
            // but not for its message.
            subscriber.subscribe("b/#", AT_LEAST_ONCE);

            // Assert: the client goes without the large one alone, and gets what waited.
            await().untilAsserted(() -> assertThat(reported).containsExactly("subscriber"));
            subscriber.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(received(subscriber, 1)).containsExactly("0001");
            subscriber.ping();
        }
    }

    @Test
    void a_later_subscription_makes_room_for_its_cursor_too_at_the_cost_of_the_oldest_messages()
            throws Exception {
        // Arrange: a client whose window of one holds a delivery that it does not answer, and
        // six small retained messages that wait for it with the cursor of their subscription,
        // which fills the memory.
        long message = PublicationMemory.sizeOf(retained("k/1", "0001"));
        long room = cursorSize("k/#", 6) + 6 * message;
        try (BrokerServer sut = settings(room).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            publisher.publish(retained("held/1", "held"));
            subscriber.subscribe("held/#", AT_LEAST_ONCE);
            Packet.Publish held = subscriber.expect(Packet.Publish.class);
            for (int number = 1; number <= 6; number++) {
                publisher.publish(retained("k/" + number, "000" + number));
            }
            publisher.publish(retained("n/1", "0007"));
            subscriber.subscribe("k/#", AT_LEAST_ONCE);

            // Act: a second subscription with one retained message of the same size, which
            // needs room for its cursor as well: as much as this many of the oldest messages
            // give back.
            int lost = (int) Math.ceilDiv(cursorSize("n/#", 1) + message, message);
            subscriber.subscribe("n/#", AT_LEAST_ONCE);

            // Assert: the client lost those, and gets the others and the new one.
            await().untilAsserted(() -> assertThat(reported).hasSize(lost));
            subscriber.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(lost).isBetween(2, 5);
            assertThat(received(subscriber, 6 - lost + 1))
                    .startsWith("000" + (lost + 1))
                    .endsWith("0007");
            subscriber.ping();
            assertThat(reported).hasSize(lost);
        }
    }

    @Test
    void a_retained_message_whose_delivery_made_room_waits_again_for_a_later_subscription()
            throws Exception {
        // Arrange: a delivery of a retained publication waits for a session that is away, and
        // a client whose window of one holds a delivery that it does not answer. The memory
        // has room for the retained message with the cursor of a subscription to it.
        long room = cursorSize("kept/1", 1) + PublicationMemory.sizeOf(retained("kept/1", "0001"));
        try (BrokerServer sut = settings(room).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession behind = connected(sut.port(), "behind")) {
            publisher.publish(retained("held/1", "held"));
            behind.subscribe("held/#", AT_LEAST_ONCE);
            Packet.Publish held = behind.expect(Packet.Publish.class);
            leaveSubscribed(sut, "away", "kept/#", AT_LEAST_ONCE);
            publisher.publish(retained("kept/1", "0001"));

            // Act: a publication of the same size, for which the delivery of the retained one
            // makes room as the last holder of its publication. The session's client then
            // takes it, and the other client subscribes to the retained message.
            publisher.publish("kept/2", "0002", AT_LEAST_ONCE, 7);
            try (RawSession back = connected(sut.port(), "away", false, true)) {
                assertThat(received(back, 1)).containsExactly("0002");
                back.ping();
            }
            behind.subscribe("kept/1", AT_LEAST_ONCE);
            behind.ping();

            // Assert: the retained message takes its room anew and waits for that client.
            behind.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(received(behind, 1)).containsExactly("0001");
            behind.ping();
            await().untilAsserted(() -> assertThat(reported).containsExactly("away"));
        }
    }

    @Test
    void a_second_subscription_without_room_for_its_cursor_leaves_the_first_one_its_message()
            throws Exception {
        // Arrange: a client whose window of one holds a delivery that it does not answer, and
        // a retained message that waits for it with the cursor of a subscription to every
        // topic, which fills the memory.
        long room = cursorSize("#", 1) + PublicationMemory.sizeOf(retained("kept/1", "0001"));
        try (BrokerServer sut = settings(room).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            subscriber.subscribe("held", AT_LEAST_ONCE);
            publisher.publish("held", "held", AT_LEAST_ONCE, 7);
            Packet.Publish held = subscriber.expect(Packet.Publish.class);
            publisher.publish(retained("kept/1", "0001"));
            subscriber.subscribe("#", AT_LEAST_ONCE);
            subscriber.ping();

            // Act: a second subscription to the same message, with a longer filter. Its cursor
            // needs a little more room than the first one would give back, and the message,
            // which the second one would come to hold as well, would give back none.
            subscriber.subscribe("kept/1", AT_LEAST_ONCE);

            // Assert: the client gets the message once, for the first subscription.
            await().untilAsserted(() -> assertThat(reported).containsExactly("subscriber"));
            subscriber.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(received(subscriber, 1)).containsExactly("0001");
            subscriber.ping();
        }
    }

    @Test
    void a_retained_message_that_gives_nothing_back_goes_with_the_one_behind_it_that_makes_room()
            throws Exception {
        // Arrange: two retained messages wait for a client whose window of one holds a
        // delivery that it does not answer. A session that is away holds a delivery of the
        // first of them, so that one would give no room back. Besides all that, the memory
        // has room for the cursor of one more subscription.
        long room = roomOf("k/1", "0001")
                + cursorSize("k/#", 2)
                + PublicationMemory.sizeOf(retained("k/2", "0002"))
                + cursorSize("n/#", 1);
        try (BrokerServer sut = settings(room).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            publisher.publish(retained("held/1", "held"));
            subscriber.subscribe("held/#", AT_LEAST_ONCE);
            Packet.Publish held = subscriber.expect(Packet.Publish.class);
            leaveSubscribed(sut, "away", "k/1", AT_LEAST_ONCE);
            publisher.publish(retained("k/1", "0001"));
            publisher.publish(retained("k/2", "0002"));
            subscriber.subscribe("k/#", AT_LEAST_ONCE);
            publisher.publish(retained("n/1", "0003"));

            // Act: a subscription to a third message of the same size, which needs room. The
            // first of the two that wait gives none back, and the second makes the room with
            // the cursor that then holds nothing. The first counts for less than the new one
            // with its cursor, so the second may go as well.
            subscriber.subscribe("n/#", AT_LEAST_ONCE);

            // Assert: the client lost both that waited and gets the new one.
            await().untilAsserted(
                    () -> assertThat(reported).containsExactly("subscriber", "subscriber")
            );
            subscriber.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(received(subscriber, 1)).containsExactly("0003");
            subscriber.ping();
        }
    }

    @Test
    void a_subscription_made_again_takes_over_a_message_that_another_session_holds_as_well()
            throws Exception {
        // Arrange: a client whose window of one holds a delivery that it does not answer. A
        // retained message waits for it with the cursor of its subscription, and a session
        // that is away holds a delivery of the same publication. Behind the retained message
        // a publication to another topic waits for the client, and the memory is full.
        long room = roomOf("kept/1", "0001") + cursorSize("kept/1", 1) + roomOf("live", "0002");
        try (BrokerServer sut = settings(room).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            subscriber.subscribe("held", AT_LEAST_ONCE);
            publisher.publish("held", "held", AT_LEAST_ONCE, 7);
            Packet.Publish held = subscriber.expect(Packet.Publish.class);
            leaveSubscribed(sut, "away", "kept/#", AT_LEAST_ONCE);
            publisher.publish(retained("kept/1", "0001"));
            subscriber.subscribe("kept/1", AT_LEAST_ONCE);
            subscriber.subscribe("live", AT_LEAST_ONCE);
            publisher.publish("live", "0002", AT_LEAST_ONCE, 7);

            // Act: a second subscription to the retained message, with a filter of the same
            // length. Its cursor needs the room of the first one. The message needs none,
            // because it has its room with its other holders, of which the new cursor is one
            // before the first one lets go.
            subscriber.subscribe("kept/+", AT_LEAST_ONCE);

            // Assert: the copy of the first subscription made room, so the client gets the
            // message behind the publication that came between the two subscriptions.
            await().untilAsserted(() -> assertThat(reported).containsExactly("subscriber"));
            subscriber.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(received(subscriber, 2)).containsExactly("0002", "0001");
            subscriber.ping();
        }
    }

    @Test
    void a_retained_message_that_made_room_and_waits_again_takes_its_room_again()
            throws Exception {
        // Arrange: a client whose window of one holds a delivery that it does not answer, and
        // two retained messages of the same size. The memory has room for one of them with
        // the cursor of its subscription. A session that is away waits for a topic of its
        // own.
        String large = "x".repeat(400);
        long room = cursorSize("a/#", 1) + PublicationMemory.sizeOf(retained("a/1", large));
        try (BrokerServer sut = settings(room).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            publisher.publish(retained("held/1", "held"));
            subscriber.subscribe("held/#", AT_LEAST_ONCE);
            Packet.Publish held = subscriber.expect(Packet.Publish.class);
            leaveSubscribed(sut, "other", "else", AT_LEAST_ONCE);
            publisher.publish(retained("a/1", large));
            publisher.publish(retained("b/1", large));
            subscriber.subscribe("a/#", AT_LEAST_ONCE);

            // Act: a subscription to the second message, for which the first makes room, and
            // then one to the first again, for which the second makes room.
            subscriber.subscribe("b/#", AT_LEAST_ONCE);
            subscriber.subscribe("a/#", AT_LEAST_ONCE);
            subscriber.ping();

            // Assert: the first message has its room again, so that the memory is full and a
            // small delivery for the other session finds none.
            publisher.publish("else", "0009", AT_LEAST_ONCE, 7);
            await().untilAsserted(
                    () -> assertThat(reported)
                            .containsExactlyInAnyOrder("subscriber", "subscriber", "other")
            );
            subscriber.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(received(subscriber, 1)).containsExactly(large);
            subscriber.ping();
        }
    }

    @ParameterizedTest(name = "with a limit on their {0}")
    @ValueSource(strings = {"number", "bytes"})
    void a_subscription_without_room_leaves_an_earlier_one_when_no_more_may_wait(String limit)
            throws Exception {
        // Arrange: a client whose window of one holds a delivery that it does not answer, and
        // a retained message that waits for it with the cursor of a subscription to every
        // topic, which fills the memory. No second retained message may wait for the
        // session: the limit on their number, or the one on what they hold, allows one.
        long message = PublicationMemory.sizeOf(retained("a/1", "0001"));
        BrokerServer.Builder roomy = settings(cursorSize("#", 1) + message).maxInflight(1);
        BrokerServer.Builder limited = limit.equals("number")
                ? roomy.maxRetained(1)
                : roomy.maxRetainedBytes(message + 1);
        try (BrokerServer sut = limited.start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            subscriber.subscribe("held", AT_LEAST_ONCE);
            publisher.publish("held", "held", AT_LEAST_ONCE, 7);
            Packet.Publish held = subscriber.expect(Packet.Publish.class);
            publisher.publish(retained("a/1", "0001"));
            subscriber.subscribe("#", AT_LEAST_ONCE);
            subscriber.ping();

            // Act: a second subscription to the same message. Its copy is one more than may
            // wait, and its cursor needs a little more room than the first one would give
            // back, so the memory has none for it.
            subscriber.subscribe("a/#", AT_LEAST_ONCE);

            // Assert: the client goes without the second copy alone. The first one did not
            // go for a copy that then found no room.
            await().untilAsserted(() -> assertThat(reported).containsExactly("subscriber"));
            subscriber.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(received(subscriber, 1)).containsExactly("0001");
            subscriber.ping();
            assertThat(reported).containsExactly("subscriber");
        }
    }

    @Test
    void a_wider_subscription_takes_the_place_of_an_earlier_one_when_no_more_may_wait()
            throws Exception {
        // Arrange: a broker that retains three messages, which is as many as may wait for a
        // session. Two of them wait for a client whose window of one holds a delivery that it
        // does not answer, with the cursor of their subscription. Besides them, the memory
        // has room for the third message, and for what the cursor of a subscription to all
        // three holds more than the first cursor.
        long message = PublicationMemory.sizeOf(retained("a/1", "0001"));
        long room = cursorSize("#", 3) + 3 * message;
        BrokerServer.Builder limited = settings(room).maxInflight(1).maxRetained(3);
        try (BrokerServer sut = limited.start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            subscriber.subscribe("held", AT_LEAST_ONCE);
            publisher.publish("held", "held", AT_LEAST_ONCE, 7);
            Packet.Publish held = subscriber.expect(Packet.Publish.class);
            publisher.publish(retained("a/1", "0001"));
            publisher.publish(retained("a/2", "0002"));
            publisher.publish(retained("b/1", "0003"));
            subscriber.subscribe("a/#", AT_LEAST_ONCE);
            subscriber.ping();

            // Act: a subscription to every topic. Its three retained messages are more than
            // may wait beside the two, and the memory has no room for its cursor as things
            // are. The first subscription makes it: its two copies go, and with them what
            // its cursor holds, which leaves room for the new cursor and its messages.
            subscriber.subscribe("#", AT_LEAST_ONCE);

            // Assert: the client lost the two copies of the first subscription, and gets all
            // three messages once.
            await().untilAsserted(
                    () -> assertThat(reported).containsExactly("subscriber", "subscriber")
            );
            subscriber.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(received(subscriber, 3)).containsExactly("0001", "0002", "0003");
            subscriber.ping();
            assertThat(reported).containsExactly("subscriber", "subscriber");
        }
    }

    @Test
    void a_full_room_keeps_its_oldest_retained_message_when_another_session_takes_the_memory_first()
            throws Exception {
        // Arrange: a broker that retains one message, which is as many as may wait for a
        // session. It waits for a client whose window of one holds a delivery that it does
        // not answer, with the cursor of a subscription. Besides, the memory has room for a
        // small delivery and one byte, so that the cursor of a second subscription, which
        // holds two bytes more than the first, could take the place of the first. A session
        // that is away waits for a topic of its own.
        long room = cursorSize("+/1", 1)
                + PublicationMemory.sizeOf(retained("ab/1", "0001"))
                + roomOf("else", "0009")
                + 1;
        BrokerServer.Builder interfered = settings(room)
                .actors(2)
                .maxInflight(1)
                .maxRetained(1)
                .beforeHandOver(interferingWith("subscriber"));
        try (BrokerServer sut = interfered.start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            subscriber.subscribe("held", AT_LEAST_ONCE);
            publisher.publish("held", "held", AT_LEAST_ONCE, 7);
            Packet.Publish held = subscriber.expect(Packet.Publish.class);
            leaveSubscribed(sut, "other", "else", AT_LEAST_ONCE);
            publisher.publish(retained("ab/1", "0001"));
            subscriber.subscribe("+/1", AT_LEAST_ONCE);
            subscriber.ping();

            // Act: a second subscription to the same message, which is one more than may
            // wait. Once the session has counted that the first cursor makes room for the
            // second, and before it goes, a delivery for the other session takes the room
            // that the count relied on besides.
            interference = () -> publisher.publish("else", "0009", AT_LEAST_ONCE, 7);
            subscriber.subscribe("ab/1", AT_LEAST_ONCE);

            // Assert: the client goes without one copy alone, and gets the message.
            await().untilAsserted(() -> assertThat(reported).containsExactly("subscriber"));
            subscriber.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(received(subscriber, 1)).containsExactly("0001");
            subscriber.ping();
            try (RawSession back = connected(sut.port(), "other", false, true)) {
                assertThat(received(back, 1)).containsExactly("0009");
            }
            assertThat(reported).containsExactly("subscriber");
        }
    }

    @Test
    void a_cursor_for_a_message_held_elsewhere_gives_way_when_another_session_takes_the_room_first()
            throws Exception {
        // Arrange: a client whose window of one holds a delivery that it does not answer, and
        // two retained messages that wait for it with the cursor of their subscription. A
        // session that is away holds a delivery of a third retained message, and another one
        // waits for a topic of its own. Besides, the memory has room for one small delivery.
        long message = PublicationMemory.sizeOf(retained("x/1", "0001"));
        long room = roomOf("m/1", "0003")
                + cursorSize("x/#", 2)
                + 2 * message
                + roomOf("else", "0009");
        BrokerServer.Builder interfered = settings(room)
                .actors(2)
                .maxInflight(1)
                .beforeHandOver(interferingWith("subscriber"));
        try (BrokerServer sut = interfered.start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            subscriber.subscribe("held", AT_LEAST_ONCE);
            publisher.publish("held", "held", AT_LEAST_ONCE, 7);
            Packet.Publish held = subscriber.expect(Packet.Publish.class);
            leaveSubscribed(sut, "away", "m/1", AT_LEAST_ONCE);
            leaveSubscribed(sut, "other", "else", AT_LEAST_ONCE);
            publisher.publish(retained("m/1", "0003"));
            publisher.publish(retained("x/1", "0001"));
            publisher.publish(retained("x/2", "0002"));
            subscriber.subscribe("x/#", AT_LEAST_ONCE);
            subscriber.ping();

            // Act: a subscription to the third message, which needs room for its cursor
            // alone, because the message has its room with the delivery of the other session.
            // The room that is left and that of the oldest message that waits would make it.
            // Once the session has counted that, and before the oldest goes, a delivery for
            // the third session takes the room that was left.
            interference = () -> publisher.publish("else", "0009", AT_LEAST_ONCE, 7);
            subscriber.subscribe("m/1", AT_LEAST_ONCE);

            // Assert: the client goes without the third message alone and keeps what waited,
            // and the session that is away still gets its delivery of that message.
            await().untilAsserted(() -> assertThat(reported).containsExactly("subscriber"));
            subscriber.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(received(subscriber, 2)).containsExactly("0001", "0002");
            subscriber.ping();
            try (RawSession back = connected(sut.port(), "away", false, true)) {
                assertThat(received(back, 1)).containsExactly("0003");
                back.ping();
            }
            try (RawSession back = connected(sut.port(), "other", false, true)) {
                assertThat(received(back, 1)).containsExactly("0009");
            }
            assertThat(reported).containsExactly("subscriber");
        }
    }

    @Test
    void a_subscription_made_again_with_the_memory_full_takes_the_place_of_the_first_one()
            throws Exception {
        // Arrange: a client whose window of one holds a delivery that it does not answer. Six
        // retained messages wait for it with the cursor of their subscription, and behind
        // them a publication to another topic, which fills the memory.
        long room = cursorSize("k/#", 6)
                + 6 * PublicationMemory.sizeOf(retained("k/1", "0001"))
                + roomOf("live", "0007");
        try (BrokerServer sut = settings(room).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            subscriber.subscribe("held", AT_LEAST_ONCE);
            publisher.publish("held", "held", AT_LEAST_ONCE, 7);
            Packet.Publish held = subscriber.expect(Packet.Publish.class);
            for (int number = 1; number <= 6; number++) {
                publisher.publish(retained("k/" + number, "000" + number));
            }
            subscriber.subscribe("k/#", AT_LEAST_ONCE);
            subscriber.subscribe("live", AT_LEAST_ONCE);
            publisher.publish("live", "0007", AT_LEAST_ONCE, 7);

            // Act: the same subscription again. Its cursor needs room, which the copies that
            // wait for the first subscription make: all but the copy of the message that the
            // new cursor comes to hold first, whose room stays with that message.
            subscriber.subscribe("k/#", AT_LEAST_ONCE);

            // Assert: the first subscription's copies went, one after the other, for those
            // of the second, so the client gets every message once, behind the publication
            // that came between the two subscriptions.
            await().untilAsserted(() -> assertThat(reported).hasSize(6));
            subscriber.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(received(subscriber, 7))
                    .containsExactly("0007", "0001", "0002", "0003", "0004", "0005", "0006");
            subscriber.ping();
            assertThat(reported).containsOnly("subscriber").hasSize(6);
        }
    }

    @Test
    void a_session_keeps_its_oldest_retained_message_when_another_session_takes_the_room_first()
            throws Exception {
        // Arrange: a client whose window of one holds a delivery that it does not answer, and
        // a small retained message that waits for it with the cursor of its subscription.
        // Besides, the memory has room for one byte less than a small delivery and for what a
        // large retained message holds more than the small one, so that the large one and
        // its cursor need the room of the small one and its cursor. A session that is away
        // waits for a topic of its own.
        String large = "x".repeat(400);
        long room = cursorSize("b/#", 1)
                + PublicationMemory.sizeOf(retained("b/1", large))
                + roomOf("else", "0009")
                - 1;
        BrokerServer.Builder interfered = settings(room)
                .actors(2)
                .maxInflight(1)
                .beforeHandOver(interferingWith("subscriber"));
        try (BrokerServer sut = interfered.start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            publisher.publish(retained("held/1", "held"));
            subscriber.subscribe("held/#", AT_LEAST_ONCE);
            Packet.Publish held = subscriber.expect(Packet.Publish.class);
            leaveSubscribed(sut, "other", "else", AT_LEAST_ONCE);
            publisher.publish(retained("a/1", "0001"));
            publisher.publish(retained("b/1", large));
            subscriber.subscribe("a/#", AT_LEAST_ONCE);
            subscriber.ping();

            // Act: a subscription to the large one. Once the session has counted that the
            // small message and its cursor make the room, and before they go, a delivery for
            // the other session takes part of the room that the count relied on.
            interference = () -> publisher.publish("else", "0009", AT_LEAST_ONCE, 7);
            subscriber.subscribe("b/#", AT_LEAST_ONCE);

            // Assert: the client goes without the large one alone, and gets what waited.
            await().untilAsserted(() -> assertThat(reported).containsExactly("subscriber"));
            subscriber.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(received(subscriber, 1)).containsExactly("0001");
            subscriber.ping();
            try (RawSession back = connected(sut.port(), "other", false, true)) {
                assertThat(received(back, 1)).containsExactly("0009");
            }
        }
    }

    @Test
    void a_subscription_whose_retained_messages_found_no_room_leaves_no_room_taken()
            throws Exception {
        // Arrange: a client whose window of one holds a delivery that it does not answer, and
        // a large retained message. The memory has room for the cursor of a subscription to
        // it and a little more, but not for the message.
        long room = cursorSize("big/#", 1) + 100;
        try (BrokerServer sut = settings(room).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            publisher.publish(retained("held/1", "held"));
            subscriber.subscribe("held/#", AT_LEAST_ONCE);
            subscriber.expect(Packet.Publish.class);
            publisher.publish(retained("big/1", "x".repeat(4096)));
            leaveSubscribed(sut, "second", "next/1", AT_LEAST_ONCE);

            // Act: the subscription's only retained message cannot wait, which leaves its
            // cursor with nothing to hold.
            subscriber.subscribe("big/#", AT_LEAST_ONCE);

            // Assert: a delivery that waits for another session finds all of the room.
            await().untilAsserted(() -> assertThat(reported).containsExactly("subscriber"));
            String filling = fillingPayload("next/1", room);
            publisher.publish("next/1", filling, AT_LEAST_ONCE, 7);
            try (RawSession back = connected(sut.port(), "second", false, true)) {
                assertThat(received(back, 1)).containsExactly(filling);
            }
            assertThat(reported).containsExactly("subscriber");
        }
    }

    @Test
    void retained_messages_give_their_room_back_as_their_subscription_ends() throws Exception {
        // Arrange: memory for no more than a retained message that waits with room for a
        // client behind another that holds its window, and the cursor of its subscription.
        long room = waitingRetainedRoom();
        try (BrokerServer sut = settings(room).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            leaveRetainedWaiting(publisher, subscriber);
            leaveSubscribed(sut, "second", "next/1", AT_LEAST_ONCE);

            // Act
            subscriber.send(new Packet.Unsubscribe(2, List.of(new TopicFilter("kept/#")), EMPTY));
            subscriber.expect(Packet.UnsubAck.class);

            // Assert: a delivery that waits for another session finds all of the room.
            String filling = fillingPayload("next/1", room);
            publisher.publish("next/1", filling, AT_LEAST_ONCE, 7);
            try (RawSession back = connected(sut.port(), "second", false, true)) {
                assertThat(received(back, 1)).containsExactly(filling);
            }
            assertThat(reported).isEmpty();
        }
    }

    @Test
    void retained_messages_give_their_room_back_as_their_session_ends() throws Exception {
        // Arrange: memory for no more than a retained message that waits with room for a
        // client behind another that holds its window, and the cursor of its subscription.
        long room = waitingRetainedRoom();
        try (BrokerServer sut = settings(room).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher")) {
            try (RawSession subscriber = connected(sut.port(), "subscriber")) {
                leaveRetainedWaiting(publisher, subscriber);
                leaveSubscribed(sut, "second", "next/1", AT_LEAST_ONCE);

                // Act: the client's session ends with its connection.
                subscriber.disconnect();
            }

            // Assert: a delivery that waits for another session finds all of the room.
            String filling = fillingPayload("next/1", room);
            publisher.publish("next/1", filling, AT_LEAST_ONCE, 7);
            try (RawSession back = connected(sut.port(), "second", false, true)) {
                assertThat(received(back, 1)).containsExactly(filling);
            }
            assertThat(reported).isEmpty();
        }
    }

    @Test
    void a_subscription_goes_on_without_the_retained_messages_that_found_no_room_to_wait()
            throws Exception {
        // Arrange: three retained messages and a window of one. The memory has room for the
        // two that come to wait, but is a byte short of what the cursor that holds them
        // needs for itself.
        long room = cursorSize("kept/#", 3) - 1;
        try (BrokerServer sut = settings(room).maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            for (int number = 1; number <= 3; number++) {
                publisher.publish(retained("kept/" + number, "000" + number));
            }

            // Act
            subscriber.subscribe("kept/#", AT_LEAST_ONCE);

            // Assert: the client gets the one that its window took and goes without the two
            // others, and its subscription goes on.
            await().untilAsserted(
                    () -> assertThat(reported).containsExactly("subscriber", "subscriber")
            );
            assertThat(received(subscriber, 1)).containsExactly("0001");
            publisher.publish("kept/1", "live", AT_LEAST_ONCE, 7);
            assertThat(received(subscriber, 1)).containsExactly("live");
            subscriber.ping();
        }
    }

    @Test
    void a_subscription_made_again_takes_the_room_of_the_retained_messages_that_it_pushes_out()
            throws Exception {
        // Arrange: a broker that retains one message, which is as many as may wait for a
        // session, and a client whose window of one holds a delivery that it does not answer
        // while a second one waits. The memory has room for that delivery and for one
        // waiting retained message with its cursor.
        long room = roomOf("live", "0002")
                + cursorSize("kept/#", 1)
                + PublicationMemory.sizeOf(retained("kept/1", "kept"));
        BrokerServer.Builder limited = settings(room).maxInflight(1).maxRetained(1);
        try (BrokerServer sut = limited.start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            subscriber.subscribe("live", AT_LEAST_ONCE);
            publisher.publish("live", "0001", AT_LEAST_ONCE, 7);
            Packet.Publish held = subscriber.expect(Packet.Publish.class);
            publisher.publish("live", "0002", AT_LEAST_ONCE, 7);
            publisher.publish(retained("kept/1", "kept"));
            subscriber.subscribe("kept/#", AT_LEAST_ONCE);

            // Act: the same subscription again. Its retained message pushes out the one that
            // waited for the first, whose cursor then holds nothing.
            subscriber.subscribe("kept/#", AT_LEAST_ONCE);

            // Assert: the client lost the copy that was pushed out and no other, so the second
            // cursor found the room that the first one had, and its message arrives.
            await().untilAsserted(() -> assertThat(reported).containsExactly("subscriber"));
            subscriber.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(received(subscriber, 2)).containsExactly("0002", "kept");
            subscriber.ping();
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

    /// The room that leaveRetainedWaiting() leaves taken: that of the retained message that
    /// waits, and that of the cursor of the subscription to the two.
    private static long waitingRetainedRoom() {
        return cursorSize("kept/#", 2) + PublicationMemory.sizeOf(retained("kept/2", "0002"));
    }

    /// A payload with which one delivery of a publication to `topic` takes exactly `room`.
    private static String fillingPayload(String topic, long room) {
        return "x".repeat((int) (room - roomOf(topic, "")));
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

    /// What a broker runs before a session hands room over, for a test that has other sessions
    /// act at that moment: for the session of `clientId`, once, what the test has set as
    /// `interference` by then. The broker's thread waits for it under that session's inbox, so
    /// it must not need that session.
    private Consumer<String> interferingWith(String clientId) {
        return client -> {
            if (!client.equals(clientId)) {
                return;
            }
            Interference action = interference;
            interference = () -> { };
            try {
                action.run();
            } catch (Exception failure) {
                throw new IllegalStateException(failure);
            }
        };
    }

    /// Waits for `released` on a thread of the broker, which nothing interrupts while a test
    /// runs.
    private static void awaitUninterruptibly(CountDownLatch released) {
        try {
            released.await();
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }

    /// What other sessions do in between, which may fail as the clients of a test do.
    @FunctionalInterface
    private interface Interference {

        void run() throws Exception;
    }
}
