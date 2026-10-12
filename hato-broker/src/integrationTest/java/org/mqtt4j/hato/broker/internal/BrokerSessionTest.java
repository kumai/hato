package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.broker.internal.RawSession.codec;
import static org.mqtt4j.hato.broker.internal.RawSession.connect;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SERVER_UNAVAILABLE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

class BrokerSessionTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("try") // Explicit close distinguishes EOF from MQTT DISCONNECT.
    void persistent_subscriptions_survive_disconnect_and_eof(boolean graceful) throws Exception {
        try (var sut = MqttBroker.start();
                RawSession publisher = connected(sut.port(), "publisher")) {
            try (RawSession previous = connected(sut.port(), "saved", false, false)) {
                previous.subscribe("saved/#", AT_LEAST_ONCE);
                if (graceful) {
                    disconnect(previous);
                } else {
                    previous.close();
                }
            }

            try (RawSession resumed = connected(sut.port(), "saved", false, true)) {
                publisher.publish("saved/topic", "after reconnect", AT_LEAST_ONCE, 7);
                acknowledge(resumed, expectDelivery(resumed, "saved/topic", "after reconnect"));
                resumed.ping();
            }
        }
    }

    @Test
    void offline_deliveries_keep_qos_one_order_and_discard_effective_qos_zero() throws Exception {
        try (BrokerServer sut = broker(2);
                RawSession publisher = connected(sut.port(), "publisher")) {
            try (RawSession previous = connected(sut.port(), "offline", false, false)) {
                previous.subscribe("offline/#", AT_LEAST_ONCE);
                previous.subscribe("zero/#", AT_MOST_ONCE);
                disconnect(previous);
            }
            publisher.publish("offline/topic", "first", AT_LEAST_ONCE, 7);
            publisher.publish("offline/topic", "discard qos zero", AT_MOST_ONCE, 0);
            publisher.publish("zero/topic", "discard after downgrade", AT_LEAST_ONCE, 7);
            publisher.publish("offline/topic", "second", AT_LEAST_ONCE, 7);

            try (RawSession resumed = connected(sut.port(), "offline", false, true)) {
                Packet.Publish first = expectDelivery(resumed, "offline/topic", "first");
                Packet.Publish second = expectDelivery(resumed, "offline/topic", "second");
                assertThat(second.packetId()).isNotEqualTo(first.packetId());
                acknowledge(resumed, first);
                acknowledge(resumed, second);
                // A PINGRESP must be next: neither discarded publication may have been retained.
                resumed.ping();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 128})
    void a_full_window_replays_before_queued_and_new_deliveries_without_closing(int window)
            throws Exception {
        try (BrokerServer sut = broker(window);
                RawSession publisher = connected(sut.port(), "publisher")) {
            var original = new ArrayList<Packet.Publish>();
            try (RawSession previous = connected(sut.port(), "ordered", false, false)) {
                previous.subscribe("ordered", AT_LEAST_ONCE);
                for (int i = 0; i < window; i++) {
                    String payload = "inflight-" + i;
                    publisher.publish("ordered", payload, AT_LEAST_ONCE, 7);
                    original.add(expectDelivery(previous, "ordered", payload));
                }
                assertThat(original).extracting(Packet.Publish::packetId).doesNotHaveDuplicates();
                publisher.publish("ordered", "queued-first", AT_LEAST_ONCE, 7);
                publisher.publish("ordered", "queued-second", AT_LEAST_ONCE, 7);
                previous.ping();
                disconnect(previous);
            }

            // connected() consumes CONNACK first, before any resumed publication.
            try (RawSession resumed = connected(sut.port(), "ordered", false, true)) {
                var outstanding = new ArrayDeque<Packet.Publish>();
                for (Packet.Publish previous : original) {
                    Packet.Publish replay = resumed.expect(Packet.Publish.class);
                    assertThat(replay).isEqualTo(new Packet.Publish(
                            previous.topic(), previous.payload(), previous.qos(), false, true,
                            previous.packetId(), EMPTY
                    ));
                    outstanding.addLast(replay);
                }
                publisher.publish("ordered", "new-after-reconnect", AT_LEAST_ONCE, 7);
                // A full resumed window accepts control traffic without resending it again.
                resumed.ping();
                for (String payload : List.of(
                        "queued-first", "queued-second", "new-after-reconnect"
                )) {
                    acknowledge(resumed, outstanding.removeFirst());
                    Packet.Publish next = expectDelivery(resumed, "ordered", payload);
                    assertThat(outstanding).extracting(Packet.Publish::packetId)
                            .doesNotContain(next.packetId());
                    outstanding.addLast(next);
                }
                for (Packet.Publish pending : outstanding) {
                    acknowledge(resumed, pending);
                }
                resumed.ping();
            }
        }
    }

    @Test
    void acknowledgement_releases_the_byte_window_before_the_count_window_is_full()
            throws Exception {
        long bytes = codec.encode(publication("bytes", "payload", AT_LEAST_ONCE, 1)).length;
        try (BrokerServer sut = broker(2, bytes, 2, 1024, 8);
                RawSession subscriber = connected(sut.port(), "subscriber", false, false);
                RawSession publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("bytes", AT_LEAST_ONCE);
            publisher.publish("bytes", "payload", AT_LEAST_ONCE, 7);
            Packet.Publish first = expectDelivery(subscriber, "bytes", "payload");
            publisher.publish("bytes", "payload", AT_LEAST_ONCE, 7);
            subscriber.ping();

            acknowledge(subscriber, first);

            acknowledge(subscriber, expectDelivery(subscriber, "bytes", "payload"));
            subscriber.ping();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void an_idle_send_window_accepts_a_delivery_larger_than_the_pending_byte_limit(
            boolean acknowledged
    ) throws Exception {
        var qos = acknowledged ? AT_LEAST_ONCE : AT_MOST_ONCE;
        try (BrokerServer sut = broker(1, 1024, 1, 1, 8);
                RawSession subscriber = connected(sut.port(), "subscriber", false, false);
                RawSession publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("direct", AT_LEAST_ONCE);
            for (String payload : List.of("first", "second")) {
                publisher.publish("direct", payload, qos, acknowledged ? 7 : 0);

                Packet.Publish delivery = subscriber.expect(Packet.Publish.class);
                assertThat(delivery)
                        .isEqualTo(publication("direct", payload, qos, delivery.packetId()));
                if (acknowledged) {
                    assertThat(delivery.packetId()).isPositive();
                    acknowledge(subscriber, delivery);
                }
                subscriber.ping();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void overflowing_count_or_bytes_discards_only_the_target_session(
            boolean offline,
            boolean byteLimit
    ) throws Exception {
        long bytes = codec.encode(publication("bounded", "0001", AT_LEAST_ONCE, 1)).length;
        try (BrokerServer sut = broker(1, 1024, byteLimit ? 16 : 2,
                byteLimit ? 2 * bytes : 1024, 8);
                RawSession previous = connected(sut.port(), "bounded-client", false, false);
                RawSession unaffected = connected(sut.port(), "unaffected");
                RawSession publisher = connected(sut.port(), "publisher")) {
            previous.subscribe("bounded", AT_LEAST_ONCE);
            unaffected.subscribe("bounded", AT_MOST_ONCE);
            if (offline) {
                disconnect(previous);
            } else {
                publisher.publish("bounded", "held", AT_LEAST_ONCE, 7);
                expectDelivery(previous, "bounded", "held");
                assertThat(unaffected.expect(Packet.Publish.class))
                        .isEqualTo(publication("bounded", "held", AT_MOST_ONCE, 0));
            }
            for (String payload : List.of("0001", "0002")) {
                publisher.publish("bounded", payload, AT_LEAST_ONCE, 7);
                assertThat(unaffected.expect(Packet.Publish.class))
                        .isEqualTo(publication("bounded", payload, AT_MOST_ONCE, 0));
            }
            if (!offline) {
                previous.ping();
            }

            publisher.publish("bounded", "0003", AT_LEAST_ONCE, 7);

            if (!offline) {
                previous.expectClosed();
            }
            assertThat(unaffected.expect(Packet.Publish.class))
                    .isEqualTo(publication("bounded", "0003", AT_MOST_ONCE, 0));
            try (RawSession replacement = connected(sut.port(), "bounded-client", false, false)) {
                publisher.publish("bounded", "after discard", AT_LEAST_ONCE, 7);
                assertThat(unaffected.expect(Packet.Publish.class))
                        .isEqualTo(publication("bounded", "after discard", AT_MOST_ONCE, 0));
                replacement.ping();
            }
            unaffected.ping();
            publisher.ping();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_delivery_that_can_never_fit_the_window_discards_the_session(boolean offline)
            throws Exception {
        try (BrokerServer sut = broker(2, 32, 4, 1024, 8);
                RawSession previous = connected(sut.port(), "oversized", false, false);
                RawSession publisher = connected(sut.port(), "publisher")) {
            previous.subscribe("oversized", AT_LEAST_ONCE);
            if (offline) {
                disconnect(previous);
            }

            // This fits the pending queue and socket queue, but never the inflight byte window.
            publisher.publish("oversized", "x".repeat(64), AT_LEAST_ONCE, 7);

            if (!offline) {
                previous.expectClosed();
            }
            try (RawSession replacement = connected(sut.port(), "oversized", false, false)) {
                publisher.publish("oversized", "small", AT_LEAST_ONCE, 7);
                replacement.ping();
            }
            publisher.ping();
        }
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void takeover_preserves_only_a_persistent_session_resumed_without_cleaning(
            boolean previousClean,
            boolean replacementClean
    ) throws Exception {
        try (BrokerServer sut = broker(1);
                RawSession previous = connected(sut.port(), "same-id", previousClean, false);
                RawSession publisher = connected(sut.port(), "publisher")) {
            previous.subscribe("old", AT_LEAST_ONCE);
            publisher.publish("old", "inflight", AT_LEAST_ONCE, 7);
            Packet.Publish inflight = expectDelivery(previous, "old", "inflight");
            publisher.publish("old", "queued", AT_LEAST_ONCE, 7);
            previous.ping();
            boolean retained = !previousClean && !replacementClean;

            try (RawSession replacement = connected(
                    sut.port(), "same-id", replacementClean, retained
            )) {
                previous.expectClosed();
                if (retained) {
                    assertThat(replacement.expect(Packet.Publish.class))
                            .isEqualTo(new Packet.Publish(
                                    inflight.topic(), inflight.payload(), AT_LEAST_ONCE,
                                    false, true, inflight.packetId(), EMPTY
                            ));
                    acknowledge(replacement, inflight);
                    acknowledge(replacement, expectDelivery(replacement, "old", "queued"));
                }
                replacement.ping();
                publisher.publish("old", "after takeover", AT_LEAST_ONCE, 7);
                if (retained) {
                    acknowledge(replacement,
                            expectDelivery(replacement, "old", "after takeover"));
                }
                replacement.ping();
                // Old reader/writer cleanup must not remove the replacement's new subscriptions.
                replacement.subscribe("new", AT_MOST_ONCE);
                publisher.publish("new", "still registered", AT_LEAST_ONCE, 7);
                assertThat(replacement.expect(Packet.Publish.class))
                        .isEqualTo(publication("new", "still registered", AT_MOST_ONCE, 0));
            }
        }
    }

    @Test
    void takeover_discards_queued_qos_zero_without_losing_qos_one() throws Exception {
        try (BrokerServer sut = broker(1);
                RawSession previous = connected(sut.port(), "same-id", false, false);
                RawSession publisher = connected(sut.port(), "publisher")) {
            previous.subscribe("takeover/#", AT_LEAST_ONCE);
            previous.subscribe("zero/#", AT_MOST_ONCE);
            publisher.publish("takeover/topic", "inflight", AT_LEAST_ONCE, 7);
            Packet.Publish inflight = expectDelivery(previous, "takeover/topic", "inflight");
            publisher.publish("takeover/topic", "queued", AT_LEAST_ONCE, 7);
            publisher.publish("takeover/topic", "discard qos zero", AT_MOST_ONCE, 0);
            publisher.publish("zero/topic", "discard after downgrade", AT_LEAST_ONCE, 7);
            previous.ping();

            try (RawSession replacement = connected(sut.port(), "same-id", false, true)) {
                previous.expectClosed();
                assertThat(replacement.expect(Packet.Publish.class)).isEqualTo(new Packet.Publish(
                        inflight.topic(), inflight.payload(), AT_LEAST_ONCE,
                        false, true, inflight.packetId(), EMPTY
                ));
                acknowledge(replacement, inflight);
                acknowledge(replacement,
                        expectDelivery(replacement, "takeover/topic", "queued"));
                // The previous connection's queued QoS 0 deliveries must not follow the replay.
                replacement.ping();
                publisher.publish("takeover/topic", "after takeover", AT_LEAST_ONCE, 7);
                acknowledge(replacement,
                        expectDelivery(replacement, "takeover/topic", "after takeover"));
                replacement.ping();
            }
        }
    }

    @Test
    void the_session_limit_allows_resumption_and_clean_replacement_returns_its_slot()
            throws Exception {
        try (BrokerServer sut = broker(2, 1024, 4, 1024, 1);
                RawSession previous = connected(sut.port(), "retained", false, false)) {
            assertPersistentRefused(sut.port(), "new-id");
            previous.ping();
            try (RawSession resumed = connected(sut.port(), "retained", false, true)) {
                previous.expectClosed();
                resumed.ping();
                disconnect(resumed);
            }
            assertPersistentRefused(sut.port(), "new-id");
            try (RawSession clean = connected(sut.port(), "unrelated-clean")) {
                clean.ping();
            }
            try (RawSession clearing = connected(sut.port(), "retained", true, false);
                    RawSession accepted = connected(sut.port(), "new-id", false, false)) {
                clearing.ping();
                accepted.ping();
            }
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit broker shutdown precedes rebinding its listening port.
    void restarting_on_the_same_port_discards_persistent_state() throws Exception {
        try (var sut = MqttBroker.start();
                RawSession previous = connected(sut.port(), "restart", false, false)) {
            previous.subscribe("restart", AT_LEAST_ONCE);
            disconnect(previous);
            previous.close();
            var address = new InetSocketAddress(InetAddress.getLoopbackAddress(), sut.port());

            sut.close();

            // close() does not wait for a blocked accept to release the native listener.
            try (var restarted = await().atMost(Duration.ofSeconds(5))
                        .ignoreException(BindException.class)
                        .until(() -> MqttBroker.start(address), value -> value != null);
                    RawSession replacement = connected(restarted.port(), "restart", false, false);
                    RawSession publisher = connected(restarted.port(), "publisher")) {
                publisher.publish("restart", "discarded subscription", AT_LEAST_ONCE, 7);
                replacement.ping();
                replacement.subscribe("restart", AT_LEAST_ONCE);
                publisher.publish("restart", "new subscription", AT_LEAST_ONCE, 7);
                acknowledge(replacement,
                        expectDelivery(replacement, "restart", "new subscription"));
                replacement.ping();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void credentials_are_accepted_without_authentication(boolean password) throws Exception {
        try (var sut = MqttBroker.start(); var client = RawSession.open(sut.port())) {
            client.send(new Packet.Connect(
                    "credentials", MqttVersion.V3_1_1, true, 0, null, "unvalidated-user",
                    password ? new byte[] {0, (byte) 0xFF} : null, EMPTY
            ));

            assertThat(client.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(false, SUCCESS, EMPTY));
            client.ping();
        }
    }

    private static BrokerServer broker(int maxInflight) throws IOException {
        return broker(maxInflight, 64 * 1024, 16, 64 * 1024, 16);
    }

    private static BrokerServer broker(
            int maxInflight,
            long maxInflightBytes,
            int maxQueued,
            long maxQueuedBytes,
            int maxSessions
    ) throws IOException {
        return BrokerServer.builder()
                .connectTimeout(Duration.ofSeconds(5))
                .sendBytes(64 * 1024)
                .maxInflight(maxInflight)
                .maxInflightBytes(maxInflightBytes)
                .maxQueued(maxQueued)
                .maxQueuedBytes(maxQueuedBytes)
                .maxSessions(maxSessions)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }

    private static Packet.Publish expectDelivery(RawSession receiver, String topic, String payload)
            throws Exception {
        Packet.Publish delivery = receiver.expect(Packet.Publish.class);
        assertThat(delivery.packetId()).isPositive();
        assertThat(delivery)
                .isEqualTo(publication(topic, payload, AT_LEAST_ONCE, delivery.packetId()));
        return delivery;
    }

    private static void acknowledge(RawSession receiver, Packet.Publish publication)
            throws IOException {
        receiver.send(new Packet.PubAck(publication.packetId(), SUCCESS, EMPTY));
    }

    private static void disconnect(RawSession connection) throws Exception {
        connection.send(new Packet.Disconnect(SUCCESS, EMPTY));
        connection.expectClosed();
    }

    private static void assertPersistentRefused(int port, String clientId) throws Exception {
        try (var refused = RawSession.open(port)) {
            refused.send(connect(clientId, false, 0));
            assertThat(refused.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(false, SERVER_UNAVAILABLE, EMPTY));
            refused.expectClosed();
        }
    }
}
