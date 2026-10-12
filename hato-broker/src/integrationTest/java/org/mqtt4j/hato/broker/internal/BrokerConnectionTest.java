package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HexFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;
import org.mqtt4j.hato.broker.internal.common.value.Topic;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.broker.internal.RawSession.codec;
import static org.mqtt4j.hato.broker.internal.RawSession.connect;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

class BrokerConnectionTest {

    @Test
    void public_api_starts_on_a_free_port_and_accepts_connect_ping_disconnect() throws Exception {
        try (var sut = MqttBroker.start(); var client = RawSession.open(sut.port())) {
            assertThat(sut.port()).isBetween(1, 65535);
            client.send(connect("lifecycle", true, 0));
            assertThat(client.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(false, SUCCESS, EMPTY));
            // [MQTT-3.12.4-1], [MQTT-3.14.4-1].
            client.ping();
            client.send(new Packet.Disconnect(SUCCESS, EMPTY));
            client.expectClosed();
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close verifies shutdown and idempotence before cleanup.
    void close_releases_connections_and_the_requested_listening_port() throws Exception {
        var address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        try (var sut = MqttBroker.start(address); var client = connected(sut.port(), "close")) {
            int port = sut.port();
            sut.close();
            sut.close();
            client.expectClosed();
            var sameAddress = new InetSocketAddress(address.getAddress(), port);
            try (var restarted = MqttBroker.start(sameAddress);
                    var next = connected(restarted.port(), "restarted")) {
                next.ping();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {3, 6})
    void unsupported_protocol_level_is_refused_before_closing(int level) throws Exception {
        byte[] bytes = codec.encode(connect("unsupported-level", true, 0));
        assertThat(bytes[8]).isEqualTo((byte) 4);
        bytes[8] = (byte) level;
        try (var sut = MqttBroker.start(); var client = RawSession.open(sut.port())) {
            client.sendRaw(bytes);
            // [MQTT-3.1.2-2]: MQTT 3.1.1 CONNACK return code 0x01.
            assertThat(client.expect(Packet.ConnAck.class)).isEqualTo(new Packet.ConnAck(
                    false, ReasonCode.UNSUPPORTED_PROTOCOL_VERSION, EMPTY
            ));
            client.expectClosed();
        }
    }

    @Test
    void mqtt_5_connect_is_refused_until_the_broker_supports_it() throws Exception {
        // The common codec accepts MQTT 5.0, but the broker still speaks MQTT 3.1.1.
        byte[] bytes = PacketCodec.of(MqttVersion.V5).encode(
                new Packet.Connect(
                        "unsupported-version",
                        MqttVersion.V5,
                        true,
                        0,
                        null,
                        null,
                        null,
                        EMPTY
                )
        );
        try (var sut = MqttBroker.start(); var client = RawSession.open(sut.port())) {
            client.sendRaw(bytes);
            assertThat(client.expect(Packet.ConnAck.class)).isEqualTo(
                    new Packet.ConnAck(false, ReasonCode.UNSUPPORTED_PROTOCOL_VERSION, EMPTY)
            );
            client.expectClosed();
        }
    }

    @Test
    void mqtt_5_connect_is_refused_by_level_even_when_its_body_is_malformed() throws Exception {
        // [MQTT-3.1.2-2]: the Remaining Length below is not minimally encoded, which only the
        // MQTT 5.0 codec rejects; the version refusal must come first.
        byte[] bytes = HexFormat.of().parseHex("108e0000044d5154540502003c00000163");
        try (var sut = MqttBroker.start(); var client = RawSession.open(sut.port())) {
            client.sendRaw(bytes);
            assertThat(client.expect(Packet.ConnAck.class)).isEqualTo(
                    new Packet.ConnAck(false, ReasonCode.UNSUPPORTED_PROTOCOL_VERSION, EMPTY)
            );
            client.expectClosed();
        }
    }

    @Test
    void the_first_packet_must_be_connect() throws Exception {
        try (var sut = MqttBroker.start(); var client = RawSession.open(sut.port())) {
            // [MQTT-3.1.0-1].
            client.send(new Packet.PingReq());
            client.expectClosed();
        }
    }

    @Test
    void a_second_connect_closes_the_connection() throws Exception {
        try (var sut = MqttBroker.start(); var client = connected(sut.port(), "twice")) {
            // [MQTT-3.1.0-2].
            client.send(connect("twice", true, 0));
            client.expectClosed();
        }
    }

    @Test
    void a_duplicate_client_id_replaces_the_connection_and_discards_old_subscriptions()
            throws Exception {
        try (var sut = MqttBroker.start(); var previous = connected(sut.port(), "same-id")) {
            previous.subscribe("old/#", AT_LEAST_ONCE);
            try (var replacement = connected(sut.port(), "same-id");
                    var publisher = connected(sut.port(), "publisher")) {
                // [MQTT-3.1.4-2], [MQTT-3.1.2-6].
                previous.expectClosed();
                publisher.publish("old/topic", "removed", AT_LEAST_ONCE, 7);
                replacement.ping();

                // Closing the previous connection must not unregister its replacement.
                replacement.subscribe("new/#", AT_MOST_ONCE);
                publisher.publish("new/topic", "delivered", AT_MOST_ONCE, 0);
                assertThat(replacement.expect(Packet.Publish.class))
                        .isEqualTo(publication("new/topic", "delivered", AT_MOST_ONCE, 0));
            }
        }
    }

    @Test
    void empty_clean_session_ids_are_assigned_distinct_connections() throws Exception {
        try (var sut = MqttBroker.start();
                var first = connected(sut.port(), "");
                var second = connected(sut.port(), "")) {
            // [MQTT-3.1.3-6]: the second anonymous client must not take over the first.
            first.ping();
            second.ping();
        }
    }

    @Test
    void clean_session_zero_creates_and_then_resumes_a_session() throws Exception {
        try (var sut = MqttBroker.start()) {
            try (RawSession first = connected(sut.port(), "persistent", false, false)) {
                first.send(new Packet.Disconnect(SUCCESS, EMPTY));
                first.expectClosed();
            }
            try (RawSession resumed = connected(sut.port(), "persistent", false, true)) {
                resumed.ping();
            }
        }
    }

    @Test
    void will_requests_are_refused_until_will_publication_is_supported() throws Exception {
        var will = new Packet.Will(new Topic("will"), new byte[] {1}, AT_MOST_ONCE, false, EMPTY);
        try (var sut = MqttBroker.start(); var client = RawSession.open(sut.port())) {
            client.send(new Packet.Connect(
                    "with-will", MqttVersion.V3_1_1, true, 0, will, null, null, EMPTY
            ));
            assertThat(client.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(false, ReasonCode.SERVER_UNAVAILABLE, EMPTY));
            client.expectClosed();
        }
    }

    @Test
    void empty_client_id_with_clean_session_zero_is_refused_as_an_invalid_identifier()
            throws Exception {
        byte[] bytes = codec.encode(connect("", true, 0));
        bytes[9] = 0;
        try (var sut = MqttBroker.start(); var client = RawSession.open(sut.port())) {
            // [MQTT-3.1.3-8]: persistence requires a nonempty identifier.
            client.sendRaw(bytes);
            assertThat(client.expect(Packet.ConnAck.class)).isEqualTo(new Packet.ConnAck(
                    false, ReasonCode.CLIENT_IDENTIFIER_NOT_VALID, EMPTY
            ));
            client.expectClosed();
        }
    }

    @Test
    void partial_connect_bytes_do_not_extend_the_connect_deadline() throws Exception {
        byte[] bytes = codec.encode(connect("slow-connect-client", true, 0));
        try (var sut = shortConnectDeadline(); var client = RawSession.open(sut.port())) {
            // Section 3.1.4: a per-read SO_TIMEOUT would stay open while these bytes arrive.
            dripUntilClosed(client, bytes, Duration.ofSeconds(2));
        }
    }

    @Test
    void partial_publish_bytes_do_not_extend_the_keep_alive_deadline() throws Exception {
        try (var sut = MqttBroker.start(); var client = RawSession.open(sut.port())) {
            client.send(connect("slow-publish", true, 1));
            client.expect(Packet.ConnAck.class);
            byte[] bytes = codec.encode(publication("slow", "x".repeat(8192), AT_MOST_ONCE, 0));
            // [MQTT-3.1.2-24]: only a complete packet resets the 1.5-second deadline.
            dripUntilClosed(client, bytes, Duration.ofMillis(2500));
        }
    }

    private static BrokerServer shortConnectDeadline() throws IOException {
        return BrokerServer.builder()
                .connectTimeout(Duration.ofMillis(600))
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }

    private static void dripUntilClosed(RawSession client, byte[] bytes, Duration deadline)
            throws Exception {
        var sent = new AtomicInteger();
        try (var executor = Executors.newSingleThreadScheduledExecutor()) {
            executor.scheduleAtFixedRate(() -> {
                int index = sent.get();
                if (index >= bytes.length) {
                    return;
                }
                try {
                    client.sendRaw(new byte[] {bytes[index]});
                    sent.incrementAndGet();
                } catch (IOException _) {
                    // The reader verifies closure; a write racing the broker's close is expected.
                }
            }, 0, 100, MILLISECONDS);
            client.expectClosed(deadline);
            assertThat(sent.get()).as("bytes sent without completing the packet")
                    .isBetween(3, bytes.length - 1);
        }
    }
}
