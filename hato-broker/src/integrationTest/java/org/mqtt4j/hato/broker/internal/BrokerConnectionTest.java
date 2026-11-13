package org.mqtt4j.hato.broker.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;
import org.mqtt4j.hato.broker.internal.common.value.Topic;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.broker.internal.BrokerSessionRaceTest.awaitRelease;
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
            // close() has released the listening port when it returns.
            try (
                    var restarted = MqttBroker.start(sameAddress);
                    RawSession next = connected(restarted.port(), "restarted")
            ) {
                next.ping();
            }
        }
    }

    @Test
    void close_returns_once_every_selector_has_closed_its_connections() throws Exception {
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (
                BrokerServer sut = BrokerServer.builder().beforeProcess(packet -> {
                    if (packet instanceof Packet.PingReq) {
                        held.countDown();
                        awaitRelease(release);
                    }
                }).start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                RawSession client = connected(sut.port(), "client");
                ExecutorService closer = Executors.newSingleThreadExecutor()
        ) {
            // Arrange: the selector of the client's connection stops on its PINGREQ.
            int port = sut.port();
            client.send(new Packet.PingReq());
            awaitRelease(held);

            // Act
            Future<?> closing = closer.submit(sut::close);

            // Assert: close() waits for that selector, which has yet to close the connection.
            assertThatThrownBy(() -> closing.get(200, MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            release.countDown();
            closing.get(5, SECONDS);
            assertThat(selectors(port)).isZero();
            client.expectClosed();
        } finally {
            release.countDown();
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

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void requests_sent_faster_than_their_responses_are_read_wait_without_closing_the_connection(
            MqttVersion version
    ) throws Exception {
        // Arrange: the broker announces no Receive Maximum, so a client may send this many QoS 1
        // publications at once, far more than the broker queues responses for. On one carrier,
        // the broker's reader handles the whole burst before its writer runs, so the responses
        // fill the queue; with more carriers the writer usually keeps up.
        var burst = new ByteArrayOutputStream();
        for (int id = 1; id <= 2000; id++) {
            burst.writeBytes(PacketCodec.of(version).encode(
                    publication("burst", "x", AT_LEAST_ONCE, id)
            ));
        }
        try (var sut = MqttBroker.start();
                RawSession client = version == MqttVersion.V5
                        ? RawSession.connectedV5(sut.port(), "burst")
                        : connected(sut.port(), "burst")) {

            // Act
            client.sendRaw(burst.toByteArray());

            // Assert: every publication gets its PUBACK, in order, on the same connection.
            for (int id = 1; id <= 2000; id++) {
                assertThat(client.expect(Packet.PubAck.class).packetId()).isEqualTo(id);
            }
            client.ping();
        }
    }

    @Test
    void publications_by_topic_alias_sent_faster_than_their_responses_are_read_wait_too()
            throws Exception {
        // Arrange: the first publication sets the alias, and the others carry it alone.
        var alias = new Properties(List.of(new Property.TwoByteValue(PropertyId.TOPIC_ALIAS, 1)));
        var v5 = PacketCodec.of(MqttVersion.V5);
        var burst = new ByteArrayOutputStream();
        burst.writeBytes(v5.encode(new Packet.Publish(
                new Topic("burst"),
                new byte[] {'x'},
                AT_LEAST_ONCE,
                false,
                false,
                1,
                alias
        )));
        for (int id = 2; id <= 2000; id++) {
            burst.writeBytes(v5.encode(new Packet.AliasPublish(
                    new byte[] {'x'},
                    AT_LEAST_ONCE,
                    false,
                    false,
                    id,
                    alias
            )));
        }
        try (
                var sut = MqttBroker.start();
                var client = RawSession.connectedV5(sut.port(), "burst")
        ) {
            // Act
            client.sendRaw(burst.toByteArray());

            // Assert
            for (int id = 1; id <= 2000; id++) {
                assertThat(client.expect(Packet.PubAck.class).packetId()).isEqualTo(id);
            }
            client.ping();
        }
    }

    @Test
    void all_selectors_keep_serving_through_repeated_disconnects() throws Exception {
        try (var sut = MqttBroker.start()) {
            long selectors = selectors(sut.port());
            assertThat(selectors).isPositive();
            for (int i = 0; i < 2_000; i++) {
                try (RawSession client = connected(sut.port(), "churn")) {
                    client.send(new Packet.Disconnect(SUCCESS, EMPTY));
                }
            }
            for (long i = 0; i < selectors; i++) {
                try (RawSession client = connected(sut.port(), "after-churn")) {
                    client.ping();
                }
            }
            assertThat(selectors(sut.port())).isEqualTo(selectors);
        }
    }

    @Test
    void a_broker_with_one_processor_serves_with_fewer_threads() throws Exception {
        var address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        try (
                BrokerServer sut = BrokerServer.start(address, 1);
                RawSession subscriber = connected(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession other = connected(sut.port(), "other")
        ) {
            // Act: three connections share the selectors, and their sessions the pool.
            subscriber.subscribe("news", AT_LEAST_ONCE);
            publisher.publish("news", "first", AT_LEAST_ONCE, 1);
            Packet.Publish delivery = subscriber.expect(Packet.Publish.class);
            subscriber.send(new Packet.PubAck(delivery.packetId(), SUCCESS, EMPTY));
            other.ping();
            subscriber.ping();

            // Assert
            assertThat(delivery.topic().name()).isEqualTo("news");
            assertThat(threads(sut.port(), "selector-")).isEqualTo(2);
            assertThat(threads(sut.port(), "actor-")).isEqualTo(1);
        }
    }

    @Test
    void connections_take_no_threads_of_their_own() throws Exception {
        try (var sut = MqttBroker.start()) {
            // Arrange: the broker's threads have all started with it.
            int port = sut.port();
            long before = threads(port);

            // Act
            var clients = new ArrayList<RawSession>();
            try {
                for (int number = 0; number < 50; number++) {
                    RawSession client = connected(port, "client-" + number);
                    clients.add(client);
                    client.ping();
                }

                // Assert
                assertThat(threads(port)).isEqualTo(before);
            } finally {
                for (RawSession client : clients) {
                    client.close();
                }
            }
        }
    }

    @Test
    void a_client_that_half_closes_can_read_the_brokers_final_packet() throws Exception {
        var v5 = PacketCodec.of(MqttVersion.V5);
        try (
                var sut = MqttBroker.start();
                var client = new Socket(InetAddress.getLoopbackAddress(), sut.port())
        ) {
            client.setSoTimeout(5_000);
            client.getOutputStream().write(
                    v5.encode(RawSession.connectV5("half-close", true, 0, 0))
            );
            byte[] connAck = v5.encode(RawSession.acceptedV5(false));
            assertThat(client.getInputStream().readNBytes(connAck.length)).isEqualTo(connAck);

            // PINGRESP from the client is a protocol error detected on its actor.
            client.getOutputStream().write(v5.encode(new Packet.PingResp()));
            client.shutdownOutput();

            byte[] disconnect = v5.encode(new Packet.Disconnect(ReasonCode.PROTOCOL_ERROR, EMPTY));
            assertThat(client.getInputStream().readNBytes(disconnect.length))
                    .isEqualTo(disconnect);
            assertThat(client.getInputStream().read()).isEqualTo(-1);
        }
    }

    @Test
    void a_connection_that_another_thread_ends_stays_open_until_its_selector_closes_it()
            throws Exception {
        var selectorOf = new ConcurrentHashMap<String, Thread>();
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (
                BrokerServer sut = BrokerServer.builder().beforeProcess(packet -> {
                    if (packet instanceof Packet.Connect connect) {
                        selectorOf.put(connect.clientId(), Thread.currentThread());
                    } else if (packet instanceof Packet.PingReq) {
                        held.countDown();
                        awaitRelease(release);
                    }
                }).start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                RawSession holder = connected(sut.port(), "holder")
        ) {
            // Arrange: connections go to the selectors in turn, so after one for each other
            // selector the next shares the holder's, which the holder's PINGREQ then stops.
            long selectors = selectors(sut.port());
            for (long i = 1; i < selectors; i++) {
                connected(sut.port(), "filler").close();
            }
            try (var victim = new Socket(InetAddress.getLoopbackAddress(), sut.port())) {
                victim.setSoTimeout(5_000);
                victim.getOutputStream().write(codec.encode(connect("victim", true, 0)));
                byte[] connAck = codec.encode(new Packet.ConnAck(false, SUCCESS, EMPTY));
                assertThat(victim.getInputStream().readNBytes(connAck.length)).isEqualTo(connAck);
                assertThat(selectorOf.get("victim")).isSameAs(selectorOf.get("holder"));
                holder.send(new Packet.PingReq());
                awaitRelease(held);

                // Act: the calling thread ends the connection, as a timer or an actor does.
                sut.disconnect("victim");

                // Assert: the peer sees the end only once the selector closes the channel.
                assertThat(sut.connectedClients()).doesNotContain("victim");
                victim.setSoTimeout(200);
                assertThatThrownBy(() -> victim.getInputStream().read())
                        .isInstanceOf(SocketTimeoutException.class);
                release.countDown();
                victim.setSoTimeout(5_000);
                assertThat(victim.getInputStream().read()).isEqualTo(-1);
                holder.expect(Packet.PingResp.class);
            }
        } finally {
            release.countDown();
        }
    }

    @Test
    void an_error_in_the_work_of_one_connection_ends_that_connection_alone() throws Exception {
        try (
                BrokerServer sut = BrokerServer.builder().beforeProcess(packet -> {
                    if (packet instanceof Packet.Publish publish
                            && publish.topic().name().equals("failing")) {
                        throw new AssertionError("injected failure in one connection");
                    }
                }).start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                RawSession failing = connected(sut.port(), "failing");
                RawSession other = connected(sut.port(), "other")
        ) {
            // Arrange
            long selectors = selectors(sut.port());

            // Act
            failing.send(publication("failing", "", AT_MOST_ONCE, 0));

            // Assert: new connections go to the selectors in turn, so each selector serves one.
            failing.expectClosed();
            other.ping();
            for (long i = 0; i < selectors; i++) {
                try (RawSession client = connected(sut.port(), "after-failure")) {
                    client.ping();
                }
            }
            assertThat(selectors(sut.port())).isEqualTo(selectors);
        }
    }

    @Test
    void a_failed_selector_closes_the_listener_and_its_peers() throws Exception {
        var address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        var failing = new AtomicBoolean();
        try (
                BrokerServer sut = BrokerServer.builder().beforeSelect(() -> {
                    if (failing.compareAndSet(true, false)) {
                        throw new AssertionError("injected selector failure");
                    }
                }).start(address);
                RawSession client = connected(sut.port(), "trigger");
                RawSession other = connected(sut.port(), "other")
        ) {
            int port = sut.port();
            failing.set(true);
            // A publication without a response wakes a selector, which fails before it waits.
            client.send(publication("unheard", "", AT_MOST_ONCE, 0));

            client.expectClosed();
            other.expectClosed();
            await().until(() -> selectors(port) == 0);
            var sameAddress = new InetSocketAddress(address.getAddress(), port);
            try (var listener = MqttBroker.start(sameAddress)) {
                assertThat(listener.port()).isEqualTo(port);
            }
        }
    }

    @Test
    void an_interrupted_selector_ends_and_closes_the_listener_and_its_peers() throws Exception {
        var address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        try (
                var sut = MqttBroker.start(address);
                RawSession client = connected(sut.port(), "connected")
        ) {
            // Arrange
            int port = sut.port();
            String name = "hato-broker-" + port + "-selector-0";
            Thread selector = Thread.getAllStackTraces().keySet().stream()
                    .filter(thread -> thread.getName().equals(name))
                    .findFirst()
                    .orElseThrow();

            // Act
            selector.interrupt();

            // Assert: no selector is left, which an interrupted one that went on would be.
            client.expectClosed();
            await().until(() -> selectors(port) == 0);
            var sameAddress = new InetSocketAddress(address.getAddress(), port);
            try (var listener = MqttBroker.start(sameAddress)) {
                assertThat(listener.port()).isEqualTo(port);
            }
        }
    }

    private static long selectors(int port) {
        return threads(port, "selector-");
    }

    // How many threads the broker on `port` has, whatever they do.
    private static long threads(int port) {
        return threads(port, "");
    }

    // How many threads of the broker on `port` have names that go on with `kind`.
    private static long threads(int port, String kind) {
        String prefix = "hato-broker-" + port + "-" + kind;
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().startsWith(prefix))
                .count();
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
