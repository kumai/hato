package org.mqtt4j.hato.broker.internal;

import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.broker.internal.common.io.FrameReader;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;
import org.mqtt4j.hato.broker.internal.common.value.Topic;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.broker.internal.BrokerSessionRaceTest.awaitRelease;
import static org.mqtt4j.hato.broker.internal.RawSession.acceptedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.codec;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.RawSession.subscription;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.EXACTLY_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

/// The Will: published when its connection ends without a normal DISCONNECT, retained when it
/// asks to be, and refused when MQTT 5.0 asks to delay it.
class BrokerWillTest {

    private static final String WILL_TOPIC = "will";

    @ParameterizedTest
    @EnumSource(Qos.class)
    void closing_the_network_connection_publishes_the_will(Qos qos) throws Exception {
        try (var sut = MqttBroker.start();
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            // Arrange
            subscriber.subscribe(WILL_TOPIC, EXACTLY_ONCE);
            RawSession client = connectedWithWill(sut.port(), "closing", will("gone", qos, false));

            // Act
            client.close();

            // Assert
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(delivery("gone", qos, false));
        }
    }

    @Test
    void an_expired_keep_alive_publishes_the_will() throws Exception {
        var timer = new BrokerDeadlineTest.ManualTimer();
        try (BrokerServer sut = BrokerServer.builder().scheduler(timer).start(loopback());
                RawSession subscriber = connected(sut.port(), "subscriber");
                var client = RawSession.open(sut.port())) {
            // Arrange: a keep-alive of one second ends the connection after 1.5 silent seconds.
            subscriber.subscribe(WILL_TOPIC, EXACTLY_ONCE);
            client.send(connect(MqttVersion.V3_1_1, "silent", 1, will("silent", AT_LEAST_ONCE)));
            client.expect(Packet.ConnAck.class);

            // Act
            timer.clock.advance(Duration.ofMillis(1500));

            // Assert
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(delivery("silent", AT_LEAST_ONCE, false));
        }
    }

    @Test
    void a_protocol_violation_publishes_the_will() throws Exception {
        try (var sut = MqttBroker.start();
                RawSession subscriber = connected(sut.port(), "subscriber");
                RawSession client = connectedWithWill(
                        sut.port(),
                        "violating",
                        will("violated", AT_LEAST_ONCE, false)
                )) {
            // Arrange
            subscriber.subscribe(WILL_TOPIC, EXACTLY_ONCE);

            // Act: a second CONNECT breaks the protocol.
            client.send(connect(MqttVersion.V3_1_1, "violating", 0, null));

            // Assert
            client.expectClosed();
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(delivery("violated", AT_LEAST_ONCE, false));
        }
    }

    @Test
    void a_connection_taken_over_publishes_its_will_after_the_connack_of_the_new_one()
            throws Exception {
        try (var sut = MqttBroker.start(); var first = RawSession.open(sut.port())) {
            // Arrange: the persistent session subscribes to its own Will.
            first.send(new Packet.Connect(
                    "owner",
                    MqttVersion.V3_1_1,
                    false,
                    0,
                    will("replaced", AT_LEAST_ONCE, false),
                    null,
                    null,
                    EMPTY
            ));
            first.expect(Packet.ConnAck.class);
            first.subscribe(WILL_TOPIC, AT_LEAST_ONCE);

            // Act
            try (RawSession second = connected(sut.port(), "owner", false, true)) {
                // Assert: the session resumes, and then receives the Will of its old connection.
                assertThat(second.expect(Packet.Publish.class))
                        .isEqualTo(delivery("replaced", AT_LEAST_ONCE, false));
                first.expectClosed();
            }
        }
    }

    @Test
    void the_will_of_a_taken_over_connection_precedes_what_the_new_connection_publishes()
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession subscriber = connected(sut.port(), "subscriber");
                var first = RawSession.open(sut.port())) {
            // Arrange
            subscriber.subscribe(WILL_TOPIC, EXACTLY_ONCE);
            first.send(connect(MqttVersion.V3_1_1, "owner", 0, will("replaced", AT_LEAST_ONCE)));
            first.expect(Packet.ConnAck.class);

            // Act: a new connection takes the identifier over and publishes without waiting for
            // CONNACK, as MQTT allows.
            try (var second = RawSession.open(sut.port())) {
                second.send(connect(MqttVersion.V3_1_1, "owner", 0, null));
                second.send(publication(WILL_TOPIC, "after", AT_MOST_ONCE, 0));
                second.expect(Packet.ConnAck.class);

                // Assert: the broker publishes the Will as it accepts the new connection.
                assertThat(subscriber.expect(Packet.Publish.class))
                        .isEqualTo(delivery("replaced", AT_LEAST_ONCE, false));
                assertThat(subscriber.expect(Packet.Publish.class))
                        .isEqualTo(publication(WILL_TOPIC, "after", AT_MOST_ONCE, 0));
                first.expectClosed();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_normal_disconnect_discards_the_will(MqttVersion version) throws Exception {
        try (var sut = MqttBroker.start();
                RawSession subscriber = connected(sut.port(), "subscriber");
                var client = RawSession.open(sut.port(), version)) {
            // Arrange
            subscriber.subscribe(WILL_TOPIC, EXACTLY_ONCE);
            client.send(connect(version, "leaving", 0, will("left", AT_LEAST_ONCE)));
            client.expect(Packet.ConnAck.class);

            // Act
            client.disconnect();

            // Assert: the broker discarded the Will before it closed the connection.
            subscriber.ping();
        }
    }

    @Test
    @SuppressWarnings("try") // The client resets its connection itself, before the cleanup.
    void a_disconnect_that_arrived_before_a_write_failed_discards_the_will() throws Exception {
        var armed = new AtomicBoolean();
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (
                BrokerServer sut = BrokerServer.builder()
                        .beforeDeliveryWrite(() -> {
                            if (armed.compareAndSet(true, false)) {
                                held.countDown();
                                awaitRelease(release);
                            }
                        })
                        .start(loopback());
                RawSession subscriber = connected(sut.port(), "subscriber");
                var leaving = new Socket(InetAddress.getLoopbackAddress(), sut.port());
                RawSession publisher = connected(sut.port(), "publisher")
        ) {
            // Arrange: the broker is about to write a delivery to the client that leaves.
            subscriber.subscribe(WILL_TOPIC, EXACTLY_ONCE);
            leaving.setSoTimeout(5_000);
            var reader = new FrameReader(leaving.getInputStream(), 1024);
            leaving.getOutputStream().write(codec.encode(
                    connect(MqttVersion.V3_1_1, "leaving", 0, will("left", AT_LEAST_ONCE))
            ));
            assertThat(codec.decode(reader.read().orElseThrow()))
                    .isInstanceOf(Packet.ConnAck.class);
            leaving.getOutputStream().write(codec.encode(
                    new Packet.Subscribe(1, List.of(subscription("feed", AT_MOST_ONCE)), EMPTY)
            ));
            assertThat(codec.decode(reader.read().orElseThrow())).isInstanceOf(Packet.SubAck.class);
            armed.set(true);
            publisher.publish("feed", "held", AT_MOST_ONCE, 0);
            awaitRelease(held);

            // Act: DISCONNECT and then a reset reach the broker, whose write now fails. The
            // round trip of another client gives the reset the time to arrive.
            leaving.getOutputStream().write(codec.encode(new Packet.Disconnect(SUCCESS, EMPTY)));
            leaving.setSoLinger(true, 0);
            leaving.close();
            publisher.ping();
            release.countDown();

            // Assert: the broker discarded the Will before the connection ended.
            await().until(() -> !sut.connectedClients().contains("leaving"));
            subscriber.ping();
        } finally {
            release.countDown();
        }
    }

    @Test
    @SuppressWarnings("try") // The client resets its connection itself, before the cleanup.
    void a_disconnect_behind_packets_that_waited_when_a_write_failed_discards_the_will()
            throws Exception {
        int count = 20;
        var armed = new AtomicBoolean();
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var stalling = new AtomicBoolean(true);
        var stalled = new CountDownLatch(1);
        var resume = new CountDownLatch(1);
        var decoded = new AtomicInteger();
        try (
                BrokerServer sut = BrokerServer.builder()
                        .beforeDeliveryWrite(() -> {
                            if (armed.compareAndSet(true, false)) {
                                held.countDown();
                                awaitRelease(release);
                            }
                        })
                        .beforeProcess(packet -> {
                            if (isBurst(packet)) {
                                decoded.incrementAndGet();
                            }
                        })
                        .beforeHandle(packet -> {
                            if (isBurst(packet) && stalling.compareAndSet(true, false)) {
                                stalled.countDown();
                                awaitRelease(resume);
                            }
                        })
                        .start(loopback());
                RawSession subscriber = connected(sut.port(), "subscriber");
                var leaving = new Socket(InetAddress.getLoopbackAddress(), sut.port());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession observer = connected(sut.port(), "observer")
        ) {
            // Arrange: the broker is about to write a delivery to the client that leaves.
            subscriber.subscribe(WILL_TOPIC, EXACTLY_ONCE);
            observer.subscribe("burst", AT_MOST_ONCE);
            leaving.setSoTimeout(5_000);
            var reader = new FrameReader(leaving.getInputStream(), 1024);
            leaving.getOutputStream().write(codec.encode(
                    connect(MqttVersion.V3_1_1, "leaving", 0, will("left", AT_LEAST_ONCE))
            ));
            assertThat(codec.decode(reader.read().orElseThrow()))
                    .isInstanceOf(Packet.ConnAck.class);
            leaving.getOutputStream().write(codec.encode(
                    new Packet.Subscribe(1, List.of(subscription("feed", AT_MOST_ONCE)), EMPTY)
            ));
            assertThat(codec.decode(reader.read().orElseThrow())).isInstanceOf(Packet.SubAck.class);
            armed.set(true);
            publisher.publish("feed", "held", AT_MOST_ONCE, 0);
            awaitRelease(held);

            // Act: more publications than a session lets wait, DISCONNECT, and then a reset
            // reach the broker, whose write now fails. The session stalls on the first
            // publication, so one of the others waits for it, with DISCONNECT behind.
            var burst = new ByteArrayOutputStream();
            for (int number = 1; number <= count; number++) {
                burst.writeBytes(codec.encode(
                        publication("burst", Integer.toString(number), AT_MOST_ONCE, 0)
                ));
            }
            burst.writeBytes(codec.encode(new Packet.Disconnect(SUCCESS, EMPTY)));
            leaving.getOutputStream().write(burst.toByteArray());
            leaving.setSoLinger(true, 0);
            leaving.close();
            publisher.ping();
            release.countDown();
            awaitRelease(stalled);
            await().until(() -> decoded.get() >= 17);
            resume.countDown();

            // Assert: every publication went on, and the broker discarded the Will.
            for (int number = 1; number <= count; number++) {
                Packet.Publish delivery = observer.expect(Packet.Publish.class);
                assertThat(new String(delivery.payload(), UTF_8))
                        .isEqualTo(Integer.toString(number));
            }
            await().until(() -> !sut.connectedClients().contains("leaving"));
            subscriber.ping();
        } finally {
            release.countDown();
            resume.countDown();
        }
    }

    @Test
    void a_disconnect_before_a_packet_that_breaks_the_protocol_discards_the_will()
            throws Exception {
        try (
                BrokerServer sut = BrokerServer.builder()
                        .beforeHandle(packet -> {
                            if (packet instanceof Packet.Publish publish
                                    && publish.topic().name().equals("ahead")) {
                                // The selector reads on while the session handles what came
                                // before DISCONNECT.
                                LockSupport.parkNanos(Duration.ofMillis(100).toNanos());
                            }
                        })
                        .start(loopback());
                RawSession subscriber = connected(sut.port(), "subscriber");
                RawSession client = connectedWithWill(
                        sut.port(),
                        "leaving",
                        will("left", AT_LEAST_ONCE, false)
                )
        ) {
            // Arrange
            subscriber.subscribe(WILL_TOPIC, EXACTLY_ONCE);

            // Act: a publication, DISCONNECT, and a packet of a reserved type, in one write.
            var bytes = new ByteArrayOutputStream();
            bytes.writeBytes(codec.encode(publication("ahead", "first", AT_MOST_ONCE, 0)));
            bytes.writeBytes(codec.encode(new Packet.Disconnect(SUCCESS, EMPTY)));
            bytes.writeBytes(new byte[] {0, 0});
            client.sendRaw(bytes.toByteArray());

            // Assert: the connection ends, and the broker discarded the Will.
            client.expectClosed();
            await().until(() -> !sut.connectedClients().contains("leaving"));
            subscriber.ping();
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = ReasonCode.class,
            names = {"DISCONNECT_WITH_WILL_MESSAGE", "UNSPECIFIED_ERROR"}
    )
    void an_mqtt_5_disconnect_with_another_reason_code_publishes_the_will(ReasonCode reason)
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession subscriber = connected(sut.port(), "subscriber");
                var client = RawSession.open(sut.port(), MqttVersion.V5)) {
            // Arrange
            subscriber.subscribe(WILL_TOPIC, EXACTLY_ONCE);
            client.send(connect(MqttVersion.V5, "leaving", 0, will("left", AT_LEAST_ONCE)));
            assertThat(client.expect(Packet.ConnAck.class)).isEqualTo(acceptedV5(false));

            // Act
            client.send(new Packet.Disconnect(reason, EMPTY));

            // Assert
            client.expectClosed();
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(delivery("left", AT_LEAST_ONCE, false));
        }
    }

    @Test
    void a_retained_will_reaches_current_subscribers_and_stays_for_later_ones() throws Exception {
        try (var sut = MqttBroker.start();
                RawSession current = connected(sut.port(), "current")) {
            // Arrange
            current.subscribe(WILL_TOPIC, EXACTLY_ONCE);
            RawSession client = connectedWithWill(
                    sut.port(),
                    "retaining",
                    will("retained", AT_LEAST_ONCE, true)
            );

            // Act
            client.close();

            // Assert: like any retained publication, with RETAIN 0 for the current subscriber.
            assertThat(current.expect(Packet.Publish.class))
                    .isEqualTo(delivery("retained", AT_LEAST_ONCE, false));
            try (RawSession later = connected(sut.port(), "later")) {
                later.subscribe(WILL_TOPIC, AT_LEAST_ONCE);
                assertThat(later.expect(Packet.Publish.class))
                        .isEqualTo(delivery("retained", AT_LEAST_ONCE, true));
            }
        }
    }

    @Test
    void an_empty_retained_will_removes_the_retained_message() throws Exception {
        try (var sut = MqttBroker.start();
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession current = connected(sut.port(), "current")) {
            // Arrange
            publisher.publish(new Packet.Publish(
                    new Topic(WILL_TOPIC),
                    "kept".getBytes(UTF_8),
                    AT_LEAST_ONCE,
                    true,
                    false,
                    1,
                    EMPTY
            ));
            current.subscribe(WILL_TOPIC, AT_LEAST_ONCE);
            assertThat(current.expect(Packet.Publish.class).retain()).isTrue();
            RawSession client = connectedWithWill(
                    sut.port(),
                    "clearing",
                    will("", AT_MOST_ONCE, true)
            );

            // Act
            client.close();

            // Assert
            assertThat(current.expect(Packet.Publish.class))
                    .isEqualTo(delivery("", AT_MOST_ONCE, false));
            try (RawSession later = connected(sut.port(), "later")) {
                later.subscribe(WILL_TOPIC, AT_LEAST_ONCE);
                later.ping();
            }
        }
    }

    @Test
    void an_mqtt_5_will_carries_its_properties_without_the_delay() throws Exception {
        var properties = new Properties(List.of(
                new Property.ByteValue(PropertyId.PAYLOAD_FORMAT_INDICATOR, 1),
                new Property.UserProperty("first", "1"),
                new Property.FourByteValue(PropertyId.WILL_DELAY_INTERVAL, 0),
                new Property.StringValue(PropertyId.CONTENT_TYPE, "text/plain"),
                new Property.UserProperty("second", "2")
        ));
        try (var sut = MqttBroker.start();
                RawSession current = connectedV5(sut.port(), "current");
                RawSession older = connected(sut.port(), "older")) {
            // Arrange: a Will Delay Interval of zero publishes the Will at once.
            current.subscribe(WILL_TOPIC, AT_LEAST_ONCE);
            older.subscribe(WILL_TOPIC, AT_LEAST_ONCE);
            var client = RawSession.open(sut.port(), MqttVersion.V5);
            client.send(connect(
                    MqttVersion.V5,
                    "propertied",
                    0,
                    new Packet.Will(
                            new Topic(WILL_TOPIC),
                            "text".getBytes(UTF_8),
                            AT_LEAST_ONCE,
                            false,
                            properties
                    )
            ));
            assertThat(client.expect(Packet.ConnAck.class)).isEqualTo(acceptedV5(false));

            // Act
            client.close();

            // Assert: MQTT 3.1.1 subscribers receive none of the properties.
            assertThat(current.expect(Packet.Publish.class).properties())
                    .isEqualTo(new Properties(List.of(
                            new Property.ByteValue(PropertyId.PAYLOAD_FORMAT_INDICATOR, 1),
                            new Property.UserProperty("first", "1"),
                            new Property.StringValue(PropertyId.CONTENT_TYPE, "text/plain"),
                            new Property.UserProperty("second", "2")
                    )));
            assertThat(older.expect(Packet.Publish.class).properties()).isEqualTo(EMPTY);
        }
    }

    private static Packet.Will will(String payload, Qos qos, boolean retain) {
        return new Packet.Will(new Topic(WILL_TOPIC), payload.getBytes(UTF_8), qos, retain, EMPTY);
    }

    private static Packet.Will will(String payload, Qos qos) {
        return will(payload, qos, false);
    }

    /// A CONNECT for a session that ends with its connection.
    private static Packet.Connect connect(
            MqttVersion version,
            String clientId,
            int keepAlive,
            Packet.@Nullable Will will
    ) {
        return new Packet.Connect(clientId, version, true, keepAlive, will, null, null, EMPTY);
    }

    private static RawSession connectedWithWill(int port, String clientId, Packet.Will will)
            throws Exception {
        RawSession session = RawSession.open(port);
        try {
            session.send(connect(MqttVersion.V3_1_1, clientId, 0, will));
            assertThat(session.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(false, SUCCESS, EMPTY));
            return session;
        } catch (Exception | AssertionError failure) {
            session.close();
            throw failure;
        }
    }

    /// The Will as the first delivery to a subscriber whose QoS is at least the Will's.
    private static Packet.Publish delivery(String payload, Qos qos, boolean retain) {
        return new Packet.Publish(
                new Topic(WILL_TOPIC),
                payload.getBytes(UTF_8),
                qos,
                retain,
                false,
                (qos == AT_MOST_ONCE) ? 0 : 1,
                EMPTY
        );
    }

    private static boolean isBurst(Packet packet) {
        return packet instanceof Packet.Publish publish
                && publish.topic().name().equals("burst");
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }
}
