package org.mqtt4j.hato.broker.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;
import org.mqtt4j.hato.broker.internal.common.value.Topic;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.broker.internal.RawSession.codec;
import static org.mqtt4j.hato.broker.internal.RawSession.connectV5;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.RawSession.subscription;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.EXACTLY_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

/// MQTT 5.0 connections, and deliveries between clients of either protocol version.
class BrokerMqtt5Test {

    // Every kind of publication property that a client may send, with a repeated user property.
    private static final Properties PROPERTIES = new Properties(List.of(
            new Property.ByteValue(PropertyId.PAYLOAD_FORMAT_INDICATOR, 1),
            new Property.FourByteValue(PropertyId.MESSAGE_EXPIRY_INTERVAL, 3600),
            new Property.StringValue(PropertyId.CONTENT_TYPE, "text/plain"),
            new Property.StringValue(PropertyId.RESPONSE_TOPIC, "replies/1"),
            new Property.BinaryValue(PropertyId.CORRELATION_DATA, new byte[] {1, 2}),
            new Property.UserProperty("origin", "first"),
            new Property.UserProperty("origin", "second")
    ));

    @Test
    void an_mqtt_5_connect_is_accepted_with_an_mqtt_5_connack() throws Exception {
        try (
                var sut = MqttBroker.start();
                var client = RawSession.open(sut.port(), MqttVersion.V5)
        ) {
            // Act
            client.send(connectV5("accepted", true, 0, 0));

            // Assert: only the MQTT 5.0 form, which has a reason code, decodes here.
            assertThat(client.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(false, SUCCESS, EMPTY));
            client.ping();
            client.disconnect();
        }
    }

    @Test
    void a_malformed_mqtt_5_connect_closes_the_connection_without_connack() throws Exception {
        // Arrange: MQTT 5.0 forbids the Remaining Length below, which is not minimally encoded.
        byte[] bytes = HexFormat.of().parseHex("108e0000044d5154540502003c00000163");
        try (
                var sut = MqttBroker.start();
                var client = RawSession.open(sut.port(), MqttVersion.V5)
        ) {
            // Act
            client.sendRaw(bytes);

            // Assert
            client.expectClosed();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("refusals")
    void refused_connect_requests_get_an_mqtt_5_connack_with_the_reason(Refusal refusal)
            throws Exception {
        try (
                var sut = MqttBroker.start();
                var client = RawSession.open(sut.port(), MqttVersion.V5)
        ) {
            // Act
            client.send(refusal.request());

            // Assert
            assertThat(client.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(false, refusal.reason(), EMPTY));
            client.expectClosed();
        }
    }

    static Stream<Refusal> refusals() {
        var will = new Packet.Will(new Topic("will"), new byte[] {1}, AT_MOST_ONCE, false, EMPTY);
        var authentication = new Properties(List.of(
                new Property.StringValue(PropertyId.AUTHENTICATION_METHOD, "SCRAM-SHA-256")
        ));
        return Stream.of(
                new Refusal(
                        "a Will",
                        new Packet.Connect(
                                "with-will",
                                MqttVersion.V5,
                                true,
                                0,
                                will,
                                null,
                                null,
                                EMPTY
                        ),
                        ReasonCode.SERVER_UNAVAILABLE
                ),
                new Refusal(
                        "an authentication method",
                        new Packet.Connect(
                                "with-authentication",
                                MqttVersion.V5,
                                true,
                                0,
                                null,
                                null,
                                null,
                                authentication
                        ),
                        ReasonCode.BAD_AUTHENTICATION_METHOD
                )
        );
    }

    @Test
    void a_positive_session_expiry_keeps_the_session_for_clean_start_zero() throws Exception {
        try (var sut = MqttBroker.start(); var publisher = connectedV5(sut.port(), "publisher")) {
            // Arrange
            try (var first = connectedV5(sut.port(), "kept", false, 60, false)) {
                first.subscribe("kept/#", AT_LEAST_ONCE);
                first.disconnect();
            }
            publisher.publish("kept/topic", "while offline", AT_LEAST_ONCE, 1);

            // Act
            try (var resumed = connectedV5(sut.port(), "kept", false, 60, true)) {
                // Assert
                Packet.Publish delivery = resumed.expect(Packet.Publish.class);
                assertThat(delivery).isEqualTo(publication(
                        "kept/topic",
                        "while offline",
                        AT_LEAST_ONCE,
                        delivery.packetId()
                ));
            }
        }
    }

    @Test
    void clean_start_with_a_positive_session_expiry_replaces_the_kept_session() throws Exception {
        try (var sut = MqttBroker.start(); var publisher = connectedV5(sut.port(), "publisher")) {
            // Arrange
            try (var first = connectedV5(sut.port(), "replaced", false, 60, false)) {
                first.subscribe("old/#", AT_LEAST_ONCE);
                first.disconnect();
            }

            // Act
            try (var replacement = connectedV5(sut.port(), "replaced", true, 60, false)) {
                replacement.disconnect();
            }

            // Assert: the new session was kept, without the old subscription.
            publisher.publish("old/topic", "not delivered", AT_LEAST_ONCE, 1);
            try (var resumed = connectedV5(sut.port(), "replaced", false, 60, true)) {
                resumed.ping();
            }
        }
    }

    @Test
    void zero_session_expiry_ends_a_resumed_session_with_its_connection() throws Exception {
        try (var sut = MqttBroker.start()) {
            // Arrange
            try (var first = connectedV5(sut.port(), "ending", false, 60, false)) {
                first.disconnect();
            }

            // Act
            try (var resumed = connectedV5(sut.port(), "ending", false, 0, true)) {
                resumed.disconnect();
            }

            // Assert
            try (var next = connectedV5(sut.port(), "ending", false, 60, false)) {
                next.ping();
            }
        }
    }

    @Test
    void a_session_continues_only_with_the_protocol_version_that_created_it() throws Exception {
        try (var sut = MqttBroker.start()) {
            // Arrange
            try (var first = connectedV5(sut.port(), "versioned", false, 60, false)) {
                first.disconnect();
            }

            // Act
            try (var other = connected(sut.port(), "versioned", false, false)) {
                other.disconnect();
            }

            // Assert: the MQTT 3.1.1 connection made its own session.
            try (var resumed = connected(sut.port(), "versioned", false, true)) {
                resumed.ping();
            }
        }
    }

    @Test
    void the_session_limit_counts_only_sessions_kept_after_their_connection() throws Exception {
        try (
                BrokerServer sut = BrokerServer.builder().maxSessions(1).start(loopback());
                var ending = connectedV5(sut.port(), "ending", false, 0, false);
                var kept = connectedV5(sut.port(), "kept", true, 60, false);
                var refused = RawSession.open(sut.port(), MqttVersion.V5)
        ) {
            // Act
            refused.send(connectV5("refused", true, 0, 60));

            // Assert
            assertThat(refused.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(false, ReasonCode.SERVER_UNAVAILABLE, EMPTY));
            refused.expectClosed();
            ending.ping();
            kept.ping();
        }
    }

    @Test
    void an_empty_identifier_is_assigned_and_returned_in_connack() throws Exception {
        try (var sut = MqttBroker.start()) {
            // Arrange
            String assigned;
            try (var client = RawSession.open(sut.port(), MqttVersion.V5)) {
                // Act
                client.send(connectV5("", false, 0, 60));

                // Assert
                Packet.ConnAck connAck = client.expect(Packet.ConnAck.class);
                assertThat(connAck.reasonCode()).isEqualTo(SUCCESS);
                assigned = connAck.properties().find(PropertyId.ASSIGNED_CLIENT_IDENTIFIER)
                        .map(property -> ((Property.StringValue) property).value())
                        .orElseThrow();
                client.disconnect();
            }
            // The assigned identifier names the kept session.
            try (var resumed = connectedV5(sut.port(), assigned, false, 60, true)) {
                resumed.ping();
            }
        }
    }

    @ParameterizedTest(name = "from {0} to {1}")
    @CsvSource({"V3_1_1, V3_1_1", "V3_1_1, V5", "V5, V3_1_1", "V5, V5"})
    void deliveries_keep_properties_only_from_one_mqtt_5_client_to_another(
            MqttVersion from,
            MqttVersion to
    ) throws Exception {
        try (
                var sut = MqttBroker.start();
                var subscriber = clean(sut.port(), "subscriber", to);
                var publisher = clean(sut.port(), "publisher", from)
        ) {
            // Arrange
            subscriber.subscribe("properties/#", AT_LEAST_ONCE);
            Properties sent = (from == MqttVersion.V5) ? PROPERTIES : EMPTY;

            // Act
            publish(publisher, "properties/topic", sent);

            // Assert: the connections stay open whatever the versions.
            Packet.Publish delivery = subscriber.expect(Packet.Publish.class);
            assertThat(delivery).isEqualTo(publicationWith(
                    "properties/topic",
                    (to == MqttVersion.V5) ? sent : EMPTY,
                    delivery.packetId()
            ));
            publisher.ping();
            subscriber.ping();
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void queued_deliveries_keep_properties_only_for_a_resumed_mqtt_5_session(
            MqttVersion version
    ) throws Exception {
        try (var sut = MqttBroker.start(); var publisher = connectedV5(sut.port(), "publisher")) {
            // Arrange
            try (var subscriber = kept(sut.port(), "subscriber", version, false)) {
                subscriber.subscribe("queued/#", AT_LEAST_ONCE);
                subscriber.disconnect();
            }
            publish(publisher, "queued/topic", PROPERTIES);

            // Act
            try (var resumed = kept(sut.port(), "subscriber", version, true)) {
                // Assert
                Packet.Publish delivery = resumed.expect(Packet.Publish.class);
                assertThat(delivery).isEqualTo(publicationWith(
                        "queued/topic",
                        (version == MqttVersion.V5) ? PROPERTIES : EMPTY,
                        delivery.packetId()
                ));
                resumed.ping();
                publisher.ping();
            }
        }
    }

    @Test
    void a_delivery_is_sized_without_properties_that_its_mqtt_3_1_1_receiver_never_gets()
            throws Exception {
        // Arrange: the window fits the MQTT 3.1.1 form of the delivery exactly.
        long size = codec.encode(publication("sized/topic", "payload", AT_LEAST_ONCE, 1)).length;
        try (
                BrokerServer sut = BrokerServer.builder().maxInflightBytes(size).start(loopback());
                var subscriber = connected(sut.port(), "subscriber");
                var publisher = connectedV5(sut.port(), "publisher")
        ) {
            subscriber.subscribe("sized/#", AT_LEAST_ONCE);

            // Act
            publish(publisher, "sized/topic", PROPERTIES);

            // Assert
            Packet.Publish delivery = subscriber.expect(Packet.Publish.class);
            assertThat(delivery).isEqualTo(
                    publication("sized/topic", "payload", AT_LEAST_ONCE, delivery.packetId())
            );
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("violations")
    void a_violation_closes_the_connection_without_forwarding_anything(Packet violation)
            throws Exception {
        try (
                var sut = MqttBroker.start();
                var subscriber = connectedV5(sut.port(), "subscriber");
                var client = connectedV5(sut.port(), "violating")
        ) {
            // Arrange
            subscriber.subscribe("violation/#", AT_MOST_ONCE);

            // Act
            client.send(violation);

            // Assert
            client.expectClosed();
            subscriber.ping();
        }
    }

    static Stream<Named<Packet>> violations() {
        byte[] payload = "payload".getBytes(UTF_8);
        var alias = new Properties(List.of(new Property.TwoByteValue(PropertyId.TOPIC_ALIAS, 1)));
        var identifier = new Properties(List.of(
                new Property.VariableByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER, 1)
        ));
        var topic = new Topic("violation/topic");
        return Stream.of(
                Named.of(
                        "a Topic Alias",
                        new Packet.Publish(topic, payload, AT_MOST_ONCE, false, false, 0, alias)
                ),
                Named.of(
                        "a topic given only by its alias",
                        new Packet.AliasPublish(payload, AT_MOST_ONCE, false, false, 0, alias)
                ),
                Named.of(
                        "a Subscription Identifier",
                        new Packet.Publish(
                                topic,
                                payload,
                                AT_MOST_ONCE,
                                false,
                                false,
                                0,
                                identifier
                        )
                ),
                Named.of("AUTH", new Packet.Auth(SUCCESS, EMPTY)),
                Named.of("a second CONNECT", connectV5("violating", true, 0, 0))
        );
    }

    @Test
    void mqtt_5_acknowledgements_report_each_subscription_and_unsubscription() throws Exception {
        try (var sut = MqttBroker.start(); var client = connectedV5(sut.port(), "acknowledged")) {
            // Act
            client.send(new Packet.Subscribe(
                    1,
                    List.of(
                            subscription("a/#", AT_LEAST_ONCE),
                            subscription("b/#", EXACTLY_ONCE),
                            subscription("c/#", AT_MOST_ONCE)
                    ),
                    EMPTY
            ));
            Packet.SubAck subAck = client.expect(Packet.SubAck.class);
            client.send(new Packet.Unsubscribe(
                    2,
                    List.of(
                            new TopicFilter("a/#"),
                            new TopicFilter("never/#"),
                            new TopicFilter("a/#")
                    ),
                    EMPTY
            ));
            Packet.UnsubAck unsubAck = client.expect(Packet.UnsubAck.class);

            // Assert
            assertThat(subAck.reasonCodes()).containsExactly(
                    ReasonCode.GRANTED_QOS_1,
                    ReasonCode.GRANTED_QOS_2,
                    SUCCESS
            );
            assertThat(unsubAck.reasonCodes()).containsExactly(
                    SUCCESS,
                    ReasonCode.NO_SUBSCRIPTION_EXISTED,
                    ReasonCode.NO_SUBSCRIPTION_EXISTED
            );
        }
    }

    @Test
    void a_refused_pubrec_ends_the_exchange_without_pubrel() throws Exception {
        try (
                BrokerServer sut = BrokerServer.builder().maxInflight(1).start(loopback());
                var subscriber = connectedV5(sut.port(), "subscriber");
                var publisher = connectedV5(sut.port(), "publisher")
        ) {
            // Arrange: the second delivery waits for the only place in the window.
            subscriber.subscribe("refused/#", EXACTLY_ONCE);
            publisher.publish("refused/topic", "first", EXACTLY_ONCE, 1);
            publisher.publish("refused/topic", "second", EXACTLY_ONCE, 2);
            Packet.Publish first = subscriber.expect(Packet.Publish.class);

            // Act
            subscriber.send(new Packet.PubRec(
                    first.packetId(),
                    ReasonCode.UNSPECIFIED_ERROR,
                    EMPTY
            ));

            // Assert: the freed window carries the next delivery instead of a PUBREL.
            Packet.Publish second = subscriber.expect(Packet.Publish.class);
            assertThat(second).isEqualTo(
                    publication("refused/topic", "second", EXACTLY_ONCE, second.packetId())
            );
        }
    }

    record Refusal(String name, Packet.Connect request, ReasonCode reason) {

        @Override
        public String toString() {
            return name;
        }
    }

    private static Packet.Publish publicationWith(
            String topic,
            Properties properties,
            int packetId
    ) {
        return new Packet.Publish(
                new Topic(topic),
                "payload".getBytes(UTF_8),
                AT_LEAST_ONCE,
                false,
                false,
                packetId,
                properties
        );
    }

    private static void publish(RawSession publisher, String topic, Properties properties)
            throws Exception {
        publisher.send(publicationWith(topic, properties, 1));
        assertThat(publisher.expect(Packet.PubAck.class))
                .isEqualTo(new Packet.PubAck(1, SUCCESS, EMPTY));
    }

    private static RawSession clean(int port, String clientId, MqttVersion version)
            throws Exception {
        return switch (version) {
            case V3_1_1 -> connected(port, clientId);
            case V5 -> connectedV5(port, clientId);
        };
    }

    private static RawSession kept(
            int port,
            String clientId,
            MqttVersion version,
            boolean sessionPresent
    ) throws Exception {
        return switch (version) {
            case V3_1_1 -> connected(port, clientId, false, sessionPresent);
            case V5 -> connectedV5(port, clientId, false, 60, sessionPresent);
        };
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }
}
