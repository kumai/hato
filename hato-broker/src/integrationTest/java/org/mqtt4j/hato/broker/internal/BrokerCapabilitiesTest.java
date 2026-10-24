package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.Topic;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mqtt4j.hato.broker.internal.RawSession.connect;
import static org.mqtt4j.hato.broker.internal.RawSession.connectV5;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.RawSession.subscription;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.EXACTLY_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.GRANTED_QOS_1;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.GRANTED_QOS_2;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.PACKET_TOO_LARGE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.QOS_NOT_SUPPORTED;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.RECEIVE_MAXIMUM_EXCEEDED;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.RETAIN_NOT_SUPPORTED;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.TOPIC_ALIAS_INVALID;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED;

/// The MQTT 5.0 capabilities that a test can narrow: what CONNACK announces, and how the broker
/// holds a client to it.
class BrokerCapabilitiesTest {

    @Test
    void a_lower_maximum_qos_is_announced_and_lowers_the_qos_granted_to_subscriptions()
            throws Exception {
        // Arrange: Maximum QoS 1.
        var capabilities = new Capabilities(AT_LEAST_ONCE, true, true, true, null, null, 16, null);
        try (BrokerServer sut = broker(capabilities); var client = open(sut)) {
            // Act
            Packet.ConnAck connAck = accepted(client, "limited");
            client.send(new Packet.Subscribe(
                    1,
                    List.of(subscription("limited/#", EXACTLY_ONCE)),
                    EMPTY
            ));

            // Assert: the broker still accepts a SUBSCRIBE for QoS 2 [MQTT-3.2.2-10].
            assertThat(connAck.properties().find(PropertyId.MAXIMUM_QOS))
                    .contains(new Property.ByteValue(PropertyId.MAXIMUM_QOS, 1));
            assertThat(client.expect(Packet.SubAck.class))
                    .isEqualTo(new Packet.SubAck(1, List.of(GRANTED_QOS_1), EMPTY));
        }
    }

    @Test
    void a_publication_above_the_maximum_qos_ends_the_connection_and_forwards_nothing()
            throws Exception {
        // Arrange: Maximum QoS 1.
        var capabilities = new Capabilities(AT_LEAST_ONCE, true, true, true, null, null, 16, null);
        try (BrokerServer sut = broker(capabilities); var client = open(sut)) {
            accepted(client, "limited");
            client.subscribe("limited/#", AT_LEAST_ONCE);

            // Act
            client.send(publication("limited/topic", "too high", EXACTLY_ONCE, 1));

            // Assert
            assertThat(client.expect(Packet.Disconnect.class))
                    .isEqualTo(new Packet.Disconnect(QOS_NOT_SUPPORTED, EMPTY));
            client.expectClosed();
        }
    }

    @Test
    void a_will_above_the_maximum_qos_or_retained_without_retained_messages_is_refused()
            throws Exception {
        // Arrange: Maximum QoS 1 and no retained messages.
        var capabilities = new Capabilities(AT_LEAST_ONCE, false, true, true, null, null, 16, null);
        try (
                BrokerServer sut = broker(capabilities);
                var aboveMaximum = open(sut);
                var retained = open(sut)
        ) {
            // Act
            aboveMaximum.send(withWill("above-maximum", will(EXACTLY_ONCE, false)));
            retained.send(withWill("retained", will(AT_MOST_ONCE, true)));

            // Assert [MQTT-3.2.2-12] [MQTT-3.2.2-13]
            assertThat(aboveMaximum.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(false, QOS_NOT_SUPPORTED, EMPTY));
            aboveMaximum.expectClosed();
            assertThat(retained.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(false, RETAIN_NOT_SUPPORTED, EMPTY));
            retained.expectClosed();
        }
    }

    @Test
    void without_retained_messages_a_retained_publication_ends_the_connection() throws Exception {
        // Arrange: no retained messages.
        var capabilities = new Capabilities(EXACTLY_ONCE, false, true, true, null, null, 16, null);
        try (BrokerServer sut = broker(capabilities); var client = open(sut)) {
            Packet.ConnAck connAck = accepted(client, "retaining");

            // Act
            client.send(new Packet.Publish(
                    new Topic("retained/topic"),
                    "kept".getBytes(UTF_8),
                    AT_MOST_ONCE,
                    true,
                    false,
                    0,
                    EMPTY
            ));

            // Assert
            assertThat(connAck.properties().find(PropertyId.RETAIN_AVAILABLE))
                    .contains(new Property.ByteValue(PropertyId.RETAIN_AVAILABLE, 0));
            assertThat(client.expect(Packet.Disconnect.class))
                    .isEqualTo(new Packet.Disconnect(RETAIN_NOT_SUPPORTED, EMPTY));
            client.expectClosed();
        }
    }

    @Test
    void without_wildcards_a_subscribe_with_one_ends_the_connection() throws Exception {
        // Arrange: no wildcard subscriptions.
        var capabilities = new Capabilities(EXACTLY_ONCE, true, false, true, null, null, 16, null);
        try (BrokerServer sut = broker(capabilities); var client = open(sut)) {
            Packet.ConnAck connAck = accepted(client, "wildcards");
            client.subscribe("plain/topic", AT_MOST_ONCE);

            // Act
            client.send(new Packet.Subscribe(
                    2,
                    List.of(
                            subscription("other/topic", AT_MOST_ONCE),
                            subscription("plain/+", AT_MOST_ONCE)
                    ),
                    EMPTY
            ));

            // Assert: MQTT 5.0 section 3.2.2.3.11.
            var none = new Property.ByteValue(PropertyId.WILDCARD_SUBSCRIPTION_AVAILABLE, 0);
            assertThat(connAck.properties().find(PropertyId.WILDCARD_SUBSCRIPTION_AVAILABLE))
                    .contains(none);
            assertThat(client.expect(Packet.Disconnect.class))
                    .isEqualTo(new Packet.Disconnect(WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED, EMPTY));
            client.expectClosed();
        }
    }

    @Test
    void without_subscription_identifiers_a_subscribe_with_one_ends_the_connection()
            throws Exception {
        // Arrange: no Subscription Identifiers.
        var capabilities = new Capabilities(EXACTLY_ONCE, true, true, false, null, null, 16, null);
        try (BrokerServer sut = broker(capabilities); var client = open(sut)) {
            Packet.ConnAck connAck = accepted(client, "identified");
            var identifier = new Property.VariableByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER, 1);
            var request = new Packet.Subscribe(
                    1,
                    List.of(subscription("identified/#", AT_MOST_ONCE)),
                    new Properties(List.of(identifier))
            );

            // Act
            client.send(request);

            // Assert: MQTT 5.0 section 3.2.2.3.12.
            var none = new Property.ByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER_AVAILABLE, 0);
            assertThat(connAck.properties().find(PropertyId.SUBSCRIPTION_IDENTIFIER_AVAILABLE))
                    .contains(none);
            var refusal = new Packet.Disconnect(SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED, EMPTY);
            assertThat(client.expect(Packet.Disconnect.class)).isEqualTo(refusal);
            client.expectClosed();
        }
    }

    @Test
    void more_publications_without_a_response_than_the_receive_maximum_end_the_connection()
            throws Exception {
        // Arrange: Receive Maximum 2.
        var capabilities = new Capabilities(EXACTLY_ONCE, true, true, true, 2, null, 16, null);
        try (BrokerServer sut = broker(capabilities); var client = open(sut)) {
            Packet.ConnAck connAck = accepted(client, "eager");
            // Two QoS 2 publications wait for PUBREL, and releasing one leaves room for one more.
            client.send(publication("eager/topic", "1", EXACTLY_ONCE, 1));
            client.send(publication("eager/topic", "2", EXACTLY_ONCE, 2));
            assertThat(client.expect(Packet.PubRec.class).packetId()).isEqualTo(1);
            assertThat(client.expect(Packet.PubRec.class).packetId()).isEqualTo(2);
            client.send(new Packet.PubRel(1, SUCCESS, EMPTY));
            assertThat(client.expect(Packet.PubComp.class).packetId()).isEqualTo(1);
            client.send(publication("eager/topic", "3", EXACTLY_ONCE, 3));
            assertThat(client.expect(Packet.PubRec.class).packetId()).isEqualTo(3);

            // Act
            client.send(publication("eager/topic", "4", AT_LEAST_ONCE, 4));

            // Assert: MQTT 5.0 section 3.3.4.
            assertThat(connAck.properties().find(PropertyId.RECEIVE_MAXIMUM))
                    .contains(new Property.TwoByteValue(PropertyId.RECEIVE_MAXIMUM, 2));
            assertThat(client.expect(Packet.Disconnect.class))
                    .isEqualTo(new Packet.Disconnect(RECEIVE_MAXIMUM_EXCEEDED, EMPTY));
            client.expectClosed();
        }
    }

    @Test
    void a_puback_that_a_test_holds_back_leaves_its_publication_without_a_response()
            throws Exception {
        // Arrange: Receive Maximum 1, and the client's PUBACKs held back.
        var capabilities = new Capabilities(EXACTLY_ONCE, true, true, true, 1, null, 16, null);
        try (BrokerServer sut = broker(capabilities); var client = open(sut)) {
            accepted(client, "held");
            sut.holdResponses(Packet.PubAck.class, "held");
            client.send(publication("held/topic", "1", AT_LEAST_ONCE, 1));
            client.ping();

            // Act
            client.send(publication("held/topic", "2", AT_LEAST_ONCE, 2));

            // Assert
            assertThat(client.expect(Packet.Disconnect.class))
                    .isEqualTo(new Packet.Disconnect(RECEIVE_MAXIMUM_EXCEEDED, EMPTY));
            client.expectClosed();
        }
    }

    @Test
    void a_packet_larger_than_the_maximum_packet_size_ends_the_connection() throws Exception {
        // Arrange: Maximum Packet Size 64, and publications of exactly 64 bytes and one more.
        var capabilities = new Capabilities(EXACTLY_ONCE, true, true, true, null, 64, 16, null);
        int header = PacketCodec.of(MqttVersion.V5)
                .encode(publication("sized", "", AT_MOST_ONCE, 0))
                .length;
        try (BrokerServer sut = broker(capabilities); var client = open(sut)) {
            Packet.ConnAck connAck = accepted(client, "sized");
            client.send(publication("sized", "x".repeat(64 - header), AT_MOST_ONCE, 0));
            client.ping();

            // Act
            client.send(publication("sized", "x".repeat(65 - header), AT_MOST_ONCE, 0));

            // Assert: MQTT 5.0 section 3.2.2.3.6.
            assertThat(connAck.properties().find(PropertyId.MAXIMUM_PACKET_SIZE))
                    .contains(new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, 64));
            assertThat(client.expect(Packet.Disconnect.class))
                    .isEqualTo(new Packet.Disconnect(PACKET_TOO_LARGE, EMPTY));
            client.expectClosed();
        }
    }

    @Test
    void a_lower_topic_alias_maximum_is_announced_and_bounds_the_aliases() throws Exception {
        // Arrange: Topic Alias Maximum 2.
        var capabilities = new Capabilities(EXACTLY_ONCE, true, true, true, null, null, 2, null);
        try (BrokerServer sut = broker(capabilities); var client = open(sut)) {
            Packet.ConnAck connAck = accepted(client, "aliasing");
            client.send(aliased(2));
            client.ping();

            // Act
            client.send(aliased(3));

            // Assert
            assertThat(connAck.properties().find(PropertyId.TOPIC_ALIAS_MAXIMUM))
                    .contains(new Property.TwoByteValue(PropertyId.TOPIC_ALIAS_MAXIMUM, 2));
            assertThat(client.expect(Packet.Disconnect.class))
                    .isEqualTo(new Packet.Disconnect(TOPIC_ALIAS_INVALID, EMPTY));
            client.expectClosed();
        }
    }

    @Test
    void a_topic_alias_maximum_of_zero_is_not_announced_and_allows_no_alias() throws Exception {
        // Arrange: no Topic Aliases.
        var capabilities = new Capabilities(EXACTLY_ONCE, true, true, true, null, null, 0, null);
        try (BrokerServer sut = broker(capabilities); var client = open(sut)) {
            Packet.ConnAck connAck = accepted(client, "unaliased");

            // Act
            client.send(aliased(1));

            // Assert: an absent maximum means 0 [MQTT-3.2.2-18].
            assertThat(connAck.properties().find(PropertyId.TOPIC_ALIAS_MAXIMUM)).isEmpty();
            assertThat(client.expect(Packet.Disconnect.class))
                    .isEqualTo(new Packet.Disconnect(TOPIC_ALIAS_INVALID, EMPTY));
            client.expectClosed();
        }
    }

    @Test
    void narrowed_capabilities_leave_mqtt_3_1_1_connections_as_they_were() throws Exception {
        // Arrange: every capability narrowed, which MQTT 3.1.1 cannot announce.
        var capabilities = new Capabilities(AT_MOST_ONCE, false, false, false, 1, 64, 0, 1);
        try (BrokerServer sut = broker(capabilities); var client = RawSession.open(sut.port())) {
            client.send(connect("unaffected", true, 0));
            assertThat(client.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(false, SUCCESS, EMPTY));

            // Act
            client.send(new Packet.Subscribe(
                    1,
                    List.of(subscription("unaffected/#", EXACTLY_ONCE)),
                    EMPTY
            ));
            assertThat(client.expect(Packet.SubAck.class))
                    .isEqualTo(new Packet.SubAck(1, List.of(GRANTED_QOS_2), EMPTY));
            client.send(new Packet.Publish(
                    new Topic("elsewhere/topic"),
                    "x".repeat(100).getBytes(UTF_8),
                    EXACTLY_ONCE,
                    true,
                    false,
                    1,
                    EMPTY
            ));
            client.send(publication("elsewhere/other", "second", EXACTLY_ONCE, 2));

            // Assert: a large, retained QoS 2 publication is accepted, and so is a second one
            // before the first is released.
            assertThat(client.expect(Packet.PubRec.class).packetId()).isEqualTo(1);
            assertThat(client.expect(Packet.PubRec.class).packetId()).isEqualTo(2);
            client.ping();
        }
    }

    @ParameterizedTest(name = "Receive Maximum {0}, Maximum Packet Size {1}, "
            + "Topic Alias Maximum {2}, Server Keep Alive {3}")
    @CsvSource({
            "0,     ,  16,",
            "65536, ,  16,",
            ",      0, 16,",
            ",      ,  -1,",
            ",      ,  65536,",
            ",      ,  16, -1",
            ",      ,  16, 65536"
    })
    void a_capability_that_mqtt_5_cannot_announce_is_refused(
            @Nullable Integer receiveMaximum,
            @Nullable Integer maximumPacketSize,
            int topicAliasMaximum,
            @Nullable Integer serverKeepAlive
    ) {
        assertThatThrownBy(() -> new Capabilities(
                EXACTLY_ONCE,
                true,
                true,
                true,
                receiveMaximum,
                maximumPacketSize,
                topicAliasMaximum,
                serverKeepAlive
        )).isInstanceOf(IllegalArgumentException.class);
    }

    private static BrokerServer broker(Capabilities capabilities) throws IOException {
        return BrokerServer.builder()
                .capabilities(capabilities)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }

    private static RawSession open(BrokerServer broker) throws IOException {
        return RawSession.open(broker.port(), MqttVersion.V5);
    }

    /// Connects `clientId` with MQTT 5.0 and returns the CONNACK that accepted it.
    private static Packet.ConnAck accepted(RawSession client, String clientId) throws Exception {
        client.send(connectV5(clientId, true, 0, 0));
        Packet.ConnAck connAck = client.expect(Packet.ConnAck.class);
        assertThat(connAck.reasonCode()).isEqualTo(SUCCESS);
        return connAck;
    }

    private static Packet.Will will(Qos qos, boolean retain) {
        return new Packet.Will(new Topic("will"), new byte[] {1}, qos, retain, EMPTY);
    }

    private static Packet.Connect withWill(String clientId, Packet.Will will) {
        return new Packet.Connect(clientId, MqttVersion.V5, true, 0, will, null, null, EMPTY);
    }

    /// A QoS 0 publication that sets the Topic Alias `alias`.
    private static Packet.Publish aliased(int alias) {
        return new Packet.Publish(
                new Topic("aliased/topic"),
                "aliased".getBytes(UTF_8),
                AT_MOST_ONCE,
                false,
                false,
                0,
                new Properties(List.of(new Property.TwoByteValue(PropertyId.TOPIC_ALIAS, alias)))
        );
    }
}
