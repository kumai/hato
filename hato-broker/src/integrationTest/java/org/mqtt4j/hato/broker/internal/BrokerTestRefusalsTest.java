package org.mqtt4j.hato.broker.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;
import org.mqtt4j.hato.broker.internal.common.value.Topic;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.RawSession.subscription;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.EXACTLY_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

/// The refusals and disconnections with a reason that the test controls make, on the wire: what
/// each packet carries for MQTT 3.1.1 and MQTT 5.0 clients, and what a refusal leaves behind.
class BrokerTestRefusalsTest {

    @Test
    void a_client_that_asks_for_no_problem_information_gets_acknowledgements_without_strings()
            throws Exception {
        try (BrokerServer sut = BrokerServer.start(loopback());
                RawSession client = accepted(sut, connect("quiet", problemInformation(0)))) {
            // Arrange
            sut.refusePublications("refused/#", 0x97, "over quota");
            sut.refuseSubscriptions("guarded/#", 0x87, "no access");

            // Act
            client.send(publication("refused/one", "one", AT_LEAST_ONCE, 1));
            client.send(publication("refused/two", "two", EXACTLY_ONCE, 2));
            client.send(new Packet.Subscribe(
                    3,
                    List.of(subscription("guarded/#", AT_MOST_ONCE)),
                    EMPTY
            ));

            // Assert
            assertThat(client.expect(Packet.PubAck.class))
                    .isEqualTo(new Packet.PubAck(1, ReasonCode.QUOTA_EXCEEDED, EMPTY));
            assertThat(client.expect(Packet.PubRec.class))
                    .isEqualTo(new Packet.PubRec(2, ReasonCode.QUOTA_EXCEEDED, EMPTY));
            assertThat(client.expect(Packet.SubAck.class)).isEqualTo(new Packet.SubAck(
                    3,
                    List.of(ReasonCode.NOT_AUTHORIZED),
                    EMPTY
            ));
        }
    }

    @Test
    void connack_and_disconnect_carry_their_strings_without_problem_information()
            throws Exception {
        try (BrokerServer sut = BrokerServer.start(loopback());
                RawSession refused = RawSession.open(sut.port(), MqttVersion.V5);
                RawSession client = accepted(sut, connect("quiet", problemInformation(0)))) {
            // Arrange
            sut.refuseConnections("refused", 0x87, "not you", null);

            // Act
            refused.send(connect("refused", problemInformation(0)));
            sut.disconnect("quiet", 0x98, "maintenance", null);

            // Assert
            assertThat(refused.expect(Packet.ConnAck.class)).isEqualTo(new Packet.ConnAck(
                    false,
                    ReasonCode.NOT_AUTHORIZED,
                    reasonString("not you")
            ));
            assertThat(client.expect(Packet.Disconnect.class)).isEqualTo(new Packet.Disconnect(
                    ReasonCode.ADMINISTRATIVE_ACTION,
                    reasonString("maintenance")
            ));
        }
    }

    @Test
    void a_reason_string_that_would_exceed_the_clients_maximum_packet_size_is_left_out()
            throws Exception {
        try (BrokerServer sut = BrokerServer.start(loopback());
                RawSession client = accepted(sut, connect("small", maximumPacketSize(32)))) {
            // Arrange
            sut.refusePublications("short/#", 0x97, "full");
            sut.refusePublications("long/#", 0x97, "full ".repeat(10));

            // Act
            client.send(publication("short/topic", "fits", AT_LEAST_ONCE, 1));
            client.send(publication("long/topic", "does not fit", AT_LEAST_ONCE, 2));

            // Assert
            assertThat(client.expect(Packet.PubAck.class)).isEqualTo(new Packet.PubAck(
                    1,
                    ReasonCode.QUOTA_EXCEEDED,
                    reasonString("full")
            ));
            assertThat(client.expect(Packet.PubAck.class))
                    .isEqualTo(new Packet.PubAck(2, ReasonCode.QUOTA_EXCEEDED, EMPTY));
        }
    }

    @Test
    void a_refusal_leaves_out_the_reason_string_and_then_the_server_reference_to_fit()
            throws Exception {
        try (BrokerServer sut = BrokerServer.start(loopback());
                RawSession nearly = RawSession.open(sut.port(), MqttVersion.V5);
                RawSession tight = RawSession.open(sut.port(), MqttVersion.V5)) {
            // Arrange: room for the short Server Reference alone, and then for neither.
            String longer = "elsewhere.example:1883 ".repeat(3);
            sut.refuseConnections("nearly", 0x9C, "maintenance ".repeat(5), "other:1883");
            sut.refuseConnections("tight", 0x9C, "maintenance", longer);

            // Act
            nearly.send(connect("nearly", maximumPacketSize(24)));
            tight.send(connect("tight", maximumPacketSize(24)));

            // Assert
            var reference = new Property.StringValue(PropertyId.SERVER_REFERENCE, "other:1883");
            assertThat(nearly.expect(Packet.ConnAck.class)).isEqualTo(new Packet.ConnAck(
                    false,
                    ReasonCode.USE_ANOTHER_SERVER,
                    new Properties(List.of(reference))
            ));
            assertThat(tight.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(false, ReasonCode.USE_ANOTHER_SERVER, EMPTY));
        }
    }

    @Test
    void a_refused_qos_2_publication_leaves_its_packet_identifier_to_a_new_message()
            throws Exception {
        try (BrokerServer sut = BrokerServer.start(loopback());
                RawSession subscriber = connectedV5(sut.port(), "subscriber");
                RawSession publisher = connectedV5(sut.port(), "publisher")) {
            // Arrange
            subscriber.subscribe("open/#", EXACTLY_ONCE);
            sut.refusePublications("refused/#", 0x97, null);
            publisher.send(publication("refused/topic", "refused", EXACTLY_ONCE, 7));
            assertThat(publisher.expect(Packet.PubRec.class))
                    .isEqualTo(new Packet.PubRec(7, ReasonCode.QUOTA_EXCEEDED, EMPTY));

            // Act: the identifier comes back for a new message, without DUP.
            publisher.publish(publication("open/topic", "new", EXACTLY_ONCE, 7));

            // Assert
            assertThat(subscriber.expect(Packet.Publish.class).topic())
                    .isEqualTo(new Topic("open/topic"));
        }
    }

    @Test
    void an_mqtt_3_1_1_client_gets_failure_0x80_for_a_subscription_refused_with_a_reason()
            throws Exception {
        try (BrokerServer sut = BrokerServer.start(loopback());
                RawSession client = connected(sut.port(), "classic")) {
            // Arrange
            sut.refuseSubscriptions("guarded/#", 0x87, "no access");

            // Act
            client.send(new Packet.Subscribe(
                    1,
                    List.of(subscription("guarded/#", AT_MOST_ONCE)),
                    EMPTY
            ));

            // Assert
            assertThat(client.expect(Packet.SubAck.class)).isEqualTo(new Packet.SubAck(
                    1,
                    List.of(ReasonCode.UNSPECIFIED_ERROR),
                    EMPTY
            ));
        }
    }

    @Test
    void a_suback_carries_the_reason_string_of_the_first_filter_refused_with_one()
            throws Exception {
        try (BrokerServer sut = BrokerServer.start(loopback());
                RawSession client = connectedV5(sut.port(), "subscriber")) {
            // Arrange
            sut.refuseSubscriptions("silent/#", 0x80, null);
            sut.refuseSubscriptions("first/#", 0x87, "first");
            sut.refuseSubscriptions("second/#", 0x97, "second");

            // Act
            client.send(new Packet.Subscribe(
                    1,
                    List.of(
                            subscription("plain/#", AT_MOST_ONCE),
                            subscription("silent/#", AT_MOST_ONCE),
                            subscription("first/#", AT_MOST_ONCE),
                            subscription("second/#", AT_MOST_ONCE)
                    ),
                    EMPTY
            ));

            // Assert
            assertThat(client.expect(Packet.SubAck.class)).isEqualTo(new Packet.SubAck(
                    1,
                    List.of(
                            SUCCESS,
                            ReasonCode.UNSPECIFIED_ERROR,
                            ReasonCode.NOT_AUTHORIZED,
                            ReasonCode.QUOTA_EXCEEDED
                    ),
                    reasonString("first")
            ));
        }
    }

    @Test
    void a_held_refusal_of_a_qos_2_publication_counts_against_the_receive_maximum()
            throws Exception {
        try (BrokerServer sut = BrokerServer.start(loopback());
                RawSession client = RawSession.open(sut.port(), MqttVersion.V5)) {
            // Arrange: one unanswered publication at a time, and a refusal that waits.
            sut.announce(2, true, true, true, 1, 268_435_460, 16);
            client.send(connect("holding", EMPTY));
            client.expect(Packet.ConnAck.class);
            sut.holdResponses(Packet.PubRec.class, "holding");
            sut.refusePublications("refused/#", 0x97, null);
            client.send(publication("refused/topic", "refused", EXACTLY_ONCE, 1));

            // Act
            client.send(publication("open/topic", "one too many", AT_LEAST_ONCE, 2));

            // Assert: the held PUBREC has yet to answer the first publication.
            assertThat(client.expect(Packet.Disconnect.class).reasonCode())
                    .isEqualTo(ReasonCode.RECEIVE_MAXIMUM_EXCEEDED);
        }
    }

    @Test
    void a_will_on_a_refused_topic_is_published_all_the_same() throws Exception {
        try (BrokerServer sut = BrokerServer.start(loopback());
                RawSession watcher = connected(sut.port(), "watcher")) {
            // Arrange
            watcher.subscribe("left/#", AT_MOST_ONCE);
            sut.refusePublications("left/#", 0x87, null);
            var will = new Packet.Will(
                    new Topic("left/leaving"),
                    "gone".getBytes(UTF_8),
                    AT_MOST_ONCE,
                    false,
                    EMPTY
            );
            RawSession client = accepted(sut, new Packet.Connect(
                    "leaving",
                    MqttVersion.V5,
                    true,
                    0,
                    will,
                    null,
                    null,
                    EMPTY
            ));

            // Act
            client.close();

            // Assert
            assertThat(watcher.expect(Packet.Publish.class).topic())
                    .isEqualTo(new Topic("left/leaving"));
        }
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }

    /// An MQTT 5.0 connection that `request` opens, which the broker accepted.
    private static RawSession accepted(BrokerServer broker, Packet.Connect request)
            throws Exception {
        RawSession client = RawSession.open(broker.port(), MqttVersion.V5);
        client.send(request);
        assertThat(client.expect(Packet.ConnAck.class).reasonCode()).isEqualTo(SUCCESS);
        return client;
    }

    /// An MQTT 5.0 CONNECT of a clean session without keep-alive, with `properties`.
    private static Packet.Connect connect(String clientId, Properties properties) {
        return new Packet.Connect(
                clientId,
                MqttVersion.V5,
                true,
                0,
                null,
                null,
                null,
                properties
        );
    }

    private static Properties problemInformation(int value) {
        return new Properties(List.of(
                new Property.ByteValue(PropertyId.REQUEST_PROBLEM_INFORMATION, value)
        ));
    }

    private static Properties maximumPacketSize(long bytes) {
        return new Properties(List.of(
                new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, bytes)
        ));
    }

    private static Properties reasonString(String value) {
        return new Properties(List.of(new Property.StringValue(PropertyId.REASON_STRING, value)));
    }
}
