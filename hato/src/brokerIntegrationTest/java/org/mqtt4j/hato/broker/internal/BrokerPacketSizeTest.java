package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.PacketCodec;
import org.mqtt4j.hato.internal.protocol.Properties;
import org.mqtt4j.hato.internal.protocol.Property;
import org.mqtt4j.hato.internal.protocol.PropertyId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.Qos.EXACTLY_ONCE;
import static org.mqtt4j.hato.ReasonCode.PACKET_TOO_LARGE;
import static org.mqtt4j.hato.ReasonCode.SUCCESS;
import static org.mqtt4j.hato.broker.internal.RawSession.connect;
import static org.mqtt4j.hato.broker.internal.RawSession.connectV5;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.internal.protocol.Properties.EMPTY;

/// The broker's limits on the size of the packets that it takes from its clients.
class BrokerPacketSizeTest {

    @Test
    void an_mqtt_3_1_1_connection_that_sends_a_packet_above_the_limit_closes_without_a_packet()
            throws Exception {
        // Arrange: a limit of 64 bytes, and a publication of exactly that size.
        try (BrokerServer sut = broker(64, 64);
                RawSession client = RawSession.connected(sut.port(), "sized")) {
            client.send(sized(MqttVersion.V3_1_1, 64));
            client.ping();

            // Act
            client.send(sized(MqttVersion.V3_1_1, 65));

            // Assert: MQTT 3.1.1 has no packet that tells a client why.
            client.expectClosed();
        }
    }

    @Test
    void an_mqtt_5_client_learns_of_the_limit_and_why_a_packet_above_it_ends_the_connection()
            throws Exception {
        // Arrange: a limit of 64 bytes, and a publication of exactly that size.
        try (BrokerServer sut = broker(64, 64);
                RawSession client = RawSession.open(sut.port(), MqttVersion.V5)) {
            client.send(connectV5("sized", true, 0, 0));
            Packet.ConnAck connAck = client.expect(Packet.ConnAck.class);
            client.send(sized(MqttVersion.V5, 64));
            client.ping();

            // Act
            client.send(sized(MqttVersion.V5, 65));

            // Assert: MQTT 5.0 section 3.2.2.3.6.
            assertThat(connAck.properties().find(PropertyId.MAXIMUM_PACKET_SIZE))
                    .contains(new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, 64));
            assertThat(client.expect(Packet.Disconnect.class))
                    .isEqualTo(new Packet.Disconnect(PACKET_TOO_LARGE, EMPTY));
            client.expectClosed();
        }
    }

    @ParameterizedTest(name = "a limit of {0} bytes and an announced {1}")
    @CsvSource({"64, 128", "128, 64"})
    void the_smaller_of_the_limit_and_an_announced_maximum_packet_size_holds(
            int limit,
            int announced
    ) throws Exception {
        // Arrange: a test announces a Maximum Packet Size of its own.
        try (BrokerServer sut = announcing(limit, announced);
                RawSession client = RawSession.open(sut.port(), MqttVersion.V5)) {
            client.send(connectV5("sized", true, 0, 0));
            Packet.ConnAck connAck = client.expect(Packet.ConnAck.class);
            client.send(sized(MqttVersion.V5, 64));
            client.ping();

            // Act
            client.send(sized(MqttVersion.V5, 65));

            // Assert
            assertThat(connAck.properties().find(PropertyId.MAXIMUM_PACKET_SIZE))
                    .contains(new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, 64));
            assertThat(client.expect(Packet.Disconnect.class))
                    .isEqualTo(new Packet.Disconnect(PACKET_TOO_LARGE, EMPTY));
            client.expectClosed();
        }
    }

    @Test
    void a_test_that_announces_a_larger_maximum_packet_size_leaves_the_limit_in_place()
            throws Exception {
        // Arrange: a limit of 64 bytes.
        try (BrokerServer sut = broker(64, 64);
                RawSession client = RawSession.open(sut.port(), MqttVersion.V5)) {
            // Act: the test control announces 128 bytes to the clients from now on.
            sut.announce(2, true, true, true, true, 65_535, 128, 16);
            client.send(connectV5("sized", true, 0, 0));

            // Assert
            assertThat(client.expect(Packet.ConnAck.class).properties()
                    .find(PropertyId.MAXIMUM_PACKET_SIZE))
                    .contains(new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, 64));
        }
    }

    @Test
    void a_limit_that_the_framing_format_sets_anyway_is_not_announced() throws Exception {
        try (BrokerServer sut = broker(Connection.MAX_PACKET_SIZE, Connection.MAX_PACKET_SIZE);
                RawSession client = RawSession.open(sut.port(), MqttVersion.V5)) {
            client.send(connectV5("unbounded", true, 0, 0));

            Packet.ConnAck connAck = client.expect(Packet.ConnAck.class);
            assertThat(connAck.reasonCode()).isEqualTo(SUCCESS);
            assertThat(connAck.properties().find(PropertyId.MAXIMUM_PACKET_SIZE)).isEmpty();
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_connect_above_its_own_limit_closes_the_connection_without_connack(MqttVersion version)
            throws Exception {
        // Arrange: a CONNECT may have 64 bytes, other packets more. This identifier makes a
        // CONNECT of exactly 64 bytes.
        String fitting = "c".repeat(64 - connectSize(version, ""));
        try (BrokerServer sut = broker(1024, 64);
                RawSession accepted = RawSession.open(sut.port(), version);
                RawSession refused = RawSession.open(sut.port(), version)) {
            accepted.send(connectOf(version, fitting));
            assertThat(accepted.expect(Packet.ConnAck.class).reasonCode()).isEqualTo(SUCCESS);

            // Act: one byte more.
            refused.send(connectOf(version, fitting + "c"));

            // Assert: the broker cannot tell the version of a client before its CONNECT.
            refused.expectClosed();
            accepted.ping();
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_broker_takes_a_connect_of_64_kib_from_an_unknown_peer_and_no_larger(MqttVersion version)
            throws Exception {
        // Arrange: a broker with its default limits, and an identifier that makes a CONNECT of
        // exactly 64 KiB. Its Remaining Length takes a byte more than that of a short one.
        int length = 64 * 1024 - connectSize(version, "");
        while (connectSize(version, "c".repeat(length)) > 64 * 1024) {
            length--;
        }
        String fitting = "c".repeat(length);
        assertThat(connectSize(version, fitting)).isEqualTo(64 * 1024);
        try (BrokerServer sut = BrokerServer.builder()
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                RawSession accepted = RawSession.open(sut.port(), version);
                RawSession refused = RawSession.open(sut.port(), version)) {
            accepted.send(connectOf(version, fitting));
            assertThat(accepted.expect(Packet.ConnAck.class).reasonCode()).isEqualTo(SUCCESS);

            // Act: one byte more.
            refused.send(connectOf(version, fitting + "c"));

            // Assert
            refused.expectClosed();
            accepted.ping();
        }
    }

    @ParameterizedTest(name = "{0} user properties in the CONNECT, {1} in its Will")
    @CsvSource({"257, 0", "0, 257"})
    void a_connect_with_more_properties_than_the_broker_takes_closes_without_connack(
            int connectProperties,
            int willProperties
    ) throws Exception {
        try (BrokerServer sut = BrokerServer.builder()
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                RawSession accepted = RawSession.open(sut.port(), MqttVersion.V5);
                RawSession refused = RawSession.open(sut.port(), MqttVersion.V5)) {
            // Arrange: a client whose CONNECT and Will carry 256 properties each, which the
            // broker takes.
            accepted.send(withProperties("accepted", 256, 256));
            assertThat(accepted.expect(Packet.ConnAck.class).reasonCode()).isEqualTo(SUCCESS);

            // Act
            refused.send(withProperties("refused", connectProperties, willProperties));

            // Assert
            refused.expectClosed();
            accepted.ping();
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void an_accepted_client_may_send_packets_larger_than_a_connect(MqttVersion version)
            throws Exception {
        // Arrange: a CONNECT may have 64 bytes, other packets 1,024.
        try (BrokerServer sut = broker(1024, 64);
                RawSession client = RawSession.open(sut.port(), version)) {
            client.send(connectOf(version, "sized"));
            assertThat(client.expect(Packet.ConnAck.class).reasonCode()).isEqualTo(SUCCESS);

            // Act
            client.send(sized(version, 1024));

            // Assert
            client.ping();
        }
    }

    private static BrokerServer broker(int maxPacketSize, int maxConnectSize) throws IOException {
        return BrokerServer.builder()
                .maxPacketSize(maxPacketSize)
                .maxConnectSize(maxConnectSize)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }

    /// A broker with a packet limit of `maxPacketSize` bytes that a test makes announce a
    /// Maximum Packet Size of `announced` bytes.
    private static BrokerServer announcing(int maxPacketSize, int announced) throws IOException {
        var capabilities = new Capabilities(
                EXACTLY_ONCE,
                true,
                true,
                true,
                true,
                null,
                announced,
                16,
                null
        );
        return BrokerServer.builder()
                .maxPacketSize(maxPacketSize)
                .capabilities(capabilities)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }

    private static Packet.Connect connectOf(MqttVersion version, String clientId) {
        return switch (version) {
            case V3_1_1 -> connect(clientId, true, 0);
            case V5 -> connectV5(clientId, true, 0, 0);
        };
    }

    /// An MQTT 5.0 CONNECT of `clientId` with `connectProperties` User Properties, and a Will with
    /// `willProperties` of them.
    private static Packet.Connect withProperties(
            String clientId,
            int connectProperties,
            int willProperties
    ) {
        return new Packet.Connect(
                clientId,
                MqttVersion.V5,
                true,
                0,
                new Packet.Will(
                        new Topic("wills/" + clientId),
                        new byte[] {1},
                        AT_MOST_ONCE,
                        false,
                        userProperties(willProperties)
                ),
                null,
                null,
                userProperties(connectProperties)
        );
    }

    private static Properties userProperties(int count) {
        return new Properties(Collections.nCopies(count, new Property.UserProperty("k", "v")));
    }

    private static int connectSize(MqttVersion version, String clientId) {
        return PacketCodec.of(version).encode(connectOf(version, clientId)).length;
    }

    /// A QoS 0 publication whose packet has exactly `size` bytes in `version`.
    private static Packet.Publish sized(MqttVersion version, int size) {
        PacketCodec codec = PacketCodec.of(version);
        // The Remaining Length takes a second byte from 128 bytes on.
        int empty = codec.encode(publication("sized", "", AT_MOST_ONCE, 0)).length;
        int payload = size - empty - (size - 2 >= 128 ? 1 : 0);
        Packet.Publish publication = publication("sized", "x".repeat(payload), AT_MOST_ONCE, 0);
        assertThat(codec.encode(publication)).hasSize(size);
        return publication;
    }
}
