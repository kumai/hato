package org.mqtt4j.hato.testkit;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.testkit.internal.common.io.FrameReader;
import org.mqtt4j.hato.testkit.internal.common.io.FrameWriter;
import org.mqtt4j.hato.testkit.internal.common.protocol.Packet;
import org.mqtt4j.hato.testkit.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.testkit.internal.common.protocol.Properties;
import org.mqtt4j.hato.testkit.internal.common.protocol.Property;
import org.mqtt4j.hato.testkit.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.testkit.internal.common.value.MqttVersion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/// What an announcement puts in CONNACK, of which the client's API shows only a part. Each test
/// reads CONNACK on a raw MQTT 5.0 connection, with the codec that the test kit embeds.
@Timeout(60)
class AnnouncedCapabilitiesTest {

    @Test
    void each_announced_capability_reaches_connack() throws Exception {
        try (var broker = MqttBroker.start()) {
            // Arrange
            var sut = BrokerControls.of(broker);
            BrokerCapabilities announced = BrokerCapabilities.defaults()
                    .withMaximumQos(Qos.AT_MOST_ONCE)
                    .withRetainAvailable(false)
                    .withWildcardSubscriptionAvailable(false)
                    .withReceiveMaximum(7)
                    .withMaximumPacketSize(1_024)
                    .withTopicAliasMaximum(3);

            // Act
            sut.announce(announced);
            Packet.ConnAck ack = connAck(broker);

            // Assert
            assertThat(ack.properties().entries()).containsExactlyInAnyOrder(
                    new Property.ByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER_AVAILABLE, 0),
                    new Property.ByteValue(PropertyId.SHARED_SUBSCRIPTION_AVAILABLE, 0),
                    new Property.ByteValue(PropertyId.MAXIMUM_QOS, 0),
                    new Property.ByteValue(PropertyId.RETAIN_AVAILABLE, 0),
                    new Property.ByteValue(PropertyId.WILDCARD_SUBSCRIPTION_AVAILABLE, 0),
                    new Property.TwoByteValue(PropertyId.RECEIVE_MAXIMUM, 7),
                    new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, 1_024),
                    new Property.TwoByteValue(PropertyId.TOPIC_ALIAS_MAXIMUM, 3)
            );
        }
    }

    @Test
    void announcing_the_defaults_again_restores_the_connack_of_a_new_broker() throws Exception {
        try (var broker = MqttBroker.start()) {
            // Arrange
            var sut = BrokerControls.of(broker);
            Packet.ConnAck original = connAck(broker);
            BrokerCapabilities narrowed = BrokerCapabilities.defaults()
                    .withMaximumQos(Qos.AT_LEAST_ONCE)
                    .withReceiveMaximum(1)
                    .withTopicAliasMaximum(0);
            sut.announce(narrowed);

            // Act
            sut.announce(BrokerCapabilities.defaults());
            Packet.ConnAck ack = connAck(broker);

            // Assert: the defaults leave out of CONNACK what a new broker leaves out.
            assertThat(ack).isEqualTo(original);
        }
    }

    // Connects to `broker` as an MQTT 5.0 client that starts clean, and returns the broker's
    // answer once the connection is closed again. Reading the answer gives up after 5 seconds.
    private static Packet.ConnAck connAck(MqttBroker broker) throws IOException {
        var codec = PacketCodec.of(MqttVersion.V5);
        var connect = new Packet.Connect(
                "hato" + UUID.randomUUID().toString().replace("-", "").substring(0, 19),
                MqttVersion.V5,
                true,
                0,
                null,
                null,
                null,
                Properties.EMPTY
        );
        try (var socket = new Socket(InetAddress.getLoopbackAddress(), broker.port())) {
            socket.setSoTimeout(5_000);
            new FrameWriter(socket.getOutputStream()).write(codec.encode(connect));
            byte[] frame = new FrameReader(socket.getInputStream(), 1_024).read().orElseThrow();
            return assertInstanceOf(Packet.ConnAck.class, codec.decode(frame));
        }
    }
}
