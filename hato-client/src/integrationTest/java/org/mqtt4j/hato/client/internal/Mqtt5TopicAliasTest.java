package org.mqtt4j.hato.client.internal;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.Properties;
import org.mqtt4j.hato.client.internal.common.protocol.Property;
import org.mqtt4j.hato.client.internal.common.protocol.PropertyId;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.endpoint;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.listen;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// A raw MQTT 5.0 peer sends Topic Aliases within and beyond the maximum that the client
/// announces in CONNECT.
@Timeout(30)
class Mqtt5TopicAliasTest {

    @Test
    void the_client_announces_16_aliases_and_delivers_the_topic_that_each_stands_for()
            throws Exception {
        try (ServerSocket server = listen(); MqttClient sut = client(server)) {
            // Arrange
            CompletableFuture<ConnAck> connected = sut.connect();
            try (ScriptedBroker peer = ScriptedBroker.accept(server, MqttVersion.V5)) {
                var connect = (Packet.Connect) peer.receive();
                peer.send(new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY));
                connected.get(5, SECONDS);
                Subscription subscription = peer.subscribePull(
                        sut,
                        "sensors/#",
                        Qos.AT_MOST_ONCE,
                        AckMode.ON_RECEIVE,
                        8
                );

                // Act
                peer.send(aliased("sensors/1", 16));
                peer.send(new Packet.AliasPublish(
                        new byte[] {2},
                        Qos.AT_MOST_ONCE,
                        false,
                        false,
                        0,
                        alias(16)
                ));

                // Assert
                assertThat(connect.properties().find(PropertyId.TOPIC_ALIAS_MAXIMUM)).contains(
                        new Property.TwoByteValue(PropertyId.TOPIC_ALIAS_MAXIMUM, 16)
                );
                for (int payload : List.of(1, 2)) {
                    MqttMessage message = receive(subscription);
                    assertThat(message.topic()).isEqualTo(new Topic("sensors/1"));
                    assertThat(message.payload()).containsExactly(payload);
                }
            }
        }
    }

    @Test
    void an_alias_beyond_the_announcement_ends_the_connection_as_topic_alias_invalid()
            throws Exception {
        try (ServerSocket server = listen(); MqttClient sut = client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut, MqttVersion.V5)) {
            // Act
            peer.send(aliased("sensors/1", 17));

            // Assert
            assertThat(peer.receive())
                    .isEqualTo(new Packet.Disconnect(ReasonCode.TOPIC_ALIAS_INVALID, EMPTY));
            peer.expectClosed();
        }
    }

    @Test
    void an_alias_that_no_publication_set_ends_the_connection_as_a_protocol_error()
            throws Exception {
        try (ServerSocket server = listen(); MqttClient sut = client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut, MqttVersion.V5)) {
            // Act
            peer.send(new Packet.AliasPublish(
                    new byte[] {1},
                    Qos.AT_MOST_ONCE,
                    false,
                    false,
                    0,
                    alias(1)
            ));

            // Assert
            assertThat(peer.receive())
                    .isEqualTo(new Packet.Disconnect(ReasonCode.PROTOCOL_ERROR, EMPTY));
            peer.expectClosed();
        }
    }

    private static MqttClient client(ServerSocket server) {
        return MqttClient.builder()
                .server(endpoint(server))
                .version(MqttVersion.V5)
                .clientId("mqtt5-topic-aliases")
                .reconnectPolicy(ReconnectPolicy.disabled())
                .build();
    }

    private static Properties alias(int alias) {
        return new Properties(List.of(new Property.TwoByteValue(PropertyId.TOPIC_ALIAS, alias)));
    }

    /// A QoS 0 publication on `topic` with payload 1 that sets the Topic Alias `alias`.
    private static Packet.Publish aliased(String topic, int alias) {
        return new Packet.Publish(
                new Topic(topic),
                new byte[] {1},
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                alias(alias)
        );
    }

    private static MqttMessage receive(Subscription subscription) throws Exception {
        ReceiveResult.Received received = assertInstanceOf(
                ReceiveResult.Received.class,
                subscription.receive(Duration.ofSeconds(5))
        );
        return received.delivery().message();
    }
}
