package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;

/// The client against a broker's own WebSocket listener: the round trip at every QoS level, in
/// both protocol versions, and the end after `disconnect()`. Each product's subclass starts its
/// broker and names the listener.
abstract class WebSocketRoundTripTest {

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void every_qos_level_makes_the_round_trip_over_websocket(MqttVersion version)
            throws Exception {
        var ends = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient sut = MqttClient.builder()
                .server(endpoint())
                .version(version)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .connectionListener(ends::add)
                .build()) {
            // Arrange: the subscription grants QoS 2, so every level arrives with its own QoS.
            String topic = uniqueTopic();
            sut.connect().get(5, SECONDS);
            PullSubscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.EXACTLY_ONCE)
            );
            subscription.established().get(5, SECONDS);

            for (Qos qos : Qos.values()) {
                var message = MqttMessage.of(
                        new Topic(topic),
                        new byte[] {(byte) qos.level()},
                        qos,
                        false
                );

                // Act
                sut.publish(message).get(5, SECONDS);

                // Assert
                assertThat(assertInstanceOf(
                        ReceiveResult.Received.class,
                        subscription.receive(Duration.ofSeconds(5))
                ).delivery().message()).isEqualTo(message);
            }
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
        }
    }

    /// The `ws` URI of the broker's WebSocket listener.
    abstract URI endpoint();
}
