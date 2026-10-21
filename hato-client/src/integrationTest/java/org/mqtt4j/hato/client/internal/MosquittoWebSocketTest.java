package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.Mosquitto;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;

/// The client against Mosquitto's WebSocket listener, which is Mosquitto's own WebSocket
/// implementation: the round trip at every QoS level and the end after `disconnect()`.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class MosquittoWebSocketTest {

    private static final GenericContainer<?> broker = Mosquitto.container();

    static boolean dockerIsAvailableOrRequired() {
        return Docker.availableOrRequired();
    }

    @BeforeAll
    static void start() {
        Docker.start(broker);
    }

    @AfterAll
    static void stop() {
        broker.stop();
    }

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
            Subscription subscription = sut.subscribe(
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

    // Mosquitto serves MQTT on every path of its WebSocket listener.
    private static URI endpoint() {
        return URI.create("ws://" + broker.getHost() + ":"
                + broker.getMappedPort(Mosquitto.WEBSOCKET_PORT) + "/mqtt");
    }
}
