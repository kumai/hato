package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/// The round trips of [Mqtt5RoundTripTest], and those that only Mosquitto, EMQX, and HiveMQ CE
/// support: shared subscriptions, with members on MQTT 5.0 and on MQTT 3.1.1. The embedded
/// broker has no shared subscriptions.
abstract class Mqtt5ProductRoundTripTest extends Mqtt5RoundTripTest {

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_shared_subscription_hands_each_publication_to_one_member_of_its_group(
            MqttVersion version
    ) throws Exception {
        // HiveMQ CE keeps undelivered messages for a group after its members leave, so each run
        // has its own group and topic.
        String unique = UUID.randomUUID().toString().replace("-", "");
        var topic = new Topic("hato/shared/" + unique);
        var filter = new TopicFilter("$share/g" + unique + "/" + topic.name());
        var first = new LinkedBlockingQueue<Integer>();
        var second = new LinkedBlockingQueue<Integer>();
        try (
                MqttClient one = member(version);
                MqttClient two = member(version);
                MqttClient publisher = client("hato" + unique.substring(0, 19), Duration.ZERO)
        ) {
            // Arrange
            for (MqttClient member : List.of(one, two, publisher)) {
                member.connect().get(5, SECONDS);
            }
            one.subscribe(
                    Subscribe.of(filter, Qos.AT_LEAST_ONCE),
                    delivery -> first.add((int) delivery.message().payload()[0])
            ).established().get(5, SECONDS);
            two.subscribe(
                    Subscribe.of(filter, Qos.AT_LEAST_ONCE),
                    delivery -> second.add((int) delivery.message().payload()[0])
            ).established().get(5, SECONDS);

            // Act
            for (int i = 0; i < 10; i++) {
                publisher.publish(MqttMessage.of(
                        topic,
                        new byte[] {(byte) i},
                        Qos.AT_LEAST_ONCE,
                        false
                )).get(5, SECONDS);
            }

            // Assert: every publication reaches exactly one member.
            await().untilAsserted(() -> assertThat(first.size() + second.size()).isEqualTo(10));
            var received = new ArrayList<Integer>(first);
            received.addAll(second);
            assertThat(received).containsExactlyInAnyOrderElementsOf(
                    IntStream.range(0, 10).boxed().toList()
            );
            // A broker that does not take turns can hand a burst to one member.
            if (takesTurns(version)) {
                assertThat(first).isNotEmpty();
                assertThat(second).isNotEmpty();
            }
        }
    }

    /// Whether the broker hands a burst of publications to the members of a shared subscription
    /// in turn, for members on `version`, as recorded for it.
    abstract boolean takesTurns(MqttVersion version);

    private MqttClient member(MqttVersion version) {
        return MqttClient.builder()
                .server(endpoint())
                .version(version)
                .clientId("hato" + UUID.randomUUID().toString().replace("-", "").substring(0, 19))
                .reconnectPolicy(ReconnectPolicy.disabled())
                .build();
    }
}
