package org.mqtt4j.hato.examples;

import java.net.URI;

import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;

import static java.nio.charset.StandardCharsets.UTF_8;

/// Subscribes to the sensor readings from the root README, publishes one, and prints it as it
/// comes back, until Enter is pressed.
public final class ReadingRoundTrip {

    void main() {
        try (
                MqttClient client = MqttClient.builder()
                        .server(URI.create("mqtt://localhost:1883"))
                        .build()
        ) {
            client.connect().join();
            // The message handler runs for each message that the subscription receives.
            client.subscribe(
                    Subscribe.of(new TopicFilter("sensors/+/temperature"), Qos.AT_LEAST_ONCE),
                    delivery -> show(delivery.message())
            ).established().join();

            client.publish(MqttMessage.of(
                    new Topic("sensors/kitchen/temperature"),
                    "21.5".getBytes(UTF_8),
                    Qos.AT_LEAST_ONCE,
                    false
            )).join();

            IO.println("Press Enter to stop.");
            IO.readln();
            client.disconnect().join();
        }
    }

    private static void show(MqttMessage message) {
        IO.println(message.topic() + ": " + new String(message.payload(), UTF_8));
    }
}
