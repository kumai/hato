package org.mqtt4j.hato.examples;

import java.net.URI;

import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.TopicFilter;

import static java.nio.charset.StandardCharsets.UTF_8;

/// Prints the sensor readings from the root README until Enter is pressed.
public final class SubscribeReadings {

    void main() {
        try (MqttClient client = MqttClient.builder()
                .server(URI.create("mqtt://localhost:1883"))
                .build()) {
            client.connect().join();
            client.subscribe(
                    Subscribe.of(new TopicFilter("sensors/+/temperature"), Qos.AT_MOST_ONCE),
                    delivery -> show(delivery.message())
            ).established().join();

            IO.println("Waiting for readings. Press Enter to stop.");
            IO.readln();
            client.disconnect().join();
        }
    }

    private static void show(MqttMessage message) {
        IO.println(message.topic() + ": " + new String(message.payload(), UTF_8));
    }
}
