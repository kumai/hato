package org.mqtt4j.mqtj.examples;

import java.net.URI;

import org.mqtt4j.mqtj.MqttClient;
import org.mqtt4j.mqtj.MqttMessage;
import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.Qos;
import org.mqtt4j.mqtj.Topic;

import static java.nio.charset.StandardCharsets.UTF_8;

/// Publishes the temperature reading from the root README to a local broker.
public final class PublishReading {

    private PublishReading() {
    }

    public static void main(String[] args) {
        try (MqttClient client = MqttClient.builder()
                .server(URI.create("mqtt://localhost:1883"))
                .version(MqttVersion.V3_1_1)
                .build()) {
            client.connect().join();
            client.publish(MqttMessage.of(
                    new Topic("sensors/kitchen/temperature"),
                    "21.5".getBytes(UTF_8),
                    Qos.AT_MOST_ONCE,
                    false
            )).join();
            client.disconnect().join();
        }
    }
}
