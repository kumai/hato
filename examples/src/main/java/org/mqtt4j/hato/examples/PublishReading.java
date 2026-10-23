package org.mqtt4j.hato.examples;

import java.net.URI;

import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Topic;

import static java.nio.charset.StandardCharsets.UTF_8;

/// Publishes the temperature reading from the client guide to a local broker.
public final class PublishReading {

    void main() {
        try (
                MqttClient client = MqttClient.builder()
                        .server(URI.create("mqtt://localhost:1883"))
                        .build()
        ) {
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
