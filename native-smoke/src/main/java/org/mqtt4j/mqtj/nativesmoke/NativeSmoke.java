package org.mqtt4j.mqtj.nativesmoke;

import java.net.URI;

import org.mqtt4j.mqtj.MqttClient;
import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.broker.MqttBroker;

import static java.util.concurrent.TimeUnit.SECONDS;

/// Verifies that the packaged client and broker connect over TCP in a native image.
public final class NativeSmoke {

    private NativeSmoke() {
    }

    public static void main(String[] args) throws Exception {
        try (MqttBroker broker = MqttBroker.start();
                MqttClient client = MqttClient.builder()
                        .server(URI.create("mqtt://localhost:" + broker.port()))
                        .version(MqttVersion.V3_1_1)
                        .build()) {
            if (client.connect().get(5, SECONDS).sessionPresent()) {
                throw new AssertionError("Clean connection resumed a session");
            }
            client.disconnect().get(5, SECONDS);
        }
        System.out.println("mqtj native smoke: connect/disconnect OK");
    }
}
