package org.mqtt4j.mqtj.nativesmoke;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

import org.mqtt4j.mqtj.MqttClient;
import org.mqtt4j.mqtj.MqttMessage;
import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.Qos;
import org.mqtt4j.mqtj.Subscribe;
import org.mqtt4j.mqtj.Subscription;
import org.mqtt4j.mqtj.Topic;
import org.mqtt4j.mqtj.TopicFilter;
import org.mqtt4j.mqtj.broker.MqttBroker;

import static java.util.concurrent.TimeUnit.SECONDS;

/// Verifies that the packaged client and broker exchange QoS 0 over TCP in a native image.
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
            var received = new CompletableFuture<MqttMessage>();
            Subscription subscription = client.subscribe(
                    Subscribe.of(new TopicFilter("smoke/#"), Qos.AT_MOST_ONCE),
                    delivery -> received.complete(delivery.message())
            ).get(5, SECONDS);
            var message = new MqttMessage(new Topic("smoke/message"), new byte[] {42},
                    Qos.AT_MOST_ONCE, false);
            client.publish(message).get(5, SECONDS);
            if (!message.equals(received.get(5, SECONDS))) {
                throw new AssertionError("QoS 0 delivery changed the message");
            }
            subscription.unsubscribe().get(5, SECONDS);
            client.disconnect().get(5, SECONDS);
        }
        System.out.println("mqtj native smoke: QoS 0 publish/subscribe/unsubscribe OK");
    }
}
