package org.mqtt4j.hato.examples;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import org.mqtt4j.hato.AckMode;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReceiveResult;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.MqttBroker;

import static java.nio.charset.StandardCharsets.UTF_8;

/// Publishes to an embedded broker and reads the messages back: readings through a handler,
/// alarms by receiving on the main thread and acknowledging them after printing.
public final class Hello {

    private Hello() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        try (MqttBroker broker = MqttBroker.start();
                MqttClient client = MqttClient.builder()
                        .server(URI.create("mqtt://localhost:" + broker.port()))
                        .version(MqttVersion.V3_1_1)
                        .build()) {
            client.connect().join();

            var readingShown = new CountDownLatch(1);
            Subscription readings = client.subscribe(
                    Subscribe.of(new TopicFilter("sensors/+/temperature"), Qos.AT_MOST_ONCE),
                    delivery -> {
                        show("handler", delivery.message());
                        readingShown.countDown();
                    }
            );
            Subscription alarms = client.subscribe(
                    Subscribe.of(new TopicFilter("alarms/#"), Qos.AT_LEAST_ONCE)
                            .withAckMode(AckMode.MANUAL)
            );
            readings.established().join();
            alarms.established().join();

            client.publish(message("sensors/kitchen/temperature", "21.5", Qos.AT_MOST_ONCE))
                    .join();
            client.publish(message("alarms/kitchen/smoke", "detected", Qos.AT_LEAST_ONCE))
                    .join();

            readingShown.await();
            if (alarms.receive(Duration.ofSeconds(5))
                    instanceof ReceiveResult.Received(var delivery)) {
                show("receive", delivery.message());
                delivery.ack();
            }

            readings.unsubscribe().join();
            alarms.unsubscribe().join();
            client.disconnect().join();
        }
    }

    private static MqttMessage message(String topic, String payload, Qos qos) {
        return MqttMessage.of(new Topic(topic), payload.getBytes(UTF_8), qos, false);
    }

    private static void show(String via, MqttMessage message) {
        System.out.println(via + ": " + message.topic() + " = "
                + new String(message.payload(), UTF_8));
    }
}
