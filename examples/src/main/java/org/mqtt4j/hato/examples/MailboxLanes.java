package org.mqtt4j.hato.examples;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.mqtt4j.hato.DeliveryOptions;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.MqttBroker;

import static java.nio.charset.StandardCharsets.UTF_8;

/// Runs each device's telemetry on the device's mailbox: a single-thread executor of the
/// application that also runs the device's other work. Each device's messages run one at a time
/// in arrival order, and different devices run at the same time. The shutdown at the end stops
/// the mailboxes only after the client has terminated.
public final class MailboxLanes {

    void main() throws IOException, InterruptedException {
        try (MqttBroker broker = MqttBroker.start()) {
            MqttClient client = MqttClient.builder()
                    .server(URI.create("mqtt://localhost:" + broker.port()))
                    // The embedded broker speaks MQTT 3.1.1.
                    .version(MqttVersion.V3_1_1)
                    .build();
            // One mailbox for each device, made when its first message arrives.
            var mailboxes = new ConcurrentHashMap<String, ExecutorService>();
            var processed = new CountDownLatch(4);

            try {
                client.connect().join();
                Subscription telemetry = client.subscribe(
                        Subscribe.of(new TopicFilter("devices/+/telemetry"), Qos.AT_LEAST_ONCE),
                        DeliveryOptions.lanes(message -> mailboxes.computeIfAbsent(
                                device(message),
                                _ -> Executors.newSingleThreadExecutor()
                        )),
                        delivery -> {
                            show("processed", delivery.message());
                            processed.countDown();
                        }
                );
                telemetry.established().join();

                publish(client, "kitchen", "21.5");
                publish(client, "garage", "18.0");
                publish(client, "kitchen", "21.7");
                publish(client, "garage", "18.2");
                processed.await();
                // A mailbox runs the application's own work as well, here a report per device.
                mailboxes.forEach((device, mailbox) ->
                        mailbox.execute(() -> System.out.println("report: " + device)));
            } finally {
                shutDown(client, mailboxes);
            }
        }
    }

    /// Shuts down in three steps, each relying on the one before it.
    private static void shutDown(MqttClient client, Map<String, ExecutorService> mailboxes) {
        // 1. Closing the client ends new deliveries. A handler already running on a mailbox may
        //    still finish.
        client.close();
        // 2. Once the client has terminated, none of its handlers runs, and it never gives a
        //    mailbox anything again.
        client.terminated().join();
        // 3. Only now stop the mailboxes. close() runs the work that they still hold, while the
        //    client's tasks among it do nothing; shutdownNow() would discard that work instead.
        for (ExecutorService mailbox : mailboxes.values()) {
            mailbox.close();
        }
    }

    private static String device(MqttMessage message) {
        return message.topic().name().split("/")[1];
    }

    private static void publish(MqttClient client, String device, String reading) {
        var message = MqttMessage.of(
                new Topic("devices/" + device + "/telemetry"),
                reading.getBytes(UTF_8),
                Qos.AT_LEAST_ONCE,
                false
        );
        client.publish(message).join();
    }

    private static void show(String outcome, MqttMessage message) {
        System.out.println(outcome + ": " + message.topic() + " = "
                + new String(message.payload(), UTF_8));
    }
}
