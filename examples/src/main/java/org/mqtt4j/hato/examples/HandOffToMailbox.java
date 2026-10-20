package org.mqtt4j.hato.examples;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.mqtt4j.hato.AckMode;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttDelivery;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.MqttBroker;

import static java.nio.charset.StandardCharsets.UTF_8;

/// Hands telemetry from an embedded broker to a mailbox that the application owns. A thread of
/// the application processes each message and then acknowledges it, and the shutdown at the
/// end follows an order in which no message goes unnoticed.
public final class HandOffToMailbox {

    void main() throws IOException, InterruptedException {
        try (MqttBroker broker = MqttBroker.start()) {
            MqttClient client = MqttClient.builder()
                    .server(URI.create("mqtt://localhost:" + broker.port()))
                    .build();
            // The application's own mailbox and the worker thread that processes it.
            var mailbox = new ArrayBlockingQueue<MqttDelivery>(1_000);
            var processed = new CountDownLatch(3);
            ExecutorService worker = Executors.newSingleThreadExecutor();
            worker.execute(() -> processUntilStopped(mailbox, processed));

            try {
                client.connect().join();
                // The acceptor only offers: a full mailbox refuses a message instead of waiting.
                Subscription telemetry = client.handOff(
                        Subscribe.of(new TopicFilter("devices/+/telemetry"), Qos.AT_LEAST_ONCE)
                                .withAckMode(AckMode.MANUAL),
                        mailbox::offer
                );
                telemetry.established().join();

                publish(client, "kitchen", "21.5");
                publish(client, "garage", "unavailable");
                publish(client, "attic", "19.0");
                processed.await();
            } finally {
                shutDown(client, mailbox, worker);
            }
        }
    }

    /// Shuts down in four steps, each relying on the one before it.
    private static void shutDown(
            MqttClient client,
            BlockingQueue<MqttDelivery> mailbox,
            ExecutorService worker
    ) {
        // 1. Closing the client ends new hand-offs. A call to the acceptor that is already under
        //    way may still finish.
        client.close();
        // 2. Once the client has terminated, it never calls the acceptor again, so nothing more
        //    enters the mailbox.
        client.terminated().join();
        // 3. Process or discard what is left. This example discards it, while the worker may
        //    still finish a message that it took. Calling ack() has no effect any more; a
        //    persistent session receives the unacknowledged messages again when it next
        //    connects.
        var left = new ArrayList<MqttDelivery>();
        mailbox.drainTo(left);
        for (MqttDelivery delivery : left) {
            show("discarded", delivery.message());
        }
        // 4. Only now close what the processing uses, such as this worker or a database.
        worker.shutdownNow();
        worker.close();
    }

    private static void processUntilStopped(
            BlockingQueue<MqttDelivery> mailbox,
            CountDownLatch processed
    ) {
        try {
            while (true) {
                process(mailbox.take());
                processed.countDown();
            }
        } catch (InterruptedException _) {
            // The shutdown stops the worker once the mailbox is no longer needed.
        }
    }

    /// Processes one reading and then acknowledges it. The application drops a reading that is
    /// not a number by its own rule and acknowledges it all the same: an unacknowledged message
    /// would hold back the acknowledgements of later ones.
    private static void process(MqttDelivery delivery) {
        var reading = new String(delivery.message().payload(), UTF_8);
        show(reading.matches("-?\\d+(\\.\\d+)?") ? "processed" : "dropped", delivery.message());
        delivery.ack();
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
