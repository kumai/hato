package org.mqtt4j.hato.nativesmoke;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

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

import static java.util.concurrent.TimeUnit.SECONDS;

/// Verifies that the packaged client and broker exchange QoS 0 and QoS 1 messages over TCP in
/// a native image, both through a handler and through receiving on the caller's thread.
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
            var handled = new LinkedBlockingQueue<MqttMessage>();
            Subscription push = client.subscribe(
                    Subscribe.of(new TopicFilter("smoke/push"), Qos.AT_LEAST_ONCE),
                    delivery -> handled.add(delivery.message())
            );
            Subscription pull = client.subscribe(
                    Subscribe.of(new TopicFilter("smoke/pull"), Qos.AT_LEAST_ONCE)
                            .withAckMode(AckMode.MANUAL)
            );
            push.established().get(5, SECONDS);
            pull.established().get(5, SECONDS);

            for (Qos qos : List.of(Qos.AT_MOST_ONCE, Qos.AT_LEAST_ONCE)) {
                var pushed = MqttMessage.of(new Topic("smoke/push"), new byte[] {42}, qos, false);
                client.publish(pushed).get(5, SECONDS);
                if (!pushed.equals(handled.poll(5, SECONDS))) {
                    throw new AssertionError(qos + " delivery to a handler changed the message");
                }

                var pulled = MqttMessage.of(new Topic("smoke/pull"), new byte[] {7}, qos, false);
                client.publish(pulled).get(5, SECONDS);
                if (!(pull.receive(Duration.ofSeconds(5))
                        instanceof ReceiveResult.Received(var delivery))
                        || !pulled.equals(delivery.message())) {
                    throw new AssertionError(qos + " delivery to a receiver changed the message");
                }
                delivery.ack();
            }

            push.unsubscribe().get(5, SECONDS);
            pull.unsubscribe().get(5, SECONDS);
            if (!(pull.receive() instanceof ReceiveResult.Ended)) {
                throw new AssertionError("A removed subscription kept receiving");
            }
            client.disconnect().get(5, SECONDS);
        }
        System.out.println("hato native smoke: QoS 0/1 through handler and receive OK");
    }
}
