package org.mqtt4j.hato.comparison;

import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.TopicFilter;

/// Hato with its defaults, apart from the protocol version.
///
/// Each subscription runs its handler on a virtual thread of its own and acknowledges a QoS 1
/// message once the handler has returned. The client reads the socket regardless of its handlers,
/// holding up to 1,024 messages per subscription.
final class HatoSubscriber implements SubscriberClient {

    private final MqttClient client;

    private final AtomicLong connectionsLost = new AtomicLong();

    // Set before close() disconnects, so that its own ending is not counted.
    private volatile boolean closing;

    HatoSubscriber(String host, int port, String clientId) {
        client = MqttClient.builder()
                .server(URI.create("mqtt://" + host + ":" + port))
                .clientId(clientId)
                .version(MqttVersion.V3_1_1)
                .connectionListener(ending -> {
                    if (!closing) {
                        connectionsLost.incrementAndGet();
                        System.err.println("hato: connection ended: " + ending);
                    }
                })
                .build();
        client.connect().join();
    }

    @Override
    public void subscribe(String filter, int qos, Handler handler) {
        Subscription subscription = client.subscribe(
                Subscribe.of(new TopicFilter(filter), Qos.of(qos)),
                delivery -> {
                    // The payload is a copy: the message keeps its own bytes.
                    byte[] payload = delivery.message().payload();
                    handler.handle(Payloads.sequence(payload), Payloads.sentMicros(payload));
                }
        );
        subscription.established().join();
    }

    @Override
    public long connectionsLost() {
        return connectionsLost.get();
    }

    @Override
    public void close() {
        closing = true;
        try {
            client.disconnect().join();
        } finally {
            client.close();
        }
    }
}
