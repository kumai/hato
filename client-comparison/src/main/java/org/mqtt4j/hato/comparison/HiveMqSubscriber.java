package org.mqtt4j.hato.comparison;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;

/// HiveMQ's asynchronous client for MQTT 3.1.1.
///
/// By default it acknowledges a QoS 1 message as soon as the message is handed to its callback;
/// manual acknowledgement at the end of the callback makes it acknowledge when the other clients
/// do. Callbacks run on RxJava's computation threads, the client's default, one at a time per
/// subscription. The client reads the socket regardless of its callbacks, holding up to 65,535
/// messages.
final class HiveMqSubscriber implements SubscriberClient {

    private final Mqtt3AsyncClient client;

    private final AtomicLong connectionsLost = new AtomicLong();

    // Set before close() disconnects, so that its own ending is not counted.
    private volatile boolean closing;

    HiveMqSubscriber(String host, int port, String clientId) {
        client = MqttClient.builder()
                .useMqttVersion3()
                .identifier(clientId)
                .serverHost(host)
                .serverPort(port)
                .addDisconnectedListener(context -> {
                    if (!closing) {
                        connectionsLost.incrementAndGet();
                        System.err.println("hivemq: disconnected: " + context.getCause());
                    }
                })
                .buildAsync();
        client.connectWith()
                .cleanSession(true)
                .send()
                .join();
    }

    @Override
    public void subscribe(String filter, int qos, Handler handler) {
        client.subscribeWith()
                .topicFilter(filter)
                .qos(qos == 0 ? MqttQos.AT_MOST_ONCE : MqttQos.AT_LEAST_ONCE)
                .callback(publish -> {
                    // The payload is a read-only view of the message's bytes, not a copy.
                    ByteBuffer payload = publish.getPayload().orElseThrow();
                    handler.handle(Payloads.sequence(payload), Payloads.sentMicros(payload));
                    // Only messages that need an acknowledgement accept one.
                    if (publish.getQos() != MqttQos.AT_MOST_ONCE) {
                        publish.acknowledge();
                    }
                })
                .manualAcknowledgement(true)
                .send()
                .join();
    }

    @Override
    public long connectionsLost() {
        return connectionsLost.get();
    }

    @Override
    public void close() {
        closing = true;
        client.disconnect().join();
    }
}
