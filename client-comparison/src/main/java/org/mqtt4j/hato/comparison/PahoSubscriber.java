package org.mqtt4j.hato.comparison;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/// Eclipse Paho's asynchronous client for MQTT 3.1.1.
///
/// Session state stays in memory instead of Paho's default files, and each subscription has a
/// message listener of its own. Paho runs every listener of a client on one callback thread,
/// stops reading the socket while ten messages wait for it, and acknowledges a QoS 1 message once
/// the listener has returned.
///
/// With reconnection, Paho reconnects by itself, and once it has, this client subscribes again to
/// every topic filter from `connectComplete`, without waiting for the broker's answers. Compare
/// [PahoBlockingSubscriber], which waits for each.
final class PahoSubscriber implements SubscriberClient {

    private final MqttAsyncClient client;

    private final AtomicLong connectionsLost = new AtomicLong();

    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();

    private record Subscription(String filter, int qos, IMqttMessageListener listener) {}

    PahoSubscriber(String host, int port, String clientId, int keepAliveSeconds, boolean reconnect)
            throws MqttException {
        client = new MqttAsyncClient(
                "tcp://" + host + ":" + port,
                clientId,
                new MemoryPersistence()
        );
        // Messages go to the subscriptions' listeners; this callback sees only the connection.
        client.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnected, String serverUri) {
                if (reconnected) {
                    resubscribe();
                }
            }

            @Override
            public void connectionLost(Throwable cause) {
                connectionsLost.incrementAndGet();
                System.err.println("paho: connection lost: " + cause);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }
        });
        var options = new MqttConnectOptions();
        options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
        options.setCleanSession(true);
        options.setKeepAliveInterval(keepAliveSeconds);
        options.setAutomaticReconnect(reconnect);
        client.connect(options).waitForCompletion();
    }

    @Override
    public void subscribe(String filter, int qos, Handler handler) throws MqttException {
        IMqttMessageListener listener = (_, message) -> {
            // The payload is the message's own array, not a copy.
            byte[] payload = message.getPayload();
            handler.handle(Payloads.sequence(payload), Payloads.sentMicros(payload));
        };
        client.subscribe(filter, qos, null, null, listener).waitForCompletion();
        subscriptions.add(new Subscription(filter, qos, listener));
    }

    private void resubscribe() {
        for (Subscription subscription : subscriptions) {
            try {
                client.subscribe(
                        subscription.filter(),
                        subscription.qos(),
                        null,
                        null,
                        subscription.listener()
                );
            } catch (MqttException e) {
                System.err.println("paho: could not subscribe again to " + subscription.filter()
                        + ": " + e);
            }
        }
    }

    @Override
    public long connectionsLost() {
        return connectionsLost.get();
    }

    @Override
    public void close() {
        try {
            client.disconnect().waitForCompletion();
            client.close();
        } catch (MqttException e) {
            throw new IllegalStateException("Paho did not disconnect", e);
        }
    }
}
