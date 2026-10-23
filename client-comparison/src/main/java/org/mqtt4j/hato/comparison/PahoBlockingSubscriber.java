package org.mqtt4j.hato.comparison;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/// Eclipse Paho's blocking `MqttClient`, set up as an application that leaves reconnection to
/// Paho might use it, for comparing how such an application recovers from a lost connection.
///
/// The client waits up to ten seconds for each operation, with automatic reconnection, a clean
/// session, a connection timeout of three seconds, and session state in memory. After each
/// reconnection it subscribes again to every topic filter, one at a time and in the order of a
/// `HashMap`, from `connectComplete`, which Paho calls on its callback thread. Each subscription
/// waits for the broker's answer there; Paho releases that wait on its receiving thread as soon
/// as the answer arrives, so the listeners are held up only that long.
final class PahoBlockingSubscriber implements SubscriberClient {

    private static final long TIME_TO_WAIT_MILLIS = 10_000;

    private final MqttClient client;

    private final AtomicLong connectionsLost = new AtomicLong();

    // Keyed by topic filter in a HashMap, which sets the order in which they are made again. The
    // callback thread reads it after reconnecting, so it is guarded.
    private final Map<String, Subscription> subscriptions = new HashMap<>();

    private record Subscription(int qos, IMqttMessageListener listener) {}

    PahoBlockingSubscriber(String host, int port, String clientId, int keepAliveSeconds)
            throws MqttException {
        client = new MqttClient("tcp://" + host + ":" + port, clientId, new MemoryPersistence());
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
                System.err.println("paho-blocking: connection lost: " + cause);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }
        });
        client.setTimeToWait(TIME_TO_WAIT_MILLIS);
        var options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(3);
        options.setMaxInflight(100);
        options.setKeepAliveInterval(keepAliveSeconds);
        client.connect(options);
    }

    @Override
    public void subscribe(String filter, int qos, Handler handler) throws MqttException {
        IMqttMessageListener listener = (_, message) -> {
            byte[] payload = message.getPayload();
            handler.handle(Payloads.sequence(payload), Payloads.sentMicros(payload));
        };
        client.subscribe(filter, qos, listener);
        synchronized (subscriptions) {
            subscriptions.put(filter, new Subscription(qos, listener));
        }
    }

    /// Subscribes again to every topic filter, each subscription waiting for its answer.
    private void resubscribe() {
        synchronized (subscriptions) {
            subscriptions.forEach((filter, subscription) -> {
                try {
                    client.subscribe(filter, subscription.qos(), subscription.listener());
                } catch (MqttException e) {
                    System.err.println("paho-blocking: could not subscribe again to " + filter
                            + ": " + e);
                }
            });
        }
    }

    @Override
    public long connectionsLost() {
        return connectionsLost.get();
    }

    @Override
    public void close() {
        try {
            try {
                if (client.isConnected()) {
                    client.disconnect();
                }
            } finally {
                client.close();
            }
        } catch (MqttException e) {
            throw new IllegalStateException("Paho did not disconnect", e);
        }
    }
}
