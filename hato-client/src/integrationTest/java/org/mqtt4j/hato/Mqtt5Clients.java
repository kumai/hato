package org.mqtt4j.hato;

import java.net.URI;
import java.time.Duration;

import org.mqtt4j.hato.client.internal.ClientActor;
import org.mqtt4j.hato.client.internal.DefaultTimerScheduler;
import org.mqtt4j.hato.client.internal.TcpTransport;
import org.mqtt4j.hato.spi.TimerScheduler;

/// Builds MQTT 5.0 clients for integration tests through the client's internal constructor,
/// because the builder does not accept MQTT 5.0 yet. Once it does, tests use the builder.
public final class Mqtt5Clients {

    private Mqtt5Clients() {
    }

    /// A TCP client of `endpoint` that keeps its session for `sessionExpiry` and does not
    /// reconnect.
    public static MqttClient client(URI endpoint, String clientId, Duration sessionExpiry) {
        TimerScheduler timer = DefaultTimerScheduler.instance;
        ReconnectPolicy policy = ReconnectPolicy.disabled();
        return new ClientActor(
                endpoint,
                clientId,
                Duration.ofSeconds(60),
                Duration.ofSeconds(30),
                16 * 1024 * 1024,
                timer,
                _ -> { },
                id -> new TcpTransport(id, timer),
                MqttVersion.V5,
                sessionExpiry,
                null,
                null,
                policy.backoff(),
                policy.initialRetry(),
                Duration.ofSeconds(60),
                DeliveryOptions::execution
        );
    }
}
