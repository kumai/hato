package org.mqtt4j.hato.comparison;

import java.util.Locale;

/// The client libraries under comparison, and the ways of using them that the comparison tells
/// apart.
enum Library {
    /// Eclipse Paho's asynchronous client, used as the comparison needs it.
    PAHO,
    /// Eclipse Paho's blocking client, which subscribes again after each reconnection and waits
    /// for every answer; see [PahoBlockingSubscriber].
    PAHO_BLOCKING,
    HATO,
    HIVEMQ;

    /// Connects a client of this library to the broker at `host` and `port`, asking for a
    /// keep-alive of `keepAliveSeconds`, 60 being every library's default.
    ///
    /// With `reconnect`, the client reconnects by itself after it loses its connection and makes
    /// its subscriptions again, using what its library offers for that. Hato does so in any case,
    /// by default, and `PAHO_BLOCKING` in any case.
    SubscriberClient connect(
            String host,
            int port,
            String clientId,
            int keepAliveSeconds,
            boolean reconnect
    ) throws Exception {
        return switch (this) {
            case PAHO -> new PahoSubscriber(host, port, clientId, keepAliveSeconds, reconnect);
            case PAHO_BLOCKING ->
                    new PahoBlockingSubscriber(host, port, clientId, keepAliveSeconds);
            case HATO -> new HatoSubscriber(host, port, clientId, keepAliveSeconds);
            case HIVEMQ -> new HiveMqSubscriber(host, port, clientId, keepAliveSeconds, reconnect);
        };
    }

    /// The name used in arguments and results, such as `paho-blocking`.
    String label() {
        return name().toLowerCase(Locale.ROOT).replace('_', '-');
    }

    static Library of(String label) {
        return valueOf(label.toUpperCase(Locale.ROOT).replace('-', '_'));
    }
}
