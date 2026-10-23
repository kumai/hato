package org.mqtt4j.hato.comparison;

import java.util.Locale;

/// The client libraries under comparison.
enum Library {
    PAHO,
    HATO,
    HIVEMQ;

    /// Connects a client of this library to the broker at `host` and `port`, asking for a
    /// keep-alive of `keepAliveSeconds`; 60 is every library's default.
    SubscriberClient connect(String host, int port, String clientId, int keepAliveSeconds)
            throws Exception {
        return switch (this) {
            case PAHO -> new PahoSubscriber(host, port, clientId, keepAliveSeconds);
            case HATO -> new HatoSubscriber(host, port, clientId, keepAliveSeconds);
            case HIVEMQ -> new HiveMqSubscriber(host, port, clientId, keepAliveSeconds);
        };
    }

    /// The name used in arguments and results.
    String label() {
        return name().toLowerCase(Locale.ROOT);
    }

    static Library of(String label) {
        return valueOf(label.toUpperCase(Locale.ROOT));
    }
}
