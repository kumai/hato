package org.mqtt4j.hato.broker.internal;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.internal.common.value.Qos;

import static java.util.Objects.requireNonNull;

// What the broker announces to an MQTT 5.0 client in CONNACK and then holds the client to: the
// capabilities that a test can narrow. A null value is not announced, which MQTT 5.0 reads as no
// limit, and so is a maximum QoS of 2, retained messages, wildcards, and Subscription
// Identifiers. A connection keeps what it was accepted with.
record Capabilities(
        Qos maximumQos,
        boolean retainAvailable,
        boolean wildcardSubscriptionAvailable,
        boolean subscriptionIdentifiersAvailable,
        @Nullable Integer receiveMaximum,
        @Nullable Integer maximumPacketSize,
        int topicAliasMaximum,
        @Nullable Integer serverKeepAlive
) {

    // The broker's own: no limit, and up to 16 Topic Aliases, the client's own value.
    // The aliases of one connection hold at most this many topics of up to 65,535 bytes each.
    static final Capabilities DEFAULT = new Capabilities(
            Qos.EXACTLY_ONCE,
            true,
            true,
            true,
            null,
            null,
            16,
            null
    );

    // What an MQTT 3.1.1 connection works by: it learns of no limit, so none applies, and it has
    // no Topic Aliases.
    static final Capabilities UNANNOUNCED = new Capabilities(
            Qos.EXACTLY_ONCE,
            true,
            true,
            true,
            null,
            null,
            0,
            null
    );

    // Throws IllegalArgumentException for a value that MQTT 5.0 cannot announce.
    Capabilities {
        requireNonNull(maximumQos, "maximumQos must not be null");
        requireRange(receiveMaximum, 1, 65_535, "receiveMaximum");
        requireRange(maximumPacketSize, 1, Connection.MAX_PACKET_SIZE, "maximumPacketSize");
        requireRange(topicAliasMaximum, 0, 65_535, "topicAliasMaximum");
        requireRange(serverKeepAlive, 0, 65_535, "serverKeepAlive");
    }

    private static void requireRange(
            @Nullable Integer value,
            int minimum,
            int maximum,
            String name
    ) {
        if (value != null && (value < minimum || value > maximum)) {
            throw new IllegalArgumentException(
                    name + " must be between " + minimum + " and " + maximum
            );
        }
    }
}
