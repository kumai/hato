package org.mqtt4j.hato.testkit;

import java.util.Objects;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.Qos;

import static java.util.Objects.requireNonNull;

/// The capabilities that an embedded broker announces to MQTT 5.0 clients in CONNACK and then
/// holds their connections to, for [BrokerControls#announce(BrokerCapabilities)]. Start from
/// [#defaults()] and change what a test needs with the `with` methods, which return copies.
///
/// The broker refuses an MQTT 5.0 connection whose Will the capabilities forbid, and ends a
/// connection that sends what they forbid, with the reason code that MQTT 5.0 defines for it.
/// CONNACK leaves out each capability whose value is what MQTT 5.0 assumes without it: QoS 2,
/// retained messages and wildcard subscriptions available, a Receive Maximum of 65,535, the
/// largest packet that MQTT allows, and no Topic Aliases. Whatever the value, the broker
/// announces that it supports neither shared subscriptions nor Subscription Identifiers.
///
/// Experimental, like [BrokerControls].
public final class BrokerCapabilities {

    /// The largest MQTT packet, including the fixed header: a Remaining Length of 268435455
    /// bytes after one byte of packet type and four bytes of length.
    private static final int LARGEST_PACKET_SIZE = 268_435_460;

    private static final BrokerCapabilities DEFAULTS = new BrokerCapabilities(
            Qos.EXACTLY_ONCE,
            true,
            true,
            65_535,
            LARGEST_PACKET_SIZE,
            16
    );

    private final Qos maximumQos;

    private final boolean retainAvailable;

    private final boolean wildcardSubscriptionAvailable;

    private final int receiveMaximum;

    private final int maximumPacketSize;

    private final int topicAliasMaximum;

    private BrokerCapabilities(
            Qos maximumQos,
            boolean retainAvailable,
            boolean wildcardSubscriptionAvailable,
            int receiveMaximum,
            int maximumPacketSize,
            int topicAliasMaximum
    ) {
        this.maximumQos = maximumQos;
        this.retainAvailable = retainAvailable;
        this.wildcardSubscriptionAvailable = wildcardSubscriptionAvailable;
        this.receiveMaximum = receiveMaximum;
        this.maximumPacketSize = maximumPacketSize;
        this.topicAliasMaximum = topicAliasMaximum;
    }

    /// What the broker announces until a test announces something else: the values that MQTT
    /// 5.0 assumes, except that each client may set up to 16 Topic Aliases.
    public static BrokerCapabilities defaults() {
        return DEFAULTS;
    }

    /// Returns a copy with a different highest QoS at which a client may publish, Wills
    /// included. The broker grants subscriptions at most this QoS as well.
    ///
    /// @throws NullPointerException if `maximumQos` is null
    public BrokerCapabilities withMaximumQos(Qos maximumQos) {
        return new BrokerCapabilities(
                requireNonNull(maximumQos, "maximumQos must not be null"),
                retainAvailable,
                wildcardSubscriptionAvailable,
                receiveMaximum,
                maximumPacketSize,
                topicAliasMaximum
        );
    }

    /// Returns a copy with a different answer to whether a client may publish retained
    /// messages, Wills included.
    public BrokerCapabilities withRetainAvailable(boolean retainAvailable) {
        return new BrokerCapabilities(
                maximumQos,
                retainAvailable,
                wildcardSubscriptionAvailable,
                receiveMaximum,
                maximumPacketSize,
                topicAliasMaximum
        );
    }

    /// Returns a copy with a different answer to whether a client may subscribe to filters
    /// that contain wildcards.
    public BrokerCapabilities withWildcardSubscriptionAvailable(
            boolean wildcardSubscriptionAvailable
    ) {
        return new BrokerCapabilities(
                maximumQos,
                retainAvailable,
                wildcardSubscriptionAvailable,
                receiveMaximum,
                maximumPacketSize,
                topicAliasMaximum
        );
    }

    /// Returns a copy with a different number of QoS 1 and QoS 2 publications that a client may
    /// have unanswered at once. A publication counts until the broker sends its PUBACK or
    /// PUBCOMP, so one whose response [BrokerControls] holds back goes on counting.
    ///
    /// @throws IllegalArgumentException if `receiveMaximum` is not between 1 and 65535
    public BrokerCapabilities withReceiveMaximum(int receiveMaximum) {
        if (receiveMaximum < 1 || receiveMaximum > 65_535) {
            throw new IllegalArgumentException("Receive Maximum must be 1..65535");
        }
        return new BrokerCapabilities(
                maximumQos,
                retainAvailable,
                wildcardSubscriptionAvailable,
                receiveMaximum,
                maximumPacketSize,
                topicAliasMaximum
        );
    }

    /// Returns a copy with a different largest packet, in bytes including the fixed header,
    /// that a client may send.
    ///
    /// @throws IllegalArgumentException if `maximumPacketSize` is not between 1 and 268435460
    public BrokerCapabilities withMaximumPacketSize(int maximumPacketSize) {
        if (maximumPacketSize < 1 || maximumPacketSize > LARGEST_PACKET_SIZE) {
            throw new IllegalArgumentException(
                    "Maximum Packet Size must be 1.." + LARGEST_PACKET_SIZE + " bytes"
            );
        }
        return new BrokerCapabilities(
                maximumQos,
                retainAvailable,
                wildcardSubscriptionAvailable,
                receiveMaximum,
                maximumPacketSize,
                topicAliasMaximum
        );
    }

    /// Returns a copy with a different number of Topic Aliases that a client may set for its
    /// connection; with 0, a client may set none.
    ///
    /// @throws IllegalArgumentException if `topicAliasMaximum` is not between 0 and 65535
    public BrokerCapabilities withTopicAliasMaximum(int topicAliasMaximum) {
        if (topicAliasMaximum < 0 || topicAliasMaximum > 65_535) {
            throw new IllegalArgumentException("Topic Alias Maximum must be 0..65535");
        }
        return new BrokerCapabilities(
                maximumQos,
                retainAvailable,
                wildcardSubscriptionAvailable,
                receiveMaximum,
                maximumPacketSize,
                topicAliasMaximum
        );
    }

    /// The highest QoS at which a client may publish, Wills included.
    public Qos maximumQos() {
        return maximumQos;
    }

    /// Whether a client may publish retained messages, Wills included.
    public boolean retainAvailable() {
        return retainAvailable;
    }

    /// Whether a client may subscribe to filters that contain wildcards.
    public boolean wildcardSubscriptionAvailable() {
        return wildcardSubscriptionAvailable;
    }

    /// The number of QoS 1 and QoS 2 publications that a client may have unanswered at once.
    public int receiveMaximum() {
        return receiveMaximum;
    }

    /// The largest packet, in bytes including the fixed header, that a client may send.
    public int maximumPacketSize() {
        return maximumPacketSize;
    }

    /// The number of Topic Aliases that a client may set for its connection.
    public int topicAliasMaximum() {
        return topicAliasMaximum;
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof BrokerCapabilities that
                && maximumQos == that.maximumQos
                && retainAvailable == that.retainAvailable
                && wildcardSubscriptionAvailable == that.wildcardSubscriptionAvailable
                && receiveMaximum == that.receiveMaximum
                && maximumPacketSize == that.maximumPacketSize
                && topicAliasMaximum == that.topicAliasMaximum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                maximumQos,
                retainAvailable,
                wildcardSubscriptionAvailable,
                receiveMaximum,
                maximumPacketSize,
                topicAliasMaximum
        );
    }

    @Override
    public String toString() {
        return "BrokerCapabilities[maximumQos=" + maximumQos
                + ", retainAvailable=" + retainAvailable
                + ", wildcardSubscriptionAvailable=" + wildcardSubscriptionAvailable
                + ", receiveMaximum=" + receiveMaximum
                + ", maximumPacketSize=" + maximumPacketSize
                + ", topicAliasMaximum=" + topicAliasMaximum + "]";
    }
}
