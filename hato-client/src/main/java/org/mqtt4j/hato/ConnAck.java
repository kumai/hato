package org.mqtt4j.hato;

import java.util.Objects;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// The broker's successful connection acknowledgement, with the limits that the broker announced
/// for the connection. MQTT 3.1.1 brokers announce none, so a 3.1.1 connection reports the
/// values that MQTT 5.0 assumes when a limit is not announced: QoS 2, retained messages and
/// wildcard and shared subscriptions available, and the largest packet that MQTT allows.
public final class ConnAck {

    /// The largest MQTT packet, including the fixed header: a Remaining Length of 268435455
    /// bytes after one byte of packet type and four bytes of length.
    private static final int LARGEST_PACKET_SIZE = 268_435_460;

    private final boolean sessionPresent;

    private final long connectionAttempt;

    private final Qos maximumQos;

    private final boolean retainAvailable;

    private final boolean wildcardSubscriptionAvailable;

    private final boolean sharedSubscriptionAvailable;

    private final int maximumPacketSize;

    private ConnAck(
            boolean sessionPresent,
            long connectionAttempt,
            Qos maximumQos,
            boolean retainAvailable,
            boolean wildcardSubscriptionAvailable,
            boolean sharedSubscriptionAvailable,
            int maximumPacketSize
    ) {
        this.sessionPresent = sessionPresent;
        this.connectionAttempt = connectionAttempt;
        this.maximumQos = maximumQos;
        this.retainAvailable = retainAvailable;
        this.wildcardSubscriptionAvailable = wildcardSubscriptionAvailable;
        this.sharedSubscriptionAvailable = sharedSubscriptionAvailable;
        this.maximumPacketSize = maximumPacketSize;
    }

    /// Describes a successful acknowledgement of connection attempt number `connectionAttempt`,
    /// and whether the broker resumed a session, from a broker that announced no limits. The
    /// `with` methods describe the limits that a broker announced.
    ///
    /// @throws IllegalArgumentException if `connectionAttempt` is not positive
    public static ConnAck of(boolean sessionPresent, long connectionAttempt) {
        if (connectionAttempt < 1) {
            throw new IllegalArgumentException("Attempt numbers start at 1");
        }
        return new ConnAck(
                sessionPresent,
                connectionAttempt,
                Qos.EXACTLY_ONCE,
                true,
                true,
                true,
                LARGEST_PACKET_SIZE
        );
    }

    /// Returns a copy with a different highest QoS at which the broker accepts publications.
    public ConnAck withMaximumQos(Qos maximumQos) {
        return new ConnAck(
                sessionPresent,
                connectionAttempt,
                requireNonNull(maximumQos, "maximumQos"),
                retainAvailable,
                wildcardSubscriptionAvailable,
                sharedSubscriptionAvailable,
                maximumPacketSize
        );
    }

    /// Returns a copy with a different answer to whether the broker accepts retained
    /// publications.
    public ConnAck withRetainAvailable(boolean retainAvailable) {
        return new ConnAck(
                sessionPresent,
                connectionAttempt,
                maximumQos,
                retainAvailable,
                wildcardSubscriptionAvailable,
                sharedSubscriptionAvailable,
                maximumPacketSize
        );
    }

    /// Returns a copy with a different answer to whether the broker accepts wildcard
    /// subscriptions.
    public ConnAck withWildcardSubscriptionAvailable(boolean wildcardSubscriptionAvailable) {
        return new ConnAck(
                sessionPresent,
                connectionAttempt,
                maximumQos,
                retainAvailable,
                wildcardSubscriptionAvailable,
                sharedSubscriptionAvailable,
                maximumPacketSize
        );
    }

    /// Returns a copy with a different answer to whether the broker accepts shared
    /// subscriptions.
    public ConnAck withSharedSubscriptionAvailable(boolean sharedSubscriptionAvailable) {
        return new ConnAck(
                sessionPresent,
                connectionAttempt,
                maximumQos,
                retainAvailable,
                wildcardSubscriptionAvailable,
                sharedSubscriptionAvailable,
                maximumPacketSize
        );
    }

    /// Returns a copy with a different largest packet that the broker accepts.
    ///
    /// @throws IllegalArgumentException if `maximumPacketSize` is not between 1 and 268435460
    public ConnAck withMaximumPacketSize(int maximumPacketSize) {
        if (maximumPacketSize < 1 || maximumPacketSize > LARGEST_PACKET_SIZE) {
            throw new IllegalArgumentException(
                    "Maximum packet size must be 1.." + LARGEST_PACKET_SIZE + " bytes"
            );
        }
        return new ConnAck(
                sessionPresent,
                connectionAttempt,
                maximumQos,
                retainAvailable,
                wildcardSubscriptionAvailable,
                sharedSubscriptionAvailable,
                maximumPacketSize
        );
    }

    /// Whether the broker resumed an existing session.
    public boolean sessionPresent() {
        return sessionPresent;
    }

    /// The number of the connection attempt that this acknowledgement established. A client
    /// numbers its attempts consecutively from 1, whether the application's
    /// [MqttClient#connect()] or an automatic reconnection started them.
    public long connectionAttempt() {
        return connectionAttempt;
    }

    /// The highest QoS at which the broker accepts publications on this connection.
    public Qos maximumQos() {
        return maximumQos;
    }

    /// Whether the broker accepts publications with the retain flag on this connection.
    public boolean retainAvailable() {
        return retainAvailable;
    }

    /// Whether the broker accepts subscriptions whose filters contain wildcards.
    public boolean wildcardSubscriptionAvailable() {
        return wildcardSubscriptionAvailable;
    }

    /// Whether the broker accepts shared subscriptions.
    public boolean sharedSubscriptionAvailable() {
        return sharedSubscriptionAvailable;
    }

    /// The largest packet, in bytes including the fixed header, that the broker accepts on this
    /// connection.
    public int maximumPacketSize() {
        return maximumPacketSize;
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof ConnAck that
                && sessionPresent == that.sessionPresent
                && connectionAttempt == that.connectionAttempt
                && maximumQos == that.maximumQos
                && retainAvailable == that.retainAvailable
                && wildcardSubscriptionAvailable == that.wildcardSubscriptionAvailable
                && sharedSubscriptionAvailable == that.sharedSubscriptionAvailable
                && maximumPacketSize == that.maximumPacketSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                sessionPresent,
                connectionAttempt,
                maximumQos,
                retainAvailable,
                wildcardSubscriptionAvailable,
                sharedSubscriptionAvailable,
                maximumPacketSize
        );
    }

    @Override
    public String toString() {
        return "ConnAck[sessionPresent=" + sessionPresent
                + ", connectionAttempt=" + connectionAttempt
                + ", maximumQos=" + maximumQos
                + ", retainAvailable=" + retainAvailable
                + ", wildcardSubscriptionAvailable=" + wildcardSubscriptionAvailable
                + ", sharedSubscriptionAvailable=" + sharedSubscriptionAvailable
                + ", maximumPacketSize=" + maximumPacketSize + "]";
    }
}
