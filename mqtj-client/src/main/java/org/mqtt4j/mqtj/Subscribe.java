package org.mqtt4j.mqtj;

import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.mqtj.client.internal.common.protocol.SharedTopicFilter;

import static java.util.Objects.requireNonNull;

/// A request for one subscription. Shared filters use `$share/{group}/{filter}` syntax.
/// The original filter, including a shared group, identifies duplicate subscriptions.
///
/// Instances are immutable values; each `with` method returns a copy with one setting changed.
/// A setting left unchanged takes its default, which can depend on how the subscription
/// receives messages.
///
/// All QoS values can be represented; the current client accepts QoS 0 and QoS 1 subscriptions.
public final class Subscribe {

    private static final int DEFAULT_BUFFER_CAPACITY = 1024;

    private final TopicFilter filter;

    private final Qos qos;

    // Null leaves the choice to the subscription style.
    private final @Nullable AckMode ackMode;

    private final int bufferCapacity;

    private Subscribe(TopicFilter filter, Qos qos, @Nullable AckMode ackMode, int bufferCapacity) {
        this.filter = filter;
        this.qos = qos;
        this.ackMode = ackMode;
        this.bufferCapacity = bufferCapacity;
    }

    /// Requests `filter` with `qos` as the maximum quality of service, the default
    /// acknowledgement mode, and a buffer capacity of 1024.
    ///
    /// @throws IllegalArgumentException if shared syntax is invalid
    public static Subscribe of(TopicFilter filter, Qos qos) {
        requireNonNull(filter, "Filter must not be null");
        requireNonNull(qos, "QoS must not be null");
        SharedTopicFilter.parse(filter);
        return new Subscribe(filter, qos, null, DEFAULT_BUFFER_CAPACITY);
    }

    /// Returns a copy that uses `ackMode` instead of the default.
    public Subscribe withAckMode(AckMode ackMode) {
        requireNonNull(ackMode, "Ack mode must not be null");
        return new Subscribe(filter, qos, ackMode, bufferCapacity);
    }

    /// Returns a copy with a different [buffer capacity][#bufferCapacity()].
    ///
    /// @throws IllegalArgumentException if `bufferCapacity` is not positive
    public Subscribe withBufferCapacity(int bufferCapacity) {
        if (bufferCapacity < 1) {
            throw new IllegalArgumentException("Buffer capacity must be positive");
        }
        return new Subscribe(filter, qos, ackMode, bufferCapacity);
    }

    /// The filter sent to the broker.
    public TopicFilter filter() {
        return filter;
    }

    /// The maximum quality of service requested.
    public Qos qos() {
        return qos;
    }

    /// When this subscription permits acknowledgement of a QoS 1 delivery. Empty uses the
    /// default: [AckMode#AFTER_HANDLER] with a handler, or [AckMode#ON_RECEIVE] for pull
    /// subscriptions.
    public Optional<AckMode> ackMode() {
        return Optional.ofNullable(ackMode);
    }

    /// The maximum number of local deliveries this subscription holds at once. Pull subscriptions
    /// count queued deliveries and QoS 1 MANUAL deliveries awaiting ack; ON_RECEIVE and QoS 0
    /// release the slot at handoff. Push subscriptions also count running handlers and deliveries
    /// whose PUBACK is pending; see [AckMode]. The client also bounds the deliveries and bytes
    /// held across all of its subscriptions. A message that exceeds any of these bounds ends the
    /// connection.
    public int bufferCapacity() {
        return bufferCapacity;
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof Subscribe that
                && filter.equals(that.filter)
                && qos == that.qos
                && ackMode == that.ackMode
                && bufferCapacity == that.bufferCapacity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(filter, qos, ackMode, bufferCapacity);
    }

    @Override
    public String toString() {
        return "Subscribe[filter=" + filter + ", qos=" + qos + ", ackMode="
                + (ackMode == null ? "default" : ackMode) + ", bufferCapacity=" + bufferCapacity
                + "]";
    }
}
