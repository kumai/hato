package org.mqtt4j.hato;

import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.client.internal.common.protocol.SharedTopicFilter;

import static java.util.Objects.requireNonNull;

/// A request for one subscription. Shared filters use `$share/{group}/{filter}` syntax.
/// The original filter, including a shared group, identifies duplicate subscriptions.
///
/// Instances are immutable values; each `with` method returns a copy with one setting changed.
/// A setting left unchanged takes its default, which can depend on how the subscription
/// receives messages.
///
/// Subscriptions support QoS 0, QoS 1, and QoS 2.
public final class Subscribe {

    private static final int DEFAULT_BUFFER_CAPACITY = 1024;

    private final TopicFilter filter;

    private final Qos qos;

    // Null leaves the choice to the subscription style.
    private final @Nullable AckMode ackMode;

    private final int bufferCapacity;

    private final FailurePolicy failurePolicy;

    private Subscribe(
            TopicFilter filter,
            Qos qos,
            @Nullable AckMode ackMode,
            int bufferCapacity,
            FailurePolicy failurePolicy
    ) {
        this.filter = filter;
        this.qos = qos;
        this.ackMode = ackMode;
        this.bufferCapacity = bufferCapacity;
        this.failurePolicy = failurePolicy;
    }

    /// Requests `filter` with `qos` as the maximum quality of service, the default
    /// acknowledgement mode, a buffer capacity of 1024, and [FailurePolicy#STOP].
    ///
    /// @throws IllegalArgumentException if shared syntax is invalid
    public static Subscribe of(TopicFilter filter, Qos qos) {
        requireNonNull(filter, "Filter must not be null");
        requireNonNull(qos, "QoS must not be null");
        SharedTopicFilter.parse(filter);
        return new Subscribe(filter, qos, null, DEFAULT_BUFFER_CAPACITY, FailurePolicy.STOP);
    }

    /// Returns a copy that uses `ackMode` instead of the default.
    public Subscribe withAckMode(AckMode ackMode) {
        requireNonNull(ackMode, "Ack mode must not be null");
        return new Subscribe(filter, qos, ackMode, bufferCapacity, failurePolicy);
    }

    /// Returns a copy with a different [buffer capacity][#bufferCapacity()].
    ///
    /// @throws IllegalArgumentException if `bufferCapacity` is not positive
    public Subscribe withBufferCapacity(int bufferCapacity) {
        if (bufferCapacity < 1) {
            throw new IllegalArgumentException("Buffer capacity must be positive");
        }
        return new Subscribe(filter, qos, ackMode, bufferCapacity, failurePolicy);
    }

    /// Returns a copy that handles handler failures with `failurePolicy` instead of
    /// [FailurePolicy#STOP].
    public Subscribe withFailurePolicy(FailurePolicy failurePolicy) {
        requireNonNull(failurePolicy, "Failure policy must not be null");
        return new Subscribe(filter, qos, ackMode, bufferCapacity, failurePolicy);
    }

    /// The filter sent to the broker.
    public TopicFilter filter() {
        return filter;
    }

    /// The maximum quality of service requested.
    public Qos qos() {
        return qos;
    }

    /// When this subscription permits acknowledgement of a QoS 1 or 2 delivery. Empty uses the
    /// default: [AckMode#AFTER_HANDLER] with a handler, or [AckMode#ON_RECEIVE] for pull and
    /// hand-off subscriptions.
    public Optional<AckMode> ackMode() {
        return Optional.ofNullable(ackMode);
    }

    /// The maximum number of local deliveries this subscription holds at once. Pull subscriptions
    /// count queued deliveries and QoS 1/2 MANUAL deliveries awaiting ack; ON_RECEIVE and QoS 0
    /// release the slot at handoff. Hand-off subscriptions count deliveries waiting for their
    /// acceptor and release the slot when it accepts one, in either acknowledgement mode. Push
    /// subscriptions also count running handlers and deliveries whose PUBACK or PUBREC is
    /// pending; see [AckMode]. The client also bounds deliveries and bytes held across all of its
    /// subscriptions.
    ///
    /// What happens beyond these bounds depends on the QoS of the received message, which can
    /// exceed the requested one when another subscription of the client also matches. A QoS 0
    /// message that finds this subscription full is dropped for this subscription only, and
    /// other matching subscriptions still receive it. One that does not fit the client's bounds
    /// is dropped for every subscription. Waiting deliveries are kept, and the connection goes
    /// on. Drops are logged as a warning the first time for each subscription and bound, and at
    /// debug level after that. A QoS 1 or 2 message that exceeds any bound ends the connection
    /// instead. A small capacity for QoS 0 subscriptions leaves room in the client's bounds for
    /// QoS 1 and 2 messages.
    public int bufferCapacity() {
        return bufferCapacity;
    }

    /// What this subscription does when its handler throws; see [FailurePolicy]. A pull
    /// subscription has no handler and rejects [FailurePolicy#SKIP].
    public FailurePolicy failurePolicy() {
        return failurePolicy;
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof Subscribe that
                && filter.equals(that.filter)
                && qos == that.qos
                && ackMode == that.ackMode
                && bufferCapacity == that.bufferCapacity
                && failurePolicy == that.failurePolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(filter, qos, ackMode, bufferCapacity, failurePolicy);
    }

    @Override
    public String toString() {
        return "Subscribe[filter=" + filter + ", qos=" + qos + ", ackMode="
                + (ackMode == null ? "default" : ackMode) + ", bufferCapacity=" + bufferCapacity
                + ", failurePolicy=" + failurePolicy + "]";
    }
}
