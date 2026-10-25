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
/// Subscriptions support QoS 0, QoS 1, and QoS 2. The subscription options No Local, Retain As
/// Published, and Retain Handling need MQTT 5.0: an MQTT 3.1.1 client refuses a request that
/// changes any of them from its default with [IllegalArgumentException].
public final class Subscribe {

    private static final int DEFAULT_BUFFER_CAPACITY = 1024;

    private final TopicFilter filter;

    private final Qos qos;

    // Null leaves the choice to the subscription style.
    private final @Nullable AckMode ackMode;

    private final int bufferCapacity;

    private final FailurePolicy failurePolicy;

    private final boolean noLocal;

    private final boolean retainAsPublished;

    private final RetainHandling retainHandling;

    private Subscribe(
            TopicFilter filter,
            Qos qos,
            @Nullable AckMode ackMode,
            int bufferCapacity,
            FailurePolicy failurePolicy,
            boolean noLocal,
            boolean retainAsPublished,
            RetainHandling retainHandling
    ) {
        this.filter = filter;
        this.qos = qos;
        this.ackMode = ackMode;
        this.bufferCapacity = bufferCapacity;
        this.failurePolicy = failurePolicy;
        this.noLocal = noLocal;
        this.retainAsPublished = retainAsPublished;
        this.retainHandling = retainHandling;
    }

    /// Requests `filter` with `qos` as the maximum quality of service, the default
    /// acknowledgement mode, a buffer capacity of 1024, [FailurePolicy#STOP], and the MQTT 5.0
    /// defaults for the subscription options: No Local and Retain As Published off, and
    /// [RetainHandling#SEND].
    ///
    /// @throws IllegalArgumentException if shared syntax is invalid
    public static Subscribe of(TopicFilter filter, Qos qos) {
        requireNonNull(filter, "Filter must not be null");
        requireNonNull(qos, "QoS must not be null");
        SharedTopicFilter.parse(filter);
        return new Subscribe(
                filter,
                qos,
                null,
                DEFAULT_BUFFER_CAPACITY,
                FailurePolicy.STOP,
                false,
                false,
                RetainHandling.SEND
        );
    }

    /// Returns a copy that uses `ackMode` instead of the default.
    public Subscribe withAckMode(AckMode ackMode) {
        requireNonNull(ackMode, "Ack mode must not be null");
        return new Subscribe(
                filter,
                qos,
                ackMode,
                bufferCapacity,
                failurePolicy,
                noLocal,
                retainAsPublished,
                retainHandling
        );
    }

    /// Returns a copy with a different [buffer capacity][#bufferCapacity()].
    ///
    /// @throws IllegalArgumentException if `bufferCapacity` is not positive
    public Subscribe withBufferCapacity(int bufferCapacity) {
        if (bufferCapacity < 1) {
            throw new IllegalArgumentException("Buffer capacity must be positive");
        }
        return new Subscribe(
                filter,
                qos,
                ackMode,
                bufferCapacity,
                failurePolicy,
                noLocal,
                retainAsPublished,
                retainHandling
        );
    }

    /// Returns a copy that handles handler failures with `failurePolicy` instead of
    /// [FailurePolicy#STOP].
    public Subscribe withFailurePolicy(FailurePolicy failurePolicy) {
        requireNonNull(failurePolicy, "Failure policy must not be null");
        return new Subscribe(
                filter,
                qos,
                ackMode,
                bufferCapacity,
                failurePolicy,
                noLocal,
                retainAsPublished,
                retainHandling
        );
    }

    /// Returns a copy that asks the broker not to deliver this client's own publications to this
    /// subscription (the MQTT 5.0 No Local option), or to deliver them as usual.
    ///
    /// @throws IllegalArgumentException if `noLocal` is true for a shared subscription, which
    ///     MQTT 5.0 does not allow
    public Subscribe withNoLocal(boolean noLocal) {
        if (noLocal && SharedTopicFilter.parse(filter).isPresent()) {
            throw new IllegalArgumentException("A shared subscription cannot set No Local");
        }
        return new Subscribe(
                filter,
                qos,
                ackMode,
                bufferCapacity,
                failurePolicy,
                noLocal,
                retainAsPublished,
                retainHandling
        );
    }

    /// Returns a copy that asks the broker to keep the retain flag that each publisher set on
    /// the messages that it delivers to this subscription (the MQTT 5.0 Retain As Published
    /// option), or to clear it as usual; see [MqttMessage#retain()].
    public Subscribe withRetainAsPublished(boolean retainAsPublished) {
        return new Subscribe(
                filter,
                qos,
                ackMode,
                bufferCapacity,
                failurePolicy,
                noLocal,
                retainAsPublished,
                retainHandling
        );
    }

    /// Returns a copy that decides when the broker sends the matching retained messages as it
    /// makes this subscription (the MQTT 5.0 Retain Handling option).
    public Subscribe withRetainHandling(RetainHandling retainHandling) {
        requireNonNull(retainHandling, "Retain handling must not be null");
        return new Subscribe(
                filter,
                qos,
                ackMode,
                bufferCapacity,
                failurePolicy,
                noLocal,
                retainAsPublished,
                retainHandling
        );
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
    /// exceed the requested one when another subscription of the client also matches. A QoS 1 or
    /// 2 message that exceeds any bound ends the connection. To stay within them, the client
    /// drops QoS 0 messages instead, possibly before a bound is reached, and the connection goes
    /// on; [ClientStats.Receiving#dropped()] counts the drops by reason. Which QoS 0 messages it
    /// drops, and when, is not part of this contract and may change between versions. A small
    /// capacity for QoS 0 subscriptions leaves room in the client's bounds for QoS 1 and 2
    /// messages.
    public int bufferCapacity() {
        return bufferCapacity;
    }

    /// What this subscription does when its handler throws; see [FailurePolicy]. A pull
    /// subscription has no handler and rejects [FailurePolicy#SKIP].
    public FailurePolicy failurePolicy() {
        return failurePolicy;
    }

    /// Whether the broker withholds this client's own publications from this subscription.
    public boolean noLocal() {
        return noLocal;
    }

    /// Whether the broker keeps the publisher's retain flag on the messages that it delivers to
    /// this subscription.
    public boolean retainAsPublished() {
        return retainAsPublished;
    }

    /// When the broker sends the matching retained messages as it makes this subscription.
    public RetainHandling retainHandling() {
        return retainHandling;
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof Subscribe that
                && filter.equals(that.filter)
                && qos == that.qos
                && ackMode == that.ackMode
                && bufferCapacity == that.bufferCapacity
                && failurePolicy == that.failurePolicy
                && noLocal == that.noLocal
                && retainAsPublished == that.retainAsPublished
                && retainHandling == that.retainHandling;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                filter,
                qos,
                ackMode,
                bufferCapacity,
                failurePolicy,
                noLocal,
                retainAsPublished,
                retainHandling
        );
    }

    @Override
    public String toString() {
        return "Subscribe[filter=" + filter + ", qos=" + qos + ", ackMode="
                + (ackMode == null ? "default" : ackMode) + ", bufferCapacity=" + bufferCapacity
                + ", failurePolicy=" + failurePolicy + ", noLocal=" + noLocal
                + ", retainAsPublished=" + retainAsPublished
                + ", retainHandling=" + retainHandling + "]";
    }
}
