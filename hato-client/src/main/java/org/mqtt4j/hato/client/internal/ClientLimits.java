package org.mqtt4j.hato.client.internal;

/// A client's two capacity settings, and the internal limits that follow from them. The builder
/// checks the settings' ranges. The derived limits leave room beside the settings, so that a
/// small setting does not crowd QoS 0 publications, subscriptions, or control, and a large one
/// does not overflow the limits that it drives; they are internal and may change.
///
/// @param inflightPublishes the QoS 1 and QoS 2 publish exchanges that may be unfinished,
///     including those waiting to be sent and those that a persistent session keeps
/// @param subscriptions the subscription registrations, and the delivery lanes, that the client
///     may hold
public record ClientLimits(int inflightPublishes, int subscriptions) {

    /// The builder's defaults.
    public static final ClientLimits DEFAULT = new ClientLimits(64, 256);

    // QoS 0 publications share the operation slots with QoS 1 and QoS 2, so a small inflight
    // limit still leaves them this many.
    private static final int MIN_OPERATIONS = 64;

    // QoS 0 publications, subscriptions, and control share the regular send queue with the
    // exchanges, so a small inflight limit still leaves them this many entries.
    private static final int MIN_REGULAR_SENDS = 128;

    // The control events besides send completions, such as timers and connection changes.
    private static final int OTHER_CONTROL_EVENTS = 112;

    /// The operation slots that publish() holds until its future completes: one for each
    /// exchange that may be unfinished, and never fewer than [#MIN_OPERATIONS].
    int operations() {
        return Math.max(MIN_OPERATIONS, inflightPublishes);
    }

    /// The regular send queue's entries: twice the exchanges that may be unfinished, and never
    /// fewer than [#MIN_REGULAR_SENDS].
    int regularSends() {
        return Math.max(MIN_REGULAR_SENDS, 2 * inflightPublishes);
    }

    /// The actor's control events. Every queued send posts one completion as a control event,
    /// so the completions of a full send queue fit beside the other control events.
    int controlEvents() {
        return regularSends() + ClientIo.RESERVED_SEND_LIMIT + OTHER_CONTROL_EVENTS;
    }
}
