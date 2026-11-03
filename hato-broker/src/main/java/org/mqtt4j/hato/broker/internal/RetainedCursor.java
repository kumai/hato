package org.mqtt4j.hato.broker.internal;

import java.util.List;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

// The retained messages that a subscription matched as the client made it, on their way to the
// session. They wait at one place in the session's queue, behind the deliveries queued before the
// subscription and ahead of those queued after it, and go out one at a time as the window and
// the send queue take them. They count against the session's room for retained messages rather
// than the delivery queue's. Each arrives with RETAIN 1 at the lower of its QoS and the granted
// one, and carries the subscription's identifier [MQTT-3.3.4-3]. Like the session, it belongs to
// routing.
final class RetainedCursor implements Pending {

    final TopicFilter filter;

    private final Qos granted;

    private final List<Integer> identifiers;

    private final List<Routing.Retained> matched;

    private final MqttVersion version;

    // The position in `matched` of the next retained message to go out.
    private int next;

    // Whether QoS 0 deliveries are left out, because the connection they belong to has ended.
    private boolean acknowledgedOnly;

    // The delivery of the retained message at `next`, made when the session first looks at it
    // and kept until it goes out.
    private @Nullable Delivery current;

    RetainedCursor(
            Packet.Subscription subscription,
            List<Integer> identifiers,
            List<Routing.Retained> matched,
            MqttVersion version
    ) {
        filter = subscription.filter();
        granted = subscription.qos();
        this.identifiers = identifiers;
        this.matched = matched;
        this.version = version;
    }

    // Whether a retained message is left.
    boolean hasNext() {
        return next < matched.size();
    }

    // The delivery of the next retained message, or null once none is left.
    @Nullable Delivery current() {
        if (current == null && hasNext()) {
            Routing.Retained entry = matched.get(next);
            Packet.Publish publication = entry.publication();
            current = new Delivery(
                    publication,
                    entry.received(),
                    Qos.of(Math.min(granted.level(), publication.qos().level())),
                    true,
                    identifiers,
                    version
            );
        }
        return current;
    }

    // Moves past the next retained message, which went out or was dropped, and returns its size
    // as the broker counts it for retaining (an MQTT 5.0 PUBLISH). Called only while one is left.
    long advance() {
        long bytes = matched.get(next).byteCount();
        current = null;
        next++;
        return bytes;
    }

    // The number of retained messages left.
    int remainingCount() {
        return matched.size() - next;
    }

    // The size of the retained messages left, as `advance()` counts it.
    long remainingBytes() {
        long bytes = 0;
        for (int position = next; position < matched.size(); position++) {
            bytes += matched.get(position).byteCount();
        }
        return bytes;
    }

    // Whether `delivery`, this cursor's current one, goes without being sent: QoS 0 belongs to
    // the connection, and that connection has ended.
    boolean leavesOut(Delivery delivery) {
        return acknowledgedOnly && delivery.qos == Qos.AT_MOST_ONCE;
    }

    // Leaves out the QoS 0 deliveries that have not gone out, as their connection ends.
    void keepAcknowledgedOnly() {
        acknowledgedOnly = true;
    }
}
