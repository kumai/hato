package org.mqtt4j.hato.broker.internal;

import java.util.List;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

import static java.util.Objects.requireNonNull;

// The retained messages that a subscription matched as the client made it, on their way to the
// session. They wait at one place in the session's queue, behind the deliveries queued before the
// subscription and ahead of those queued after it, and go out one at a time as the window and
// the send queue take them. They count against the session's room for retained messages rather
// than the delivery queue's. Each arrives with RETAIN 1 at the lower of its QoS and the granted
// one, and carries the subscription's identifier [MQTT-3.3.4-3]. It belongs to the session's
// actor, and waits under the session's inbox.
//
// The cursor holds the messages themselves, which the store may replace or drop meanwhile, so
// what waits here is memory of its own. It lets go of each message as that goes out or is
// dropped, and the session takes room in the broker's memory for those that still wait once it
// has sent what it could: for the first ones, in the order they go out.
final class RetainedCursor implements Pending {

    final TopicFilter filter;

    private final Qos granted;

    private final List<Integer> identifiers;

    // The retained messages in the order they go out. One that went out or was dropped leaves
    // null behind.
    private final RetainedMessages.@Nullable Message[] matched;

    private final MqttVersion version;

    // Where the cursor stands among what its session has queued: the session's number for it.
    final long order;

    // The position in `matched` of the next retained message to go out, or its length once
    // none is left.
    private int next;

    // The position behind the messages that have room in the broker's memory: those that are
    // left before it have room, and those from it on have none yet.
    private int roomEnd;

    // How many retained messages are left.
    private int remaining;

    // Whether QoS 0 deliveries are left out, because the connection they belong to has ended.
    private boolean acknowledgedOnly;

    // Whether the session has taken room in the broker's memory for the messages that are
    // left, which it does once the cursor waits. The session's, under its inbox.
    boolean hasRoom;

    // The delivery of the retained message at `next`, made when the session first looks at it
    // and kept until it goes out.
    private @Nullable Delivery current;

    RetainedCursor(
            Packet.Subscription subscription,
            List<Integer> identifiers,
            List<RetainedMessages.Message> matched,
            MqttVersion version,
            long order
    ) {
        filter = subscription.filter();
        granted = subscription.qos();
        this.identifiers = identifiers;
        this.matched = matched.toArray(new RetainedMessages.Message[0]);
        this.version = version;
        this.order = order;
        remaining = this.matched.length;
    }

    // Whether a retained message is left.
    boolean hasNext() {
        return remaining > 0;
    }

    // The delivery of the next retained message. Called only while one is left.
    Delivery current() {
        Delivery delivery = current;
        if (delivery == null) {
            RetainedMessages.Message entry = requireNonNull(matched[next]);
            delivery = new Delivery(
                    entry.publication(),
                    entry.memory(),
                    entry.received(),
                    qosOf(entry),
                    true,
                    identifiers,
                    version
            );
            current = delivery;
        }
        return delivery;
    }

    // The QoS at which the next retained message goes out. Called only while one is left.
    Qos nextQos() {
        return qosOf(requireNonNull(matched[next]));
    }

    // The QoS at which `message` goes out: the lower of its own and the granted one.
    Qos qosOf(RetainedMessages.Message message) {
        return Qos.of(Math.min(granted.level(), message.publication().qos().level()));
    }

    // Whether the next retained message has room in the broker's memory. Called only while one
    // is left.
    boolean nextHasRoom() {
        return next < roomEnd;
    }

    // Moves past the next retained message, which went out or was dropped, lets go of it, and
    // returns it. Called only while one is left.
    RetainedMessages.Message advance() {
        RetainedMessages.Message message = requireNonNull(matched[next]);
        remove(next);
        return message;
    }

    // The first retained message that has no room in the broker's memory yet, or null if all
    // that are left have.
    RetainedMessages.@Nullable Message withoutRoom() {
        while (roomEnd < matched.length && matched[roomEnd] == null) {
            roomEnd++;
        }
        return (roomEnd < matched.length) ? matched[roomEnd] : null;
    }

    // Notes that the retained message that withoutRoom() returned has taken its room.
    void roomTaken() {
        roomEnd++;
    }

    // Lets go of the retained message that withoutRoom() returned, which the session goes
    // without.
    void dropWithoutRoom() {
        remove(roomEnd);
    }

    // The position of the oldest retained message that is left and has room in the broker's
    // memory, or -1 if none has.
    int firstWithRoom() {
        return nextWithRoom(next - 1);
    }

    // The position of the next retained message after `position` that is left and has room,
    // or -1 if none has.
    int nextWithRoom(int position) {
        for (int following = position + 1; following < roomEnd; following++) {
            if (matched[following] != null) {
                return following;
            }
        }
        return -1;
    }

    // The retained message at `position`, which firstWithRoom() or nextWithRoom() returned.
    RetainedMessages.Message at(int position) {
        return requireNonNull(matched[position]);
    }

    // Has `action` take each retained message that is left and has room in the broker's
    // memory, oldest first.
    void eachWithRoom(Consumer<RetainedMessages.Message> action) {
        for (int position = next; position < roomEnd; position++) {
            RetainedMessages.Message message = matched[position];
            if (message != null) {
                action.accept(message);
            }
        }
    }

    // Lets go of the message at `position`, and moves on to the next one that is left if it
    // was the next to go out.
    private void remove(int position) {
        matched[position] = null;
        remaining--;
        if (position != next) {
            return;
        }
        current = null;
        while (next < matched.length && matched[next] == null) {
            next++;
        }
        roomEnd = Math.max(roomEnd, next);
    }

    // The number of retained messages left.
    int remainingCount() {
        return remaining;
    }

    // What the retained messages left hold on the heap, as the retained limits count it.
    long remainingBytes() {
        long bytes = 0;
        for (int position = next; position < matched.length; position++) {
            RetainedMessages.Message message = matched[position];
            if (message != null) {
                bytes += message.memory().bytes();
            }
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
