package org.mqtt4j.hato.broker.internal;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.internal.concurrent.Timer;
import org.mqtt4j.hato.internal.log.Log;
import org.mqtt4j.hato.internal.protocol.Packet;

// The retained message of each topic, in the order they were stored, within the retained limits.
// The monitor of this object is the topic lock, which every caller holds. A retained publication
// is stored and delivered under it, and a new subscription takes the retained messages that it
// matches and starts to receive deliveries under it, so the subscription receives the
// publication once, either as a retained message or as a delivery, and its later deliveries
// follow its retained messages.
//
// A new subscription finds the messages that its filter selects by the levels of their topics,
// without going through the others.
final class RetainedMessages {

    private static final Log log = Log.of(RetainedMessages.class);

    // The retained limits: how many messages the broker keeps, and what they hold on the heap
    // in all, their publications (see PublicationMemory) and the levels of their topics in the
    // index (see TopicTree.sizeOf).
    private final int maxCount;

    private final long maxBytes;

    // The broker's time, by which the lifetimes of the messages pass.
    private final Timer clock;

    // The port that the broker listens on, for logs.
    private final int port;

    // By the names of their topics: strings, whose order the map's bins fall back on when their
    // hash codes collide, which names chosen to collide would make slow to look up otherwise.
    private final Map<String, Message> messages = new LinkedHashMap<>();

    // The same messages by the levels of their topics.
    private final TopicTree<Message> byTopic = new TopicTree<>();

    // The place in the order stored that the next message takes.
    private long nextPlace;

    private long bytes;

    // How many messages the store keeps, as size() says, for count(); written under the topic
    // lock.
    private volatile int count;

    // Whether a kept message has a Message Expiry Interval, and if so a time before which none
    // of them has used its interval up, so that nobody needs to look at them before it. The
    // time is the earliest end of a lifetime among the messages that the store kept at its
    // last look or has taken since. One that has left since can only make the time too early,
    // which costs one look that finds nothing.
    private boolean expiring;

    private long earliestExpiry;

    // Whether the retained limits have refused a message yet; only the first refusal warns.
    private boolean limitReached;

    RetainedMessages(int maxCount, long maxBytes, Timer clock, int port) {
        this.maxCount = maxCount;
        this.maxBytes = maxBytes;
        this.clock = clock;
        this.port = port;
    }

    // Makes `publication`, which the broker received at `received`, the retained message of its
    // topic in place of the previous one [MQTT-3.3.1-5]. An empty payload only removes the
    // previous one [MQTT-3.3.1-6] [MQTT-3.3.1-7]. A message beyond the retained limits is not
    // kept either, which leaves its topic without a retained message rather than with an
    // outdated one. The messages whose lifetime has passed make room first. `memory` says what
    // the publication holds on the heap, which with what the levels of its topic hold in the
    // index is what the message counts for here, and is the room in the broker's memory for
    // deliveries that the deliveries of the retained message share with those of the
    // publication itself.
    void store(Packet.Publish publication, long received, PublicationMemory memory) {
        Message previous = messages.remove(publication.topic().name());
        if (previous != null) {
            bytes -= counted(previous);
            byTopic.remove(publication.topic());
            count = messages.size();
        }
        if (publication.payload().length == 0) {
            return;
        }
        var message = new Message(
                nextPlace,
                publication,
                received,
                Delivery.lifetimeOf(publication),
                memory
        );
        long size = counted(message);
        if (!fits(size)) {
            // The lifetimes are judged by the time now, not by `received`: the publication may
            // have waited for the topic lock since then, and a message that expired meanwhile
            // must not keep it out.
            discardExpired(clock.nanoTime());
        }
        if (!fits(size)) {
            if (!limitReached) {
                limitReached = true;
                log.warn(
                        "Broker on port {} reached its retained message limits and keeps no"
                                + " retained message for topic {}; later refusals log at DEBUG",
                        port,
                        publication.topic().name()
                );
            } else {
                log.debug(
                        "Broker on port {} keeps no retained message for topic {}: limits reached",
                        port,
                        publication.topic().name()
                );
            }
            return;
        }
        messages.put(publication.topic().name(), message);
        byTopic.put(publication.topic(), message);
        nextPlace++;
        bytes += size;
        noteExpiry(message);
        count = messages.size();
    }

    // How many messages the store keeps, as the retained limit counts them: a message whose
    // lifetime has passed counts until the store finds it so.
    int size() {
        return messages.size();
    }

    // The same as size(), as it was a moment ago, for a caller without the topic lock: the
    // application's threads, virtual ones among them, take no lock that the broker's threads
    // wait for.
    int count() {
        return count;
    }

    // The place in the order stored that the next message takes: every message stored so far has
    // an earlier one.
    long nextPlace() {
        return nextPlace;
    }

    // What a retained `publication` counts for against the byte limit, as store() counts it
    // when its memory is the publication's own, for tests that set the limit.
    static long sizeOf(Packet.Publish publication) {
        return PublicationMemory.sizeOf(publication) + TopicTree.sizeOf(publication.topic());
    }

    // What `message` counts for against the byte limit: what its publication holds on the
    // heap, and what the levels of its topic hold in the index.
    private static long counted(Message message) {
        return message.memory().bytes() + TopicTree.sizeOf(message.publication().topic());
    }

    // Whether the retained limits have room for one more message that counts `size` bytes.
    private boolean fits(long size) {
        return messages.size() < maxCount && size <= maxBytes - bytes;
    }

    // Takes account of the lifetime of `message`, which the store keeps: the earliest end is
    // its own if that comes before the others. A message without a lifetime changes nothing.
    private void noteExpiry(Message message) {
        if (message.lifetime() < 0) {
            return;
        }
        long expiry = message.received() + message.lifetime() * 1_000_000_000L;
        if (!expiring || expiry - earliestExpiry < 0) {
            earliestExpiry = expiry;
        }
        expiring = true;
    }

    // Discards the messages whose Message Expiry Interval has passed by `now` (MQTT 5.0 section
    // 3.3.1.3). They are looked for when a subscription looks and when a message finds no room,
    // rather than with a timer each: an expired one stays in memory until then, within the
    // retained limits, but keeps no message out that reaches the store after its time.
    //
    // Nothing is looked at before the earliest lifetime can have passed, so a store that is
    // full of lasting messages refuses a further one without going through them.
    // Ceiling: each time a lifetime has passed, the next look goes through all the retained
    // messages, so a client that keeps short-lived retained messages coming has the broker go
    // through them as often. An index of the lifetimes would find the expired ones without
    // that.
    private void discardExpired(long now) {
        if (!expiring || now - earliestExpiry < 0) {
            return;
        }
        expiring = false;
        Iterator<Message> kept = messages.values().iterator();
        while (kept.hasNext()) {
            Message message = kept.next();
            if (message.expired(now)) {
                kept.remove();
                byTopic.remove(message.publication().topic());
                bytes -= counted(message);
            } else {
                noteExpiry(message);
            }
        }
        count = messages.size();
    }

    // The retained messages whose topics `filter` matches, in the order they were stored,
    // without those whose Message Expiry Interval has passed.
    List<Message> matching(TopicFilter filter) {
        discardExpired(clock.nanoTime());
        var found = new ArrayList<Message>();
        byTopic.match(filter, found::add);
        found.sort(Comparator.comparingLong(Message::place));
        return found;
    }

    // Forgets every retained message, as the broker closes.
    void clear() {
        messages.clear();
        byTopic.clear();
        bytes = 0;
        count = 0;
        expiring = false;
    }

    // A retained message: its place in the order stored, which no other message has, since a
    // later message of the same topic takes a place of its own at the end; when the broker
    // received it; its Message Expiry Interval in seconds, which counts from then, or -1 without
    // one; and what it holds on the heap: its share of the retained size limit, and the room
    // that its deliveries share in the broker's memory for deliveries. The interval is read from
    // the publication once, because a look at the lifetimes goes through every message.
    record Message(
            long place,
            Packet.Publish publication,
            long received,
            long lifetime,
            PublicationMemory memory
    ) {

        boolean expired(long now) {
            return Delivery.expired(lifetime, received, now);
        }
    }
}
