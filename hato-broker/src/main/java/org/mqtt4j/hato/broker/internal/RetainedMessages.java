package org.mqtt4j.hato.broker.internal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.mqtt4j.hato.broker.internal.common.concurrent.Timer;
import org.mqtt4j.hato.broker.internal.common.log.Log;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Topic;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

// The retained message of each topic, in the order they were stored, within the retained limits.
// The monitor of this object is the topic lock, which every caller holds. A retained publication
// is stored and delivered under it, and a new subscription takes the retained messages that it
// matches and starts to receive deliveries under it, so the subscription receives the
// publication once, either as a retained message or as a delivery, and its later deliveries
// follow its retained messages.
//
// A new subscription scans them all, which the retained limits keep short; an index by topic
// level would replace the scan if productization needs it.
final class RetainedMessages {

    private static final Log log = Log.of(RetainedMessages.class);

    // The retained limits: how many messages the broker keeps, and their bytes in all.
    private final int maxCount;

    private final long maxBytes;

    // The broker's time, by which the lifetimes of the messages pass.
    private final Timer clock;

    // The port that the broker listens on, for logs.
    private final int port;

    private final Map<Topic, Message> messages = new LinkedHashMap<>();

    private long bytes;

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
    // outdated one. The messages whose lifetime has passed make room first. `memory` is the
    // publication's room in the broker's memory for deliveries, which the deliveries of the
    // retained message share with those of the publication itself.
    void store(Packet.Publish publication, long received, PublicationMemory memory) {
        Message previous = messages.remove(publication.topic());
        if (previous != null) {
            bytes -= previous.byteCount();
        }
        if (publication.payload().length == 0) {
            return;
        }
        // A retained message counts as the MQTT 5.0 PUBLISH that carries it with its properties.
        int byteCount = PacketCodec.of(MqttVersion.V5).publishSize(
                publication.topic(),
                publication.qos(),
                publication.properties(),
                publication.payload().length
        );
        var message = new Message(
                publication,
                byteCount,
                received,
                Delivery.lifetimeOf(publication),
                memory
        );
        if (!fits(message)) {
            // The lifetimes are judged by the time now, not by `received`: the publication may
            // have waited for the topic lock since then, and a message that expired meanwhile
            // must not keep it out.
            discardExpired(clock.nanoTime());
        }
        if (!fits(message)) {
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
        messages.put(publication.topic(), message);
        bytes += message.byteCount();
        noteExpiry(message);
    }

    // Whether the retained limits have room for `message`.
    private boolean fits(Message message) {
        return messages.size() < maxCount && message.byteCount() <= maxBytes - bytes;
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
                bytes -= message.byteCount();
            } else {
                noteExpiry(message);
            }
        }
    }

    // The retained messages whose topics `filter` matches, in the order they were stored,
    // without those whose Message Expiry Interval has passed.
    List<Message> matching(TopicFilter filter) {
        discardExpired(clock.nanoTime());
        var matched = new ArrayList<Message>();
        for (Message message : messages.values()) {
            if (filter.matches(message.publication().topic())) {
                matched.add(message);
            }
        }
        return matched;
    }

    // Forgets every retained message, as the broker closes.
    void clear() {
        messages.clear();
        bytes = 0;
        expiring = false;
    }

    // A retained message, its share of the retained size limit, when the broker received it,
    // its Message Expiry Interval in seconds, which counts from then, or -1 without one, and
    // the room that its deliveries share in the broker's memory for deliveries. The interval is
    // read from the publication once, because a look at the lifetimes goes through every
    // message.
    record Message(
            Packet.Publish publication,
            long byteCount,
            long received,
            long lifetime,
            PublicationMemory memory
    ) {

        boolean expired(long now) {
            return Delivery.expired(lifetime, received, now);
        }
    }
}
