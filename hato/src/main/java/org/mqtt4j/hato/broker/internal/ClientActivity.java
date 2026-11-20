package org.mqtt4j.hato.broker.internal;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;

// What the client of one connection has sent there, as the actor of its session handled it, for
// the application's list of clients: its PUBLISH packets, the publications and subscriptions
// that its scope refused, and the last topic or filter of each kind. The broker keeps one only
// when the application asks it to record the activity of its clients.
//
// Only the actor writes, one thread at a time, so a value costs it a plain read and an ordered
// write, with no atomic operation, as for Traffic; a reader sees each value whole, as it was a
// moment ago. A count goes after the text and the time that come with it, so a reader that finds
// a count finds them as well, or later ones.
final class ClientActivity {

    // How many characters of a topic or a filter the record shows: a client chooses their length,
    // up to 65,535 bytes, and the record keeps one of each kind for every connection. A longer one
    // shows as its beginning, which is what tells topics apart in practice, marked as truncated.
    static final int MAX_TEXT = 256;

    // The places of the counts and of the time of the last PUBLISH.
    private static final int RECEIVED = 0;

    private static final int RECEIVED_AT = 1;

    private static final int REFUSED_PUBLICATIONS = 2;

    private static final int REFUSED_SUBSCRIPTIONS = 3;

    // The places of the texts.
    private static final int TOPIC = 0;

    private static final int REFUSED_TOPIC = 1;

    private static final int REFUSED_FILTER = 2;

    private final AtomicLongArray counts = new AtomicLongArray(4);

    private final AtomicReferenceArray<@Nullable String> texts = new AtomicReferenceArray<>(3);

    // Notes a PUBLISH packet to `topic`, its alias resolved, which the broker received at `at` by
    // its clock. Called on the actor.
    void received(Topic topic, long at) {
        keep(TOPIC, topic.name());
        counts.setRelease(RECEIVED_AT, at);
        increment(RECEIVED);
    }

    // Notes a publication to `topic` that the client's scope refused, after received() noted its
    // packet. Called on the actor.
    void refusedPublication(Topic topic) {
        keep(REFUSED_TOPIC, topic.name());
        increment(REFUSED_PUBLICATIONS);
    }

    // Notes a subscription to `filter` that the client's scope refused. Called on the actor.
    void refusedSubscription(TopicFilter filter) {
        keep(REFUSED_FILTER, filter.pattern());
        increment(REFUSED_SUBSCRIPTIONS);
    }

    // What the record holds, as it was a moment ago, with the time since the last PUBLISH as of
    // `now` by the broker's clock, and `delivered`, the PUBLISH packets that the connection's loop
    // has written to the client. Any thread.
    BrokerServer.ActivityValues values(long now, long delivered) {
        long received = counts.getAcquire(RECEIVED);
        long refusedPublications = counts.getAcquire(REFUSED_PUBLICATIONS);
        long refusedSubscriptions = counts.getAcquire(REFUSED_SUBSCRIPTIONS);
        String topic = kept(TOPIC, received);
        String refusedTopic = kept(REFUSED_TOPIC, refusedPublications);
        String refusedFilter = kept(REFUSED_FILTER, refusedSubscriptions);
        return new BrokerServer.ActivityValues(
                received,
                shown(topic),
                truncated(topic),
                Math.max(0, now - counts.getAcquire(RECEIVED_AT)),
                delivered,
                refusedPublications,
                shown(refusedTopic),
                truncated(refusedTopic),
                refusedSubscriptions,
                shown(refusedFilter),
                truncated(refusedFilter)
        );
    }

    private void increment(int index) {
        counts.setRelease(index, counts.getPlain(index) + 1);
    }

    // Keeps `text` at `index`: whole if it is within MAX_TEXT characters, or else as its first
    // MAX_TEXT + 1, the last of which only tells that the text went on, so that a reader finds
    // both the text and whether the record cut it in the one string that it reads. What the
    // record holds there already stays when it would only be written again: the topic that an
    // alias stands for, or the beginning of a long text that the client sends again, which then
    // costs no copy.
    private void keep(int index, String text) {
        String kept = texts.getPlain(index);
        if (kept == text) {
            return;
        }
        if (text.length() <= MAX_TEXT) {
            texts.setRelease(index, text);
        } else if (kept == null || kept.length() <= MAX_TEXT || !text.startsWith(kept)) {
            texts.setRelease(index, text.substring(0, MAX_TEXT + 1));
        }
    }

    // The text that the record keeps at `index`, or null while `count`, which goes with that
    // text, has yet to find one.
    private @Nullable String kept(int index, long count) {
        return (count == 0) ? null : texts.getAcquire(index);
    }

    // What a reader sees of `kept`, a text as the record keeps it: the text itself if it is
    // whole, or else its first MAX_TEXT characters, one fewer if the last of them would be the
    // first half of a surrogate pair.
    private static @Nullable String shown(@Nullable String kept) {
        if (kept == null || kept.length() <= MAX_TEXT) {
            return kept;
        }
        int end = Character.isHighSurrogate(kept.charAt(MAX_TEXT - 1)) ? MAX_TEXT - 1 : MAX_TEXT;
        return kept.substring(0, end);
    }

    // Whether the record cut `kept`, a text as it keeps it.
    private static boolean truncated(@Nullable String kept) {
        return kept != null && kept.length() > MAX_TEXT;
    }
}
