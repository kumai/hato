package org.mqtt4j.hato.broker.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.broker.internal.common.concurrent.Timer;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.value.Topic;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;

/// The store of retained messages at its limits, on a clock that the tests set.
class RetainedMessagesTest {

    private final Clock clock = new Clock();

    @Test
    void a_message_that_expired_while_a_publication_waited_for_the_store_makes_room_for_it() {
        // Arrange: a store for one message keeps one whose lifetime of a second began at 0.
        var sut = new RetainedMessages(1, 1024, clock, 0);
        store(sut, retained("old", 1), 0);

        // Act: the next publication arrives after 0.9 s and reaches the store after 1.1 s, as
        // one that waited for the topic lock does.
        clock.now = 1_100_000_000L;
        store(sut, retained("new", 60), 900_000_000L);

        // Assert
        assertThat(kept(sut)).containsExactly("new");
    }

    @Test
    void messages_make_room_one_after_the_other_as_their_lifetimes_pass() {
        // Arrange: a store for two messages keeps one that lives for 5 s and one for 10 s.
        var sut = new RetainedMessages(2, 1024, clock, 0);
        store(sut, retained("short", 5), 0);
        store(sut, retained("long", 10), 0);

        // Act: a further message comes just before each lifetime has passed, and as it has.
        var keptAfterEach = new ArrayList<List<String>>();
        for (long seconds : new long[] {4, 5, 9, 10}) {
            clock.now = seconds * 1_000_000_000L;
            store(sut, retained("at-" + seconds, 60), clock.now);
            keptAfterEach.add(kept(sut));
        }

        // Assert: it finds room only once a message has expired.
        assertThat(keptAfterEach).containsExactly(
                List.of("short", "long"),
                List.of("long", "at-5"),
                List.of("long", "at-5"),
                List.of("at-5", "at-10")
        );
    }

    /// Hands `publication`, which arrived at `received`, to `store`. Its room in the broker's
    /// memory for deliveries plays no part in what the store keeps.
    private static void store(RetainedMessages store, Packet.Publish publication, long received) {
        store.store(publication, received, new PublicationMemory(1));
    }

    /// The topics of the retained messages that `store` keeps now, in the order stored.
    private static List<String> kept(RetainedMessages store) {
        return store.matching(new TopicFilter("#")).stream()
                .map(message -> message.publication().topic().name())
                .toList();
    }

    /// A retained publication to `topic` with a Message Expiry Interval of `lifetime` seconds.
    private static Packet.Publish retained(String topic, long lifetime) {
        var expiry = new Property.FourByteValue(PropertyId.MESSAGE_EXPIRY_INTERVAL, lifetime);
        return new Packet.Publish(
                new Topic(topic),
                new byte[] {1},
                AT_MOST_ONCE,
                true,
                false,
                0,
                new Properties(List.of(expiry))
        );
    }

    /// The broker's time as the tests set it. The store sets no timers.
    private static final class Clock implements Timer {

        private long now;

        @Override
        public long nanoTime() {
            return now;
        }

        @Override
        public Cancellable schedule(Duration delay, Runnable task) {
            throw new UnsupportedOperationException("The store sets no timers");
        }
    }
}
