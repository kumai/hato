package org.mqtt4j.hato.broker.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.internal.concurrent.Timer;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.Properties;
import org.mqtt4j.hato.internal.protocol.Property;
import org.mqtt4j.hato.internal.protocol.PropertyId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.Qos.AT_MOST_ONCE;

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

    @Test
    void a_filter_finds_the_messages_whose_topics_it_selects_in_the_order_stored() {
        // Arrange: the connection states of three devices and a factsheet, where device 1
        // reports again after the others, which puts its message last.
        var sut = new RetainedMessages(16, 1 << 20, clock, 0);
        store(sut, retained("devices/1/connection", 60), 0);
        store(sut, retained("devices/2/connection", 60), 0);
        store(sut, retained("devices/2/factsheet", 60), 0);
        store(sut, retained("devices/3/connection", 60), 0);
        store(sut, retained("devices/1/connection", 60), 0);

        // Act & Assert
        assertThat(topics(sut, "devices/+/connection")).containsExactly(
                "devices/2/connection",
                "devices/3/connection",
                "devices/1/connection"
        );
        assertThat(topics(sut, "devices/2/#"))
                .containsExactly("devices/2/connection", "devices/2/factsheet");
        assertThat(topics(sut, "devices/3/connection")).containsExactly("devices/3/connection");
        assertThat(topics(sut, "devices/4/connection")).isEmpty();
    }

    @Test
    void a_message_that_leaves_the_store_is_found_no_more() {
        // Arrange: one message that an empty one removes, and one whose lifetime passes.
        var sut = new RetainedMessages(16, 1 << 20, clock, 0);
        store(sut, retained("devices/1/connection", 60), 0);
        store(sut, retained("devices/2/connection", 5), 0);

        // Act
        store(sut, removal("devices/1/connection"), 0);
        clock.now = 5_000_000_000L;

        // Assert
        assertThat(topics(sut, "devices/#")).isEmpty();
        store(sut, retained("devices/1/connection", 60), clock.now);
        assertThat(topics(sut, "devices/#")).containsExactly("devices/1/connection");
        sut.clear();
        assertThat(topics(sut, "#")).isEmpty();
    }

    @Test
    void the_byte_limit_counts_what_the_levels_of_a_topic_hold_in_the_index() {
        // Arrange: a byte limit that holds a message whose topic has one level of ten
        // characters. A topic of as many characters in six levels takes more in the index.
        Packet.Publish flat = retained("aaaaaaaaaa", 60);
        var sut = new RetainedMessages(16, 1 + TopicTree.sizeOf(flat.topic()), clock, 0);

        // Act
        store(sut, retained("a/a/a/a/a/", 60), 0);
        List<String> keptOfDeep = kept(sut);
        store(sut, flat, 0);

        // Assert
        assertThat(keptOfDeep).isEmpty();
        assertThat(kept(sut)).containsExactly("aaaaaaaaaa");
    }

    @Test
    void messages_whose_topics_hash_alike_are_stored_at_once() {
        // Arrange: room for 16,384 topics with one hash code, each of fourteen pairs of "Aa" or
        // "BB", which hash alike.
        var sut = new RetainedMessages(16_384, Long.MAX_VALUE, clock, 0);
        var topics = new ArrayList<String>();
        for (int bits = 0; bits < 16_384; bits++) {
            var topic = new StringBuilder();
            for (int pair = 0; pair < 14; pair++) {
                topic.append(((bits >> pair) & 1) == 0 ? "Aa" : "BB");
            }
            topics.add(topic.toString());
        }
        long started = System.nanoTime();

        // Act
        for (String topic : topics) {
            store(sut, retained(topic, 60), 0);
        }

        // Assert: far within the three seconds that a store by the topics' records took for
        // these.
        assertThat(Duration.ofNanos(System.nanoTime() - started))
                .isLessThan(Duration.ofSeconds(1));
        assertThat(sut.size()).isEqualTo(16_384);
    }

    /// The topics of the retained messages that `filter` finds in `store`, in the order given.
    private static List<String> topics(RetainedMessages store, String filter) {
        return store.matching(new TopicFilter(filter)).stream()
                .map(message -> message.publication().topic().name())
                .toList();
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

    /// A retained publication to `topic` with an empty payload, which removes the message of the
    /// topic.
    private static Packet.Publish removal(String topic) {
        return new Packet.Publish(
                new Topic(topic),
                new byte[0],
                AT_MOST_ONCE,
                true,
                false,
                0,
                Properties.EMPTY
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
