package org.mqtt4j.hato.broker.internal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;

import static org.assertj.core.api.Assertions.assertThat;

class TopicTreeTest {

    // Levels that topics and filters are built from: literals that differ, an empty level, and
    // a level that starts with $, which matters only at the start of a topic.
    private static final List<String> LITERALS = List.of("a", "b", "", "$a");

    @Test
    void finds_the_topics_that_topic_filter_matching_selects_for_every_small_pair() {
        // Arrange: every topic of up to four levels, in one tree.
        List<Topic> topics = topics();
        var sut = new TopicTree<Topic>();
        for (Topic topic : topics) {
            sut.put(topic, topic);
        }

        // Act & Assert: every filter of up to three levels.
        for (TopicFilter filter : filters()) {
            assertThat(found(sut, filter))
                    .as(filter.pattern())
                    .containsExactlyInAnyOrderElementsOf(matching(topics, filter));
        }
    }

    @Test
    void removed_topics_are_found_no_more_and_leave_no_levels_behind() {
        // Arrange
        List<Topic> topics = topics();
        var sut = new TopicTree<Topic>();
        for (Topic topic : topics) {
            sut.put(topic, topic);
        }
        var kept = new ArrayList<Topic>();
        var removed = new ArrayList<Topic>();
        for (int i = 0; i < topics.size(); i++) {
            if (i % 2 == 0) {
                kept.add(topics.get(i));
            } else {
                removed.add(topics.get(i));
            }
        }

        // Act
        for (Topic topic : removed) {
            sut.remove(topic);
        }

        // Assert
        for (TopicFilter filter : filters()) {
            assertThat(found(sut, filter))
                    .as(filter.pattern())
                    .containsExactlyInAnyOrderElementsOf(matching(kept, filter));
        }
        for (Topic topic : kept) {
            sut.remove(topic);
        }
        assertThat(sut.isEmpty()).isTrue();
    }

    @Test
    void a_topic_holds_the_last_value_put() {
        // Arrange
        var sut = new TopicTree<String>();
        var topic = new Topic("devices/7/connection");

        // Act
        sut.put(topic, "first");
        sut.put(topic, "second");

        // Assert
        assertThat(found(sut, new TopicFilter("devices/+/connection"))).containsExactly("second");
    }

    @Test
    void a_topic_deeper_than_any_stack_is_found_level_by_level() {
        // Arrange: a topic of 20,000 levels.
        var sut = new TopicTree<String>();
        String topic = "a/".repeat(19_999) + "a";
        sut.put(new Topic(topic), "deep");

        // Act & Assert
        assertThat(found(sut, new TopicFilter("#"))).containsExactly("deep");
        assertThat(found(sut, new TopicFilter("+/".repeat(19_999) + "+"))).containsExactly("deep");
        assertThat(found(sut, new TopicFilter("+/".repeat(19_999) + "+/+"))).isEmpty();
    }

    private static <V> List<V> found(TopicTree<V> tree, TopicFilter filter) {
        var values = new ArrayList<V>();
        tree.match(filter, values::add);
        return values;
    }

    private static Set<Topic> matching(List<Topic> topics, TopicFilter filter) {
        var selected = new HashSet<Topic>();
        for (Topic topic : topics) {
            if (filter.matches(topic)) {
                selected.add(topic);
            }
        }
        return selected;
    }

    /// Every valid filter of one to three levels: literals and + at any level, # as the last.
    private static List<TopicFilter> filters() {
        var inner = new ArrayList<>(LITERALS);
        inner.add("+");
        var last = new ArrayList<>(inner);
        last.add("#");
        var filters = new ArrayList<TopicFilter>();
        for (List<String> pattern : sequences(inner, last, 3)) {
            String text = String.join("/", pattern);
            // The empty pattern is the only invalid one.
            if (!text.isEmpty()) {
                filters.add(new TopicFilter(text));
            }
        }
        return filters;
    }

    /// Every valid topic of one to four levels.
    private static List<Topic> topics() {
        var topics = new ArrayList<Topic>();
        for (List<String> pattern : sequences(LITERALS, LITERALS, 4)) {
            String text = String.join("/", pattern);
            if (!text.isEmpty()) {
                topics.add(new Topic(text));
            }
        }
        return topics;
    }

    /// The sequences of one to `depth` levels whose last level comes from `last` and the others
    /// from `inner`.
    private static List<List<String>> sequences(List<String> inner, List<String> last, int depth) {
        var sequences = new ArrayList<List<String>>();
        List<List<String>> prefixes = List.of(List.of());
        for (int length = 1; length <= depth; length++) {
            for (List<String> prefix : prefixes) {
                for (String level : last) {
                    var sequence = new ArrayList<>(prefix);
                    sequence.add(level);
                    sequences.add(sequence);
                }
            }
            var longer = new ArrayList<List<String>>();
            for (List<String> prefix : prefixes) {
                for (String level : inner) {
                    var sequence = new ArrayList<>(prefix);
                    sequence.add(level);
                    longer.add(sequence);
                }
            }
            prefixes = longer;
        }
        return sequences;
    }
}
