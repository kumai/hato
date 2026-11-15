package org.mqtt4j.hato.client.internal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;

import static org.assertj.core.api.Assertions.assertThat;

class FilterTreeTest {

    // Levels that filters and topics are built from: literals that differ, an empty level, and
    // a level that starts with $, which matters only at the start of a topic.
    private static final List<String> LITERALS = List.of("a", "b", "", "$a");

    @Test
    void selects_the_filters_that_topic_filter_matching_selects_for_every_small_pair() {
        // Arrange: every filter of up to three levels, in one tree.
        List<TopicFilter> filters = filters();
        var sut = new FilterTree<TopicFilter>();
        for (TopicFilter filter : filters) {
            sut.add(filter, filter);
        }

        // Act & Assert: every topic of up to four levels.
        for (Topic topic : topics()) {
            assertThat(selected(sut, topic))
                    .as(topic.name())
                    .containsExactlyInAnyOrderElementsOf(matching(filters, topic));
        }
    }

    @Test
    void removed_filters_select_nothing_and_leave_no_levels_behind() {
        // Arrange
        List<TopicFilter> filters = filters();
        var sut = new FilterTree<TopicFilter>();
        for (TopicFilter filter : filters) {
            sut.add(filter, filter);
        }
        var kept = new ArrayList<TopicFilter>();
        var removed = new ArrayList<TopicFilter>();
        for (int i = 0; i < filters.size(); i++) {
            if (i % 2 == 0) {
                kept.add(filters.get(i));
            } else {
                removed.add(filters.get(i));
            }
        }

        // Act
        for (TopicFilter filter : removed) {
            sut.remove(filter, filter);
        }

        // Assert
        for (Topic topic : topics()) {
            assertThat(selected(sut, topic))
                    .as(topic.name())
                    .containsExactlyInAnyOrderElementsOf(matching(kept, topic));
        }
        for (TopicFilter filter : kept) {
            sut.remove(filter, filter);
        }
        assertThat(sut.isEmpty()).isTrue();
    }

    @Test
    void a_filter_holds_several_values_and_loses_only_the_one_removed() {
        // Arrange
        var sut = new FilterTree<String>();
        var filter = new TopicFilter("devices/+/state");
        sut.add(filter, "first");
        sut.add(filter, "second");
        sut.add(new TopicFilter("devices/#"), "all");

        // Act
        sut.remove(filter, "first");
        sut.remove(filter, "absent");
        sut.remove(new TopicFilter("devices/+/other"), "second");

        // Assert
        assertThat(selected(sut, new Topic("devices/7/state")))
                .containsExactlyInAnyOrder("second", "all");
    }

    @Test
    void a_topic_deeper_than_any_stack_is_matched_level_by_level() {
        // Arrange: 20,000 levels, each a + of the filter and an "a" of the topic.
        var sut = new FilterTree<String>();
        String levels = "+/".repeat(19_999) + "+";
        sut.add(new TopicFilter(levels), "deep");
        sut.add(new TopicFilter(levels + "/#"), "deeper");

        // Act & Assert
        String topic = "a/".repeat(19_999) + "a";
        assertThat(selected(sut, new Topic(topic))).containsExactlyInAnyOrder("deep", "deeper");
        assertThat(selected(sut, new Topic(topic + "/a"))).containsExactly("deeper");
    }

    /// The values that `tree` passes for `topic`, failing if it passes one twice.
    private static <T> List<T> selected(FilterTree<T> tree, Topic topic) {
        var values = new ArrayList<T>();
        tree.match(topic, values::add);
        assertThat(new HashSet<>(values)).as("each value once").hasSameSizeAs(values);
        return values;
    }

    private static Set<TopicFilter> matching(List<TopicFilter> filters, Topic topic) {
        var selected = new HashSet<TopicFilter>();
        for (TopicFilter filter : filters) {
            if (filter.matches(topic)) {
                selected.add(filter);
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
