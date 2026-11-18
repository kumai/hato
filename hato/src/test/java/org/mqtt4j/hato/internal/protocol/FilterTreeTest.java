package org.mqtt4j.hato.internal.protocol;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;

import static org.assertj.core.api.Assertions.assertThat;

class FilterTreeTest {

    // Levels that filters and topics are built from: literals that differ, an empty level, and
    // a level that starts with $, which matters only at the start of a topic.
    private static final List<String> LITERALS = List.of("a", "b", "", "$a");

    @ParameterizedTest(name = "concurrent: {0}")
    @ValueSource(booleans = {false, true})
    void selects_the_filters_that_topic_filter_matching_selects_for_every_small_pair(
            boolean concurrent
    ) {
        // Arrange: every filter of up to three levels, in one tree.
        List<TopicFilter> filters = filters();
        FilterTree<TopicFilter> sut = tree(concurrent);
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

    @ParameterizedTest(name = "concurrent: {0}")
    @ValueSource(booleans = {false, true})
    void removed_filters_select_nothing_and_leave_no_levels_behind(boolean concurrent) {
        // Arrange
        List<TopicFilter> filters = filters();
        FilterTree<TopicFilter> sut = tree(concurrent);
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

    @ParameterizedTest(name = "concurrent: {0}")
    @ValueSource(booleans = {false, true})
    void a_filter_holds_several_values_and_loses_only_the_one_removed(boolean concurrent) {
        // Arrange
        FilterTree<String> sut = tree(concurrent);
        var filter = new TopicFilter("devices/+/state");
        sut.add(filter, "first");
        sut.add(filter, "second");
        sut.add(filter, "third");
        sut.add(new TopicFilter("devices/#"), "all");

        // Act
        sut.remove(filter, "first");
        sut.remove(filter, "absent");
        sut.remove(new TopicFilter("devices/+/other"), "second");

        // Assert
        assertThat(selected(sut, new Topic("devices/7/state")))
                .containsExactlyInAnyOrder("second", "third", "all");
        sut.remove(filter, "second");
        assertThat(selected(sut, new Topic("devices/7/state")))
                .containsExactlyInAnyOrder("third", "all");
        sut.remove(filter, "third");
        sut.remove(new TopicFilter("devices/#"), "all");
        assertThat(sut.isEmpty()).isTrue();
    }

    @ParameterizedTest(name = "concurrent: {0}")
    @ValueSource(booleans = {false, true})
    void a_value_added_twice_under_a_filter_is_held_once(boolean concurrent) {
        // Arrange
        FilterTree<String> sut = tree(concurrent);
        var filter = new TopicFilter("devices/+/state");

        // Act
        sut.add(filter, "only");
        sut.add(filter, "only");
        sut.remove(filter, "only");

        // Assert
        assertThat(selected(sut, new Topic("devices/7/state"))).isEmpty();
        assertThat(sut.isEmpty()).isTrue();
    }

    @ParameterizedTest(name = "concurrent: {0}")
    @ValueSource(booleans = {false, true})
    void a_match_counts_the_filters_that_passed_values(boolean concurrent) {
        // Arrange: one value under two overlapping filters, and another filter of its own.
        FilterTree<String> sut = tree(concurrent);
        sut.add(new TopicFilter("devices/+/state"), "overlapping");
        sut.add(new TopicFilter("devices/#"), "overlapping");
        sut.add(new TopicFilter("devices/7/state"), "single");
        sut.add(new TopicFilter("devices/7/state"), "other");
        var passed = new ArrayList<String>();

        // Act & Assert
        assertThat(sut.match(new Topic("devices/7/state"), passed::add)).isEqualTo(3);
        assertThat(passed)
                .containsExactlyInAnyOrder("overlapping", "overlapping", "single", "other");
        assertThat(sut.match(new Topic("devices/7/other"), _ -> {})).isEqualTo(1);
        assertThat(sut.match(new Topic("elsewhere"), _ -> {})).isZero();
    }

    @ParameterizedTest(name = "concurrent: {0}")
    @ValueSource(booleans = {false, true})
    void a_topic_deeper_than_any_stack_is_matched_level_by_level(boolean concurrent) {
        // Arrange: 20,000 levels, each a + of the filter and an "a" of the topic.
        FilterTree<String> sut = tree(concurrent);
        String levels = "+/".repeat(19_999) + "+";
        sut.add(new TopicFilter(levels), "deep");
        sut.add(new TopicFilter(levels + "/#"), "deeper");

        // Act & Assert
        String topic = "a/".repeat(19_999) + "a";
        assertThat(selected(sut, new Topic(topic))).containsExactlyInAnyOrder("deep", "deeper");
        assertThat(selected(sut, new Topic(topic + "/a"))).containsExactly("deeper");
    }

    @Test
    void a_concurrent_match_during_changes_passes_every_value_that_stays() throws Exception {
        // Arrange: values that stay under three filters that select a/b/c, and a writer that
        // keeps adding and removing others beside them, so that the holders of those filters
        // go between one value and a set, and the levels of a sibling filter come and go.
        FilterTree<String> sut = FilterTree.concurrent();
        List<TopicFilter> staying = List.of(
                new TopicFilter("a/b/c"),
                new TopicFilter("a/+/c"),
                new TopicFilter("a/#")
        );
        for (TopicFilter filter : staying) {
            sut.add(filter, "stays");
        }
        var sibling = new TopicFilter("a/b/c/d/e");
        var done = new AtomicBoolean();
        var failure = new AtomicReference<@Nullable String>();
        Thread writer = Thread.ofPlatform().start(() -> {
            for (int i = 0; i < 200_000; i++) {
                for (TopicFilter filter : staying) {
                    sut.add(filter, "comes and goes");
                }
                sut.add(sibling, "comes and goes");
                for (TopicFilter filter : staying) {
                    sut.remove(filter, "comes and goes");
                }
                sut.remove(sibling, "comes and goes");
            }
            done.set(true);
        });

        // Act: match while the writer runs.
        var topic = new Topic("a/b/c");
        while (!done.get()) {
            var passed = new ArrayList<String>();
            int sources = sut.match(topic, passed::add);
            long staysPassed = passed.stream().filter("stays"::equals).count();
            if (staysPassed != 3 || sources != 3) {
                failure.compareAndSet(null, "passed " + passed + " from " + sources + " filters");
            }
        }
        writer.join();

        // Assert
        assertThat(failure.get()).isNull();
        var passed = new ArrayList<String>();
        assertThat(sut.match(topic, passed::add)).isEqualTo(3);
        assertThat(passed).containsExactly("stays", "stays", "stays");
    }

    private static <T> FilterTree<T> tree(boolean concurrent) {
        return concurrent ? FilterTree.concurrent() : new FilterTree<>();
    }

    /// The values that `tree` passes for `topic`, failing if it passes one twice.
    private static <T> List<T> selected(FilterTree<T> tree, Topic topic) {
        var values = new ArrayList<T>();
        int _ = tree.match(topic, values::add);
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
