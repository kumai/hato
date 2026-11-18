package org.mqtt4j.hato.internal.protocol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;

/// Topic filters arranged by their levels, so that finding the filters that select a topic
/// takes time that grows with the topic's levels and with the filters that select it, not with
/// the number of filters. Selection follows [TopicFilter#matches(Topic)]: `+` takes one level,
/// including an empty one, a final `#` takes zero or more levels, and a leading wildcard does
/// not select a topic whose name starts with `$`. Each filter holds a set of values.
///
/// A tree made with [#FilterTree()] serves one thread at a time, as the client's actor uses it.
/// A tree made with [#concurrent()] lets [#match] run on any number of threads, also while the
/// tree changes; its changes must come one at a time, which the caller orders. A match then
/// passes every value that was added before it began and not removed since, and may or may not
/// pass one that is added or removed while it runs.
public final class FilterTree<T> {

    // A topic level never contains a wildcard, so the levels of a filter's `+` can share the
    // children of a node with the topic's own levels.
    private static final String SINGLE_LEVEL = "+";

    private static final String MULTI_LEVEL = "#";

    private final boolean concurrent;

    private final Node<T> root = new Node<>();

    // The nodes that match() advances level by level, kept for its next call in a tree for one
    // thread, which runs it for every received publication. A concurrent tree's matches take
    // their own.
    private ArrayList<Node<T>> reached = new ArrayList<>();

    private ArrayList<Node<T>> spare = new ArrayList<>();

    /// Makes a tree for one thread at a time.
    public FilterTree() {
        this(false);
    }

    private FilterTree(boolean concurrent) {
        this.concurrent = concurrent;
    }

    /// {@return a tree whose matches can run on any number of threads, also during a change}
    /// The caller makes its changes one at a time.
    public static <T> FilterTree<T> concurrent() {
        return new FilterTree<>(true);
    }

    /// Adds `value` under `filter`. Does nothing when `filter` holds `value` already.
    public void add(TopicFilter filter, T value) {
        Node<T> node = root;
        for (String level : levels(filter.pattern())) {
            // Validation keeps # to the final level.
            if (level.equals(MULTI_LEVEL)) {
                node.multiLevel = with(node.multiLevel, value);
                return;
            }
            node = childFor(node, level);
        }
        node.exact = with(node.exact, value);
    }

    /// Removes `value` from `filter`, together with the levels that no filter uses any longer.
    /// Does nothing when `filter` does not hold `value`.
    public void remove(TopicFilter filter, T value) {
        String[] levels = levels(filter.pattern());
        // The nodes from the root to the filter's last literal or + level.
        var path = new ArrayList<Node<T>>(levels.length + 1);
        Node<T> node = root;
        path.add(node);
        boolean multiLevel = false;
        for (String level : levels) {
            if (level.equals(MULTI_LEVEL)) {
                multiLevel = true;
                break;
            }
            Map<String, Node<T>> children = node.children;
            node = (children == null) ? null : children.get(level);
            if (node == null) {
                return;
            }
            path.add(node);
        }
        Object before = multiLevel ? node.multiLevel : node.exact;
        if (before == null || !holds(before, value)) {
            return;
        }
        if (multiLevel) {
            node.multiLevel = without(before, value);
        } else {
            node.exact = without(before, value);
        }
        // Unlink the nodes that hold nothing any more, deepest first. The node at path index i
        // is its parent's child for levels[i - 1]. A concurrent match that is on its way through
        // an unlinked node finds it empty, as it would after the removal.
        for (int i = path.size() - 1; i > 0 && path.get(i).isEmpty(); i--) {
            Node<T> parent = path.get(i - 1);
            Map<String, Node<T>> children = parent.children;
            if (children != null) {
                children.remove(levels[i - 1]);
                if (children.isEmpty()) {
                    parent.children = null;
                }
            }
        }
    }

    /// Passes every value whose filter selects `topic` to `sink`, in no particular order. A value
    /// that several selecting filters hold comes once from each.
    ///
    /// @return how many filters passed values: when it is at most one, no value came twice
    public int match(Topic topic, Consumer<? super T> sink) {
        String name = topic.name();
        // [MQTT-4.7.2-1]: a leading wildcard does not select a topic whose name starts with $.
        boolean system = name.startsWith("$");
        int sources = 0;
        // The nodes whose filters' levels select the topic's levels so far, advanced one level
        // at a time rather than recursively, so a deep topic cannot exhaust the stack. Every
        // node is reached by one path only, so each holds its values at one depth.
        ArrayList<Node<T>> current = concurrent ? new ArrayList<>() : reached;
        ArrayList<Node<T>> next = concurrent ? new ArrayList<>() : spare;
        current.clear();
        next.clear();
        current.add(root);
        // Each level ends at a / or at the end of the name, so "a/" has the levels "a" and "".
        int start = 0;
        for (int depth = 0; start <= name.length() && !current.isEmpty(); depth++) {
            int end = name.indexOf('/', start);
            if (end < 0) {
                end = name.length();
            }
            String level = name.substring(start, end);
            start = end + 1;
            boolean leading = depth == 0 && system;
            for (int i = 0; i < current.size(); i++) {
                Node<T> node = current.get(i);
                // A final # here takes this level and the rest.
                if (!leading) {
                    sources += pass(node.multiLevel, sink);
                }
                Map<String, Node<T>> children = node.children;
                if (children == null) {
                    continue;
                }
                Node<T> literal = children.get(level);
                if (literal != null) {
                    next.add(literal);
                }
                Node<T> any = leading ? null : children.get(SINGLE_LEVEL);
                if (any != null) {
                    next.add(any);
                }
            }
            ArrayList<Node<T>> advanced = next;
            next = current;
            current = advanced;
            next.clear();
        }
        // The topic has no level left: the filters that end here select it, and so does a final
        // # that takes no level.
        for (Node<T> node : current) {
            sources += pass(node.exact, sink);
            sources += pass(node.multiLevel, sink);
        }
        if (!concurrent) {
            reached = current;
            spare = next;
        }
        return sources;
    }

    /// No filter holds a value, and no level is left, for tests.
    boolean isEmpty() {
        return root.isEmpty();
    }

    private Node<T> childFor(Node<T> node, String level) {
        Map<String, Node<T>> children = node.children;
        if (children == null) {
            var child = new Node<T>();
            Map<String, Node<T>> made = concurrent ? new ConcurrentHashMap<>() : new HashMap<>();
            made.put(level, child);
            // A concurrent match sees the map with its first child, or no map.
            node.children = made;
            return child;
        }
        Node<T> child = children.get(level);
        if (child == null) {
            child = new Node<>();
            children.put(level, child);
        }
        return child;
    }

    // The values of a node are held as nothing (null), one value, or Values, so that the many
    // filters that one value holds alone cost no set. A change replaces the holder in one write,
    // so a concurrent match sees it whole, before or after the change.
    private Object with(@Nullable Object holder, T value) {
        if (holder == null) {
            return value;
        }
        if (holder instanceof Values<?> values) {
            values.add(value);
            return holder;
        }
        if (holder.equals(value)) {
            return holder;
        }
        var values = new Values<T>(concurrent);
        values.add(holder);
        values.add(value);
        return values;
    }

    // The holder without `value`, which it holds: one value again, when a set comes down to one,
    // or nothing.
    private @Nullable Object without(Object holder, T value) {
        if (!(holder instanceof Values<?> values)) {
            return null;
        }
        Values<T> held = castValues(values);
        held.set.remove(value);
        if (held.set.size() > 1) {
            return held;
        }
        for (T left : held.set) {
            return left;
        }
        return null;
    }

    private static boolean holds(Object holder, Object value) {
        if (holder instanceof Values<?> values) {
            return values.set.contains(value);
        }
        return holder.equals(value);
    }

    // Passes the values of `holder` to `sink`, and returns 1 if there were any, otherwise 0.
    private int pass(@Nullable Object holder, Consumer<? super T> sink) {
        if (holder == null) {
            return 0;
        }
        if (holder instanceof Values<?> values) {
            Values<T> held = castValues(values);
            held.set.forEach(sink);
        } else {
            sink.accept(cast(holder));
        }
        return 1;
    }

    @SuppressWarnings("unchecked")
    private static <T> T cast(Object value) {
        return (T) value;
    }

    @SuppressWarnings("unchecked")
    private static <T> Values<T> castValues(Values<?> values) {
        return (Values<T>) values;
    }

    // Keeps trailing empty levels: "a/" has the levels "a" and "".
    private static String[] levels(String text) {
        return text.split("/", -1);
    }

    // The values of two or more filters' that end at one node: a set that a concurrent match
    // can go through while a change adds to it or removes from it.
    private static final class Values<T> {

        final Set<T> set;

        Values(boolean concurrent) {
            set = concurrent ? ConcurrentHashMap.newKeySet() : new HashSet<>();
        }

        void add(Object value) {
            set.add(cast(value));
        }
    }

    // A level of filters. The fields are volatile so that a concurrent match sees what a change
    // wrote; a tree for one thread pays for that only on reads.
    private static final class Node<T> {

        // The next levels of the filters that pass through this node, + included; null until
        // the first, and again once the last has gone.
        volatile @Nullable Map<String, Node<T>> children;

        // The values of the filters that end at this node: null, one value, or Values.
        volatile @Nullable Object exact;

        // The values of the filters whose final # follows this node, held as `exact` is.
        volatile @Nullable Object multiLevel;

        boolean isEmpty() {
            Map<String, Node<T>> next = children;
            return (next == null || next.isEmpty()) && exact == null && multiLevel == null;
        }
    }
}
