package org.mqtt4j.hato.broker.internal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;

// Values by the levels of their topics, so that finding the values whose topics a filter selects
// takes time that grows with the filter's levels and with the topics that its wildcards pass
// through, not with every topic. Selection follows TopicFilter.matches(Topic): + takes one
// level, including an empty one, a final # takes zero or more levels, and a leading wildcard
// does not select a topic whose name starts with $. Each topic holds one value. For one thread
// at a time; the retained messages use it under the topic lock.
final class TopicTree<V> {

    private static final String SINGLE_LEVEL = "+";

    private static final String MULTI_LEVEL = "#";

    // What one level of a topic holds in the tree at most, beside its characters, by the
    // estimates of PublicationMemory: its node, the entry that leads to it in its parent's map,
    // a map of its own children with the first table of a HashMap, and the string of the level.
    private static final long LEVEL_BYTES = PublicationMemory.OBJECT_BYTES
            + PublicationMemory.OBJECT_BYTES + 2L * PublicationMemory.REFERENCE_BYTES
            + 2L * PublicationMemory.OBJECT_BYTES
            + PublicationMemory.ARRAY_BYTES + 16L * PublicationMemory.REFERENCE_BYTES
            + PublicationMemory.OBJECT_BYTES + PublicationMemory.ARRAY_BYTES;

    private final Node<V> root = new Node<>();

    // What the levels of `topic` hold in a tree at most, with two bytes for each character. The
    // topics that share levels share their nodes, which this counts for each of them, so that
    // the sum over the topics of a tree bounds what the tree holds.
    static long sizeOf(Topic topic) {
        String name = topic.name();
        return levelsOf(name) * LEVEL_BYTES + 2L * name.length();
    }

    // How many levels a topic or a filter has: one more than the separators in `name`.
    static long levelsOf(String name) {
        long levels = 1;
        for (int i = 0; i < name.length(); i++) {
            if (name.charAt(i) == '/') {
                levels++;
            }
        }
        return levels;
    }

    // Makes `value` the value of `topic`, in place of the one that it had.
    void put(Topic topic, V value) {
        Node<V> node = root;
        for (String level : levels(topic.name())) {
            Map<String, Node<V>> children = node.children;
            if (children == null) {
                children = new HashMap<>();
                node.children = children;
            }
            node = children.computeIfAbsent(level, _ -> new Node<>());
        }
        node.value = value;
    }

    // Takes the value of `topic` out, together with the levels that no topic uses any longer.
    void remove(Topic topic) {
        String[] levels = levels(topic.name());
        // The nodes from the root to the topic's last level.
        var path = new ArrayList<Node<V>>(levels.length + 1);
        Node<V> node = root;
        path.add(node);
        for (String level : levels) {
            Map<String, Node<V>> children = node.children;
            node = (children == null) ? null : children.get(level);
            if (node == null) {
                return;
            }
            path.add(node);
        }
        node.value = null;
        // Unlink the nodes that hold nothing any more, deepest first. The node at path index i
        // is its parent's child for levels[i - 1].
        for (int i = path.size() - 1; i > 0 && path.get(i).isEmpty(); i--) {
            Node<V> parent = path.get(i - 1);
            Map<String, Node<V>> children = parent.children;
            if (children != null) {
                children.remove(levels[i - 1]);
                if (children.isEmpty()) {
                    parent.children = null;
                }
            }
        }
    }

    void clear() {
        root.children = null;
        root.value = null;
    }

    // Passes the value of every topic that `filter` selects to `sink`, in no particular order.
    void match(TopicFilter filter, Consumer<? super V> sink) {
        String[] levels = levels(filter.pattern());
        // The nodes whose topics' levels the filter's levels select so far, advanced one level
        // at a time rather than recursively, so a deep filter cannot exhaust the stack.
        var current = new ArrayList<Node<V>>();
        current.add(root);
        for (int depth = 0; depth < levels.length && !current.isEmpty(); depth++) {
            String level = levels[depth];
            // [MQTT-4.7.2-1]: a leading wildcard does not select a topic whose name starts
            // with $.
            boolean leading = depth == 0;
            if (level.equals(MULTI_LEVEL)) {
                // Validation keeps # to the final level, where it takes the level that it
                // follows and every level below it.
                for (Node<V> node : current) {
                    passAll(node, leading, sink);
                }
                return;
            }
            var next = new ArrayList<Node<V>>();
            for (Node<V> node : current) {
                Map<String, Node<V>> children = node.children;
                if (children == null) {
                    continue;
                }
                if (level.equals(SINGLE_LEVEL)) {
                    for (Map.Entry<String, Node<V>> child : children.entrySet()) {
                        if (!(leading && child.getKey().startsWith("$"))) {
                            next.add(child.getValue());
                        }
                    }
                } else {
                    Node<V> literal = children.get(level);
                    if (literal != null) {
                        next.add(literal);
                    }
                }
            }
            current = next;
        }
        for (Node<V> node : current) {
            V value = node.value;
            if (value != null) {
                sink.accept(value);
            }
        }
    }

    // Nothing is held and no level is left, for tests.
    boolean isEmpty() {
        return root.isEmpty();
    }

    // Passes the value of `top` and of every node below it, without the children whose levels
    // start with $ when `leading` says that a leading wildcard takes them.
    private void passAll(Node<V> top, boolean leading, Consumer<? super V> sink) {
        var pending = new ArrayDeque<Node<V>>();
        V own = top.value;
        if (own != null) {
            sink.accept(own);
        }
        Map<String, Node<V>> first = top.children;
        if (first != null) {
            for (Map.Entry<String, Node<V>> child : first.entrySet()) {
                if (!(leading && child.getKey().startsWith("$"))) {
                    pending.push(child.getValue());
                }
            }
        }
        while (!pending.isEmpty()) {
            Node<V> node = pending.pop();
            V value = node.value;
            if (value != null) {
                sink.accept(value);
            }
            Map<String, Node<V>> children = node.children;
            if (children != null) {
                for (Node<V> child : children.values()) {
                    pending.push(child);
                }
            }
        }
    }

    // Keeps trailing empty levels: "a/" has the levels "a" and "".
    private static String[] levels(String text) {
        return text.split("/", -1);
    }

    private static final class Node<V> {

        // The next levels of the topics that pass through this node; null until the first, and
        // again once the last has gone.
        @Nullable Map<String, Node<V>> children;

        // The value of the topic that ends at this node.
        @Nullable V value;

        boolean isEmpty() {
            return (children == null || children.isEmpty()) && value == null;
        }
    }
}
