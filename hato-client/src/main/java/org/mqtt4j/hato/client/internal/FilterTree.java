package org.mqtt4j.hato.client.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;

/// Topic filters arranged by their levels, so that finding the filters that select a topic
/// takes time that grows with the topic's levels and with the filters that select it, not with
/// the number of filters. Selection follows [TopicFilter#matches(Topic)]: `+` takes one level,
/// including an empty one, a final `#` takes zero or more levels, and a leading wildcard does
/// not select a topic whose name starts with `$`. A filter may hold several values. Not
/// thread-safe; the actor owns it.
final class FilterTree<T> {

    // A topic level never contains a wildcard, so the levels of a filter's `+` can share the
    // children of a node with the topic's own levels.
    private static final String SINGLE_LEVEL = "+";

    private static final String MULTI_LEVEL = "#";

    private final Node<T> root = new Node<>();

    /// Adds `value` under `filter`.
    void add(TopicFilter filter, T value) {
        Node<T> node = root;
        for (String level : levels(filter.pattern())) {
            // Validation keeps # to the final level.
            if (level.equals(MULTI_LEVEL)) {
                node.multiLevel.add(value);
                return;
            }
            node = node.children.computeIfAbsent(level, _ -> new Node<>());
        }
        node.exact.add(value);
    }

    /// Removes `value` from `filter`, together with the levels that no filter uses any longer.
    /// Does nothing when `filter` does not hold `value`.
    void remove(TopicFilter filter, T value) {
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
            node = node.children.get(level);
            if (node == null) {
                return;
            }
            path.add(node);
        }
        boolean removed = multiLevel ? node.multiLevel.remove(value) : node.exact.remove(value);
        if (!removed) {
            return;
        }
        // Unlink the nodes that hold nothing any more, deepest first. The node at path index i
        // is its parent's child for levels[i - 1].
        for (int i = path.size() - 1; i > 0 && path.get(i).isEmpty(); i--) {
            path.get(i - 1).children.remove(levels[i - 1]);
        }
    }

    /// Passes every value whose filter selects `topic` to `sink`, once each and in no particular
    /// order.
    void match(Topic topic, Consumer<? super T> sink) {
        String name = topic.name();
        String[] levels = levels(name);
        // [MQTT-4.7.2-1]: a leading wildcard does not select a topic whose name starts with $.
        boolean system = name.startsWith("$");
        // The nodes whose filters' levels select the topic's levels so far, advanced one level
        // at a time rather than recursively, so a deep topic cannot exhaust the stack. Every
        // node is reached by one path only, so each holds its values at one depth.
        var current = new ArrayList<Node<T>>();
        var next = new ArrayList<Node<T>>();
        current.add(root);
        for (int depth = 0; depth < levels.length && !current.isEmpty(); depth++) {
            boolean leading = depth == 0 && system;
            for (Node<T> node : current) {
                // A final # here takes this level and the rest.
                if (!leading) {
                    node.multiLevel.forEach(sink);
                }
                Node<T> literal = node.children.get(levels[depth]);
                if (literal != null) {
                    next.add(literal);
                }
                Node<T> any = leading ? null : node.children.get(SINGLE_LEVEL);
                if (any != null) {
                    next.add(any);
                }
            }
            ArrayList<Node<T>> reached = next;
            next = current;
            current = reached;
            next.clear();
        }
        // The topic has no level left: the filters that end here select it, and so does a final
        // # that takes no level.
        for (Node<T> node : current) {
            node.exact.forEach(sink);
            node.multiLevel.forEach(sink);
        }
    }

    /// No filter holds a value, and no level is left, for tests.
    boolean isEmpty() {
        return root.isEmpty();
    }

    // Keeps trailing empty levels: "a/" has the levels "a" and "".
    private static String[] levels(String text) {
        return text.split("/", -1);
    }

    private static final class Node<T> {

        // The next levels of the filters that pass through this node, + included.
        final Map<String, Node<T>> children = new HashMap<>();

        // The values of the filters that end at this node.
        final List<T> exact = new ArrayList<>();

        // The values of the filters whose final # follows this node.
        final List<T> multiLevel = new ArrayList<>();

        boolean isEmpty() {
            return children.isEmpty() && exact.isEmpty() && multiLevel.isEmpty();
        }
    }
}
