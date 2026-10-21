package org.mqtt4j.hato.client.internal;

import java.util.HashMap;
import java.util.Map;

import org.mqtt4j.hato.Topic;

/// The Topic Aliases that the client sets on one MQTT 5.0 connection. The first topics that it
/// publishes on the connection take aliases, first come first served, and keep them until the
/// connection ends; later topics go without one. An alias is recorded only once the send queue
/// accepts the PUBLISH that sets it. The queue writes in the order in which it accepts, so that
/// PUBLISH reaches the broker before any PUBLISH that carries the alias alone.
final class OutgoingAliases {

    // At most this many aliases, whatever the broker allows. A topic takes up to 65,535 bytes,
    // so the table holds at most about 1 MiB. The value is provisional, as is the Topic Alias
    // Maximum that the client announces, and measurements may change it.
    static final int LIMIT = 16;

    private final Map<Topic, Integer> aliases = new HashMap<>();

    /// The alias for a PUBLISH on `topic`, on a connection whose broker takes up to `maximum`
    /// aliases: the topic's alias, which replaces the topic; a new alias, sent with the topic,
    /// while aliases remain; or none.
    TopicAlias choose(Topic topic, int maximum) {
        Integer alias = aliases.get(topic);
        if (alias != null) {
            return new TopicAlias(alias, false);
        }
        // Aliases are never reassigned, so the next one is one past those in use.
        if (aliases.size() < Math.min(maximum, LIMIT)) {
            return new TopicAlias(aliases.size() + 1, true);
        }
        return TopicAlias.NONE;
    }

    /// Records the alias that a PUBLISH on `topic` set, once the send queue accepted it.
    void accepted(Topic topic, TopicAlias alias) {
        if (alias.value() != 0 && alias.withTopic()) {
            aliases.put(topic, alias.value());
        }
    }
}
