package org.mqtt4j.hato.client.internal;

/// How a PUBLISH that the client sends uses a Topic Alias: not at all ([#NONE]), to set a new
/// alias for its topic, which it still carries (`withTopic`), or in place of a topic that an
/// earlier PUBLISH on the connection set the alias for.
record TopicAlias(int value, boolean withTopic) {

    /// No alias: the PUBLISH carries its topic only.
    static final TopicAlias NONE = new TopicAlias(0, true);
}
