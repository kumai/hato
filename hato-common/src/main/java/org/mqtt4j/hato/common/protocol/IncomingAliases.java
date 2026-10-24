package org.mqtt4j.hato.common.protocol;

import java.util.OptionalInt;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.common.value.ReasonCode;
import org.mqtt4j.hato.common.value.Topic;

/// The Topic Aliases that the other side sets on one MQTT 5.0 connection. A PUBLISH that carries
/// both a topic and an alias sets the alias, and a later PUBLISH can carry the alias alone. The
/// mapping lasts for its connection only [MQTT-3.3.2-7], so the owner of the connection keeps one
/// and resolves each received PUBLISH before anything else handles it; nothing after that sees an
/// alias. Not thread safe.
///
/// The mapping holds at most the announced maximum of topics, each of up to 65,535 bytes.
///
/// See [MQTT 5.0, section 3.3.2.3.4: Topic Alias](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
public final class IncomingAliases {

    // The topic of each alias, indexed by the alias. Index 0 stays empty, as 0 is no alias.
    private final @Nullable Topic[] topics;

    /// Accepts the aliases from 1 to `maximum`, the Topic Alias Maximum that this side announced.
    ///
    /// @throws IllegalArgumentException if `maximum` is not between 1 and 65,535
    public IncomingAliases(int maximum) {
        if (maximum < 1 || maximum > 65_535) {
            throw new IllegalArgumentException("Topic Alias Maximum must be between 1 and 65535");
        }
        topics = new Topic[maximum + 1];
    }

    /// `publish` without its Topic Alias, after setting the alias to its topic when it carries
    /// one. A later topic for the same alias replaces the earlier one.
    ///
    /// @throws PacketDecodeException with Topic Alias invalid when the alias exceeds the announced
    ///     maximum
    public Packet.Publish set(Packet.Publish publish) {
        OptionalInt alias = alias(publish.properties());
        if (alias.isEmpty()) {
            return publish;
        }
        topics[checked(alias.getAsInt())] = publish.topic();
        return new Packet.Publish(
                publish.topic(),
                publish.payload(),
                publish.qos(),
                publish.retain(),
                publish.dup(),
                publish.packetId(),
                withoutAlias(publish.properties())
        );
    }

    /// The PUBLISH that `publish` stands for: its alias resolved to the topic that an earlier
    /// PUBLISH on this connection set, and the alias removed.
    ///
    /// @throws PacketDecodeException with Topic Alias invalid when the alias exceeds the announced
    ///     maximum, or with Protocol Error when no PUBLISH on this connection set it
    public Packet.Publish resolve(Packet.AliasPublish publish) {
        // The codec decodes a PUBLISH without a topic only when it carries an alias.
        int alias = checked(alias(publish.properties()).orElseThrow());
        Topic topic = topics[alias];
        if (topic == null) {
            throw new PacketDecodeException(
                    ReasonCode.PROTOCOL_ERROR,
                    "Topic Alias " + alias + " was not set"
            );
        }
        return new Packet.Publish(
                topic,
                publish.payload(),
                publish.qos(),
                publish.retain(),
                publish.dup(),
                publish.packetId(),
                withoutAlias(publish.properties())
        );
    }

    // The other side may use the aliases from 1 to the maximum announced to it
    // [MQTT-3.3.2-9] [MQTT-3.3.2-12]. The codec already refuses 0.
    private int checked(int alias) {
        int maximum = topics.length - 1;
        if (alias < 1 || alias > maximum) {
            throw new PacketDecodeException(
                    ReasonCode.TOPIC_ALIAS_INVALID,
                    "Topic Alias " + alias + " exceeds " + maximum
            );
        }
        return alias;
    }

    // The codec reads a Topic Alias as a Two Byte Integer.
    private static OptionalInt alias(Properties properties) {
        return switch (properties.find(PropertyId.TOPIC_ALIAS).orElse(null)) {
            case Property.TwoByteValue(var _, var value) -> OptionalInt.of(value);
            case null, default -> OptionalInt.empty();
        };
    }

    private static Properties withoutAlias(Properties properties) {
        return new Properties(properties.entries().stream()
                .filter(property -> property.id() != PropertyId.TOPIC_ALIAS)
                .toList());
    }
}
