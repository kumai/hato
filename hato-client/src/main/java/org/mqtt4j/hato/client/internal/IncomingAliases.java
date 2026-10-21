package org.mqtt4j.hato.client.internal;

import java.util.OptionalLong;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.client.internal.common.protocol.Properties;
import org.mqtt4j.hato.client.internal.common.protocol.PropertyId;

/// The Topic Aliases that the broker sets on one MQTT 5.0 connection. A PUBLISH that carries
/// both a topic and an alias sets the alias, and a later PUBLISH can carry the alias alone. The
/// mapping lasts for its connection only [MQTT-3.3.2-7]. The serialized transport callback of
/// the connection owns it and resolves each PUBLISH before the session core sees it, so the core
/// never handles an alias.
final class IncomingAliases {

    // The Topic Alias Maximum that the client announces in CONNECT. A topic takes up to 65,535
    // bytes, so the mapping holds at most about 1 MiB. The value is provisional: it covers
    // Mosquitto's default of 10 aliases, and measurements may change it.
    static final int TOPIC_ALIAS_MAXIMUM = 16;

    // The topic of each alias, indexed by the alias. Index 0 stays empty, as 0 is no alias.
    private final @Nullable Topic[] topics = new Topic[TOPIC_ALIAS_MAXIMUM + 1];

    /// `publish` without its Topic Alias, after setting the alias to its topic when it carries
    /// one. A later topic for the same alias replaces the earlier one.
    ///
    /// @throws PacketDecodeException with Topic Alias invalid when the alias exceeds the announced
    ///     maximum
    Packet.Publish set(Packet.Publish publish) {
        OptionalLong alias = ConnectionEstablishment.number(
                publish.properties(),
                PropertyId.TOPIC_ALIAS
        );
        if (alias.isEmpty()) {
            return publish;
        }
        topics[checked(alias.getAsLong())] = publish.topic();
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
    Packet.Publish resolve(Packet.AliasPublish publish) {
        // The codec decodes a PUBLISH without a topic only when it carries an alias.
        int alias = checked(ConnectionEstablishment.number(
                publish.properties(),
                PropertyId.TOPIC_ALIAS
        ).orElseThrow());
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

    // The broker may use the aliases from 1 to the maximum that the client announced (MQTT 5.0
    // section 3.3.2.3.4). The codec already refuses 0.
    private static int checked(long alias) {
        if (alias > TOPIC_ALIAS_MAXIMUM) {
            throw new PacketDecodeException(
                    ReasonCode.TOPIC_ALIAS_INVALID,
                    "Topic Alias " + alias + " exceeds " + TOPIC_ALIAS_MAXIMUM
            );
        }
        return (int) alias;
    }

    private static Properties withoutAlias(Properties properties) {
        return new Properties(properties.entries().stream()
                .filter(property -> property.id() != PropertyId.TOPIC_ALIAS)
                .toList());
    }
}
