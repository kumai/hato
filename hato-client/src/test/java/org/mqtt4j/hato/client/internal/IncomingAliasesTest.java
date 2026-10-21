package org.mqtt4j.hato.client.internal;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.client.internal.common.protocol.Properties;
import org.mqtt4j.hato.client.internal.common.protocol.Property;
import org.mqtt4j.hato.client.internal.common.protocol.PropertyId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IncomingAliasesTest {

    private static final Property USER_PROPERTY = new Property.UserProperty("site", "plant-3");

    private final IncomingAliases sut = new IncomingAliases();

    @Test
    void an_alias_that_a_publication_sets_resolves_a_later_publication_without_a_topic() {
        // Act
        Packet.Publish first = sut.set(publication("a/b", 7, 1));
        Packet.Publish later = sut.resolve(aliasOnly(8, 1));

        // Assert: both lose the alias and keep their other properties.
        assertThat(first).isEqualTo(publication("a/b", 7));
        assertThat(later).isEqualTo(publication("a/b", 8));
    }

    @Test
    void a_later_topic_for_the_same_alias_replaces_the_earlier_one() {
        // Arrange
        sut.set(publication("a/b", 7, 1));

        // Act
        sut.set(publication("c/d", 8, 1));

        // Assert
        assertThat(sut.resolve(aliasOnly(9, 1)).topic()).isEqualTo(new Topic("c/d"));
    }

    @Test
    void a_publication_without_an_alias_passes_unchanged() {
        // Arrange
        Packet.Publish publication = publication("a/b", 7);

        // Act & Assert
        assertThat(sut.set(publication)).isSameAs(publication);
    }

    @Test
    void the_largest_announced_alias_is_accepted_and_the_next_one_is_invalid() {
        // Act
        sut.set(publication("a/b", 7, 16));

        // Assert
        assertThat(sut.resolve(aliasOnly(8, 16)).topic()).isEqualTo(new Topic("a/b"));
        assertThatThrownBy(() -> sut.set(publication("a/b", 9, 17)))
                .isInstanceOfSatisfying(
                        PacketDecodeException.class,
                        failure -> assertThat(failure.reasonCode())
                                .isEqualTo(ReasonCode.TOPIC_ALIAS_INVALID)
                );
        assertThatThrownBy(() -> sut.resolve(aliasOnly(10, 17)))
                .isInstanceOfSatisfying(
                        PacketDecodeException.class,
                        failure -> assertThat(failure.reasonCode())
                                .isEqualTo(ReasonCode.TOPIC_ALIAS_INVALID)
                );
    }

    @Test
    void an_alias_that_no_publication_set_is_a_protocol_error() {
        // Act & Assert
        assertThatThrownBy(() -> sut.resolve(aliasOnly(7, 2)))
                .isInstanceOfSatisfying(
                        PacketDecodeException.class,
                        failure -> assertThat(failure.reasonCode())
                                .isEqualTo(ReasonCode.PROTOCOL_ERROR)
                );
    }

    /// A QoS 1 publication on `topic` with a User Property, and the Topic Alias `alias` if given.
    private static Packet.Publish publication(String topic, int packetId, int... alias) {
        var properties = new ArrayList<Property>(List.of(USER_PROPERTY));
        for (int value : alias) {
            properties.add(new Property.TwoByteValue(PropertyId.TOPIC_ALIAS, value));
        }
        return new Packet.Publish(
                new Topic(topic),
                new byte[] {1},
                Qos.AT_LEAST_ONCE,
                false,
                false,
                packetId,
                new Properties(properties)
        );
    }

    /// A QoS 1 publication without a topic that carries a User Property and the alias `alias`.
    private static Packet.AliasPublish aliasOnly(int packetId, int alias) {
        return new Packet.AliasPublish(
                new byte[] {1},
                Qos.AT_LEAST_ONCE,
                false,
                false,
                packetId,
                new Properties(List.of(
                        USER_PROPERTY,
                        new Property.TwoByteValue(PropertyId.TOPIC_ALIAS, alias)
                ))
        );
    }
}
