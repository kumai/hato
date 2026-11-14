package org.mqtt4j.hato.broker.internal;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.Topic;

import static org.assertj.core.api.Assertions.assertThat;

/// What the broker counts a publication and a delivery of it for where it bounds memory: what
/// they hold on the heap, which the bytes of the packet do not tell.
class PublicationMemoryTest {

    @Test
    void a_publication_counts_for_every_byte_of_its_payload() {
        // Arrange
        Packet.Publish empty = publication("topic", 0, Properties.EMPTY);
        Packet.Publish filled = publication("topic", 4096, Properties.EMPTY);

        // Act
        long difference = PublicationMemory.sizeOf(filled) - PublicationMemory.sizeOf(empty);

        // Assert
        assertThat(difference).isEqualTo(4096);
    }

    @Test
    void a_publication_counts_for_the_characters_of_its_topic() {
        // Arrange
        Packet.Publish brief = publication("t", 8, Properties.EMPTY);
        Packet.Publish lengthy = publication("t".repeat(1001), 8, Properties.EMPTY);

        // Act
        long difference = PublicationMemory.sizeOf(lengthy) - PublicationMemory.sizeOf(brief);

        // Assert: a character takes a byte at the least.
        assertThat(difference).isGreaterThanOrEqualTo(1000);
    }

    @Test
    void user_properties_count_for_what_they_hold_which_is_many_times_their_bytes_in_the_packet() {
        // Arrange: a thousand User Properties of two strings of one character, each of which
        // takes 7 bytes in the packet.
        var entries = new ArrayList<Property>();
        for (int i = 0; i < 1000; i++) {
            entries.add(new Property.UserProperty("a", "b"));
        }
        Packet.Publish plain = publication("topic", 8, Properties.EMPTY);
        Packet.Publish heavy = publication("topic", 8, new Properties(entries));
        int packet = PacketCodec.of(MqttVersion.V5).encode(heavy).length;

        // Act
        long difference = PublicationMemory.sizeOf(heavy) - PublicationMemory.sizeOf(plain);

        // Assert: each is an object with two strings, which take 120 bytes on a JVM that
        // compresses references, and more on one that does not.
        assertThat(packet).isLessThan(7100);
        assertThat(difference).isGreaterThanOrEqualTo(1000 * 120);
    }

    @Test
    void string_and_binary_properties_count_for_their_values() {
        // Arrange
        Packet.Publish plain = publication("topic", 8, Properties.EMPTY);
        Packet.Publish typed = publication("topic", 8, new Properties(List.of(
                new Property.StringValue(PropertyId.CONTENT_TYPE, "x".repeat(1000))
        )));
        Packet.Publish correlated = publication("topic", 8, new Properties(List.of(
                new Property.BinaryValue(PropertyId.CORRELATION_DATA, new byte[1000])
        )));

        // Act
        long size = PublicationMemory.sizeOf(plain);
        long withString = PublicationMemory.sizeOf(typed);
        long withBinary = PublicationMemory.sizeOf(correlated);

        // Assert
        assertThat(withString - size).isGreaterThanOrEqualTo(1000);
        assertThat(withBinary - size).isGreaterThanOrEqualTo(1000);
    }

    @Test
    void a_delivery_that_adds_subscription_identifiers_counts_for_its_own_properties() {
        // Arrange: a publication with a thousand properties, which a delivery without
        // identifiers shares as they are.
        var entries = new ArrayList<Property>();
        for (int i = 0; i < 1000; i++) {
            entries.add(new Property.UserProperty("a", "b"));
        }
        Packet.Publish publication = publication("topic", 8, new Properties(entries));
        var memory = new PublicationMemory(publication);

        // Act
        var shared = new Delivery(
                publication,
                memory,
                0,
                Qos.AT_LEAST_ONCE,
                false,
                List.of(),
                MqttVersion.V5
        );
        var identified = new Delivery(
                publication,
                memory,
                0,
                Qos.AT_LEAST_ONCE,
                false,
                List.of(7),
                MqttVersion.V5
        );

        // Assert: the second holds a sequence of its own with a reference to each property,
        // and both count their publication besides.
        assertThat(shared.ownBytes).isEqualTo(MemoryBudget.DELIVERY_BYTES);
        assertThat(identified.ownBytes - shared.ownBytes).isGreaterThanOrEqualTo(1001 * 4);
        assertThat(identified.memoryBytes).isEqualTo(memory.bytes() + identified.ownBytes);
    }

    private static Packet.Publish publication(String topic, int payload, Properties properties) {
        return new Packet.Publish(
                new Topic(topic),
                new byte[payload],
                Qos.AT_LEAST_ONCE,
                false,
                false,
                1,
                properties
        );
    }
}
