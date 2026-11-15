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
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

import static org.assertj.core.api.Assertions.assertThat;

/// What the broker counts a publication, a delivery of it, and the cursor of a subscription's
/// retained messages for where it bounds memory: what they hold on the heap, which the bytes
/// of the packet do not tell.
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

    @Test
    void a_cursor_counts_for_its_array_by_the_messages_that_it_was_made_for() {
        // Arrange
        var filter = new TopicFilter("kept/#");

        // Act
        long none = RetainedCursor.sizeOf(filter, 0, 0, MemoryBudget.DELIVERY_BYTES);
        long many = RetainedCursor.sizeOf(filter, 0, 1000, MemoryBudget.DELIVERY_BYTES);

        // Assert: a reference to each message, which takes eight bytes on a JVM that does not
        // compress references. The array stays as long as it was made, so the count does not
        // look at how many messages are left.
        assertThat(many - none).isEqualTo(1000 * 8);
    }

    @Test
    void a_cursor_counts_for_what_it_holds_besides_its_array() {
        // Arrange
        var filter = new TopicFilter("f");
        long delivery = MemoryBudget.DELIVERY_BYTES;

        // Act
        long plain = RetainedCursor.sizeOf(filter, 0, 1, delivery);
        long lengthy = RetainedCursor.sizeOf(new TopicFilter("f".repeat(1001)), 0, 1, delivery);
        long identified = RetainedCursor.sizeOf(filter, 1, 1, delivery);
        long heavier = RetainedCursor.sizeOf(filter, 0, 1, delivery + 1000);

        // Assert: itself, which is 96 bytes without compressed references, and the delivery
        // that it makes for its next message; the characters of its filter, a byte each at
        // the least; a list and a number for its identifier, 16 bytes each at the least; and
        // whatever the largest delivery of one of its messages holds.
        assertThat(plain).isGreaterThanOrEqualTo(96 + delivery);
        assertThat(lengthy - plain).isGreaterThanOrEqualTo(1000);
        assertThat(identified - plain).isGreaterThanOrEqualTo(32);
        assertThat(heavier - plain).isEqualTo(1000);
    }

    @Test
    void a_cursor_counts_for_the_largest_delivery_that_it_makes() {
        // Arrange: two retained messages, of which the second has a thousand properties. A
        // delivery that adds a Subscription Identifier holds a sequence of its own with a
        // reference to each of them.
        var entries = new ArrayList<Property>();
        for (int i = 0; i < 1000; i++) {
            entries.add(new Property.UserProperty("a", "b"));
        }
        Packet.Publish plain = publication("kept/1", 8, Properties.EMPTY);
        Packet.Publish heavy = publication("kept/2", 8, new Properties(entries));
        List<RetainedMessages.Message> matched = List.of(retained(plain), retained(heavy));
        var subscription = new Packet.Subscription(
                new TopicFilter("kept/#"),
                Qos.AT_LEAST_ONCE,
                false,
                false,
                0
        );

        // Act: cursors of an MQTT 5.0 subscription without and with an identifier.
        var shared = new RetainedCursor(subscription, List.of(), matched, MqttVersion.V5, 0);
        var identified = new RetainedCursor(subscription, List.of(7), matched, MqttVersion.V5, 0);

        // Assert: the second counts for the delivery of the message with the properties,
        // whichever of the two is next.
        long delivery = Delivery.ownSize(heavy, List.of(7), MqttVersion.V5);
        assertThat(delivery - MemoryBudget.DELIVERY_BYTES).isGreaterThanOrEqualTo(1001 * 4);
        assertThat(shared.ownBytes).isEqualTo(RetainedCursor.sizeOf(
                subscription.filter(),
                0,
                2,
                MemoryBudget.DELIVERY_BYTES
        ));
        assertThat(identified.ownBytes).isEqualTo(RetainedCursor.sizeOf(
                subscription.filter(),
                1,
                2,
                delivery
        ));
    }

    private static RetainedMessages.Message retained(Packet.Publish publication) {
        return new RetainedMessages.Message(
                publication,
                0,
                Delivery.lifetimeOf(publication),
                new PublicationMemory(publication)
        );
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
