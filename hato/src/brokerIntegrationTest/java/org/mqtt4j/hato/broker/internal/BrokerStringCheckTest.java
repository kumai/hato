package org.mqtt4j.hato.broker.internal;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.Properties;
import org.mqtt4j.hato.internal.protocol.Property;
import org.mqtt4j.hato.internal.protocol.PropertyId;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.ReasonCode.MALFORMED_PACKET;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;
import static org.mqtt4j.hato.internal.protocol.Properties.EMPTY;

/// The strings in the packets of clients that the broker refuses: those with a control character
/// or a noncharacter, over which clients that the broker passed them on to may drop their
/// connections.
class BrokerStringCheckTest {

    static Stream<Refused> refused() {
        return Stream.of(
                new Refused("U+0001 in a topic", publication("a\u0001b", EMPTY)),
                new Refused("U+009F in a topic", publication("a\u009Fb", EMPTY)),
                new Refused(
                        "U+007F in the name of a User Property",
                        publication("readings", userProperty("k\u007F", "v"))
                ),
                new Refused(
                        "U+FDD0 in the value of a User Property",
                        publication("readings", userProperty("k", "﷐"))
                ),
                new Refused(
                        "U+FFFE in a Response Topic",
                        publication("readings", new Properties(List.of(new Property.StringValue(
                                PropertyId.RESPONSE_TOPIC,
                                "replies/￾"
                        ))))
                ),
                new Refused(
                        "U+1FFFF in a filter",
                        new Packet.Subscribe(
                                1,
                                List.of(RawSession.subscription("a/🿿", AT_MOST_ONCE)),
                                EMPTY
                        )
                ),
                new Refused(
                        "U+0085 in the filter of an UNSUBSCRIBE",
                        new Packet.Unsubscribe(1, List.of(new TopicFilter("a\u0085")), EMPTY)
                )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("refused")
    void a_packet_with_a_control_character_or_a_noncharacter_ends_an_mqtt_5_connection(
            Refused refused
    ) throws Exception {
        try (var sut = MqttBroker.start();
                RawSession client = connectedV5(sut.port(), "client")) {
            // Act
            client.send(refused.packet());

            // Assert
            assertThat(client.expect(Packet.Disconnect.class).reasonCode())
                    .isEqualTo(MALFORMED_PACKET);
            client.expectClosed();
        }
    }

    @Test
    void an_mqtt_3_1_1_client_with_a_control_character_in_a_topic_loses_its_connection()
            throws Exception {
        try (var sut = MqttBroker.start(); RawSession client = connected(sut.port(), "client")) {
            // Act
            client.send(publication("a\u001Fb", EMPTY));

            // Assert
            client.expectClosed();
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_connect_with_a_control_character_closes_its_connection_without_connack(
            MqttVersion version
    ) throws Exception {
        try (var sut = MqttBroker.start();
                RawSession client = RawSession.open(sut.port(), version)) {
            // Act
            client.send(switch (version) {
                case V3_1_1 -> RawSession.connect("bell\u0007", true, 0);
                case V5 -> RawSession.connectV5("bell\u0007", true, 0, 0);
            });

            // Assert
            client.expectClosed();
        }
    }

    @Test
    void the_characters_beside_the_refused_ones_reach_subscribers() throws Exception {
        // Arrange: a no-break space, a byte order mark, a character beyond the first plane, and
        // the replacement character, which are no control characters or noncharacters.
        String topic = "a b﻿c😀d�";
        try (var sut = MqttBroker.start();
                RawSession subscriber = connectedV5(sut.port(), "subscriber");
                RawSession publisher = connectedV5(sut.port(), "publisher")) {
            subscriber.subscribe("#", AT_MOST_ONCE);

            // Act
            publisher.send(publication(topic, userProperty("k ", "﻿v")));

            // Assert
            Packet.Publish delivery = subscriber.expect(Packet.Publish.class);
            assertThat(delivery.topic()).isEqualTo(new Topic(topic));
            assertThat(delivery.properties().entries())
                    .containsExactly(new Property.UserProperty("k ", "﻿v"));
        }
    }

    private static Packet.Publish publication(String topic, Properties properties) {
        return new Packet.Publish(
                new Topic(topic),
                "reading".getBytes(UTF_8),
                AT_MOST_ONCE,
                false,
                false,
                0,
                properties
        );
    }

    private static Properties userProperty(String name, String value) {
        return new Properties(List.of(new Property.UserProperty(name, value)));
    }

    /// A packet that carries a refused character, and where.
    record Refused(String place, Packet packet) {

        @Override
        public String toString() {
            return place;
        }
    }
}
