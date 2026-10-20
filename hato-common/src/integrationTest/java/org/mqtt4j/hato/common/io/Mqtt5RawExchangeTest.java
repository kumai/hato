package org.mqtt4j.hato.common.io;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.common.protocol.Packet;
import org.mqtt4j.hato.common.protocol.PacketCodec;
import org.mqtt4j.hato.common.protocol.Properties;
import org.mqtt4j.hato.common.protocol.Property;
import org.mqtt4j.hato.common.value.MqttVersion;
import org.mqtt4j.hato.common.value.Qos;
import org.mqtt4j.hato.common.value.Topic;
import org.mqtt4j.hato.common.value.TopicFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.common.protocol.PropertyId.CONTENT_TYPE;
import static org.mqtt4j.hato.common.protocol.PropertyId.CORRELATION_DATA;
import static org.mqtt4j.hato.common.protocol.PropertyId.MESSAGE_EXPIRY_INTERVAL;
import static org.mqtt4j.hato.common.protocol.PropertyId.PAYLOAD_FORMAT_INDICATOR;
import static org.mqtt4j.hato.common.protocol.PropertyId.RESPONSE_TOPIC;
import static org.mqtt4j.hato.common.protocol.PropertyId.SUBSCRIPTION_IDENTIFIER;
import static org.mqtt4j.hato.common.protocol.PropertyId.TOPIC_ALIAS;
import static org.mqtt4j.hato.common.protocol.PropertyId.TOPIC_ALIAS_MAXIMUM;
import static org.mqtt4j.hato.common.value.ReasonCode.GRANTED_QOS_2;
import static org.mqtt4j.hato.common.value.ReasonCode.SUCCESS;

/// Exchanges raw MQTT 5.0 packets with a pinned broker image through the common codec alone.
///
/// Publications cross every broker alike, so they are tested here. Each subclass records how
/// its broker answers what the client's MQTT 5.0 features rely on: the CONNACK, Topic Aliases
/// towards a client that allows them, the copies for overlapping subscriptions and their
/// Subscription Identifiers, a publication that no subscription matches, an unsubscription from
/// a filter that was never subscribed, a shared subscription, and publications beyond the
/// broker's Receive Maximum. The client's tests take their expectations from these records, so
/// a new image that answers differently fails the record of that answer as well.
///
/// The records about routing subscribe with maximum QoS 0, so deliveries need no
/// acknowledgement and each subscriber reads them in the order the broker sends them.
abstract class Mqtt5RawExchangeTest {

    static final PacketCodec codec = PacketCodec.of(MqttVersion.V5);

    /// The distinct topics that [#aliasesForTopicsSentTwice] publishes to: more than any limit
    /// on aliases that the records meet, so that some topics go without one.
    static final int ALIAS_TOPICS = 20;

    /// The messages that [#recipientsOfSharedMessages] publishes.
    static final int SHARED_MESSAGES = 10;

    /// Opens a connection to the broker under test.
    abstract RawSession open() throws IOException;

    @ParameterizedTest
    @EnumSource(Qos.class)
    void publications_and_properties_cross_the_broker_at_every_qos(Qos qos) throws Exception {
        // Arrange: the publisher and subscriber use separate connections and packet identifiers.
        // The payload is not UTF-8 although its format indicator says so, and the broker
        // forwards it as it is.
        var topic = new Topic("hato/five/" + qos.level());
        var filter = new TopicFilter(topic.name());
        List<Property> messageProperties = List.of(
                new Property.ByteValue(PAYLOAD_FORMAT_INDICATOR, 1),
                new Property.StringValue(CONTENT_TYPE, "application/octet-stream"),
                new Property.StringValue(RESPONSE_TOPIC, "hato/five/reply"),
                new Property.BinaryValue(CORRELATION_DATA, new byte[] {0, (byte) 0xFF})
        );
        List<Property> userProperties = List.of(
                new Property.UserProperty("key", "first"),
                new Property.UserProperty("key", "second/あ")
        );
        var publicationProperties = new ArrayList<Property>(messageProperties);
        publicationProperties.addAll(userProperties);
        publicationProperties.add(new Property.FourByteValue(MESSAGE_EXPIRY_INTERVAL, 60));
        var payload = new byte[] {0, (byte) 0xFF, 42};
        try (
                RawSession subscriber = connected("hato-five-sub-" + qos.level());
                RawSession publisher = connected("hato-five-pub-" + qos.level())
        ) {
            var subscribe = new Packet.Subscribe(
                    1,
                    List.of(new Packet.Subscription(filter, Qos.EXACTLY_ONCE, false, false, 0)),
                    identifier(7)
            );
            subscriber.send(subscribe);
            assertThat(subscriber.expect(Packet.SubAck.class))
                    .isEqualTo(new Packet.SubAck(1, List.of(GRANTED_QOS_2), EMPTY));

            // Act
            var publication = new Packet.Publish(
                    topic,
                    payload,
                    qos,
                    false,
                    false,
                    qos == Qos.AT_MOST_ONCE ? 0 : 10,
                    new Properties(publicationProperties)
            );
            publisher.send(publication);
            switch (qos) {
                case AT_MOST_ONCE -> { }
                case AT_LEAST_ONCE -> assertThat(publisher.expect(Packet.PubAck.class))
                        .isEqualTo(new Packet.PubAck(10, SUCCESS, EMPTY));
                case EXACTLY_ONCE -> {
                    assertThat(publisher.expect(Packet.PubRec.class))
                            .isEqualTo(new Packet.PubRec(10, SUCCESS, EMPTY));
                    publisher.send(new Packet.PubRel(10, SUCCESS, EMPTY));
                    assertThat(publisher.expect(Packet.PubComp.class))
                            .isEqualTo(new Packet.PubComp(10, SUCCESS, EMPTY));
                }
            }
            Packet.Publish delivered = subscriber.expect(Packet.Publish.class);

            // Assert: the broker keeps the message properties and the order of the User
            // Properties, and adds the subscription's identifier. The order of the other
            // properties differs between brokers, and the expiry interval is what remains of
            // the message's lifetime.
            assertThat(delivered.topic()).isEqualTo(topic);
            assertThat(delivered.payload()).isEqualTo(payload);
            assertThat(delivered.qos()).isEqualTo(qos);
            List<Property> received = delivered.properties().entries();
            assertThat(received).containsAll(messageProperties);
            assertThat(received)
                    .filteredOn(property -> property instanceof Property.UserProperty)
                    .containsExactlyElementsOf(userProperties);
            assertThat(received)
                    .contains(new Property.VariableByteValue(SUBSCRIPTION_IDENTIFIER, 7));
            assertThat(received)
                    .filteredOn(property -> property.id() == MESSAGE_EXPIRY_INTERVAL)
                    .singleElement()
                    .isInstanceOfSatisfying(
                            Property.FourByteValue.class,
                            remaining -> assertThat(remaining.value()).isBetween(1L, 60L)
                    );
            switch (qos) {
                case AT_MOST_ONCE -> { }
                case AT_LEAST_ONCE -> subscriber.send(
                        new Packet.PubAck(delivered.packetId(), SUCCESS, EMPTY)
                );
                case EXACTLY_ONCE -> {
                    subscriber.send(new Packet.PubRec(delivered.packetId(), SUCCESS, EMPTY));
                    assertThat(subscriber.expect(Packet.PubRel.class))
                            .isEqualTo(new Packet.PubRel(delivered.packetId(), SUCCESS, EMPTY));
                    subscriber.send(new Packet.PubComp(delivered.packetId(), SUCCESS, EMPTY));
                }
            }
            subscriber.send(new Packet.Unsubscribe(2, List.of(filter), EMPTY));
            assertThat(subscriber.expect(Packet.UnsubAck.class))
                    .isEqualTo(new Packet.UnsubAck(2, List.of(SUCCESS), EMPTY));
            subscriber.send(new Packet.PingReq());
            subscriber.expect(Packet.PingResp.class);
        }
    }

    static Packet.Connect connect(String clientId, Properties properties) {
        return new Packet.Connect(
                clientId,
                MqttVersion.V5,
                true,
                60,
                null,
                null,
                null,
                properties
        );
    }

    static Properties properties(Property... entries) {
        return new Properties(List.of(entries));
    }

    /// The properties of a SUBSCRIBE that gives its subscriptions the identifier `value`.
    static Properties identifier(int value) {
        return properties(new Property.VariableByteValue(SUBSCRIPTION_IDENTIFIER, value));
    }

    RawSession connected(String clientId) throws Exception {
        return connected(clientId, EMPTY);
    }

    /// Connects with Clean Start and the CONNECT properties `properties`, and checks that the
    /// broker accepts the connection.
    RawSession connected(String clientId, Properties properties) throws Exception {
        RawSession session = open();
        try {
            session.send(connect(clientId, properties));
            assertThat(session.expect(Packet.ConnAck.class).reasonCode()).isEqualTo(SUCCESS);
            return session;
        } catch (Exception | AssertionError e) {
            session.close();
            throw e;
        }
    }

    /// Returns the bytes of the broker's answer to a CONNECT with the properties `properties`.
    byte[] connAckFrame(String clientId, Properties properties) throws Exception {
        try (RawSession client = open()) {
            client.send(connect(clientId, properties));
            return client.nextFrame();
        }
    }

    /// Publishes to [#ALIAS_TOPICS] distinct topics, and then to each of them again, for a
    /// subscriber that allows `topicAliasMaximum` Topic Aliases. Describes the deliveries in the
    /// order they arrive, each by the number of its topic and the way the broker named it:
    /// `n: topic` for the topic name alone, `n: topic and alias a` for the topic name together
    /// with alias `a`, and `n: alias a` for alias `a` without a topic name.
    List<String> aliasesForTopicsSentTwice(int topicAliasMaximum) throws Exception {
        String prefix = "hato/aliases/" + topicAliasMaximum;
        var end = new Topic(prefix + "/end");
        Properties allowance = properties(
                new Property.TwoByteValue(TOPIC_ALIAS_MAXIMUM, topicAliasMaximum)
        );
        try (
                RawSession subscriber = connected("hato-aliases-" + topicAliasMaximum, allowance);
                RawSession publisher = connected("hato-aliases-publisher-" + topicAliasMaximum)
        ) {
            subscribe(subscriber, 1, new TopicFilter(prefix + "/#"), EMPTY);
            for (int round = 0; round < 2; round++) {
                for (int topic = 0; topic < ALIAS_TOPICS; topic++) {
                    publisher.send(publication(new Topic(prefix + "/" + topic), topic));
                }
            }
            publisher.send(publication(end, 0));

            var descriptions = new ArrayList<String>();
            for (Packet delivery : deliveriesBefore(subscriber, end)) {
                descriptions.add(describeNaming(prefix, delivery));
            }
            return descriptions;
        }
    }

    /// The descriptions that [#aliasesForTopicsSentTwice] returns when the broker gives aliases
    /// 1 to `aliased` to the first topics it forwards, in order, sends the repeats of those topics
    /// with their alias alone, and names the other topics without an alias.
    static List<String> aliasesForTheFirst(int aliased) {
        var firstRound = new ArrayList<String>();
        var secondRound = new ArrayList<String>();
        for (int topic = 0; topic < ALIAS_TOPICS; topic++) {
            if (topic < aliased) {
                firstRound.add(topic + ": topic and alias " + (topic + 1));
                secondRound.add(topic + ": alias " + (topic + 1));
            } else {
                firstRound.add(topic + ": topic");
                secondRound.add(topic + ": topic");
            }
        }
        firstRound.addAll(secondRound);
        return firstRound;
    }

    /// Subscribes one connection with two overlapping filters, in separate SUBSCRIBE packets
    /// whose properties are `first` and `second`, and publishes once to a topic that both
    /// filters match. Returns the Subscription Identifiers of each copy that the connection
    /// receives, in the order the copies arrive.
    List<List<Integer>> identifiersOfOverlappingCopies(
            String name,
            Properties first,
            Properties second
    ) throws Exception {
        String prefix = "hato/overlap/" + name;
        // Only the first filter matches the end marker, so a single copy of it arrives, after
        // the copies of the publication.
        var end = new Topic(prefix + "/end/marker");
        try (
                RawSession subscriber = connected("hato-overlap-sub-" + name);
                RawSession publisher = connected("hato-overlap-pub-" + name)
        ) {
            subscribe(subscriber, 1, new TopicFilter(prefix + "/#"), first);
            subscribe(subscriber, 2, new TopicFilter(prefix + "/+"), second);
            publisher.send(publication(new Topic(prefix + "/both"), 1));
            publisher.send(publication(end, 0));

            var copies = new ArrayList<List<Integer>>();
            for (Packet delivery : deliveriesBefore(subscriber, end)) {
                copies.add(subscriptionIdentifiers((Packet.Publish) delivery));
            }
            return copies;
        }
    }

    /// Publishes a message that no subscription matches at `qos`, 1 or 2, with packet identifier
    /// 11, and returns the broker's answer.
    Packet answerToUnmatchedPublication(Qos qos) throws Exception {
        try (RawSession publisher = connected("hato-five-unmatched-" + qos.level())) {
            publisher.send(new Packet.Publish(
                    new Topic("hato/five/nobody"),
                    new byte[] {1},
                    qos,
                    false,
                    false,
                    11,
                    EMPTY
            ));
            return publisher.expect(Packet.class);
        }
    }

    /// Unsubscribes, with packet identifier 1, from a filter that the connection never
    /// subscribed to, and returns the broker's answer.
    Packet.UnsubAck answerToUnsubscribingWhatWasNeverSubscribed() throws Exception {
        try (RawSession client = connected("hato-five-never-subscribed")) {
            client.send(new Packet.Unsubscribe(
                    1,
                    List.of(new TopicFilter("hato/five/never")),
                    EMPTY
            ));
            return client.expect(Packet.UnsubAck.class);
        }
    }

    /// Subscribes two connections to one shared subscription and publishes [#SHARED_MESSAGES]
    /// messages to its topic. Returns, for each message in the order of publication, the member
    /// that received it: 0 for the member that subscribed first and 1 for the other. Fails when a
    /// message reaches both members, or not every message arrives.
    List<Integer> recipientsOfSharedMessages() throws Exception {
        var topic = new Topic("hato/shared");
        var filter = new TopicFilter("$share/hato/" + topic.name());
        try (
                RawSession first = connected("hato-shared-first");
                RawSession second = connected("hato-shared-second");
                RawSession publisher = connected("hato-shared-publisher")
        ) {
            subscribe(first, 1, filter, EMPTY);
            subscribe(second, 1, filter, EMPTY);
            for (int message = 0; message < SHARED_MESSAGES; message++) {
                publisher.send(publication(topic, message));
            }
            return recipients(List.of(first, second));
        }
    }

    /// Sends `accepted` QoS 2 publications without releasing any, checks that the broker accepts
    /// each with a PUBREC, then sends one more and returns the broker's answer to it.
    Packet answerToOneMoreUnreleasedPublication(int accepted) throws Exception {
        try (RawSession publisher = connected("hato-five-unreleased")) {
            for (int packetId = 1; packetId <= accepted; packetId++) {
                publisher.send(unreleasedPublication(packetId));
            }
            for (int packetId = 1; packetId <= accepted; packetId++) {
                Packet.PubRec answer = publisher.expect(Packet.PubRec.class);
                assertThat(answer.packetId()).isEqualTo(packetId);
                assertThat(answer.reasonCode().value())
                        .as("reason code that accepts publication " + packetId)
                        .isLessThan(0x80);
            }
            publisher.send(unreleasedPublication(accepted + 1));
            return publisher.expect(Packet.class);
        }
    }

    /// Subscribes `session` to `filter` with maximum QoS 0.
    private static void subscribe(
            RawSession session,
            int packetId,
            TopicFilter filter,
            Properties properties
    ) throws Exception {
        var subscription = new Packet.Subscription(filter, Qos.AT_MOST_ONCE, false, false, 0);
        session.send(new Packet.Subscribe(packetId, List.of(subscription), properties));
        assertThat(session.expect(Packet.SubAck.class))
                .isEqualTo(new Packet.SubAck(packetId, List.of(SUCCESS), EMPTY));
    }

    /// A QoS 0 publication whose one-byte payload is `number`.
    private static Packet.Publish publication(Topic topic, int number) {
        return new Packet.Publish(
                topic,
                new byte[] {(byte) number},
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                EMPTY
        );
    }

    private static Packet.Publish unreleasedPublication(int packetId) {
        return new Packet.Publish(
                new Topic("hato/five/unreleased"),
                new byte[] {1},
                Qos.EXACTLY_ONCE,
                false,
                false,
                packetId,
                EMPTY
        );
    }

    /// Reads what `subscriber` receives until a publication for `end` arrives, and returns the
    /// packets before it, in order.
    private static List<Packet> deliveriesBefore(RawSession subscriber, Topic end)
            throws Exception {
        var deliveries = new ArrayList<Packet>();
        while (true) {
            Packet delivery = subscriber.expect(Packet.class);
            if (delivery instanceof Packet.Publish publish && publish.topic().equals(end)) {
                return deliveries;
            }
            deliveries.add(delivery);
        }
    }

    /// Describes a delivery of [#aliasesForTopicsSentTwice]. A topic name tells the number of its
    /// topic; without one, the payload does.
    private static String describeNaming(String prefix, Packet delivery) {
        return switch (delivery) {
            case Packet.Publish publish -> {
                String number = publish.topic().name().substring(prefix.length() + 1);
                yield topicAlias(publish.properties())
                        .map(alias -> number + ": topic and alias " + alias)
                        .orElse(number + ": topic");
            }
            case Packet.AliasPublish publish -> publish.payload()[0] + ": alias "
                    + topicAlias(publish.properties()).orElseThrow();
            default -> throw new AssertionError("Expected a publication, got " + delivery);
        };
    }

    private static Optional<Integer> topicAlias(Properties properties) {
        return properties.find(TOPIC_ALIAS)
                .map(alias -> ((Property.TwoByteValue) alias).value());
    }

    private static List<Integer> subscriptionIdentifiers(Packet.Publish publish) {
        return publish.properties().entries().stream()
                .filter(property -> property.id() == SUBSCRIPTION_IDENTIFIER)
                .map(property -> ((Property.VariableByteValue) property).value())
                .toList();
    }

    /// Reads from each member in turn until [#SHARED_MESSAGES] messages have arrived in total, and
    /// returns the member that received each message, indexed by the number in its payload.
    private static List<Integer> recipients(List<RawSession> members) throws Exception {
        var recipientOf = new int[SHARED_MESSAGES];
        Arrays.fill(recipientOf, -1);
        Instant deadline = Instant.now().plusSeconds(10);
        int received = 0;
        while (received < SHARED_MESSAGES) {
            assertThat(Instant.now()).as("time to receive the shared messages").isBefore(deadline);
            for (int member = 0; member < members.size(); member++) {
                Optional<Packet> delivery = members.get(member).poll(Duration.ofMillis(50));
                if (delivery.isPresent()) {
                    var publish = (Packet.Publish) delivery.get();
                    int message = publish.payload()[0];
                    assertThat(recipientOf[message])
                            .as("member that received message " + message + " before")
                            .isEqualTo(-1);
                    recipientOf[message] = member;
                    received++;
                }
            }
        }
        return Arrays.stream(recipientOf).boxed().toList();
    }
}
