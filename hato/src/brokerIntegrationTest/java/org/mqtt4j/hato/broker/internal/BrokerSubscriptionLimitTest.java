package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.internal.protocol.Packet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.ReasonCode.QUOTA_EXCEEDED;
import static org.mqtt4j.hato.ReasonCode.SUCCESS;
import static org.mqtt4j.hato.ReasonCode.UNSPECIFIED_ERROR;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.RawSession.subscription;
import static org.mqtt4j.hato.internal.protocol.Properties.EMPTY;

/// The broker's limits on the subscriptions of one session: their number, and the length of
/// their filters in all.
class BrokerSubscriptionLimitTest {

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_subscription_beyond_the_limit_fails_while_the_rest_of_its_request_subscribes(
            MqttVersion version
    ) throws Exception {
        // Arrange: a session may have two subscriptions, and has one.
        try (BrokerServer sut = broker(2, 1024);
                RawSession subscriber = connectedAs(version, sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("first", AT_MOST_ONCE);

            // Act: the request asks for two more, and replaces the first one once the session
            // has both of its subscriptions.
            List<ReasonCode> results = subscribe(subscriber, "second", "third", "first");

            // Assert: only the new one finds no room, because the replacement takes none.
            ReasonCode refused = switch (version) {
                case V3_1_1 -> UNSPECIFIED_ERROR;
                case V5 -> QUOTA_EXCEEDED;
            };
            assertThat(results).containsExactly(SUCCESS, refused, SUCCESS);
            publisher.publish("third", "missed", AT_MOST_ONCE, 0);
            publisher.publish("second", "received", AT_MOST_ONCE, 0);
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(publication("second", "received", AT_MOST_ONCE, 0));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void unsubscribing_makes_room_for_another_subscription(boolean lengthLimit) throws Exception {
        // Arrange: a session may have one subscription, or filters of six characters in all,
        // and its subscription leaves no room for another one.
        try (BrokerServer sut = lengthLimit ? broker(16, 6) : broker(1, 1024);
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            subscriber.subscribe("first", AT_MOST_ONCE);
            assertThat(subscribe(subscriber, "second")).containsExactly(UNSPECIFIED_ERROR);

            // Act
            subscriber.send(new Packet.Unsubscribe(3, List.of(new TopicFilter("first")), EMPTY));
            subscriber.expect(Packet.UnsubAck.class);

            // Assert
            assertThat(subscribe(subscriber, "second")).containsExactly(SUCCESS);
        }
    }

    @Test
    void filters_that_are_longer_in_all_than_the_limit_are_refused() throws Exception {
        // Arrange: the filters of a session may have ten characters in all, and have six.
        try (BrokerServer sut = broker(16, 10);
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            subscriber.subscribe("123456", AT_MOST_ONCE);

            // Act: five more characters, then four, then the six that the session has already.
            List<ReasonCode> results = subscribe(subscriber, "12345", "1234", "123456");

            // Assert: the last one replaces a subscription, which adds no characters.
            assertThat(results).containsExactly(UNSPECIFIED_ERROR, SUCCESS, SUCCESS);
        }
    }

    @Test
    void a_new_session_has_all_of_its_room_again() throws Exception {
        // Arrange: a session whose filters take all ten characters that they may have.
        try (BrokerServer sut = broker(16, 10)) {
            try (RawSession previous = connected(sut.port(), "subscriber", false, false)) {
                previous.subscribe("1234567890", AT_MOST_ONCE);
                previous.disconnect();
            }

            // Act: Clean Session replaces the session.
            try (RawSession replacement = connected(sut.port(), "subscriber")) {
                // Assert
                assertThat(subscribe(replacement, "abcdefghij")).containsExactly(SUCCESS);
            }
        }
    }

    private static BrokerServer broker(int maxSubscriptions, long maxSubscriptionLength)
            throws IOException {
        return BrokerServer.builder()
                .maxSubscriptions(maxSubscriptions)
                .maxSubscriptionLength(maxSubscriptionLength)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }

    private static RawSession connectedAs(MqttVersion version, int port, String clientId)
            throws Exception {
        return switch (version) {
            case V3_1_1 -> connected(port, clientId);
            case V5 -> connectedV5(port, clientId);
        };
    }

    /// Asks for QoS 0 subscriptions to `filters` in one request, and returns its results.
    private static List<ReasonCode> subscribe(RawSession subscriber, String... filters)
            throws Exception {
        List<Packet.Subscription> subscriptions = Stream.of(filters)
                .map(filter -> subscription(filter, AT_MOST_ONCE))
                .toList();
        subscriber.send(new Packet.Subscribe(2, subscriptions, EMPTY));
        return subscriber.expect(Packet.SubAck.class).reasonCodes();
    }
}
