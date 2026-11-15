package org.mqtt4j.hato.broker.internal;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.broker.Authenticator;
import org.mqtt4j.hato.broker.BrokerListener;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.broker.MqttBrokerBuilder;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.Property;
import org.mqtt4j.hato.internal.protocol.PropertyId;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.BrokerRetainedTest.retained;
import static org.mqtt4j.hato.broker.internal.BrokerRetainedTest.retainedTopics;
import static org.mqtt4j.hato.broker.internal.RawSession.connectV5;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.deliverySize;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;

/// The public builder: every start is a broker of its own, and every limit and the listener
/// that the builder sets reach the broker. The tests of each limit's behavior set it on the
/// internal builder.
class BrokerBuilderTest {

    @Test
    void each_start_starts_a_broker_of_its_own() throws Exception {
        // Arrange
        MqttBrokerBuilder sut = MqttBroker.builder();
        // Act
        try (MqttBroker first = sut.start();
                MqttBroker second = sut.start();
                RawSession one = connected(first.port(), "same");
                RawSession other = connected(second.port(), "same")) {
            // Assert: a client of the same identifier at the second broker takes nothing over
            // at the first.
            assertThat(first.port()).isNotEqualTo(second.port());
            one.ping();
            other.ping();
        }
    }

    @Test
    void announces_the_packet_size_that_it_is_given() throws Exception {
        try (MqttBroker sut = MqttBroker.builder().maxPacketSize(4096).start();
                RawSession client = RawSession.open(sut.port(), MqttVersion.V5)) {
            // Act
            client.send(connectV5("client", true, 0, 0));
            Packet.ConnAck connAck = client.expect(Packet.ConnAck.class);
            // Assert
            assertThat(connAck.properties().find(PropertyId.MAXIMUM_PACKET_SIZE))
                    .contains(new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, 4096));
        }
    }

    @Test
    void lowers_a_longer_session_expiry_to_its_limit() throws Exception {
        try (MqttBroker sut = MqttBroker.builder().maxSessionExpiry(Duration.ofMinutes(1)).start();
                RawSession client = RawSession.open(sut.port(), MqttVersion.V5)) {
            // Act: the client asks for an hour.
            client.send(connectV5("client", true, 0, 3600));
            Packet.ConnAck connAck = client.expect(Packet.ConnAck.class);
            // Assert
            assertThat(connAck.properties().find(PropertyId.SESSION_EXPIRY_INTERVAL))
                    .contains(new Property.FourByteValue(PropertyId.SESSION_EXPIRY_INTERVAL, 60));
        }
    }

    @Test
    void refuses_a_client_beyond_its_connection_limit() throws Exception {
        try (MqttBroker sut = MqttBroker.builder().maxConnections(1).start();
                RawSession first = connectedV5(sut.port(), "first");
                RawSession second = RawSession.open(sut.port(), MqttVersion.V5)) {
            // Act
            second.send(connectV5("second", true, 0, 0));
            // Assert
            assertThat(second.expect(Packet.ConnAck.class).reasonCode())
                    .isEqualTo(ReasonCode.QUOTA_EXCEEDED);
            first.ping();
        }
    }

    @Test
    void refuses_a_persistent_session_beyond_its_limit() throws Exception {
        // Arrange: a persistent session, and a clean one, which the limit leaves alone.
        try (MqttBroker sut = MqttBroker.builder().maxPersistentSessions(1).start();
                RawSession first = connectedV5(sut.port(), "first", true, 60, false);
                RawSession clean = connectedV5(sut.port(), "clean");
                RawSession second = RawSession.open(sut.port(), MqttVersion.V5)) {
            // Act
            second.send(connectV5("second", true, 0, 60));
            // Assert
            assertThat(second.expect(Packet.ConnAck.class).reasonCode())
                    .isEqualTo(ReasonCode.QUOTA_EXCEEDED);
            first.ping();
            clean.ping();
        }
    }

    @Test
    void keeps_no_retained_message_beyond_its_count() throws Exception {
        try (MqttBroker sut = MqttBroker.builder().maxRetainedMessages(1).start();
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Act
            publisher.publish(retained("limited/a", "a", AT_LEAST_ONCE));
            publisher.publish(retained("limited/b", "b", AT_LEAST_ONCE));
            // Assert
            assertThat(retainedTopics(sut.port(), "subscriber", "limited/#", 1))
                    .containsExactly("limited/a");
        }
    }

    @Test
    void keeps_no_retained_message_beyond_its_memory() throws Exception {
        // Arrange: a limit that holds one of the messages below.
        long limit = PublicationMemory.sizeOf(retained("sized/a", "small", AT_LEAST_ONCE));
        try (MqttBroker sut = MqttBroker.builder().maxRetainedBytes(limit).start();
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Act
            publisher.publish(retained("sized/a", "small", AT_LEAST_ONCE));
            publisher.publish(retained("sized/b", "small", AT_LEAST_ONCE));
            // Assert
            assertThat(retainedTopics(sut.port(), "subscriber", "sized/#", 1))
                    .containsExactly("sized/a");
        }
    }

    @Test
    void a_session_misses_deliveries_beyond_its_queued_count() throws Exception {
        try (MqttBroker sut = MqttBroker.builder().maxQueuedDeliveries(1).start()) {
            assertThatTwoDeliveriesLeaveOne(sut);
        }
    }

    @Test
    void a_session_misses_deliveries_beyond_its_queued_memory() throws Exception {
        // Arrange: a limit that holds one of the deliveries below.
        long limit = deliverySize(publication("queued", "1", AT_LEAST_ONCE, 1));
        try (MqttBroker sut = MqttBroker.builder().maxQueuedDeliveryBytes(limit).start()) {
            assertThatTwoDeliveriesLeaveOne(sut);
        }
    }

    @Test
    void tells_its_listener_of_the_deliveries_that_a_session_misses() throws Exception {
        // Arrange
        var dropped = new LinkedBlockingQueue<String>();
        try (MqttBroker sut = MqttBroker.builder()
                .maxQueuedDeliveries(1)
                .listener(new BrokerListener() {
                    @Override
                    public void onDropped(String clientId, long count) {
                        dropped.add(clientId + " lost " + count);
                    }
                })
                .start()) {
            // Act
            assertThatTwoDeliveriesLeaveOne(sut);
            // Assert
            assertThat(dropped.poll(5, SECONDS)).isEqualTo("away lost 1");
        }
    }

    @Test
    void asks_its_authenticator_about_every_client() throws Exception {
        try (MqttBroker sut = MqttBroker.builder()
                .authenticator(_ -> Authenticator.Decision.refuseNotAuthorized())
                .start();
                RawSession client = RawSession.open(sut.port(), MqttVersion.V5)) {
            // Act
            client.send(connectV5("client", true, 0, 0));
            // Assert
            assertThat(client.expect(Packet.ConnAck.class).reasonCode())
                    .isEqualTo(ReasonCode.NOT_AUTHORIZED);
        }
    }

    @Test
    void gives_its_authenticator_the_time_that_it_is_given() throws Exception {
        // Arrange: a decision that waits until the test lets it go, far beyond the setting.
        var release = new CountDownLatch(1);
        try (MqttBroker sut = MqttBroker.builder()
                .authenticator(_ -> {
                    release.await();
                    return Authenticator.Decision.accept(Authenticator.Scope.all());
                })
                .authenticationTimeout(Duration.ofMillis(200))
                .start();
                RawSession client = RawSession.open(sut.port(), MqttVersion.V5)) {
            try {
                // Act
                client.send(connectV5("client", true, 0, 0));
                // Assert: refused well before the default of ten seconds.
                assertThat(client.expect(Packet.ConnAck.class).reasonCode())
                        .isEqualTo(ReasonCode.SERVER_UNAVAILABLE);
            } finally {
                release.countDown();
            }
        }
    }

    // Publishes two deliveries for a persistent session whose client is away, and checks that
    // the session resumes with one of them alone.
    private static void assertThatTwoDeliveriesLeaveOne(MqttBroker sut) throws Exception {
        try (RawSession away = connected(sut.port(), "away", false, false)) {
            away.subscribe("queued", AT_LEAST_ONCE);
            away.disconnect();
        }
        try (RawSession publisher = connected(sut.port(), "publisher")) {
            publisher.publish("queued", "1", AT_LEAST_ONCE, 1);
            publisher.publish("queued", "2", AT_LEAST_ONCE, 2);
        }
        try (RawSession back = connected(sut.port(), "away", false, true)) {
            back.expect(Packet.Publish.class);
            back.ping();
        }
    }
}
