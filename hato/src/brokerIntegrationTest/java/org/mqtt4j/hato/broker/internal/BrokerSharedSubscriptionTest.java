package org.mqtt4j.hato.broker.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.Properties;
import org.mqtt4j.hato.internal.protocol.Property;
import org.mqtt4j.hato.internal.protocol.PropertyId;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.Qos.EXACTLY_ONCE;
import static org.mqtt4j.hato.ReasonCode.SUCCESS;
import static org.mqtt4j.hato.ReasonCode.UNSPECIFIED_ERROR;
import static org.mqtt4j.hato.broker.internal.BrokerSessionRaceTest.awaitRelease;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.deliverySize;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.RawSession.subscription;
import static org.mqtt4j.hato.internal.protocol.Properties.EMPTY;

/// Shared subscriptions, `$share/{group}/{filter}`, which hand each publication to one session of
/// their group (MQTT 5.0 section 4.8.2), from MQTT 5.0 clients and, as an extension, from MQTT
/// 3.1.1 ones.
class BrokerSharedSubscriptionTest {

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void each_publication_reaches_one_member_and_the_members_take_turns(MqttVersion version)
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession first = member(sut.port(), "first", version);
                RawSession second = member(sut.port(), "second", version);
                RawSession publisher = connected(sut.port(), "publisher")) {
            first.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            second.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);

            // Act
            for (int i = 0; i < 10; i++) {
                publisher.publish("jobs/" + i, Integer.toString(i), AT_LEAST_ONCE, i + 1);
            }

            // Assert: the members, which both keep up, take the publications in turn.
            assertThat(receivedBeforePong(first)).containsExactly("0", "2", "4", "6", "8");
            assertThat(receivedBeforePong(second)).containsExactly("1", "3", "5", "7", "9");
        }
    }

    @Test
    void an_mqtt_3_1_1_member_and_an_mqtt_5_member_share_one_subscription() throws Exception {
        try (var sut = MqttBroker.start();
                RawSession older = connected(sut.port(), "older");
                RawSession newer = connectedV5(sut.port(), "newer");
                RawSession publisher = connected(sut.port(), "publisher")) {
            older.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            newer.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);

            // Act
            for (int i = 0; i < 6; i++) {
                publisher.publish("jobs/" + i, Integer.toString(i), AT_LEAST_ONCE, i + 1);
            }

            // Assert
            List<String> toOlder = receivedBeforePong(older);
            List<String> toNewer = receivedBeforePong(newer);
            assertThat(toOlder).isNotEmpty();
            assertThat(toNewer).isNotEmpty();
            var all = new ArrayList<>(toOlder);
            all.addAll(toNewer);
            assertThat(all).containsExactlyInAnyOrder(numbers(6));
        }
    }

    @Test
    void members_without_a_connection_do_not_tilt_the_turns() throws Exception {
        try (var sut = MqttBroker.start();
                RawSession first = connected(sut.port(), "first");
                RawSession second = connected(sut.port(), "second");
                RawSession publisher = connected(sut.port(), "publisher")) {
            first.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            second.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            // Arrange: a third member whose session waits for its next connection.
            try (RawSession away = connected(sut.port(), "away", false, false)) {
                away.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            }

            // Act
            for (int i = 0; i < 6; i++) {
                publisher.publish("jobs/" + i, Integer.toString(i), AT_LEAST_ONCE, i + 1);
            }

            // Assert: the turn goes on after the member that took the last publication, so the
            // member after the one without a connection gets no more than the other.
            assertThat(receivedBeforePong(first)).containsExactly("0", "2", "4");
            assertThat(receivedBeforePong(second)).containsExactly("1", "3", "5");
            try (RawSession back = connected(sut.port(), "away", false, true)) {
                back.ping();
            }
        }
    }

    @Test
    void each_group_takes_its_own_copy_and_an_ordinary_subscription_one_more() throws Exception {
        try (var sut = MqttBroker.start();
                RawSession both = connected(sut.port(), "both");
                RawSession other = connected(sut.port(), "other");
                RawSession publisher = connected(sut.port(), "publisher")) {
            both.subscribe("$share/one/jobs/#", AT_MOST_ONCE);
            both.subscribe("jobs/#", AT_MOST_ONCE);
            other.subscribe("$share/two/jobs/#", AT_MOST_ONCE);

            // Act
            publisher.publish("jobs/1", "job", AT_LEAST_ONCE, 1);

            // Assert: the client with the shared subscription of one group and an ordinary
            // subscription gets a copy for each, and the other group a copy of its own.
            assertThat(receivedBeforePong(both)).containsExactly("job", "job");
            assertThat(receivedBeforePong(other)).containsExactly("job");
        }
    }

    @Test
    void a_shared_subscription_gets_no_retained_messages_as_it_is_made_or_joined()
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession first = connectedV5(sut.port(), "first");
                RawSession second = connectedV5(sut.port(), "second");
                RawSession publisher = connected(sut.port(), "publisher")) {
            publisher.publish(new Packet.Publish(
                    new Topic("jobs/retained"),
                    "retained".getBytes(UTF_8),
                    AT_LEAST_ONCE,
                    true,
                    false,
                    1,
                    EMPTY
            ));

            // Act: Retain Handling 0 would send the retained messages to an ordinary
            // subscription.
            first.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            second.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            publisher.publish("jobs/live", "live", AT_LEAST_ONCE, 2);

            // Assert
            var all = new ArrayList<>(receivedBeforePong(first));
            all.addAll(receivedBeforePong(second));
            assertThat(all).containsExactly("live");
        }
    }

    @Test
    void each_member_receives_at_the_qos_granted_to_it() throws Exception {
        try (var sut = MqttBroker.start();
                RawSession low = connected(sut.port(), "low");
                RawSession high = connected(sut.port(), "high");
                RawSession publisher = connected(sut.port(), "publisher")) {
            low.subscribe("$share/workers/jobs/#", AT_MOST_ONCE);
            high.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);

            // Act
            publisher.publish("jobs/1", "first", EXACTLY_ONCE, 1);
            publisher.publish("jobs/2", "second", EXACTLY_ONCE, 2);

            // Assert: the members take one each, in turn.
            assertThat(low.expect(Packet.Publish.class))
                    .isEqualTo(publication("jobs/1", "first", AT_MOST_ONCE, 0));
            Packet.Publish delivered = high.expect(Packet.Publish.class);
            assertThat(delivered.qos()).isEqualTo(AT_LEAST_ONCE);
            assertThat(new String(delivered.payload(), UTF_8)).isEqualTo("second");
        }
    }

    @Test
    void a_shared_copy_carries_the_subscription_identifier_of_its_subscription_alone()
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession member = connectedV5(sut.port(), "member");
                RawSession publisher = connected(sut.port(), "publisher")) {
            subscribeWithIdentifier(member, "$share/workers/jobs/#", 5, AT_MOST_ONCE);
            subscribeWithIdentifier(member, "jobs/+", 7, AT_MOST_ONCE);

            // Act
            publisher.publish("jobs/1", "job", AT_MOST_ONCE, 0);

            // Assert: one copy for each subscription, each with its own identifier.
            List<Property> identifiers = new ArrayList<>();
            identifiers.addAll(member.expect(Packet.Publish.class).properties().entries());
            identifiers.addAll(member.expect(Packet.Publish.class).properties().entries());
            assertThat(identifiers).containsExactlyInAnyOrder(
                    new Property.VariableByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER, 5),
                    new Property.VariableByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER, 7)
            );
        }
    }

    @Test
    void subscribing_again_keeps_one_membership_and_the_first_unsubscribe_leaves()
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession twice = connected(sut.port(), "twice");
                RawSession once = connected(sut.port(), "once");
                RawSession publisher = connected(sut.port(), "publisher")) {
            twice.subscribe("$share/workers/jobs/#", AT_MOST_ONCE);
            once.subscribe("$share/workers/jobs/#", AT_MOST_ONCE);
            twice.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);

            // Act
            for (int i = 0; i < 4; i++) {
                publisher.publish("jobs/" + i, Integer.toString(i), AT_LEAST_ONCE, i + 1);
            }
            List<String> toTwice = receivedBeforePong(twice);
            List<String> toOnce = receivedBeforePong(once);
            unsubscribe(twice, "$share/workers/jobs/#");
            publisher.publish("jobs/after", "after", AT_LEAST_ONCE, 5);

            // Assert: the second SUBSCRIBE changed the QoS alone, so the members took two each,
            // and once the first UNSUBSCRIBE had left, the other member took everything.
            assertThat(toTwice).hasSize(2);
            assertThat(toOnce).hasSize(2);
            assertThat(receivedBeforePong(twice)).isEmpty();
            assertThat(receivedBeforePong(once)).containsExactly("after");
        }
    }

    @Test
    void the_shared_subscription_leaves_the_index_with_its_last_member() throws Exception {
        try (BrokerServer sut = BrokerServer.builder().start(loopback());
                RawSession first = connected(sut.port(), "first");
                RawSession second = connected(sut.port(), "second")) {
            first.subscribe("$share/workers/jobs/#", AT_MOST_ONCE);
            second.subscribe("$share/workers/jobs/#", AT_MOST_ONCE);
            var topic = new Topic("jobs/1");

            // Act
            unsubscribe(first, "$share/workers/jobs/#");
            boolean afterFirst = sut.indexedFor(topic).contains("$share/workers/jobs/#");
            unsubscribe(second, "$share/workers/jobs/#");

            // Assert: the index holds the shared subscription alone, never its members.
            assertThat(afterFirst).isTrue();
            assertThat(sut.indexedFor(topic)).isEmpty();
        }
    }

    @Test
    void a_member_without_a_connection_is_passed_over_while_another_has_one() throws Exception {
        try (var sut = MqttBroker.start();
                RawSession present = connected(sut.port(), "present");
                RawSession publisher = connected(sut.port(), "publisher")) {
            try (RawSession away = connected(sut.port(), "away", false, false)) {
                away.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            }
            present.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);

            // Act: the publisher's PINGREQ comes back once the broker has routed what came before.
            publisher.publish("jobs/1", "first", AT_LEAST_ONCE, 1);
            publisher.publish("jobs/2", "second", AT_MOST_ONCE, 0);
            publisher.ping();

            // Assert: the publications that the member without a connection would have waited
            // for, or lost at QoS 0, went to the member with one.
            assertThat(receivedBeforePong(present)).containsExactly("first", "second");
            try (RawSession back = connected(sut.port(), "away", false, true)) {
                back.ping();
            }
        }
    }

    @Test
    void while_no_member_has_a_connection_an_acknowledged_publication_waits_with_one()
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession publisher = connected(sut.port(), "publisher")) {
            for (String clientId : List.of("first", "second")) {
                try (RawSession away = connected(sut.port(), clientId, false, false)) {
                    away.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
                }
            }

            // Act
            publisher.publish("jobs/1", "lost", AT_MOST_ONCE, 0);
            publisher.publish("jobs/2", "kept", AT_LEAST_ONCE, 1);
            publisher.publish("jobs/3", "also kept", AT_LEAST_ONCE, 2);

            // Assert: each QoS 1 publication waited for the next connection of one member's
            // session, which a QoS 0 one does not.
            try (RawSession first = connected(sut.port(), "first", false, true);
                    RawSession second = connected(sut.port(), "second", false, true)) {
                assertThat(receivedBeforePong(first)).containsExactly("kept");
                assertThat(receivedBeforePong(second)).containsExactly("also kept");
            }
        }
    }

    @Test
    void a_publication_reaches_a_member_that_joined_while_the_member_found_for_it_left()
            throws Exception {
        var holding = new AtomicBoolean();
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (BrokerServer sut = holdingBeforeLastStep(holding, held, release).start(loopback());
                RawSession joining = connected(sut.port(), "joining");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: the only member, whose session waits for its next connection, and a
            // publication that the shared subscription holds before it asks that member, with
            // the members as it read them.
            try (RawSession leaving = connected(sut.port(), "leaving", false, false)) {
                leaving.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            }
            awaitAway(sut, "leaving");
            holding.set(true);
            publisher.send(publication("jobs/1", "job", AT_LEAST_ONCE, 1));
            assertThat(held.await(5, SECONDS)).isTrue();

            // Act: another member joins, and then the first one comes back and leaves.
            joining.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            try (RawSession back = connected(sut.port(), "leaving", false, true)) {
                unsubscribe(back, "$share/workers/jobs/#");
                release.countDown();

                // Assert: the member that left declines the publication, and the one that
                // joined meanwhile gets it.
                assertThat(receivedUntil(joining, 1)).containsExactly("job");
                assertThat(receivedBeforePong(back)).isEmpty();
            }
        } finally {
            release.countDown();
        }
    }

    @Test
    void the_only_member_that_comes_back_while_a_publication_looks_for_one_receives_it()
            throws Exception {
        var holding = new AtomicBoolean();
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (BrokerServer sut = holdingBeforeLastStep(holding, held, release).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: the only member, whose session waits for its next connection, and a
            // publication that the shared subscription holds once the steps for members with a
            // connection have passed that member over.
            try (RawSession away = connected(sut.port(), "away", false, false)) {
                away.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            }
            awaitAway(sut, "away");
            holding.set(true);
            publisher.send(publication("jobs/1", "job", AT_LEAST_ONCE, 1));
            assertThat(held.await(5, SECONDS)).isTrue();

            // Act: the member comes back before the shared subscription goes on.
            try (RawSession back = connected(sut.port(), "away", false, true)) {
                release.countDown();

                // Assert
                assertThat(receivedUntil(back, 1)).containsExactly("job");
            }
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_publication_that_finds_the_members_changing_lets_other_clients_go_first_and_arrives()
            throws Exception {
        // The rounds in which the shared subscription looks for a member, each of which waits
        // for the test before its last step.
        var looking = new AtomicBoolean();
        var rounds = new AtomicInteger();
        var atLastStep = new Semaphore(0);
        var goOn = new Semaphore(0);
        // A PINGREQ that holds the second actor, the PINGREQs of another client that the broker
        // has read, and the rounds made by the time that it answered the first of those.
        var holdingNextPing = new AtomicBoolean();
        var held = new CountDownLatch(1);
        var released = new CountDownLatch(1);
        var readingPings = new AtomicBoolean();
        var read = new CountDownLatch(2);
        var answeringNextPing = new AtomicBoolean();
        var roundsWhenAnswered = new AtomicInteger(-1);
        var tooLarge = new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, 64);
        String job = "x".repeat(100);
        try (BrokerServer sut = BrokerServer.builder()
                        .actors(2)
                        .beforeProcess(packet -> {
                            if (packet instanceof Packet.PingReq && readingPings.get()) {
                                read.countDown();
                            }
                        })
                        .beforeHandle(packet -> {
                            if (!(packet instanceof Packet.PingReq)) {
                                return;
                            }
                            if (holdingNextPing.compareAndSet(true, false)) {
                                held.countDown();
                                awaitRelease(released);
                            } else if (answeringNextPing.compareAndSet(true, false)) {
                                roundsWhenAnswered.set(rounds.get());
                            }
                        })
                        .beforeLastShareStep(_ -> {
                            if (looking.get()) {
                                rounds.incrementAndGet();
                                atLastStep.release();
                                awaitPermit(goOn);
                            }
                        })
                        .start(loopback());
                RawSession steady = oneAtATime(sut.port(), "steady", tooLarge);
                RawSession changing = oneAtATime(sut.port(), "changing", tooLarge);
                RawSession taking = connected(sut.port(), "taking");
                RawSession holding = connected(sut.port(), "holding");
                RawSession other = connected(sut.port(), "other");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: two members that take no packet as large as the publication, one of
            // which leaves and joins again in turn as the test changes the members.
            steady.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            changing.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            looking.set(true);
            publisher.send(publication("jobs/1", job, AT_LEAST_ONCE, 1));

            // Act: the members change in every round of two looks for a member. As the last
            // round of the first look waits, a PINGREQ holds the second actor, and another
            // client's PINGREQ waits for an actor.
            for (int round = 1; round <= 2 * SharedSubscription.ROUNDS; round++) {
                assertThat(atLastStep.tryAcquire(5, SECONDS)).isTrue();
                if (round % 2 == 1) {
                    unsubscribe(changing, "$share/workers/jobs/#");
                } else {
                    changing.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
                }
                if (round == SharedSubscription.ROUNDS) {
                    holdingNextPing.set(true);
                    holding.send(new Packet.PingReq());
                    assertThat(held.await(5, SECONDS)).isTrue();
                    answeringNextPing.set(true);
                    readingPings.set(true);
                    // The broker reads a connection's packets in order, so it has handed the
                    // first PINGREQ on to the session once it reads the second.
                    other.send(new Packet.PingReq());
                    other.send(new Packet.PingReq());
                    assertThat(read.await(5, SECONDS)).isTrue();
                    readingPings.set(false);
                    goOn.release();
                    other.expect(Packet.PingResp.class);
                    other.expect(Packet.PingResp.class);
                    released.countDown();
                    holding.expect(Packet.PingResp.class);
                } else {
                    goOn.release();
                }
            }
            assertThat(atLastStep.tryAcquire(5, SECONDS)).isTrue();
            taking.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            looking.set(false);
            goOn.release();

            // Assert: the other client went first after the rounds of the first look, and the
            // member that joined at last took the publication.
            assertThat(roundsWhenAnswered.get()).isEqualTo(SharedSubscription.ROUNDS);
            assertThat(receivedUntil(taking, 1)).containsExactly(job);
            assertThat(receivedBeforePong(steady)).isEmpty();
            assertThat(receivedBeforePong(changing)).isEmpty();
        } finally {
            looking.set(false);
            goOn.release(4 * SharedSubscription.ROUNDS);
            released.countDown();
        }
    }

    @Test
    void a_publication_goes_to_a_member_that_keeps_up_rather_than_one_whose_connection_ended()
            throws Exception {
        var timer = new BrokerDeadlineTest.ManualTimer();
        var holding = new AtomicBoolean();
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (BrokerServer sut = BrokerServer.builder()
                        .actors(4)
                        .scheduler(timer)
                        .beforeHandle(packet -> {
                            if (packet instanceof Packet.PingReq
                                    && holding.compareAndSet(true, false)) {
                                held.countDown();
                                awaitRelease(release);
                            }
                        })
                        .start(loopback());
                RawSession ending = RawSession.open(sut.port(), MqttVersion.V5);
                RawSession keeping = connected(sut.port(), "keeping");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: a member whose session outlives its connection by a minute, which joined
            // first and so is asked first, and a member that keeps up. The first member's keep-
            // alive runs out while its actor is busy, so that its connection has ended before
            // its session learns of it.
            ending.send(RawSession.connectV5("ending", true, 1, 60));
            ending.expect(Packet.ConnAck.class);
            ending.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            keeping.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            holding.set(true);
            ending.send(new Packet.PingReq());
            assertThat(held.await(5, SECONDS)).isTrue();
            timer.clock.advance(Duration.ofMillis(1500));

            // Act
            publisher.publish("jobs/1", "job", AT_LEAST_ONCE, 1);

            // Assert: the member that keeps up took the publication.
            assertThat(receivedBeforePong(keeping)).containsExactly("job");
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_member_that_falls_behind_takes_no_more_while_another_keeps_up() throws Exception {
        try (var sut = MqttBroker.start();
                RawSession slow = oneAtATime(sut.port(), "slow");
                RawSession fast = connected(sut.port(), "fast");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: a member that takes one unanswered publication at a time and answers none.
            slow.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            fast.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);

            // Act
            for (int i = 0; i < 6; i++) {
                publisher.publish("jobs/" + i, Integer.toString(i), AT_LEAST_ONCE, i + 1);
            }

            // Assert: the slow member took the first in turn, and the third, which waits behind
            // the first. From then on, something waits for it, so the fast member took the rest.
            assertThat(new String(slow.expect(Packet.Publish.class).payload(), UTF_8))
                    .isEqualTo("0");
            slow.ping();
            assertThat(receivedBeforePong(fast)).containsExactly("1", "3", "4", "5");
        }
    }

    @Test
    void a_qos_2_delivery_that_went_out_continues_with_its_member_after_it_reconnects()
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession publisher = connected(sut.port(), "publisher")) {
            int packetId;
            try (RawSession other = connected(sut.port(), "other")) {
                // Arrange: the first member in turn gets the publication and does not answer it.
                try (RawSession first = connected(sut.port(), "first", false, false)) {
                    first.subscribe("$share/workers/jobs/#", EXACTLY_ONCE);
                    other.subscribe("$share/workers/jobs/#", EXACTLY_ONCE);
                    publisher.publish("jobs/1", "once", EXACTLY_ONCE, 1);
                    packetId = first.expect(Packet.Publish.class).packetId();
                }

                // Act
                try (RawSession back = connected(sut.port(), "first", false, true)) {
                    // Assert: the member gets it again [MQTT-4.8.2-4], and the other member
                    // does not.
                    assertThat(back.expect(Packet.Publish.class))
                            .isEqualTo(new Packet.Publish(
                                    new Topic("jobs/1"),
                                    "once".getBytes(UTF_8),
                                    EXACTLY_ONCE,
                                    false,
                                    true,
                                    packetId,
                                    EMPTY
                            ));
                    other.ping();
                }
            }
        }
    }

    @Test
    void a_delivery_that_its_member_refuses_goes_to_no_other_member() throws Exception {
        try (var sut = MqttBroker.start();
                RawSession first = connectedV5(sut.port(), "first");
                RawSession other = connectedV5(sut.port(), "other");
                RawSession publisher = connected(sut.port(), "publisher")) {
            first.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            other.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            publisher.publish("jobs/1", "refused", AT_LEAST_ONCE, 1);
            Packet.Publish delivered = first.expect(Packet.Publish.class);

            // Act
            first.send(new Packet.PubAck(delivered.packetId(), UNSPECIFIED_ERROR, EMPTY));

            // Assert [MQTT-4.8.2-6]
            first.ping();
            other.ping();
        }
    }

    @Test
    void the_unacknowledged_deliveries_of_a_member_whose_session_ends_go_to_another_member()
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession other = connected(sut.port(), "other");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: a member that takes the first publication, which it does not answer, and
            // the third, which waits behind it.
            try (RawSession slow = oneAtATime(sut.port(), "slow")) {
                slow.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
                other.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
                for (int i = 0; i < 4; i++) {
                    publisher.publish("jobs/" + i, Integer.toString(i), AT_LEAST_ONCE, i + 1);
                }
                slow.expect(Packet.Publish.class);
                assertThat(receivedBeforePong(other)).containsExactly("1", "3");
            }

            // Act: the session of the member ends with its connection.

            // Assert: the one that went out, which its client may have taken, and the one that
            // waited both go to the other member, in their order.
            assertThat(receivedUntil(other, 2)).containsExactly("0", "2");
        }
    }

    @Test
    void a_qos_2_delivery_that_went_out_stays_with_its_member_as_its_session_ends()
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession other = connected(sut.port(), "other");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: as above, at QoS 2.
            try (RawSession slow = oneAtATime(sut.port(), "slow")) {
                slow.subscribe("$share/workers/jobs/#", EXACTLY_ONCE);
                other.subscribe("$share/workers/jobs/#", EXACTLY_ONCE);
                for (int i = 0; i < 4; i++) {
                    publisher.publish("jobs/" + i, Integer.toString(i), EXACTLY_ONCE, i + 1);
                }
                slow.expect(Packet.Publish.class);
                assertThat(receivedBeforePong(other)).containsExactly("1", "3");
            }

            // Act: the session of the member ends with its connection.

            // Assert: only the one that waited goes to the other member [MQTT-4.8.2-5].
            assertThat(receivedUntil(other, 1)).containsExactly("2");
            assertThat(receivedBeforePong(other)).isEmpty();
        }
    }

    @Test
    void the_deliveries_of_a_member_whose_session_outlives_its_connection_wait_until_it_ends()
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession present = connected(sut.port(), "present");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: a member whose session outlives its connection by one second takes the
            // publication and does not answer it.
            try (RawSession away = connectedV5(sut.port(), "away", true, 1, false)) {
                away.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
                present.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
                publisher.publish("jobs/1", "waits", AT_LEAST_ONCE, 1);
                away.expect(Packet.Publish.class);
            }

            // Act & Assert: while the session lasts, the publication waits with it for its
            // client to come back; once the session expires, it goes to the other member.
            assertThat(receivedBeforePong(present)).isEmpty();
            assertThat(receivedUntil(present, 1)).containsExactly("waits");
        }
    }

    @Test
    void a_delivery_that_goes_to_another_member_takes_that_members_qos_and_identifier()
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession second = connectedV5(sut.port(), "second");
                RawSession publisher = connected(sut.port(), "publisher")) {
            try (RawSession first = connectedV5(sut.port(), "first")) {
                subscribeWithIdentifier(first, "$share/workers/jobs/#", 5, AT_LEAST_ONCE);
                subscribeWithIdentifier(second, "$share/workers/jobs/#", 9, AT_MOST_ONCE);
                publisher.publish("jobs/1", "job", AT_LEAST_ONCE, 1);
                first.expect(Packet.Publish.class);
            }

            // Act: the session of the first member ends with its connection.

            // Assert
            Packet.Publish delivered = second.expect(Packet.Publish.class);
            assertThat(delivered.qos()).isEqualTo(AT_MOST_ONCE);
            assertThat(delivered.properties().entries()).containsExactly(
                    new Property.VariableByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER, 9)
            );
        }
    }

    @Test
    void a_message_that_expired_with_its_member_goes_to_no_other_member() throws Exception {
        var timer = new BrokerDeadlineTest.ManualTimer();
        String unanswered = "1".repeat(100);
        String waiting = "2".repeat(100);
        try (BrokerServer sut = BrokerServer.builder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .scheduler(timer)
                        .maxQueued(1)
                        .start(loopback());
                RawSession ending = oneAtATime(
                        sut.port(),
                        "ending",
                        new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, 64)
                );
                RawSession other = oneAtATime(sut.port(), "other");
                RawSession publisher = connectedV5(sut.port(), "publisher")) {
            // Arrange: a member that takes no packet over 64 bytes holds a message with a
            // lifetime of a second unanswered. The other member, which takes one unanswered
            // message at a time, has the next one unanswered and the one after waiting, as many
            // as its queue takes. Then the lifetime passes.
            ending.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            other.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            publisher.publish(expiring("jobs/0", "expires", 1, 1));
            ending.expect(Packet.Publish.class);
            publisher.publish("jobs/1", unanswered, AT_LEAST_ONCE, 2);
            publisher.publish("jobs/2", waiting, AT_LEAST_ONCE, 3);
            Packet.Publish first = other.expect(Packet.Publish.class);
            timer.clock.advance(Duration.ofSeconds(2));

            // Act: a new session of the first member's client takes the place of its session,
            // whose unanswered message goes on to another member before the CONNACK.
            try (RawSession _ = connectedV5(sut.port(), "ending")) {
                other.send(new Packet.PubAck(first.packetId(), SUCCESS, EMPTY));

                // Assert: the message that expired went to no member, so it pushed none out of
                // the other member's full queue.
                assertThat(receivedUntil(other, 1)).containsExactly(waiting);
            }
        }
    }

    @Test
    void a_message_whose_lifetime_passes_while_it_looks_for_a_member_pushes_out_no_waiting_one()
            throws Exception {
        var timer = new BrokerDeadlineTest.ManualTimer();
        var passing = new AtomicBoolean();
        try (BrokerServer sut = BrokerServer.builder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .scheduler(timer)
                        .maxQueued(1)
                        .beforeLastShareStep(_ -> {
                            if (passing.compareAndSet(true, false)) {
                                timer.clock.advance(Duration.ofSeconds(2));
                            }
                        })
                        .start(loopback());
                RawSession publisher = connectedV5(sut.port(), "publisher")) {
            // Arrange: a member whose session waits for its next connection with a message, as
            // many as its queue takes, and a member that holds a message with a lifetime of a
            // second unanswered.
            try (RawSession away = connected(sut.port(), "away", false, false)) {
                away.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            }
            awaitAway(sut, "away");
            publisher.publish("jobs/0", "waiting", AT_LEAST_ONCE, 1);
            try (RawSession ending = oneAtATime(sut.port(), "ending")) {
                ending.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
                publisher.publish(expiring("jobs/1", "expires", 2, 1));
                ending.expect(Packet.Publish.class);
                passing.set(true);

                // Act: a new session of the second member's client takes the place of its
                // session, whose unanswered message goes on to the other member, and the
                // lifetime passes while the shared subscription looks for one.
                try (RawSession _ = connectedV5(sut.port(), "ending");
                        RawSession back = connected(sut.port(), "away", false, true)) {
                    // Assert: the message that expired on its way pushed out none that waited.
                    assertThat(receivedUntil(back, 1)).containsExactly("waiting");
                }
            }
        }
    }

    @Test
    void a_member_whose_session_has_expired_takes_none_from_another_before_its_timer_comes()
            throws Exception {
        var timer = new BrokerDeadlineTest.ManualTimer();
        try (BrokerServer sut = BrokerServer.builder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .scheduler(timer)
                        .maxQueued(1)
                        .start(loopback());
                RawSession publisher = connectedV5(sut.port(), "publisher")) {
            // Arrange: a member whose session outlives its connection by a second, on a timer
            // that comes ten seconds late, waits with a message, as many as its queue takes. A
            // member that holds a message unanswered, and one whose session lasts, join, and
            // then the first session's second passes.
            timer.lateness = Duration.ofSeconds(10);
            try (RawSession expiring = connectedV5(sut.port(), "expiring", true, 1, false)) {
                expiring.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            }
            awaitAway(sut, "expiring");
            publisher.publish("jobs/0", "from expiring", AT_LEAST_ONCE, 1);
            try (RawSession ending = oneAtATime(sut.port(), "ending")) {
                ending.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
                publisher.publish("jobs/1", "from ending", AT_LEAST_ONCE, 2);
                ending.expect(Packet.Publish.class);
                try (RawSession lasting = connected(sut.port(), "lasting", false, false)) {
                    lasting.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
                }
                awaitAway(sut, "lasting");
                timer.clock.advance(Duration.ofSeconds(2));

                // Act: a new session of the second member's client takes the place of its
                // session, whose unanswered message goes on to another member.
                try (RawSession _ = connectedV5(sut.port(), "ending");
                        RawSession back = connected(sut.port(), "lasting", false, true)) {
                    // Assert: the message went to the member whose session lasts, and the one
                    // that waited with the expired session follows it as the timer comes.
                    assertThat(receivedUntil(back, 1)).containsExactly("from ending");
                    timer.clock.advance(Duration.ofSeconds(10));
                    assertThat(receivedUntil(back, 1)).containsExactly("from expiring");
                }
            }
        }
    }

    @Test
    void deliveries_that_go_to_another_member_keep_their_room_from_other_sessions_meanwhile()
            throws Exception {
        String payload = "x".repeat(10_000);
        var shared = new SharedSubscription("$share/workers/jobs/#", _ -> { });
        long room = deliverySize(publication("jobs/0", payload, AT_LEAST_ONCE, 1)) + shared.bytes;
        var holding = new AtomicBoolean();
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var timer = new BrokerDeadlineTest.ManualTimer();
        try (BrokerServer sut = holdingBeforeLastStep(holding, held, release)
                        .connectTimeout(Duration.ofSeconds(5))
                        .scheduler(timer)
                        .maxDeliveryMemory(3 * room + room / 2)
                        .deliveryMemoryUnit(0)
                        .start(loopback());
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: a broker whose waiting deliveries have room for three of a shared
            // subscription and half of one more, which all three take, waiting with a member
            // whose session outlives its connection by a second. Another member, and a session
            // with an ordinary subscription, wait for their next connections.
            try (RawSession ending = connectedV5(sut.port(), "ending", true, 1, false)) {
                ending.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            }
            awaitAway(sut, "ending");
            for (int i = 0; i < 3; i++) {
                publisher.publish("jobs/" + i, payload, AT_LEAST_ONCE, i + 1);
            }
            try (RawSession away = connected(sut.port(), "away", false, false)) {
                away.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            }
            try (RawSession other = connected(sut.port(), "other", false, false)) {
                other.subscribe("other/#", AT_LEAST_ONCE);
            }
            awaitAway(sut, "away");
            awaitAway(sut, "other");

            // Act: the first member's session expires, and while its first delivery goes on to
            // the other member, publications for the other session come.
            holding.set(true);
            timer.clock.advance(Duration.ofSeconds(1));
            assertThat(held.await(5, SECONDS)).isTrue();
            for (int i = 0; i < 3; i++) {
                publisher.publish("other/" + i, payload, AT_LEAST_ONCE, i + 4);
            }
            release.countDown();

            // Assert: the deliveries that go on kept their room, which the other session's
            // publications found taken, and all three wait with the other member.
            try (RawSession back = connected(sut.port(), "away", false, true)) {
                assertThat(receivedUntil(back, 3)).containsExactly(payload, payload, payload);
            }
        } finally {
            release.countDown();
        }
    }

    // Whether the connection of the member that holds the delivery unanswered ends before its
    // session does, so that the session, which outlives the connection, takes room for that
    // delivery whatever the memory has left, as it cannot go without it.
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_delivery_that_a_member_leaves_unanswered_goes_on_within_the_memory_limit(
            boolean connectionEndsFirst
    ) throws Exception {
        String payload = "x".repeat(1_000);
        var shared = new SharedSubscription("$share/workers/jobs/#", _ -> { });
        long room = deliverySize(publication("jobs/0", payload, AT_LEAST_ONCE, 1)) + shared.bytes;
        long control = deliverySize(publication("control/0", "control", AT_LEAST_ONCE, 1));
        var expiry = new Property.FourByteValue(
                PropertyId.SESSION_EXPIRY_INTERVAL,
                connectionEndsFirst ? 60 : 0
        );
        // One actor thread, so that the room that it keeps to itself is the room that the
        // test gives back to it.
        try (BrokerServer sut = BrokerServer.builder()
                        .actors(1)
                        .maxDeliveryMemory(room + room / 2 + control)
                        .start(loopback());
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: a broker whose waiting deliveries have room for one delivery of a shared
            // subscription and half of another, besides one for an ordinary subscription. A
            // member and a session with that ordinary subscription wait for their next
            // connections, the member with a delivery, and a delivery that waited and went out
            // has left its room with the actor thread. Another member holds a delivery
            // unanswered.
            try (RawSession away = connected(sut.port(), "away", false, false)) {
                away.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            }
            try (RawSession other = connected(sut.port(), "other", false, false)) {
                other.subscribe("control/#", AT_LEAST_ONCE);
            }
            try (RawSession filling = connected(sut.port(), "filling", false, false)) {
                filling.subscribe("filling/#", AT_LEAST_ONCE);
            }
            awaitAway(sut, "away");
            awaitAway(sut, "other");
            awaitAway(sut, "filling");
            publisher.publish("jobs/0", payload, AT_LEAST_ONCE, 1);
            publisher.publish("filling/0", "y".repeat(400), AT_LEAST_ONCE, 2);
            try (RawSession back = connected(sut.port(), "filling", false, true)) {
                assertThat(receivedBeforePong(back)).hasSize(1);
            }
            awaitAway(sut, "filling");
            try (RawSession ending = oneAtATime(sut.port(), "ending", expiry)) {
                ending.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
                publisher.publish("jobs/1", payload, AT_LEAST_ONCE, 3);
                ending.expect(Packet.Publish.class);
                if (connectionEndsFirst) {
                    ending.disconnect();
                    awaitAway(sut, "ending");
                }

                // Act: a new session of the member's client takes the place of its session,
                // whose unanswered delivery goes on to the member that waits, and then a
                // publication comes for the ordinary subscription.
                try (RawSession _ = connectedV5(sut.port(), "ending")) {
                    publisher.publish("control/0", "control", AT_LEAST_ONCE, 4);
                }
            }

            // Assert: the member that waits took the delivery only as the memory's limit allows,
            // so it holds one of the two, and the publication for the ordinary subscription
            // found its room.
            try (RawSession back = connected(sut.port(), "other", false, true)) {
                assertThat(receivedBeforePong(back)).containsExactly("control");
            }
            try (RawSession back = connected(sut.port(), "away", false, true)) {
                assertThat(receivedBeforePong(back)).containsExactly(payload);
            }
        }
    }

    @Test
    void a_message_that_waits_for_a_member_of_two_shared_subscriptions_goes_on_to_both()
            throws Exception {
        String payload = "x".repeat(1_000);
        Packet.Publish message = publication("jobs/0", payload, AT_LEAST_ONCE, 1);
        var shared = new SharedSubscription("$share/first/jobs/#", _ -> { });
        // What the message holds, which its deliveries share, and what a delivery of either
        // shared subscription holds beside it.
        long publicationRoom = PublicationMemory.sizeOf(message);
        long deliveryRoom = MemoryBudget.DELIVERY_BYTES + shared.bytes;
        try (BrokerServer sut = BrokerServer.builder()
                        .maxDeliveryMemory(publicationRoom * 3 / 2 + 2 * deliveryRoom)
                        .deliveryMemoryUnit(0)
                        .start(loopback());
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: a broker whose waiting deliveries have room for the message and two
            // deliveries of it, with half the message to spare. A member of two shared
            // subscriptions waits for its next connection with a delivery of the message for
            // each, and each shared subscription has another member that waits as well.
            try (RawSession ending = connected(sut.port(), "ending", false, false)) {
                ending.subscribe("$share/first/jobs/#", AT_LEAST_ONCE);
                ending.subscribe("$share/other/jobs/#", AT_LEAST_ONCE);
            }
            awaitAway(sut, "ending");
            publisher.publish(message);
            try (RawSession first = connected(sut.port(), "first", false, false)) {
                first.subscribe("$share/first/jobs/#", AT_LEAST_ONCE);
            }
            try (RawSession other = connected(sut.port(), "other", false, false)) {
                other.subscribe("$share/other/jobs/#", AT_LEAST_ONCE);
            }
            awaitAway(sut, "first");
            awaitAway(sut, "other");

            // Act: a new session of the first member's client takes the place of its session.
            try (RawSession _ = connected(sut.port(), "ending", true, false)) {
                // Assert: both deliveries went on, as the message still counted once.
                try (RawSession back = connected(sut.port(), "first", false, true)) {
                    assertThat(receivedBeforePong(back)).containsExactly(payload);
                }
                try (RawSession back = connected(sut.port(), "other", false, true)) {
                    assertThat(receivedBeforePong(back)).containsExactly(payload);
                }
            }
        }
    }

    // Whether an ordinary subscriber of the message leaves it unanswered as its connection ends,
    // so that its session holds the message with room taken whatever the memory had left, or
    // answers it at once.
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_member_does_not_hold_a_message_on_room_that_another_session_took_past_the_limit(
            boolean leftUnanswered
    ) throws Exception {
        String payload = "x".repeat(2_000);
        Packet.Publish message = publication("jobs/1", payload, AT_LEAST_ONCE, 1);
        var shared = new SharedSubscription("$share/workers/jobs/#", _ -> { });
        // What a delivery of the shared subscription holds beside the message, and half of the
        // message: the room that the actor thread keeps to itself below.
        long threadRoom = MemoryBudget.DELIVERY_BYTES
                + shared.bytes
                + PublicationMemory.sizeOf(message) / 2;
        long emptyFilling = deliverySize(publication("filling/0", "", AT_LEAST_ONCE, 2));
        String filler = "y".repeat((int) (threadRoom - emptyFilling));
        // One actor thread, so that the room that it keeps to itself is the room that the
        // test gives back to it.
        try (BrokerServer sut = BrokerServer.builder()
                        .actors(1)
                        .maxDeliveryMemory(threadRoom)
                        .start(loopback());
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: a delivery took up the memory as it waited and gave the room back to the
            // actor thread as it went out. A member waits for its next connection. An ordinary
            // subscriber and another member, whose session ends with its connection, take the
            // message.
            try (RawSession away = connected(sut.port(), "away", false, false)) {
                away.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            }
            try (RawSession filling = connected(sut.port(), "filling", false, false)) {
                filling.subscribe("filling/#", AT_LEAST_ONCE);
            }
            awaitAway(sut, "away");
            awaitAway(sut, "filling");
            publisher.publish("filling/0", filler, AT_LEAST_ONCE, 2);
            try (RawSession back = connected(sut.port(), "filling", false, true)) {
                assertThat(receivedBeforePong(back)).containsExactly(filler);
            }
            try (RawSession ordinary = connected(sut.port(), "ordinary", false, false);
                    RawSession ending = connected(sut.port(), "ending")) {
                ordinary.subscribe("jobs/#", AT_LEAST_ONCE);
                ending.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
                publisher.publish(message);
                ending.expect(Packet.Publish.class);
                if (leftUnanswered) {
                    ordinary.expect(Packet.Publish.class);
                    ordinary.disconnect();
                    awaitAway(sut, "ordinary");
                } else {
                    assertThat(receivedBeforePong(ordinary)).containsExactly(payload);
                }

                // Act: a new session of the member's client takes the place of its session,
                // whose unanswered delivery goes on to the member that waits, and then the
                // ordinary subscriber answers the message if it has not.
                connected(sut.port(), "ending").close();
            }
            if (leftUnanswered) {
                try (RawSession back = connected(sut.port(), "ordinary", false, true)) {
                    assertThat(receivedBeforePong(back)).containsExactly(payload);
                }
            }

            // Assert: the member that waits found room for its delivery but not for the
            // message, whatever room the ordinary subscriber's session took for it.
            try (RawSession back = connected(sut.port(), "away", false, true)) {
                assertThat(receivedBeforePong(back)).isEmpty();
            }
        }
    }

    @Test
    void a_delivery_that_goes_on_pushes_out_older_ones_though_one_of_them_holds_its_message()
            throws Exception {
        String job = "j".repeat(1_000);
        String other = "o".repeat(500);
        // Room for the two deliveries that the member below holds, and no more.
        long memory = deliverySize(publication("jobs/1", job, AT_LEAST_ONCE, 1))
                + deliverySize(publication("other/1", other, AT_LEAST_ONCE, 2));
        try (BrokerServer sut = BrokerServer.builder()
                        .maxDeliveryMemory(memory)
                        .deliveryMemoryUnit(0)
                        .start(loopback());
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: a member waits for its next connection with a delivery of a message for
            // an ordinary subscription and, behind it, one of another message, which fill the
            // memory. Another member, whose session ends with its connection, takes the message
            // for the shared subscription, which the member that waits made with a Subscription
            // Identifier.
            try (RawSession away = connectedV5(sut.port(), "away", true, 60, false)) {
                away.subscribe("jobs/#", AT_LEAST_ONCE);
                away.subscribe("other/#", AT_LEAST_ONCE);
                subscribeWithIdentifier(away, "$share/workers/jobs/#", 7, AT_LEAST_ONCE);
            }
            awaitAway(sut, "away");
            try (RawSession ending = connected(sut.port(), "ending")) {
                ending.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
                publisher.publish("jobs/1", job, AT_LEAST_ONCE, 1);
                ending.expect(Packet.Publish.class);
                publisher.publish("other/1", other, AT_LEAST_ONCE, 2);

                // Act: a new session of the member's client takes the place of its session,
                // whose unanswered delivery goes on to the member that waits.
                connected(sut.port(), "ending").close();
            }

            // Assert: the delivery that went on took the place of both older deliveries, as
            // the first of them gave back what it held itself alone: the message stays with the
            // delivery that went on.
            try (RawSession back = connectedV5(sut.port(), "away", false, 60, true)) {
                back.send(new Packet.PingReq());
                Packet.Publish delivered = back.expect(Packet.Publish.class);
                Packet next = back.expect(Packet.class);
                assertThat(delivered.properties().entries()).containsExactly(
                        new Property.VariableByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER, 7)
                );
                assertThat(new String(delivered.payload(), UTF_8)).isEqualTo(job);
                assertThat(next).isInstanceOf(Packet.PingResp.class);
            }
        }
    }

    @Test
    void a_failure_while_a_delivery_goes_on_loses_that_delivery_alone() throws Exception {
        var failing = new AtomicBoolean();
        try (BrokerServer sut = BrokerServer.builder()
                        .beforeLastShareStep(_ -> {
                            if (failing.compareAndSet(true, false)) {
                                throw new IllegalStateException("Injected failure");
                            }
                        })
                        .start(loopback());
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: a member waits for its next connection with two messages of a shared
            // subscription, which has another member that waits as well.
            try (RawSession ending = connected(sut.port(), "ending", false, false)) {
                ending.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            }
            awaitAway(sut, "ending");
            publisher.publish("jobs/1", "first", AT_LEAST_ONCE, 1);
            publisher.publish("jobs/2", "second", AT_LEAST_ONCE, 2);
            try (RawSession away = connected(sut.port(), "away", false, false)) {
                away.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
            }
            awaitAway(sut, "away");

            // Act: a new session of the first member's client takes the place of its session,
            // and handing the first message on to the other member fails.
            failing.set(true);
            try (RawSession _ = connected(sut.port(), "ending", true, false)) {
                // Assert: the new session started, and the second message went on. Closing the
                // broker then finds every room in its memory given back.
                try (RawSession back = connected(sut.port(), "away", false, true)) {
                    assertThat(receivedBeforePong(back)).containsExactly("second");
                }
            }
        }
    }

    @Test
    void a_shared_subscription_that_ended_hands_its_deliveries_to_no_later_one_of_its_name()
            throws Exception {
        try (BrokerServer sut = BrokerServer.builder().start(loopback());
                RawSession later = connected(sut.port(), "later");
                RawSession publisher = connected(sut.port(), "publisher")) {
            var topic = new Topic("jobs/1");
            try (RawSession only = oneAtATime(sut.port(), "only")) {
                only.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);
                publisher.publish("jobs/1", "first", AT_LEAST_ONCE, 1);
                publisher.publish("jobs/2", "second", AT_LEAST_ONCE, 2);
                only.expect(Packet.Publish.class);
            }
            // Arrange: the last member's session has ended, and with it the shared subscription.
            await().atMost(Duration.ofSeconds(5)).until(() -> sut.indexedFor(topic).isEmpty());

            // Act: a new shared subscription of the same name.
            later.subscribe("$share/workers/jobs/#", AT_LEAST_ONCE);

            // Assert: the messages of the one that ended were lost with it.
            assertThat(receivedBeforePong(later)).isEmpty();
        }
    }

    @Test
    void a_malformed_shared_filter_of_mqtt_3_1_1_is_refused_while_the_others_subscribe()
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession subscriber = connected(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Act
            subscriber.send(new Packet.Subscribe(
                    1,
                    List.of(
                            subscription("$share/team/sensors/#", AT_MOST_ONCE),
                            subscription("$share/team", AT_MOST_ONCE),
                            subscription("sensors/#", AT_MOST_ONCE)
                    ),
                    EMPTY
            ));

            // Assert: MQTT 3.1.1 refuses a filter with 0x80. The client gets a copy for its
            // shared subscription and one for its ordinary subscription, while a publication to
            // a topic that starts with `$share/` reaches neither.
            assertThat(subscriber.expect(Packet.SubAck.class)).isEqualTo(new Packet.SubAck(
                    1,
                    List.of(SUCCESS, UNSPECIFIED_ERROR, SUCCESS),
                    EMPTY
            ));
            publisher.publish("sensors/temperature", "matched", AT_LEAST_ONCE, 1);
            publisher.publish("$share/team/sensors/temperature", "unmatched", AT_LEAST_ONCE, 2);
            assertThat(receivedBeforePong(subscriber)).containsExactly("matched", "matched");
        }
    }

    // A member that connects with `version` as `clientId`, with a session that ends with its
    // connection.
    private static RawSession member(int port, String clientId, MqttVersion version)
            throws Exception {
        return switch (version) {
            case V3_1_1 -> connected(port, clientId);
            case V5 -> connectedV5(port, clientId);
        };
    }

    // The payloads of the publications that `member` receives before the answer to a PINGREQ
    // that it sends now, which it acknowledges as it receives them. The answer goes out behind
    // what the broker queued for the member before it took the PINGREQ.
    private static List<String> receivedBeforePong(RawSession member) throws Exception {
        member.send(new Packet.PingReq());
        var payloads = new ArrayList<String>();
        while (true) {
            Packet packet = member.expect(Packet.class);
            switch (packet) {
                case Packet.PingResp _ -> {
                    return payloads;
                }
                case Packet.Publish delivered -> {
                    payloads.add(new String(delivered.payload(), UTF_8));
                    int id = delivered.packetId();
                    switch (delivered.qos()) {
                        case AT_MOST_ONCE -> { }
                        case AT_LEAST_ONCE -> member.send(new Packet.PubAck(id, SUCCESS, EMPTY));
                        case EXACTLY_ONCE -> member.send(new Packet.PubRec(id, SUCCESS, EMPTY));
                    }
                }
                case Packet.PubRel release -> member.send(
                        new Packet.PubComp(release.packetId(), SUCCESS, EMPTY)
                );
                default -> throw new AssertionError("Unexpected " + packet);
            }
        }
    }

    // The payloads that `member` receives, until it has `count` of them, as the broker hands
    // them to it in its own time.
    private static List<String> receivedUntil(RawSession member, int count) {
        var payloads = new ArrayList<String>();
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            payloads.addAll(receivedBeforePong(member));
            return payloads.size() >= count;
        });
        return payloads;
    }

    // An MQTT 5.0 client `clientId` that takes one unanswered publication at a time, with a
    // session that ends with its connection, and connects with `more` properties besides.
    private static RawSession oneAtATime(int port, String clientId, Property... more)
            throws Exception {
        var properties = new ArrayList<Property>();
        properties.add(new Property.TwoByteValue(PropertyId.RECEIVE_MAXIMUM, 1));
        properties.addAll(List.of(more));
        RawSession session = RawSession.open(port, MqttVersion.V5);
        session.send(new Packet.Connect(
                clientId,
                MqttVersion.V5,
                true,
                0,
                null,
                null,
                null,
                new Properties(properties)
        ));
        session.expect(Packet.ConnAck.class);
        return session;
    }

    // The settings of a broker with four actors whose shared subscriptions wait for `release`
    // before the last step in which they look for a member to take a QoS 1 or QoS 2
    // publication, the first time that one of them comes to it once `holding` is set, after
    // counting `held` down.
    private static BrokerServer.Builder holdingBeforeLastStep(
            AtomicBoolean holding,
            CountDownLatch held,
            CountDownLatch release
    ) {
        return BrokerServer.builder()
                .actors(4)
                .beforeLastShareStep(_ -> {
                    if (holding.compareAndSet(true, false)) {
                        held.countDown();
                        awaitRelease(release);
                    }
                });
    }

    // Takes one of `permits` for an actor that waits for the test, which gives it within five
    // seconds.
    private static void awaitPermit(Semaphore permits) {
        try {
            assertThat(permits.tryAcquire(5, SECONDS)).isTrue();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }

    // Waits until the session of `clientId`, whose connection has closed, waits without one.
    private static void awaitAway(BrokerServer broker, String clientId) {
        await().atMost(Duration.ofSeconds(5)).until(() -> broker.clients().stream()
                .noneMatch(client -> client.connection().clientId().equals(clientId)));
    }

    // A QoS 1 publication of `payload` to `topic` with a lifetime of `lifetime` seconds.
    private static Packet.Publish expiring(
            String topic,
            String payload,
            int packetId,
            long lifetime
    ) {
        return new Packet.Publish(
                new Topic(topic),
                payload.getBytes(UTF_8),
                AT_LEAST_ONCE,
                false,
                false,
                packetId,
                new Properties(List.of(
                        new Property.FourByteValue(PropertyId.MESSAGE_EXPIRY_INTERVAL, lifetime)
                ))
        );
    }

    private static void subscribeWithIdentifier(
            RawSession member,
            String filter,
            int identifier,
            Qos qos
    ) throws Exception {
        member.send(new Packet.Subscribe(
                identifier,
                List.of(subscription(filter, qos)),
                new Properties(List.of(new Property.VariableByteValue(
                        PropertyId.SUBSCRIPTION_IDENTIFIER,
                        identifier
                )))
        ));
        ReasonCode granted = switch (qos) {
            case AT_MOST_ONCE -> SUCCESS;
            case AT_LEAST_ONCE -> ReasonCode.GRANTED_QOS_1;
            case EXACTLY_ONCE -> ReasonCode.GRANTED_QOS_2;
        };
        assertThat(member.expect(Packet.SubAck.class).reasonCodes()).containsExactly(granted);
    }

    private static void unsubscribe(RawSession member, String filter) throws Exception {
        member.send(new Packet.Unsubscribe(
                9,
                List.of(new TopicFilter(filter)),
                EMPTY
        ));
        member.expect(Packet.UnsubAck.class);
    }

    private static String[] numbers(int count) {
        return IntStream.range(0, count).mapToObj(Integer::toString).toArray(String[]::new);
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }
}
