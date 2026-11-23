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

import org.mqtt4j.hato.MqttVersion;
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
            subscribeWithIdentifier(member, "$share/workers/jobs/#", 5);
            subscribeWithIdentifier(member, "jobs/+", 7);

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
                    if (delivered.qos() == AT_LEAST_ONCE) {
                        member.send(new Packet.PubAck(delivered.packetId(), SUCCESS, EMPTY));
                    }
                }
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

    private static void subscribeWithIdentifier(RawSession member, String filter, int identifier)
            throws Exception {
        member.send(new Packet.Subscribe(
                identifier,
                List.of(subscription(filter, AT_MOST_ONCE)),
                new Properties(List.of(new Property.VariableByteValue(
                        PropertyId.SUBSCRIPTION_IDENTIFIER,
                        identifier
                )))
        ));
        assertThat(member.expect(Packet.SubAck.class).reasonCodes())
                .containsExactly(SUCCESS);
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
