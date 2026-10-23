package org.mqtt4j.hato.testkit;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.AckMode;
import org.mqtt4j.hato.DeliveryOptions;
import org.mqtt4j.hato.MqttDelivery;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttSubscriptionRefusedException;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.testkit.FakeMqttSubscriber.FakeDelivery;
import org.mqtt4j.hato.testkit.FakeMqttSubscriber.FakeSubscription;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FakeMqttSubscriberTest {

    private static final TopicFilter READINGS = new TopicFilter("sensors/+");
    private static final Subscribe AT_LEAST_ONCE = Subscribe.of(READINGS, Qos.AT_LEAST_ONCE);
    private static final Subscribe EXACTLY_ONCE = Subscribe.of(READINGS, Qos.EXACTLY_ONCE);
    private static final MqttMessage READING = MqttMessage.of(
            new Topic("sensors/1"),
            new byte[] {1},
            Qos.AT_LEAST_ONCE,
            false
    );

    @Test
    void grants_the_requested_qos_and_runs_the_handler_on_the_delivering_thread() {
        // Arrange
        var sut = new FakeMqttSubscriber();
        var handled = new ArrayList<MqttDelivery>();
        var handlerThreads = new ArrayList<Thread>();
        FakeSubscription returned = sut.subscribe(EXACTLY_ONCE, delivery -> {
            handled.add(delivery);
            handlerThreads.add(Thread.currentThread());
        });
        FakeSubscription subscription = sut.subscription(READINGS).orElseThrow();

        // Act
        FakeDelivery delivery = subscription.deliver(READING);

        // Assert
        assertThat(subscription).isSameAs(returned);
        assertThat(sut.subscriptions()).containsExactly(subscription);
        assertThat(subscription.request()).isEqualTo(EXACTLY_ONCE);
        assertThat(subscription.established()).isCompletedWithValue(Qos.EXACTLY_ONCE);
        assertThat(subscription.grantedQos()).contains(Qos.EXACTLY_ONCE);
        assertThat(handled).containsExactly(delivery);
        assertThat(handlerThreads).containsExactly(Thread.currentThread());
        assertThat(delivery.message()).isEqualTo(READING);
        assertThat(delivery.filter()).isEqualTo(READINGS);
        assertThat(delivery.dup()).isFalse();
        assertThat(delivery.connectionAttempt()).isEqualTo(1);
        assertThat(delivery.failure()).isEmpty();
    }

    @Test
    void records_an_acknowledgement_made_after_the_handler_returns() {
        // Arrange
        var sut = new FakeMqttSubscriber();
        var kept = new AtomicReference<MqttDelivery>();
        sut.subscribe(AT_LEAST_ONCE.withAckMode(AckMode.MANUAL), kept::set);
        FakeDelivery delivery = sut.subscription(READINGS).orElseThrow().deliver(READING);
        boolean ackedOnReturn = delivery.acked();

        // Act
        kept.get().ack();

        // Assert
        assertThat(ackedOnReturn).isFalse();
        assertThat(delivery.acked()).isTrue();
    }

    @Test
    void records_what_the_handler_throws_instead_of_throwing_it() {
        // Arrange
        var sut = new FakeMqttSubscriber();
        var failure = new IllegalStateException("cannot parse the reading");
        sut.subscribe(AT_LEAST_ONCE, _ -> {
            throw failure;
        });
        FakeSubscription subscription = sut.subscription(READINGS).orElseThrow();

        // Act
        FakeDelivery first = subscription.deliver(READING);
        FakeDelivery second = subscription.deliver(READING);

        // Assert
        assertThat(first.failure()).containsSame(failure);
        assertThat(second.failure()).containsSame(failure);
        assertThat(first.acked()).isFalse();
        assertThat(subscription.ended()).isFalse();
    }

    @Test
    void a_refusal_rule_ends_later_subscriptions_to_its_filter() {
        // Arrange
        var sut = new FakeMqttSubscriber();
        FakeSubscription before = sut.subscribe(AT_LEAST_ONCE, _ -> { });
        before.close();
        sut.refuseSubscriptions(READINGS);

        // Act
        FakeSubscription refused = sut.subscribe(AT_LEAST_ONCE, _ -> { });

        // Assert
        assertThatThrownBy(refused.established()::join)
                .cause()
                .isInstanceOfSatisfying(
                        MqttSubscriptionRefusedException.class,
                        refusal -> assertThat(refusal.reasonCode())
                                .isEqualTo(ReasonCode.UNSPECIFIED_ERROR)
                );
        assertThat(refused.ended()).isTrue();
        assertThat(refused.grantedQos()).isEmpty();
        assertThat(refused.terminated()).isCompleted();
        assertThatThrownBy(() -> refused.deliver(READING))
                .isInstanceOf(IllegalStateException.class);
        assertThat(sut.subscription(READINGS)).containsSame(refused);
        assertThat(before.grantedQos()).contains(Qos.AT_LEAST_ONCE);
    }

    @Test
    void a_grant_rule_grants_the_lower_qos_until_it_is_cleared() {
        // Arrange
        var sut = new FakeMqttSubscriber();
        sut.grantAtMost(READINGS, Qos.AT_LEAST_ONCE);
        FakeSubscription lowered = sut.subscribe(EXACTLY_ONCE, _ -> { });
        lowered.close();
        FakeSubscription kept = sut.subscribe(
                Subscribe.of(READINGS, Qos.AT_MOST_ONCE),
                _ -> { }
        );
        kept.close();

        // Act
        sut.clearSubscriptionRule(READINGS);
        FakeSubscription asRequested = sut.subscribe(EXACTLY_ONCE, _ -> { });

        // Assert
        assertThat(lowered.grantedQos()).contains(Qos.AT_LEAST_ONCE);
        assertThat(kept.grantedQos()).contains(Qos.AT_MOST_ONCE);
        assertThat(asRequested.established()).isCompletedWithValue(Qos.EXACTLY_ONCE);
    }

    @Test
    void a_second_subscription_to_the_filter_of_an_active_one_fails() {
        // Arrange
        var sut = new FakeMqttSubscriber();
        FakeSubscription first = sut.subscribe(AT_LEAST_ONCE, _ -> { });

        // Act
        FakeSubscription second = sut.subscribe(EXACTLY_ONCE, _ -> { });

        // Assert
        assertThatThrownBy(second.established()::join)
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThat(second.ended()).isTrue();
        assertThat(first.ended()).isFalse();
        first.close();
        FakeSubscription again = sut.subscribe(EXACTLY_ONCE, _ -> { });
        assertThat(again.established()).isCompletedWithValue(Qos.EXACTLY_ONCE);
    }

    @ParameterizedTest
    @EnumSource(Ending.class)
    void ending_a_subscription_stops_deliveries_and_completes_its_termination(Ending ending) {
        // Arrange
        var sut = new FakeMqttSubscriber();
        FakeSubscription subscription = sut.subscribe(AT_LEAST_ONCE, _ -> { });

        // Act
        ending.end(subscription);

        // Assert
        assertThat(subscription.ended()).isTrue();
        assertThat(subscription.terminated()).isCompleted();
        assertThatThrownBy(subscription.established()::join)
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> subscription.deliver(READING))
                .isInstanceOf(IllegalStateException.class);
        assertThat(subscription.grantedQos()).contains(Qos.AT_LEAST_ONCE);
    }

    @Test
    void termination_waits_for_the_running_handler() {
        // Arrange
        var sut = new FakeMqttSubscriber();
        var subscription = new AtomicReference<Subscription>();
        var terminatedInHandler = new AtomicBoolean(true);
        subscription.set(sut.subscribe(AT_LEAST_ONCE, _ -> {
            subscription.get().close();
            terminatedInHandler.set(subscription.get().terminated().isDone());
        }));

        // Act
        sut.subscription(READINGS).orElseThrow().deliver(READING);

        // Assert
        assertThat(terminatedInHandler).isFalse();
        assertThat(subscription.get().terminated()).isCompleted();
    }

    @Test
    void pull_placed_and_hand_off_subscriptions_are_not_supported() {
        // Arrange
        var sut = new FakeMqttSubscriber();

        // Act & Assert
        assertThatThrownBy(() -> sut.subscribe(AT_LEAST_ONCE))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> sut.subscribe(AT_LEAST_ONCE, DeliveryOptions.serial(), _ -> { }))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> sut.handOff(AT_LEAST_ONCE, _ -> true))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void finds_no_subscription_for_a_filter_that_was_never_subscribed() {
        assertThat(new FakeMqttSubscriber().subscription(READINGS)).isEmpty();
    }

    enum Ending {
        UNSUBSCRIBE {
            @Override
            void end(Subscription subscription) {
                CompletableFuture<Void> removal = subscription.unsubscribe();
                assertThat(removal).isCompleted();
            }
        },
        CLOSE {
            @Override
            void end(Subscription subscription) {
                subscription.close();
            }
        };

        abstract void end(Subscription subscription);
    }
}
