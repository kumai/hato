package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;

/// The limits of [Mqtt5RestrictedTest], and no wildcard or shared subscriptions, shared by EMQX
/// and HiveMQ CE. Mosquitto cannot withdraw either kind of subscription.
abstract class Mqtt5RestrictedSubscriptionsTest extends Mqtt5RestrictedTest {

    @Test
    void a_wildcard_subscription_is_refused_and_the_connection_goes_on() throws Exception {
        assertRefusedWhileTheConnectionGoesOn(
                uniqueTopic() + "/#",
                ReasonCode.WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED
        );
    }

    @Test
    void a_shared_subscription_is_refused_and_the_connection_goes_on() throws Exception {
        assertRefusedWhileTheConnectionGoesOn(
                "$share/hato/" + uniqueTopic(),
                ReasonCode.SHARED_SUBSCRIPTIONS_NOT_SUPPORTED
        );
    }

    @Override
    boolean wildcardSubscriptionAvailable() {
        return false;
    }

    @Override
    boolean sharedSubscriptionAvailable() {
        return false;
    }

    /// Refusing a subscription to `filter` with `reason` leaves the connection to another one.
    private void assertRefusedWhileTheConnectionGoesOn(String filter, ReasonCode reason)
            throws Exception {
        try (MqttClient sut = client()) {
            // Arrange
            sut.connect().get(5, SECONDS);

            // Act
            Subscription refused = sut.subscribe(
                    Subscribe.of(new TopicFilter(filter), Qos.AT_LEAST_ONCE)
            );

            // Assert
            assertThatThrownBy(() -> refused.established().get(5, SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOfSatisfying(
                            MqttSubscriptionRefusedException.class,
                            refusal -> assertThat(refusal.reasonCode()).isEqualTo(reason)
                    );
            assertThat(sut.subscribe(
                    Subscribe.of(new TopicFilter(uniqueTopic()), Qos.AT_LEAST_ONCE)
            ).established()).succeedsWithin(Duration.ofSeconds(5));
        }
    }
}
