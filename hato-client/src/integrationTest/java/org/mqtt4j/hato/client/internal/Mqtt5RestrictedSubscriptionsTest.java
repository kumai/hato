package org.mqtt4j.hato.client.internal;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.*;

import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;

/// The limits of [Mqtt5RestrictedTest], and no wildcard or shared subscriptions, shared by EMQX,
/// HiveMQ CE, and the embedded broker, which has no shared subscriptions at all. Mosquitto cannot
/// withdraw either kind of subscription.
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
}
