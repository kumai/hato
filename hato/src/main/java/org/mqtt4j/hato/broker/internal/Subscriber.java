package org.mqtt4j.hato.broker.internal;

// What the routing's index of subscriptions holds under a filter: a session, which matches a
// publication against its own subscriptions, or a shared subscription, which hands it to one of
// its members.
sealed interface Subscriber permits Session, SharedSubscription {
}
