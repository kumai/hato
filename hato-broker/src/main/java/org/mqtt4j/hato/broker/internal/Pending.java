package org.mqtt4j.hato.broker.internal;

// What waits in a session's delivery queue: a delivery, or the retained messages that a new
// subscription matched, which go out one at a time from their place in the queue.
sealed interface Pending permits Delivery, RetainedCursor {
}
