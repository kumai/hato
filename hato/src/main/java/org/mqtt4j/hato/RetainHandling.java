package org.mqtt4j.hato;

/// When the broker sends the retained messages that match a subscription as it makes the
/// subscription (the MQTT 5.0 Retain Handling option). See [Subscribe#withRetainHandling].
public enum RetainHandling {

    /// Send them whenever the subscription is made, as MQTT 3.1.1 does.
    SEND,

    /// Send them only when the subscription did not exist in the session. A broker that resumes
    /// no session for the client, for example after a connection with Session Present false,
    /// sees the client's re-created subscriptions as new, so retained messages can arrive after
    /// each such reconnection.
    SEND_IF_NEW_SUBSCRIPTION,

    /// Do not send them when the subscription is made.
    DO_NOT_SEND
}
