package org.mqtt4j.hato.broker;

import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.broker.internal.BrokerServer;

/// What a broker counts, as [MqttBroker#stats()] read it: what the broker holds now, and
/// lifetime counts, which cover the broker's whole life and only grow. The broker reads each
/// value at a moment of its own, without stopping its work, so the values of one reading need
/// not agree with each other. Once the broker has ended, it holds nothing, and its lifetime
/// counts stay as they were.
public final class BrokerStats {

    private final BrokerServer.Counts counts;

    BrokerStats(BrokerServer.Counts counts) {
        this.counts = counts;
    }

    /// {@return the clients that the broker serves now, as
    /// [MqttBrokerBuilder#maxConnections(int)] counts them}
    public long connections() {
        return counts.connections();
    }

    /// {@return the sessions that outlive their connections now, those of the clients that are
    /// away included, as [MqttBrokerBuilder#maxPersistentSessions(int)] counts them}
    public long persistentSessions() {
        return counts.persistentSessions();
    }

    /// {@return the retained messages that the broker keeps now, as
    /// [MqttBrokerBuilder#maxRetainedMessages(int)] counts them}
    public long retainedMessages() {
        return counts.retainedMessages();
    }

    /// {@return the deliveries that wait in the sessions now for their clients to take them}
    /// They are the deliveries at any QoS, those for clients that are away included, and the
    /// retained messages that new subscriptions have yet to receive; the deliveries on their way
    /// to a client, and those that await its acknowledgement, are not. Reading this value takes
    /// time in proportion to the sessions.
    public long queuedDeliveries() {
        return counts.queuedDeliveries();
    }

    /// {@return a lifetime count of the PUBLISH packets that clients sent at `qos`,
    /// retransmissions included}
    ///
    /// @param qos
    ///   the QoS of the packets to count
    public long received(Qos qos) {
        return counts.received()[qos.level()];
    }

    /// {@return a lifetime count of the PUBLISH packets that the broker sent to clients at
    /// `qos`, retransmissions included}
    ///
    /// @param qos
    ///   the QoS of the packets to count
    public long delivered(Qos qos) {
        return counts.delivered()[qos.level()];
    }

    /// {@return a lifetime count of the deliveries at `qos` that the broker's limits dropped}
    /// QoS 1 and QoS 2 deliveries are those that [BrokerListener#onDropped(String, long)] and
    /// [BrokerListener#onDroppedUnattributed(long)] report. The count can take a moment to
    /// include a delivery.
    ///
    /// @param qos
    ///   the QoS of the deliveries to count
    public long dropped(Qos qos) {
        return counts.dropped()[qos.level()];
    }

    /// {@return a lifetime count of the publications that the broker refused because their
    /// topics are outside the scope that the [Authenticator] gave their clients}
    public long refusedPublications() {
        return counts.refusedPublications();
    }

    /// {@return a lifetime count of the notices of connections that the broker dropped because
    /// the [BrokerListener] fell behind}
    public long droppedNotices() {
        return counts.droppedNotices();
    }

    @Override
    public String toString() {
        return "BrokerStats[connections=" + connections()
                + ", persistentSessions=" + persistentSessions()
                + ", retainedMessages=" + retainedMessages()
                + ", queuedDeliveries=" + queuedDeliveries()
                + ", received=" + byLevel(counts.received())
                + ", delivered=" + byLevel(counts.delivered())
                + ", dropped=" + byLevel(counts.dropped())
                + ", refusedPublications=" + refusedPublications()
                + ", droppedNotices=" + droppedNotices() + "]";
    }

    // The counts of QoS 0, 1, and 2, as text.
    private static String byLevel(long[] counts) {
        return "{" + Qos.AT_MOST_ONCE + "=" + counts[0]
                + ", " + Qos.AT_LEAST_ONCE + "=" + counts[1]
                + ", " + Qos.EXACTLY_ONCE + "=" + counts[2] + "}";
    }
}
