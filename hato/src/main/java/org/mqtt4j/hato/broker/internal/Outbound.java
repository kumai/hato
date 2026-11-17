package org.mqtt4j.hato.broker.internal;

import org.mqtt4j.hato.internal.protocol.Packet;

// What waits in a connection's send queue: a packet in the form that the connection's wire gives
// it as it is queued, or a delivery, which the wire prepares as it writes it.
sealed interface Outbound {

    // The bytes that the packet takes on the wire.
    int size();

    // A packet encoded as it was queued.
    record Encoded(byte[] bytes) implements Outbound {

        @Override
        public int size() {
            return bytes.length;
        }
    }

    // A delivery, with whether a connection before this one started to send it.
    record Publish(Delivery delivery, boolean sentBefore) implements Outbound {

        @Override
        public int size() {
            return delivery.byteCount;
        }
    }

    // A packet as it is, for a client in the same JVM, and the bytes that it takes on the wire.
    record Plain(Packet packet, int size) implements Outbound {}

    // No packet: the end of the output, which a connection takes as its last packet to end its
    // output without one, as for an MQTT 3.1.1 client, which has no DISCONNECT from a server.
    record End() implements Outbound {

        @Override
        public int size() {
            return 0;
        }
    }
}
