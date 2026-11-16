package org.mqtt4j.hato.broker.internal;

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
}
