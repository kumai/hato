package org.mqtt4j.mqtj.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.client.internal.common.io.SendQueue;
import org.mqtt4j.mqtj.client.internal.common.log.Log;
import org.mqtt4j.mqtj.client.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.mqtj.spi.Transport;

/// Owns one transport generation and adapts its byte callbacks to protocol events.
/// Commands come from the actor; callbacks may arrive concurrently and never call the core.
final class ClientIo {

    interface Events {

        void connected(long generation);

        void received(long generation, Packet packet, int size);

        void sent(long generation, long ticket);

        void ended(long generation, Throwable cause);

        void failed(long generation, Throwable cause);
    }

    private static final Log log = Log.of(ClientIo.class);

    private static final int REGULAR_SEND_LIMIT = 128;

    private static final int REGULAR_SEND_BYTE_LIMIT = 16 * 1024 * 1024;

    private static final int RESERVED_SEND_LIMIT = 16;

    private static final int RESERVED_SEND_BYTE_LIMIT = 128 * 1024;

    private final URI endpoint;

    private final String clientId;

    private final Duration timeout;

    private final int maxPacketSize;

    private final Transport.Factory factory;

    private final Events events;

    private final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    private @Nullable Connection current;

    ClientIo(
            URI endpoint,
            String clientId,
            Duration timeout,
            int maxPacketSize,
            Transport.Factory factory,
            Events events
    ) {
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.timeout = timeout;
        this.maxPacketSize = maxPacketSize;
        this.factory = factory;
        this.events = events;
    }

    void open(long generation) {
        Transport transport;
        try {
            transport = factory.create(clientId);
        } catch (RuntimeException failure) {
            throw new SpiException(generation, failure);
        }
        var closed = new AtomicBoolean();
        var queue = new SendQueue(
                REGULAR_SEND_LIMIT,
                REGULAR_SEND_BYTE_LIMIT,
                RESERVED_SEND_LIMIT,
                RESERVED_SEND_BYTE_LIMIT
        );
        var frames = new IncomingFrames(
                maxPacketSize,
                codec,
                (packet, size) -> {
                    log.debug("Received {} ({} bytes)", packet.getClass().getSimpleName(), size);
                    events.received(generation, packet, size);
                }
        );
        current = new Connection(generation, transport, closed, queue);
        var receiver = new Transport.Listener() {
            @Override
            public void onConnected() {
                if (!closed.get()) {
                    events.connected(generation);
                }
            }

            @Override
            public void onBytes(byte[] bytes) {
                try {
                    if (!closed.get()) {
                        log.trace("Received bytes: {}", bytes);
                        frames.accept(bytes);
                    }
                } catch (Throwable failure) {
                    events.failed(generation, failure);
                }
            }

            @Override
            public void onSent(long ticket) {
                queue.completed(ticket);
                if (!closed.get()) {
                    log.debug("Written ticket {}", ticket);
                    events.sent(generation, ticket);
                }
            }

            @Override
            public void onClosed(Throwable cause) {
                queue.ended();
                frames.clear();
                if (closed.compareAndSet(false, true)) {
                    events.ended(generation, cause);
                }
            }
        };
        try {
            transport.start(endpoint, timeout, receiver);
        } catch (RuntimeException failure) {
            throw new SpiException(generation, failure);
        }
    }

    SendQueue.OfferResult send(long generation, long ticket, Packet packet) {
        Connection connection = current;
        if (connection == null) {
            throw new IllegalStateException("No active transport");
        }
        byte[] bytes = codec.encode(packet);
        // Reserve capacity for connection progress, not for every MQTT control packet.
        boolean reserved = packet instanceof Packet.Connect
                || packet instanceof Packet.PubAck
                || packet instanceof Packet.PingReq
                || packet instanceof Packet.Disconnect;
        SendQueue.OfferResult admitted = connection.queue().offer(
                ticket,
                bytes,
                reserved
        );
        if (admitted == SendQueue.OfferResult.ACCEPTED) {
            log.debug("Queued {} ticket {} ({} bytes)",
                    packet.getClass().getSimpleName(), ticket, bytes.length);
            sendNext(generation);
        }
        return admitted;
    }

    /// Called on the actor, including after it handles a send completion. The mailbox handoff
    /// bounds throughput; move draining to a serialized I/O path only if measurements require it.
    void sendNext(long generation) {
        Connection connection = current;
        if (connection == null || connection.generation() != generation) {
            return;
        }
        Optional<SendQueue.Send> next = connection.queue().poll();
        if (next.isEmpty()) {
            return;
        }
        SendQueue.Send send = next.orElseThrow();
        log.trace("Write attempt for ticket {}: {}", send.ticket(), send.bytes());
        Transport.SendResult result;
        try {
            result = connection.transport().send(send.ticket(), send.bytes());
        } catch (RuntimeException failure) {
            throw new SpiException(generation, failure);
        }
        if (result == Transport.SendResult.CLOSED) {
            connection.queue().close();
            connection.queue().completed(send.ticket());
        }
    }

    void close() {
        Connection connection = current;
        current = null;
        if (connection != null) {
            connection.closed().set(true);
            connection.queue().close();
            try {
                connection.transport().close();
            } catch (Throwable failure) {
                log.warn("Transport close failed for {} at {}", clientId, endpoint, failure);
            }
        }
    }

    private record Connection(
            long generation,
            Transport transport,
            AtomicBoolean closed,
            SendQueue queue
    ) {
    }
}
