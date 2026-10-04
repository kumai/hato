package org.mqtt4j.mqtj.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.mqtj.MqttMessage;
import org.mqtt4j.mqtj.client.internal.common.io.SendQueue;
import org.mqtt4j.mqtj.client.internal.common.log.Log;
import org.mqtt4j.mqtj.client.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.mqtj.spi.Transport;

import static org.mqtt4j.mqtj.client.internal.common.protocol.Properties.EMPTY;

/// Adapts byte callbacks to protocol events and serializes preparation and writes off the actor.
/// One sender survives connection changes; queued and active sends share a client-wide budget.
final class ClientIo {

    interface Events {

        void connected(long generation);

        void received(long generation, Packet packet, int size);

        /// Posts a completed PUBACK write as soon as the transport reports it, even if send is
        /// still returning. This must only enqueue an event, without calling external code.
        void written(long generation, long ticket);

        /// Captures completion data outside the send monitor. The returned action only posts
        /// the event; it must not call external code or wait for the actor.
        Runnable prepareSent(long generation, long ticket);

        /// Posts a capacity wakeup for the current connection, possibly while holding the send
        /// monitor. This must only enqueue an event, without calling external code or waiting.
        void sendCapacityAvailable(long generation);

        void ended(long generation, Throwable cause);

        void failed(long generation, Throwable cause);

        void fatal(Throwable cause);
    }

    private static final Log log = Log.of(ClientIo.class);

    private static final int REGULAR_SEND_LIMIT = 128;

    private static final int REGULAR_SEND_BYTE_LIMIT = 16 * 1024 * 1024;

    private static final int RESERVED_SEND_LIMIT = 16;

    private static final int RESERVED_SEND_BYTE_LIMIT = 128 * 1024;

    // PUBACKs leave room for lifecycle control in the same budget and FIFO, including
    // reservations still owned by an earlier connection.
    private static final int ACKNOWLEDGEMENT_COUNT_HEADROOM = 2;

    private static final int ACKNOWLEDGEMENT_BYTE_HEADROOM = 64 * 1024;

    private final URI endpoint;

    private final String clientId;

    private final Duration timeout;

    private final int maxPacketSize;

    private final Transport.Factory factory;

    private final Events events;

    private final PacketCodec codec;

    private final SendQueue.Budget budget = new SendQueue.Budget(
            REGULAR_SEND_LIMIT,
            REGULAR_SEND_BYTE_LIMIT,
            RESERVED_SEND_LIMIT,
            RESERVED_SEND_BYTE_LIMIT
    );

    // Guards current, shutdown, and each Connection's mutable state. Never call Transport while
    // holding this monitor: its callbacks may acquire it inline.
    private final Object sending = new Object();

    private @Nullable Connection current;

    private boolean shutdown;

    ClientIo(
            URI endpoint,
            String clientId,
            Duration timeout,
            int maxPacketSize,
            Transport.Factory factory,
            Events events,
            PacketCodec codec
    ) {
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.timeout = timeout;
        this.maxPacketSize = maxPacketSize;
        this.factory = factory;
        this.events = events;
        this.codec = codec;
        Thread.ofVirtual().name("mqtj-" + clientId + "-sender").start(this::run);
    }

    void open(long generation) {
        Transport transport;
        try {
            transport = factory.create(clientId);
        } catch (RuntimeException failure) {
            throw new SpiException(generation, failure);
        }
        var connection = new Connection(generation, transport, new SendQueue<>(budget));
        var frames = new IncomingFrames(
                maxPacketSize,
                codec,
                (packet, size) -> {
                    log.debug("Received {} ({} bytes)", packet.getClass().getSimpleName(), size);
                    events.received(generation, packet, size);
                }
        );
        synchronized (sending) {
            current = connection;
        }
        var receiver = new Transport.Listener() {
            @Override
            public void onConnected() {
                if (!isClosed(connection)) {
                    events.connected(generation);
                }
            }

            @Override
            public void onBytes(byte[] bytes) {
                try {
                    if (!isClosed(connection)) {
                        log.trace("Received bytes: {}", bytes);
                        frames.accept(bytes);
                    }
                } catch (Throwable failure) {
                    events.failed(generation, failure);
                }
            }

            @Override
            public void onSent(long ticket) {
                boolean notify;
                boolean acknowledgement;
                synchronized (sending) {
                    if (connection.activeTicket != ticket || !connection.transportOwnsActive) {
                        if (!connection.closed) {
                            throw new IllegalStateException(
                                    "Unexpected send completion: " + ticket
                            );
                        }
                        return;
                    }
                    notify = !connection.closed;
                    acknowledgement = connection.activeAcknowledgement;
                }
                Runnable completion = null;
                try {
                    if (notify) {
                        log.debug("Written ticket {}", ticket);
                        if (acknowledgement) {
                            events.written(generation, ticket);
                        }
                        completion = events.prepareSent(generation, ticket);
                    }
                } finally {
                    synchronized (sending) {
                        connection.sendCompletion = completion;
                        connection.transportOwnsActive = false;
                        releaseCompleted(connection);
                    }
                }
            }

            @Override
            public void onClosed(Throwable cause) {
                frames.clear();
                ended(connection, cause);
            }
        };
        try {
            transport.start(endpoint, timeout, receiver);
        } catch (RuntimeException failure) {
            throw new SpiException(generation, failure);
        }
    }

    SendQueue.OfferResult publish(
            long generation,
            long ticket,
            MqttMessage message,
            int packetId
    ) {
        synchronized (sending) {
            Connection connection = current;
            if (shutdown || connection == null || connection.generation != generation) {
                return SendQueue.OfferResult.CLOSED;
            }
            // Refuse an oversized payload before even preparing its header. Final admission
            // below reserves the complete wire size, including that header, atomically.
            SendQueue.OfferResult capacity = connection.queue.checkCapacity(
                    message.payloadSize(),
                    false
            );
            if (capacity != SendQueue.OfferResult.ACCEPTED) {
                return capacity;
            }
        }
        byte[] header = codec.encodePublishHeader(
                message.topic(),
                message.qos(),
                message.retain(),
                false,
                packetId,
                EMPTY,
                message.payloadSize()
        );
        return enqueue(generation, ticket, new Outbound(header, message, false), false, "Publish");
    }

    SendQueue.OfferResult send(long generation, long ticket, Packet packet) {
        byte[] bytes = codec.encode(packet);
        // Reserve capacity for connection progress, not for every MQTT control packet.
        boolean reserved = packet instanceof Packet.Connect
                || packet instanceof Packet.PubAck
                || packet instanceof Packet.PingReq
                || packet instanceof Packet.Disconnect;
        return enqueue(
                generation,
                ticket,
                new Outbound(bytes, null, packet instanceof Packet.PubAck),
                reserved,
                packet.getClass().getSimpleName()
        );
    }

    private SendQueue.OfferResult enqueue(
            long generation,
            long ticket,
            Outbound outbound,
            boolean reserved,
            String packetName
    ) {
        synchronized (sending) {
            Connection connection = current;
            if (shutdown || connection == null || connection.generation != generation) {
                return SendQueue.OfferResult.CLOSED;
            }
            int size = outbound.size();
            SendQueue.OfferResult admitted = connection.queue.offer(
                    ticket,
                    outbound,
                    size,
                    reserved,
                    outbound.acknowledgement() ? ACKNOWLEDGEMENT_COUNT_HEADROOM : 0,
                    outbound.acknowledgement() ? ACKNOWLEDGEMENT_BYTE_HEADROOM : 0
            );
            if (admitted == SendQueue.OfferResult.ACCEPTED) {
                log.debug("Queued {} ticket {} ({} bytes)", packetName, ticket, size);
                wakeSender(connection);
            }
            return admitted;
        }
    }

    private void run() {
        try {
            while (runNext()) {
                // Each call drops its MqttMessage and frame references before the next idle wait.
            }
        } catch (Throwable failure) {
            events.fatal(failure);
        }
    }

    private boolean runNext() throws InterruptedException {
        Optional<Write> write = next();
        if (write.isEmpty()) {
            return false;
        }
        prepareAndSend(write.orElseThrow());
        return true;
    }

    private Optional<Write> next() throws InterruptedException {
        synchronized (sending) {
            while (!shutdown) {
                Connection connection = current;
                if (connection != null) {
                    Optional<SendQueue.Send<Outbound>> next = connection.queue.poll();
                    if (next.isPresent()) {
                        SendQueue.Send<Outbound> send = next.orElseThrow();
                        connection.activeTicket = send.ticket();
                        connection.activeAcknowledgement = send.value().acknowledgement();
                        connection.senderOwnsActive = true;
                        return Optional.of(new Write(connection, send));
                    }
                }
                sending.wait();
            }
            return Optional.empty();
        }
    }

    private void prepareAndSend(Write write) {
        Connection connection = write.connection();
        SendQueue.Send<Outbound> send = write.send();
        try {
            byte[] bytes = send.value().encode(codec);
            log.trace("Write attempt for ticket {}: {}", send.ticket(), bytes);
            synchronized (sending) {
                if (connection.closed) {
                    return;
                }
                // Reserve ownership before the call: completion can arrive before send returns.
                connection.transportOwnsActive = true;
            }
            Transport.SendResult result;
            try {
                result = connection.transport.send(send.ticket(), bytes);
            } catch (RuntimeException failure) {
                ended(connection, failure);
                return;
            } catch (Error failure) {
                rejectActive(connection);
                throw failure;
            }
            if (result == Transport.SendResult.CLOSED) {
                rejectActive(connection);
            }
        } finally {
            synchronized (sending) {
                connection.senderOwnsActive = false;
                releaseCompleted(connection);
            }
        }
    }

    private void rejectActive(Connection connection) {
        synchronized (sending) {
            connection.queue.close();
            // A rejected or throwing send did not transfer ownership to the transport.
            connection.transportOwnsActive = false;
        }
    }

    private void ended(Connection connection, Throwable cause) {
        synchronized (sending) {
            if (!connection.closed) {
                connection.pendingEnd = cause;
            }
            connection.closed = true;
            connection.queue.close();
            connection.transportOwnsActive = false;
            releaseCompleted(connection);
        }
    }

    /// Caller holds sending. Inline callbacks cannot release a buffer still on the sender's stack.
    private void releaseCompleted(Connection connection) {
        if (!connection.senderOwnsActive && !connection.transportOwnsActive
                && connection.activeTicket != 0) {
            connection.queue.completed(connection.activeTicket);
            connection.activeTicket = 0;
            notifySendCapacity();
            Runnable completion = connection.sendCompletion;
            connection.sendCompletion = null;
            if (completion != null) {
                // Return capacity before success is observable, and post before the next send.
                completion.run();
            }
            wakeSender(connection);
        }
        if (connection.sendCompletion == null && connection.pendingEnd != null) {
            // A successful inline write precedes EOF even if send has not returned yet.
            // Without a success to preserve, termination never waits for the sender.
            Throwable cause = connection.pendingEnd;
            connection.pendingEnd = null;
            events.ended(connection.generation, cause);
        }
    }

    /// Caller holds sending; only the current open connection can consume returned capacity.
    private void notifySendCapacity() {
        Connection connection = current;
        if (!shutdown && connection != null && !connection.closed
                && connection.queue.checkCapacity(0, true) != SendQueue.OfferResult.CLOSED) {
            events.sendCapacityAvailable(connection.generation);
        }
    }

    /// Caller holds sending, so a ready queue cannot race with the sender entering wait.
    private void wakeSender(Connection connection) {
        if (current == connection && connection.queue.hasReady()) {
            sending.notifyAll();
        }
    }

    private boolean isClosed(Connection connection) {
        synchronized (sending) {
            return connection.closed;
        }
    }

    void close() {
        Connection connection;
        synchronized (sending) {
            connection = current;
            current = null;
            if (connection != null) {
                connection.closed = true;
                connection.queue.close();
            }
            sending.notifyAll();
        }
        if (connection != null) {
            try {
                connection.transport.close();
            } catch (Throwable failure) {
                log.warn("Transport close failed for {} at {}", clientId, endpoint, failure);
            }
        }
    }

    void shutdown() {
        synchronized (sending) {
            shutdown = true;
        }
        close();
    }

    /// activeTicket names the queue's active send, or 0; its capacity returns only after neither
    /// the sender nor the transport still uses it.
    private static final class Connection {

        final long generation;
        final Transport transport;
        final SendQueue<Outbound> queue;
        boolean closed;
        long activeTicket;
        boolean activeAcknowledgement;
        boolean senderOwnsActive;
        boolean transportOwnsActive;
        @Nullable Runnable sendCompletion;
        @Nullable Throwable pendingEnd;

        Connection(long generation, Transport transport, SendQueue<Outbound> queue) {
            this.generation = generation;
            this.transport = transport;
            this.queue = queue;
        }
    }

    /// The complete packet, or a PUBLISH header whose payload the sender copies from the message.
    private record Outbound(
            byte[] header,
            @Nullable MqttMessage message,
            boolean acknowledgement
    ) {

        int size() {
            return header.length + (message == null ? 0 : message.payloadSize());
        }

        byte[] encode(PacketCodec codec) {
            return message == null ? header : codec.encodePublish(header, message.payload());
        }
    }

    private record Write(Connection connection, SendQueue.Send<Outbound> send) {
    }
}
