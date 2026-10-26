package org.mqtt4j.hato.client.internal;

import java.lang.System.Logger.Level;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Transport;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.log.Log;
import org.mqtt4j.hato.client.internal.common.protocol.IncomingAliases;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.client.internal.common.protocol.Properties;
import org.mqtt4j.hato.client.internal.common.protocol.Property;
import org.mqtt4j.hato.client.internal.common.protocol.PropertyId;

import static java.nio.charset.StandardCharsets.UTF_8;

/// Adapts byte callbacks to protocol events and serializes preparation and writes off the actor.
/// One sender survives connection changes; queued and active sends share a client-wide budget.
final class ClientIo {

    interface Events {

        void connected(long generation);

        void received(long generation, Packet packet, int size);

        /// A PINGRESP that answers a PINGREQ admitted on this connection, which may be handled
        /// ahead of earlier input. Any other PINGRESP arrives through [#received].
        void pingResponse(long generation, Packet packet, int size);

        /// Posts a completed PUBACK, PUBREC, or PUBCOMP write as soon as the transport reports
        /// it, even if send is still returning. Only enqueues an event, without external code.
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

        /// Called once, without the send monitor, after [#shutdown()] once every started
        /// transport has reported its close and the sender has exited. I/O calls no Transport
        /// method afterwards.
        void closed();
    }

    private static final Log log = Log.of(ClientIo.class);

    private static final int REGULAR_SEND_BYTE_LIMIT = 16 * 1024 * 1024;

    // PUBLISH must not consume the capacity needed to finish protocol exchanges. MQTT 3.1.1
    // PUBACK/PUBREC/PUBREL/PUBCOMP each use four bytes; the core defers them when this budget is
    // full and resumes on returned capacity, so a whole inflight window need not fit at once.
    static final int RESERVED_SEND_LIMIT = 16;

    private static final int RESERVED_SEND_BYTE_LIMIT = 1024 * 1024;

    // Acknowledgements leave two slots and byte headroom for lifecycle control (CONNECT,
    // PINGREQ, DISCONNECT) in the same budget and FIFO. Old connections still holding send
    // buffers share this budget; protocol progress must leave room for the current connection.
    private static final int ACKNOWLEDGEMENT_COUNT_HEADROOM = 2;

    private static final int ACKNOWLEDGEMENT_BYTE_HEADROOM = 64 * 1024;

    // Sends that wait together go out in one write, up to this many bytes, rather than one write
    // each: every write passes to the transport's writer and back, which costs more than a small
    // packet itself. A larger send goes alone.
    static final int BATCH_BYTE_LIMIT = 64 * 1024;

    // A PUBLISH payload this large leaves after its header, in a second write, instead of being
    // joined to it in a new array. Measured over loopback TCP, a 64 KiB payload published 5%
    // slower that way, because the extra write cost as much as the copy it saved, and payloads
    // from 128 KiB to 1 MiB published 13 to 22% faster. Such a send goes alone anyway.
    static final int APART_PAYLOAD_BYTES = 128 * 1024;

    // Receive capacity counts each property of a received packet with this many bytes beyond
    // the packet's size. Decoded, a User Property takes up to 118 bytes of memory beyond its
    // encoding, as measured with compressed references, the default for heaps under 32 GiB.
    // Without them it takes about 150, which this undercounts; such heaps dwarf the receive
    // limits anyway. Without this allowance, packets of many small properties would hold many
    // times the memory that the receive limits admit.
    static final int PROPERTY_COST = 128;

    private final URI endpoint;

    private final String clientId;

    private final int maxPacketSize;

    private final Transport.Factory factory;

    private final Events events;

    private final PacketCodec codec;

    // The payload array of a message itself, which the sender appends to its PUBLISH header
    // without copying it first and never modifies.
    private final Function<MqttMessage, byte[]> payloadOf;

    private final SendQueue.Budget budget;

    private final int regularSendLimit;

    // The most bytes of waiting sends that one write carries together; see [#BATCH_BYTE_LIMIT].
    private final int batchByteLimit;

    // Guards current, shutdown, and each Connection's mutable state. Never call Transport while
    // holding this monitor: its callbacks may acquire it inline.
    private final Object sending = new Object();

    // The one thread that writes to transports. It parks while it has nothing to write, rather
    // than waiting on sending: the JDK resumes a virtual thread that a monitor wakes through a
    // platform thread of its own, which leaves it behind the work that the carrier queues for
    // itself. On a single carrier kept busy by the reader and the actor, a due PINGREQ then
    // went unwritten until its PINGRESP deadline expired. A virtual thread that unparks the
    // sender queues it on its own carrier.
    private final Thread sender;

    private @Nullable Connection current;

    private boolean shutdown;

    private long postedEnds;

    // Started transports that have not reported their close, including those of ended
    // connections that are still closing.
    private int openTransports;

    private boolean senderEnded;

    // Set by the sender when it finds nothing to write, before it parks; cleared by the thread
    // that unparks it.
    private boolean senderIdle;

    private boolean closeReported;

    ClientIo(
            URI endpoint,
            String clientId,
            int maxPacketSize,
            int regularSendLimit,
            int batchByteLimit,
            Transport.Factory factory,
            Events events,
            PacketCodec codec,
            Function<MqttMessage, byte[]> payloadOf
    ) {
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.maxPacketSize = maxPacketSize;
        this.factory = factory;
        this.events = events;
        this.codec = codec;
        this.payloadOf = payloadOf;
        this.regularSendLimit = regularSendLimit;
        this.batchByteLimit = batchByteLimit;
        budget = new SendQueue.Budget(
                regularSendLimit,
                REGULAR_SEND_BYTE_LIMIT,
                RESERVED_SEND_LIMIT,
                RESERVED_SEND_BYTE_LIMIT
        );
        sender = Thread.ofVirtual().name("hato-" + clientId + "-sender").unstarted(this::run);
        sender.start();
    }

    void open(long generation, Duration timeout) {
        Transport transport;
        try {
            transport = factory.create(clientId);
        } catch (RuntimeException failure) {
            throw new SpiException(generation, failure);
        }
        var connection = new Connection(generation, transport, new SendQueue<>(budget));
        // The broker's Topic Aliases last for this connection, so its own callback resolves them.
        var aliases = new IncomingAliases(ConnectionEstablishment.TOPIC_ALIAS_MAXIMUM);
        var frames = new IncomingFrames(
                maxPacketSize,
                codec,
                (packet, size) -> {
                    log.debug("Received {} ({} bytes)", packet.getClass().getSimpleName(), size);
                    if (packet instanceof Packet.PingResp && answersPing(connection)) {
                        events.pingResponse(generation, packet, size);
                    } else if (packet instanceof Packet.Publish publish) {
                        Packet.Publish held = aliases.set(publish);
                        events.received(generation, held, size + propertyCost(held));
                    } else if (packet instanceof Packet.AliasPublish publish) {
                        Packet.Publish resolved = aliases.resolve(publish);
                        // Receive capacity counts the topic as if the broker had sent it, so an
                        // alias saves bandwidth without changing what a delivery costs.
                        int topicBytes = resolved.topic().name().getBytes(UTF_8).length;
                        events.received(
                                generation,
                                resolved,
                                size + topicBytes + propertyCost(resolved)
                        );
                    } else {
                        events.received(generation, packet, size + propertyCost(packet));
                    }
                }
        );
        synchronized (sending) {
            current = connection;
            openTransports++;
        }
        var receiver = new BorrowingListener() {
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
            public void onBorrowedBytes(byte[] buffer, int offset, int length) {
                try {
                    if (!isClosed(connection)) {
                        if (log.isLoggable(Level.TRACE)) {
                            log.trace("Received bytes: {}",
                                    Arrays.copyOfRange(buffer, offset, offset + length));
                        }
                        frames.acceptLent(buffer, offset, length);
                    }
                } catch (Throwable failure) {
                    events.failed(generation, failure);
                }
            }

            @Override
            public void onSent(long ticket) {
                boolean notify;
                List<SendQueue.Send<Outbound>> written;
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
                    written = connection.activeSends;
                }
                // One write carried every send of the batch, so each completes now, in order.
                var completions = new ArrayList<Runnable>(written.size());
                try {
                    if (notify) {
                        for (SendQueue.Send<Outbound> send : written) {
                            log.debug("Written ticket {}", send.ticket());
                            if (send.value().notifyWritten()) {
                                events.written(generation, send.ticket());
                            }
                            completions.add(events.prepareSent(generation, send.ticket()));
                        }
                    }
                } finally {
                    synchronized (sending) {
                        connection.sendCompletions = completions;
                        connection.transportOwnsActive = false;
                        releaseCompleted(connection);
                    }
                }
            }

            @Override
            public void onClosed(Throwable cause) {
                frames.clear();
                ended(connection, cause);
                released(connection);
            }
        };
        try {
            transport.start(endpoint, timeout, receiver);
        } catch (RuntimeException failure) {
            // A transport that failed to start reports no close to wait for.
            released(connection);
            throw new SpiException(generation, failure);
        } catch (Error failure) {
            released(connection);
            throw failure;
        }
    }

    SendQueue.OfferResult publish(
            long generation,
            long ticket,
            MqttMessage message,
            int packetId,
            @Nullable SendReceipt receipt,
            TopicAlias alias
    ) {
        // Measured outside the monitor: counting User Properties takes time in their length.
        int bodySize = MessageProperties.bodySize(message);
        synchronized (sending) {
            Connection connection = current;
            if (shutdown || connection == null || connection.generation != generation) {
                return SendQueue.OfferResult.CLOSED;
            }
            // Refuse an oversized payload or set of properties before even preparing the header.
            // Final admission below reserves the complete wire size, including that header,
            // atomically.
            SendQueue.OfferResult capacity = connection.queue.checkCapacity(
                    bodySize,
                    false
            );
            if (capacity != SendQueue.OfferResult.ACCEPTED) {
                return capacity;
            }
        }
        byte[] header = publishHeader(message, packetId, alias);
        return enqueue(
                generation,
                ticket,
                new Outbound(header, message, false, false, receipt),
                false,
                false,
                "Publish",
                false
        );
    }

    // Encoding sets DUP from the transport receipt, which may change until the send starts.
    private byte[] publishHeader(MqttMessage message, int packetId, TopicAlias alias) {
        Properties properties = MessageProperties.of(message);
        if (alias.value() != 0) {
            var entries = new ArrayList<>(properties.entries());
            entries.add(new Property.TwoByteValue(PropertyId.TOPIC_ALIAS, alias.value()));
            properties = new Properties(entries);
        }
        if (!alias.withTopic()) {
            return codec.encodeAliasPublishHeader(
                    message.qos(),
                    message.retain(),
                    false,
                    packetId,
                    properties,
                    message.payloadSize()
            );
        }
        return codec.encodePublishHeader(
                message.topic(),
                message.qos(),
                message.retain(),
                false,
                packetId,
                properties,
                message.payloadSize()
        );
    }

    SendQueue.OfferResult send(
            long generation,
            long ticket,
            Packet packet,
            @Nullable SendReceipt receipt
    ) {
        byte[] bytes = codec.encode(packet);
        // Reserve capacity for connection progress, not for every MQTT control packet.
        boolean acknowledgement = packet instanceof Packet.PubAck
                || packet instanceof Packet.PubRec
                || packet instanceof Packet.PubRel
                || packet instanceof Packet.PubComp;
        boolean reserved = acknowledgement || packet instanceof Packet.Connect
                || packet instanceof Packet.PingReq
                || packet instanceof Packet.Disconnect;
        return enqueue(
                generation,
                ticket,
                new Outbound(
                        bytes,
                        null,
                        packet instanceof Packet.PubAck || packet instanceof Packet.PubRec
                                || packet instanceof Packet.PubComp,
                        // CONNECT carries credentials, so its bytes never reach a log.
                        packet instanceof Packet.Connect,
                        receipt
                ),
                reserved,
                acknowledgement,
                packet.getClass().getSimpleName(),
                packet instanceof Packet.PingReq
        );
    }

    private SendQueue.OfferResult enqueue(
            long generation,
            long ticket,
            Outbound outbound,
            boolean reserved,
            boolean acknowledgement,
            String packetName,
            boolean ping
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
                    acknowledgement ? ACKNOWLEDGEMENT_COUNT_HEADROOM : 0,
                    acknowledgement ? ACKNOWLEDGEMENT_BYTE_HEADROOM : 0
            );
            if (admitted == SendQueue.OfferResult.ACCEPTED) {
                if (ping) {
                    // Counted before the sender can write it, so its answer is recognized.
                    connection.pings++;
                }
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
        } finally {
            boolean closed;
            synchronized (sending) {
                senderEnded = true;
                closed = closedLocked();
            }
            if (closed) {
                events.closed();
            }
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
        while (true) {
            synchronized (sending) {
                if (shutdown) {
                    return Optional.empty();
                }
                Connection connection = current;
                if (connection != null) {
                    List<SendQueue.Send<Outbound>> batch =
                            connection.queue.pollBatch(batchByteLimit);
                    if (!batch.isEmpty()) {
                        // The transport reports the batch's write by the ticket of its last send.
                        connection.activeTicket = batch.getLast().ticket();
                        connection.activeSends = batch;
                        connection.senderOwnsActive = true;
                        return Optional.of(new Write(connection, batch));
                    }
                }
                senderIdle = true;
            }
            // Parks outside the monitor, so that the threads that give the sender work can take
            // it. An unpark in between leaves a permit, and a spurious return only makes the
            // sender look again.
            LockSupport.park(this);
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
    }

    private void prepareAndSend(Write write) {
        Connection connection = write.connection();
        List<SendQueue.Send<Outbound>> sends = write.sends();
        try {
            Encoded encoded = encode(connection, sends);
            synchronized (sending) {
                if (connection.closed) {
                    return;
                }
                // Reserve ownership before the call: completion can arrive before send returns.
                connection.transportOwnsActive = true;
            }
            Transport.SendResult result;
            for (SendQueue.Send<Outbound> send : sends) {
                SendReceipt receipt = send.value().receipt();
                if (receipt != null) {
                    // The peer can reply before send returns or reports its write completion.
                    receipt.markStarted();
                }
            }
            try {
                result = encoded.writeTo(connection.transport, sends.getLast().ticket());
            } catch (RuntimeException failure) {
                ended(connection, failure);
                return;
            } catch (Error failure) {
                rejectActive(connection);
                throw failure;
            }
            if (result == Transport.SendResult.ACCEPTED) {
                for (SendQueue.Send<Outbound> send : sends) {
                    SendReceipt receipt = send.value().receipt();
                    if (receipt != null) {
                        receipt.markAccepted();
                    }
                }
            } else if (result == Transport.SendResult.CLOSED) {
                rejectActive(connection);
            }
        } finally {
            synchronized (sending) {
                connection.senderOwnsActive = false;
                releaseCompleted(connection);
            }
        }
    }

    /// What the one write of `sends` carries. A PUBLISH that goes alone with a payload of at least
    /// [#APART_PAYLOAD_BYTES] leaves as its header and then its payload on a transport that lets
    /// the sender write; anything else leaves as one array.
    private Encoded encode(Connection connection, List<SendQueue.Send<Outbound>> sends) {
        if (sends.size() == 1 && connection.transport instanceof BlockingWriteTransport) {
            SendQueue.Send<Outbound> send = sends.getFirst();
            MqttMessage message = send.value().message();
            if (message != null && message.payloadSize() >= APART_PAYLOAD_BYTES) {
                byte[] header = send.value().currentHeader();
                log.trace("Write attempt for ticket {}: {} bytes of header and {} of payload",
                        send.ticket(), header.length, message.payloadSize());
                return new Encoded.Apart(header, payloadOf.apply(message));
            }
        }
        return new Encoded.Whole(encode(sends));
    }

    /// The bytes of `sends`, one after another, as the one write that carries them.
    private byte[] encode(List<SendQueue.Send<Outbound>> sends) {
        if (sends.size() == 1) {
            return encode(sends.getFirst());
        }
        var parts = new ArrayList<byte[]>(sends.size());
        int length = 0;
        for (SendQueue.Send<Outbound> send : sends) {
            byte[] part = encode(send);
            parts.add(part);
            length += part.length;
        }
        var bytes = new byte[length];
        int offset = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, bytes, offset, part.length);
            offset += part.length;
        }
        return bytes;
    }

    private byte[] encode(SendQueue.Send<Outbound> send) {
        byte[] bytes = send.value().encode(codec, payloadOf);
        if (send.value().redacted()) {
            log.trace("Write attempt for ticket {}: {} bytes (redacted)",
                    send.ticket(), bytes.length);
        } else {
            log.trace("Write attempt for ticket {}: {}", send.ticket(), bytes);
        }
        return bytes;
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
            connection.activeSends = List.of();
            notifySendCapacity();
            List<Runnable> completions = connection.sendCompletions;
            connection.sendCompletions = List.of();
            // Return capacity before success is observable, and post before the next send.
            completions.forEach(Runnable::run);
            wakeSender(connection);
        }
        if (connection.sendCompletions.isEmpty() && connection.pendingEnd != null) {
            // A successful inline write precedes EOF even if send has not returned yet.
            // Without a success to preserve, termination never waits for the sender.
            Throwable cause = connection.pendingEnd;
            connection.pendingEnd = null;
            events.ended(connection.generation, cause);
            postedEnds++;
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

    /// Caller holds sending, so a ready queue cannot race with the sender going idle.
    private void wakeSender(Connection connection) {
        if (current == connection && connection.queue.hasReady()) {
            unparkSender();
        }
    }

    /// Caller holds sending. Unparks the sender if it has gone idle.
    private void unparkSender() {
        if (senderIdle) {
            senderIdle = false;
            LockSupport.unpark(sender);
        }
    }

    /// No PINGREQ precedes CONNACK, so an answering PINGRESP cannot overtake it.
    private boolean answersPing(Connection connection) {
        synchronized (sending) {
            if (connection.pings == 0) {
                return false;
            }
            connection.pings--;
            return true;
        }
    }

    /// The receive capacity that the properties of `packet` take beyond its size.
    private static int propertyCost(Packet packet) {
        return PROPERTY_COST * packet.properties().entries().size();
    }

    /// Stops reading the connection of `generation` until [#resumeReading(long)], where its
    /// transport can pause. Called on any thread, including inside the transport's callbacks.
    void pauseReading(long generation) {
        PausableTransport transport = pausable(generation);
        if (transport != null) {
            transport.pauseReading();
        }
    }

    void resumeReading(long generation) {
        PausableTransport transport = pausable(generation);
        if (transport != null) {
            transport.resumeReading();
        }
    }

    private @Nullable PausableTransport pausable(long generation) {
        Connection connection;
        synchronized (sending) {
            connection = current;
        }
        return connection != null && connection.generation == generation
                && connection.transport instanceof PausableTransport transport ? transport : null;
    }

    private boolean isClosed(Connection connection) {
        synchronized (sending) {
            return connection.closed;
        }
    }

    /// Closes the current connection like [#close()], but goes on handing over what it receives
    /// and reports its end through [Events#ended] once its transport has closed.
    void finish() {
        Connection connection;
        synchronized (sending) {
            connection = current;
            current = null;
            if (connection != null) {
                connection.queue.close();
            }
            unparkSender();
        }
        if (connection != null) {
            try {
                connection.transport.close();
            } catch (Throwable failure) {
                log.warn("Transport close failed for {} at {}", clientId, endpoint, failure);
            }
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
            unparkSender();
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
        boolean closed;
        synchronized (sending) {
            closed = closedLocked();
        }
        if (closed) {
            events.closed();
        }
    }

    /// A transport reported its close or failed to start; each counts once, even if a transport
    /// reports again.
    private void released(Connection connection) {
        boolean closed;
        synchronized (sending) {
            if (connection.released) {
                return;
            }
            connection.released = true;
            openTransports--;
            closed = closedLocked();
        }
        if (closed) {
            events.closed();
        }
    }

    /// Caller holds sending. True once, when shutdown has left nothing that calls a transport.
    private boolean closedLocked() {
        if (shutdown && openTransports == 0 && senderEnded && !closeReported) {
            closeReported = true;
            return true;
        }
        return false;
    }

    /// Sends holding the client-wide budget, including those an ended connection still owns.
    int sends() {
        return budget.count();
    }

    long sendBytes() {
        return budget.bytes();
    }

    /// The most writes of any packet that the send queue holds, in its regular and reserved
    /// quotas together.
    int sendLimit() {
        return regularSendLimit + RESERVED_SEND_LIMIT;
    }

    /// The most bytes that the send queue holds, in its regular and reserved quotas together.
    long sendByteLimit() {
        return (long) REGULAR_SEND_BYTE_LIMIT + RESERVED_SEND_BYTE_LIMIT;
    }

    /// Counts connection ends posted to the actor. The count changes under the send monitor
    /// after the post returns, so a caller that sees it can order later events after the end.
    long postedEnds() {
        synchronized (sending) {
            return postedEnds;
        }
    }

    /// activeTicket names the queue's active batch by its last send, or 0; their capacity returns
    /// only after neither the sender nor the transport still uses them.
    private static final class Connection {

        final long generation;
        final Transport transport;
        final SendQueue<Outbound> queue;
        boolean closed;
        long activeTicket;
        List<SendQueue.Send<Outbound>> activeSends = List.of();
        boolean senderOwnsActive;
        boolean transportOwnsActive;
        // The completions of a written batch, which run once its capacity has returned.
        List<Runnable> sendCompletions = List.of();
        @Nullable Throwable pendingEnd;
        // Admitted PINGREQs that no PINGRESP has answered yet.
        int pings;
        // The transport has reported its close or failed to start.
        boolean released;

        Connection(long generation, Transport transport, SendQueue<Outbound> queue) {
            this.generation = generation;
            this.transport = transport;
            this.queue = queue;
        }
    }

    /// The complete packet, or a PUBLISH header to which the sender appends the message's payload.
    private record Outbound(
            byte[] header,
            @Nullable MqttMessage message,
            boolean notifyWritten,
            boolean redacted,
            @Nullable SendReceipt receipt
    ) {

        int size() {
            return header.length + (message == null ? 0 : message.payloadSize());
        }

        byte[] encode(PacketCodec codec, Function<MqttMessage, byte[]> payloadOf) {
            if (message == null) {
                return header;
            }
            return codec.encodePublish(currentHeader(), payloadOf.apply(message));
        }

        /// The PUBLISH header as this attempt sends it, with DUP set once the transport has
        /// accepted an earlier attempt. The sole sender has returned from every earlier attempt
        /// before encoding this one, even if its queue admission preceded the old transport's
        /// send return.
        byte[] currentHeader() {
            if (receipt == null || !receipt.accepted()) {
                return header;
            }
            byte[] marked = header.clone();
            marked[0] |= 0x08;
            return marked;
        }
    }

    private record Write(Connection connection, List<SendQueue.Send<Outbound>> sends) {
    }

    /// What one write carries: one array, or a PUBLISH header and its payload, which the
    /// transport writes one after the other.
    private sealed interface Encoded {

        Transport.SendResult writeTo(Transport transport, long ticket);

        record Whole(byte[] bytes) implements Encoded {

            // Writing here saves handing the write to a thread of the transport's own.
            @Override
            public Transport.SendResult writeTo(Transport transport, long ticket) {
                return transport instanceof BlockingWriteTransport direct
                        ? direct.write(ticket, bytes)
                        : transport.send(ticket, bytes);
            }
        }

        record Apart(byte[] header, byte[] payload) implements Encoded {

            // Only a transport that lets the sender write takes the parts apart; see encode.
            @Override
            public Transport.SendResult writeTo(Transport transport, long ticket) {
                return ((BlockingWriteTransport) transport).write(ticket, header, payload);
            }
        }
    }
}
