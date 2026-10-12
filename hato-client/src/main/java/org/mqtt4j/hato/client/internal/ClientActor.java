package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.AckMode;
import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.ConnectionListener;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttDelivery;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttNotConnectedException;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.PublishResult;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReceiveResult;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.log.Log;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.spi.TimerScheduler;
import org.mqtt4j.hato.spi.Transport;

import static java.util.Objects.requireNonNull;

/// Serializes protocol decisions, isolating application continuations and listeners from I/O.
/// API methods run on their callers; the core runs on the actor. A separate sender prepares
/// publication payloads and starts writes without blocking protocol decisions.
public final class ClientActor implements MqttClient {

    private static final Log log = Log.of(ClientActor.class);

    private static final int OPERATION_LIMIT = 64;

    private static final int NOTIFICATION_LIMIT = 64;

    // A burst, such as retained messages on subscribe, can arrive faster than the actor routes
    // it, so most of it may wait here. This count bounds per-packet overhead (about 190 bytes
    // each when measured); the byte limit bounds the packets themselves.
    private static final int MAILBOX_INCOMING_LIMIT = 65_536;

    private static final int MAILBOX_CONTROL_LIMIT = 256;

    private static final int MAILBOX_COMMAND_BYTE_LIMIT = 32 * 1024 * 1024;

    private static final int RECEIVE_BYTE_LIMIT = 32 * 1024 * 1024;

    // Virtual threads are not preempted, so a busy actor keeps its carrier until it blocks. With
    // a single carrier, the sender, the transport's workers, and delivery lanes would wait out a
    // whole receive backlog, and a PINGREQ queued in time would not be written in time. Parking
    // after this much continuous work lets them run; Thread.yield() alone did not when measured
    // with one carrier. With 250 microseconds, a one-carrier burst of small publications stayed
    // within the default queue of a fast handler; with one millisecond, it overflowed. Real time
    // measures carrier use, which TimerScheduler time cannot express.
    private static final long CARRIER_SLICE_NANOS = 250_000;

    private final URI endpoint;

    private final String clientId;

    private final TimerScheduler scheduler;

    private final ConnectionListener listener;

    private final SessionCore core;

    private final ClientIo io;

    private final Mailbox mailbox;

    private final DeliveryLanes lanes;

    // Published under lifecycleLock before admitting commands for an established connection.
    private long connectedGeneration;

    private long openedGeneration;

    // Admission and stopping share this lock so shutdown cannot miss a registered operation.
    private final Object lifecycleLock = new Object();

    // One background removal per registered subscription, guarded by lifecycleLock. These
    // protocol operations have no public future and consume no public operation/completion slot.
    private final Map<Long, UnsubscribeAttempt> closingAttempts = new LinkedHashMap<>();

    private final Semaphore operationSlots = new Semaphore(OPERATION_LIMIT);

    private final Semaphore notificationSlots = new Semaphore(NOTIFICATION_LIMIT);

    private final Map<Long, Operation<?>> operations = new ConcurrentHashMap<>();

    private final AtomicLong sequence = new AtomicLong();

    private final AtomicBoolean stopping = new AtomicBoolean();

    // Fatal I/O failures bypass mailbox capacity and survive connection generation changes.
    private @Nullable Throwable fatalFailure;

    private final ArrayDeque<Disconnection> notifications = new ArrayDeque<>();

    private boolean notifying;

    public ClientActor(
            URI endpoint,
            String clientId,
            Duration keepAlive,
            Duration timeout,
            int maxPacketSize,
            TimerScheduler scheduler,
            ConnectionListener listener,
            Transport.Factory transportFactory,
            boolean cleanStart
    ) {
        this(
                endpoint,
                clientId,
                keepAlive,
                timeout,
                maxPacketSize,
                scheduler,
                listener,
                transportFactory,
                PacketCodec.of(MqttVersion.V3_1_1),
                cleanStart
        );
    }

    ClientActor(
            URI endpoint,
            String clientId,
            Duration keepAlive,
            Duration timeout,
            int maxPacketSize,
            TimerScheduler scheduler,
            ConnectionListener listener,
            Transport.Factory transportFactory,
            PacketCodec codec,
            boolean cleanStart
    ) {
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.scheduler = scheduler;
        this.listener = listener;
        mailbox = createMailbox(maxPacketSize);
        lanes = new DeliveryLanes(
                clientId,
                receiveByteLimit(maxPacketSize),
                this::deliveryFailed,
                this::deliveryReady
        );
        io = new ClientIo(
                endpoint,
                clientId,
                timeout,
                maxPacketSize,
                transportFactory,
                new IoEvents(),
                codec
        );
        core = new SessionCore(new CorePort(), clientId, timeout, keepAlive, cleanStart);
        Thread.ofVirtual().name(threadName("actor")).start(this::run);
    }

    @Override
    public CompletableFuture<ConnAck> connect() {
        return submit(true, false, 0, token -> {
            long now;
            try {
                now = scheduler.nanoTime();
            } catch (RuntimeException failure) {
                completeOperation(token, null, new MqttConnectException(
                        "Cannot read connection start time", failure
                ));
                return;
            }
            core.connect(token, now);
        }, null);
    }

    @Override
    public CompletableFuture<Void> disconnect() {
        return submit(false, false, 0, core::disconnect, null);
    }

    @Override
    public CompletableFuture<PublishResult> publish(MqttMessage message) {
        requireNonNull(message, "message");
        return submit(false, true, message.payloadSize(),
                token -> core.publish(token, message), null);
    }

    @Override
    public CompletableFuture<Subscription> subscribe(
            Subscribe request,
            Consumer<MqttDelivery> handler
    ) {
        requireNonNull(request, "request");
        requireNonNull(handler, "handler");
        return subscribeInternal(request, handler);
    }

    @Override
    public CompletableFuture<Subscription> subscribe(Subscribe request) {
        requireNonNull(request, "request");
        if (request.ackMode().orElse(AckMode.ON_RECEIVE) == AckMode.AFTER_HANDLER) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "A pull subscription has no handler to acknowledge"
            ));
        }
        return subscribeInternal(request, null);
    }

    private CompletableFuture<Subscription> subscribeInternal(
            Subscribe request,
            @Nullable Consumer<MqttDelivery> handler
    ) {
        if (request.qos() == Qos.EXACTLY_ONCE) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("QoS 2 subscribe is not supported")
            );
        }
        return submit(false, true, 0, token -> {
            // The operation token is also the subscription id in the core and lanes.
            requireNonNull(operations.get(token)).subscription =
                    new ClientSubscription(token, request, handler);
            core.subscribe(token, request);
        }, null);
    }

    private <T> CompletableFuture<T> submit(
            boolean connect,
            boolean needsConnection,
            int bytes,
            LongConsumer command,
            @Nullable UnsubscribeAttempt unsubscribe
    ) {
        synchronized (lifecycleLock) {
            Throwable refusal = null;
            if (stopping.get()) {
                refusal = new IllegalStateException("Client is closed");
            } else if (needsConnection && connectedGeneration == 0) {
                refusal = new MqttNotConnectedException();
            } else if (!operationSlots.tryAcquire()) {
                refusal = new MqttCapacityException("Operation capacity");
            } else if (connect && !notificationSlots.tryAcquire()) {
                operationSlots.release();
                refusal = new MqttCapacityException("Listener capacity");
            }
            if (refusal != null) {
                if (unsubscribe != null) {
                    unsubscribe.finish(refusal);
                }
                return CompletableFuture.failedFuture(refusal);
            }

            long token = sequence.incrementAndGet();
            long generation = connectedGeneration;
            var operation = new Operation<T>(connect, unsubscribe);
            operations.put(token, operation);
            Mailbox.OfferResult admitted = mailbox.offer(new Mailbox.Event(
                    0,
                    () -> {
                        if (needsConnection) {
                            synchronized (lifecycleLock) {
                                if (generation != connectedGeneration) {
                                    completeOperation(token, null,
                                            new MqttConnectionLostException());
                                    return;
                                }
                            }
                        }
                        command.accept(token);
                    },
                    Mailbox.Kind.COMMAND,
                    bytes
            ));
            if (admitted == Mailbox.OfferResult.FULL) {
                completeOperation(token, null,
                        new MqttCapacityException("Command mailbox capacity"));
            }
            return operation.future;
        }
    }

    @Override
    public void close() {
        synchronized (lifecycleLock) {
            stopping.set(true);
            mailbox.wake();
        }
    }

    private void run() {
        Throwable failure = null;
        try {
            Optional<Mailbox.Event> next;
            long busySince = System.nanoTime();
            while ((next = mailbox.take(stopping::get)).isPresent()) {
                Mailbox.Event event = next.orElseThrow();
                try {
                    event.action().run();
                } catch (SpiException cause) {
                    core.failed(cause.generation(), requireNonNull(cause.getCause()));
                }
                busySince = shareCarrier(busySince);
            }
        } catch (Throwable cause) {
            failure = cause;
        } finally {
            synchronized (lifecycleLock) {
                stopping.set(true);
                if (fatalFailure != null) {
                    failure = fatalFailure;
                }
            }
            try {
                try {
                    if (failure != null) {
                        log.error("Client failed for {} at {}", clientId, endpoint, failure);
                    }
                } finally {
                    core.close(failure);
                }
            } catch (Throwable cleanupFailure) {
                log.error("Shutdown failed for {} at {}", clientId, endpoint, cleanupFailure);
            } finally {
                try {
                    lanes.close(failure);
                    finishClosures(failure);
                    mailbox.discard(0);
                    Throwable cause = failure;
                    // The core has completed started operations; the remainder never started.
                    operations.keySet().forEach(token -> completeOperation(
                            token,
                            null,
                            (cause == null)
                                    ? new IllegalStateException("Client is closed")
                                    : new IllegalStateException("Client is closed", cause)
                    ));
                } finally {
                    io.shutdown();
                }
            }
        }
    }

    /// Returns when the current slice of continuous work began.
    private long shareCarrier(long busySince) {
        long now = System.nanoTime();
        if (now - busySince < CARRIER_SLICE_NANOS) {
            return busySince;
        }
        if (mailbox.size() == 0) {
            // The actor blocks next, which frees the carrier without parking.
            return now;
        }
        LockSupport.parkNanos(1);
        return System.nanoTime();
    }

    private void deliveryReady() {
        post(
                0,
                () -> core.deliveriesReady(lanes.takeReady()),
                Mailbox.Kind.DELIVERY_READY,
                0
        );
    }

    /// Caller holds lifecycleLock. Local closure has its own attempt even during unsubscribe,
    /// so a failed public removal cannot restore the permanently closed intent.
    private void requestClose(ClientSubscription subscription) {
        if (stopping.get() || subscription.retired) {
            return;
        }
        UnsubscribeAttempt current = subscription.unsubscription;
        if (current != null && current.background && !current.finished) {
            return;
        }
        var attempt = new UnsubscribeAttempt(subscription.id, true);
        subscription.unsubscription = attempt;
        closingAttempts.put(sequence.incrementAndGet(), attempt);
        post(0, this::startClosures, Mailbox.Kind.SUBSCRIPTION_CLOSE, 0);
    }

    private void startClosures() {
        var starting = new ArrayList<Map.Entry<Long, UnsubscribeAttempt>>();
        synchronized (lifecycleLock) {
            if (stopping.get()) {
                return;
            }
            for (var entry : closingAttempts.entrySet()) {
                UnsubscribeAttempt attempt = entry.getValue();
                if (!attempt.started) {
                    attempt.started = true;
                    starting.add(Map.entry(entry.getKey(), attempt));
                }
            }
        }
        for (var entry : starting) {
            core.closeSubscription(entry.getKey(), entry.getValue().subscriptionId);
        }
        core.sendCapacityAvailable(openedGeneration);
    }

    /// Also covers closures whose coalesced wake has not reached the core before termination.
    private void finishClosures(@Nullable Throwable cause) {
        synchronized (lifecycleLock) {
            var failure = (cause == null)
                    ? new MqttConnectionLostException()
                    : new MqttConnectionLostException(cause);
            for (long token : List.copyOf(closingAttempts.keySet())) {
                completeOperation(token, null, failure);
            }
        }
    }

    private void post(long generation, Runnable action, Mailbox.Kind kind, int bytes) {
        if (stopping.get()) {
            return;
        }
        Mailbox.OfferResult admitted = mailbox.offer(
                new Mailbox.Event(generation, action, kind, bytes)
        );
        if (admitted == Mailbox.OfferResult.FULL) {
            String admission = switch (kind) {
                case COMMAND -> "Command";
                case INCOMING -> "Incoming";
                case KEEP_ALIVE -> "Keep-alive";
                case CONTROL, DELIVERY_READY, SEND_CAPACITY, SUBSCRIPTION_CLOSE -> "Control";
            };
            signalFailure(generation, new MqttCapacityException(admission + " mailbox capacity"));
        }
    }

    private void signalFailure(long generation, Throwable failure) {
        terminate(generation, () -> core.failed(generation, failure));
    }

    private boolean deliveryFailed(DeliveryToken token, Throwable failure) {
        // Reject retired sessions before they can occupy the current connection's failure slot.
        // A closing client keeps the failed lane paused until shutdown removes it.
        return mailbox.terminateSession(token.session(), () -> core.deliveryFailed(token, failure))
                || stopping.get();
    }

    private boolean terminate(long generation, Runnable action) {
        if (!stopping.get()) {
            return mailbox.terminate(new Mailbox.Event(
                    generation,
                    action,
                    Mailbox.Kind.CONTROL,
                    0
            ));
        }
        return false;
    }

    /// Called by an API caller or the actor; futures complete on independent threads.
    private void completeOperation(
            long token,
            @Nullable Object result,
            @Nullable Throwable failure
    ) {
        Operation<?> operation = operations.remove(token);
        if (operation == null) {
            // Background closures have no public operation or future.
            synchronized (lifecycleLock) {
                UnsubscribeAttempt closing = closingAttempts.remove(token);
                if (closing != null) {
                    closing.finish(failure);
                }
            }
            return;
        }
        if (result instanceof ConnAck) {
            synchronized (lifecycleLock) {
                connectedGeneration = openedGeneration;
            }
            log.info("Connected {} to {}", clientId, endpoint);
        }
        if (failure != null) {
            ClientSubscription subscription = operation.subscription;
            if (subscription != null) {
                retire(subscription, null);
            }
        }
        if (operation.unsubscribe != null) {
            operation.unsubscribe.finish(failure);
        }
        if (operation.notificationReserved) {
            notificationSlots.release();
        }
        Thread.ofVirtual().name(threadName("complete-" + token)).start(() -> {
            try {
                operation.finish(result, failure);
            } finally {
                operationSlots.release();
            }
        });
    }

    private void notifyListener() {
        while (true) {
            Disconnection event;
            synchronized (notifications) {
                event = notifications.pollFirst();
                if (event == null) {
                    notifying = false;
                    return;
                }
            }
            try {
                listener.onDisconnected(event);
            } catch (Throwable t) {
                log.warn("Connection listener failed for {} at {}", clientId, endpoint, t);
            } finally {
                notificationSlots.release();
            }
        }
    }

    private String threadName(String role) {
        return "hato-" + clientId + "-" + role;
    }

    private long nanoTime(long generation) {
        try {
            return scheduler.nanoTime();
        } catch (RuntimeException failure) {
            throw new SpiException(generation, failure);
        }
    }

    /// What the client holds against its internal bounds. Tests compare this with the bounds
    /// until applications get statistics. The parts are read one after another, not atomically.
    Usage usage() {
        return new Usage(
                OPERATION_LIMIT - operationSlots.availablePermits(),
                NOTIFICATION_LIMIT - notificationSlots.availablePermits(),
                mailbox.size(),
                mailbox.bytes(),
                io.sends(),
                io.sendBytes(),
                lanes.retainedLanes(),
                lanes.retainedDeliveries(),
                lanes.retainedBytes()
        );
    }

    static Mailbox createMailbox(int maxPacketSize) {
        // Operation slots are taken before admission, so they also bound queued commands.
        return new Mailbox(
                OPERATION_LIMIT,
                MAILBOX_COMMAND_BYTE_LIMIT,
                MAILBOX_INCOMING_LIMIT,
                receiveByteLimit(maxPacketSize),
                MAILBOX_CONTROL_LIMIT
        );
    }

    /// An empty mailbox or set of lanes must admit one packet up to the configured maximum.
    private static long receiveByteLimit(int maxPacketSize) {
        return Math.max(RECEIVE_BYTE_LIMIT, maxPacketSize);
    }

    /// Called only by the core on the actor thread. Owns the actor's timer registrations.
    private final class CorePort implements SessionCore.Port {

        private final Map<SessionCore.Timer, TimerEntry> timers = new EnumMap<>(
                SessionCore.Timer.class
        );

        // Warn once per client: a broker that repeats this quirk must not flood the log.
        private boolean unsolicitedPingResponseLogged;

        @Override
        public void open(long generation, long token) {
            // Transfer the reserved notification slot from the operation to the connection.
            // A failed attempt releases it in end(); an established one in notifyListener().
            requireNonNull(operations.get(token)).notificationReserved = false;
            openedGeneration = generation;
            mailbox.activateSession(core.sessionNumber());
            mailbox.activate(generation);
            io.open(generation);
        }

        @Override
        public SendQueue.OfferResult send(
                long generation,
                long ticket,
                Packet packet,
                @Nullable SendReceipt receipt
        ) {
            return io.send(generation, ticket, packet, receipt);
        }

        @Override
        public SendQueue.OfferResult publish(
                long generation,
                long ticket,
                MqttMessage message,
                int packetId,
                @Nullable SendReceipt receipt
        ) {
            return io.publish(generation, ticket, message, packetId, receipt);
        }

        @Override
        public void schedule(long generation, SessionCore.Timer timer, Duration delay) {
            cancel(timer);
            var entry = new TimerEntry();
            timers.put(timer, entry);
            Mailbox.Kind kind = switch (timer) {
                case KEEP_ALIVE, PING -> Mailbox.Kind.KEEP_ALIVE;
                case CONNECT, DISCONNECT -> Mailbox.Kind.CONTROL;
            };
            try {
                entry.cancellation = scheduler.schedule(delay, () -> post(
                        generation,
                        () -> {
                            if (timers.get(timer) == entry) {
                                timers.remove(timer);
                                core.timer(generation, timer, nanoTime(generation));
                            }
                        },
                        kind,
                        0
                ));
            } catch (RuntimeException failure) {
                timers.remove(timer);
                throw new SpiException(generation, failure);
            }
        }

        @Override
        public void cancel(SessionCore.Timer timer) {
            TimerEntry entry = timers.remove(timer);
            if (entry != null && entry.cancellation != null) {
                try {
                    entry.cancellation.cancel();
                } catch (Throwable t) {
                    log.warn("Timer cancellation failed for {}", clientId, t);
                }
            }
        }

        @Override
        public void complete(long token, @Nullable Object result, @Nullable Throwable failure) {
            completeOperation(token, result, failure);
        }

        @Override
        public Optional<Subscription> registerSubscription(long id) {
            ClientSubscription subscription = requireNonNull(
                    requireNonNull(operations.get(id)).subscription
            );
            if (subscription.handler == null) {
                subscription.receiver = lanes.registerPull(id, subscription.request).orElse(null);
                return subscription.receiver == null ? Optional.empty() : Optional.of(subscription);
            }
            return lanes.register(id, subscription.request, subscription.handler)
                    ? Optional.of(subscription) : Optional.empty();
        }

        @Override
        public void subscribed(Subscription handle, Qos granted) {
            ((ClientSubscription) handle).granted = granted;
        }

        @Override
        public boolean deliver(
                DeliveryToken token,
                List<Long> ids,
                MqttMessage message,
                boolean dup,
                int bytes
        ) {
            return lanes.deliver(token, ids, message, dup, bytes);
        }

        @Override
        public void releaseDelivery(long token) {
            lanes.release(token);
        }

        @Override
        public void forgetDelivery(long token) {
            lanes.forget(token);
        }

        @Override
        public Map<Long, Boolean> receiveState() {
            return lanes.receiveState();
        }

        @Override
        public boolean readyDelivery(long token) {
            return lanes.isReady(token);
        }

        @Override
        public void sessionChanged(long session) {
            mailbox.activateSession(session);
            lanes.sessionDiscarded();
        }

        @Override
        public void removeSubscription(long id, Subscription handle, @Nullable Throwable cause) {
            retire((ClientSubscription) handle, cause);
        }

        @Override
        public void ignoredPingResponse() {
            if (!unsolicitedPingResponseLogged) {
                unsolicitedPingResponseLogged = true;
                log.warn("Ignoring PINGRESP without an outstanding PINGREQ for {} at {}",
                        clientId, endpoint);
            }
        }

        @Override
        public Map<Long, Boolean> end(
                long generation,
                boolean established,
                Disconnection disconnection,
                boolean preserveSession,
                boolean stopped
        ) {
            Throwable cause = disconnection.cause().orElse(null);
            synchronized (lifecycleLock) {
                connectedGeneration = 0;
                var failure = cause == null
                        ? new MqttConnectionLostException()
                        : new MqttConnectionLostException(cause);
                for (UnsubscribeAttempt closing : closingAttempts.values()) {
                    closing.completeObservers(failure);
                }
            }
            mailbox.discard(generation);
            // Reject old failures before releasing their lane pause; otherwise a late failure
            // can pause the lane again with a terminal notification that discard would erase.
            Map<Long, Boolean> handed = lanes.connectionEnded(preserveSession, stopped);
            io.close();
            if (established) {
                log.info("Disconnected {} from {}", clientId, endpoint, cause);
                synchronized (notifications) {
                    notifications.addLast(disconnection);
                    if (!notifying) {
                        notifying = true;
                        Thread.ofVirtual()
                                .name(threadName("listener"))
                                .start(ClientActor.this::notifyListener);
                    }
                }
            } else {
                notificationSlots.release();
                log.info(
                        "Connection failed for {} at {}",
                        clientId, endpoint,
                        cause
                );
            }
            return handed;
        }
    }

    /// I/O callbacks post events; fatal failures stop the actor independently of its mailbox.
    private final class IoEvents implements ClientIo.Events {

        @Override
        public void connected(long generation) {
            post(generation, () -> core.opened(generation), Mailbox.Kind.CONTROL, 0);
        }

        @Override
        public void pingResponse(long generation, Packet packet, int size) {
            post(
                    generation,
                    () -> core.received(generation, packet, nanoTime(generation), size),
                    Mailbox.Kind.KEEP_ALIVE,
                    0
            );
        }

        @Override
        public void received(long generation, Packet packet, int size) {
            post(
                    generation,
                    () -> core.received(generation, packet, nanoTime(generation), size),
                    Mailbox.Kind.INCOMING,
                    size
            );
        }

        @Override
        public Runnable prepareSent(long generation, long ticket) {
            long sentAt;
            try {
                sentAt = scheduler.nanoTime();
            } catch (RuntimeException failure) {
                return () -> signalFailure(generation, failure);
            }
            return () -> post(
                    generation,
                    () -> core.sent(generation, ticket, sentAt),
                    Mailbox.Kind.CONTROL,
                    0
            );
        }

        @Override
        public void written(long generation, long ticket) {
            post(generation, () -> core.written(generation, ticket), Mailbox.Kind.CONTROL, 0);
        }

        @Override
        public void sendCapacityAvailable(long generation) {
            post(
                    generation,
                    () -> core.sendCapacityAvailable(generation),
                    Mailbox.Kind.SEND_CAPACITY,
                    0
            );
        }

        @Override
        public void ended(long generation, Throwable cause) {
            // FIFO termination keeps preceding input and write completions, such as a final
            // CONNACK refusal that arrived just before EOF.
            if (!stopping.get()) {
                mailbox.offerTermination(new Mailbox.Event(
                        generation,
                        () -> core.failed(generation, cause),
                        Mailbox.Kind.CONTROL,
                        0
                ));
            }
        }

        @Override
        public void failed(long generation, Throwable cause) {
            signalFailure(generation, cause);
        }

        @Override
        public void fatal(Throwable failure) {
            synchronized (lifecycleLock) {
                if (fatalFailure == null) {
                    fatalFailure = failure;
                }
                stopping.set(true);
                mailbox.wake();
            }
        }
    }

    /// Operation and notification slots stay held until application continuations and listeners
    /// return; lanes and deliveries until handlers return and acknowledgements are released.
    record Usage(
            int operations,
            int notifications,
            int mailboxEvents,
            long mailboxBytes,
            int sends,
            long sendBytes,
            int lanes,
            int deliveries,
            long deliveryBytes
    ) {
    }

    private static final class TimerEntry {

        TimerScheduler.@Nullable Cancellable cancellation;
    }

    private void retire(ClientSubscription subscription, @Nullable Throwable cause) {
        synchronized (lifecycleLock) {
            subscription.retired = true;
            lanes.remove(subscription.id, cause);
        }
    }

    private final class ClientSubscription implements Subscription {

        private final long id;

        private final Subscribe request;

        private final @Nullable Consumer<MqttDelivery> handler;

        // Assigned before SUBSCRIBE is sent and published with the completed subscription future.
        private DeliveryLanes.@Nullable Receiver receiver;

        private volatile Qos granted = Qos.AT_MOST_ONCE;

        // Attempt state belongs to the client, never to a caller-mutable future.
        private @Nullable UnsubscribeAttempt unsubscription;

        private boolean locallyClosed;

        private boolean retired;

        ClientSubscription(long id, Subscribe request, @Nullable Consumer<MqttDelivery> handler) {
            this.id = id;
            this.request = request;
            this.handler = handler;
        }

        @Override
        public TopicFilter filter() {
            return request.filter();
        }

        @Override
        public Qos grantedQos() {
            return granted;
        }

        @Override
        public ReceiveResult receive() throws InterruptedException {
            return pullReceiver().receive();
        }

        @Override
        public ReceiveResult receive(Duration timeout) throws InterruptedException {
            return pullReceiver().receive(timeout);
        }

        private DeliveryLanes.Receiver pullReceiver() {
            DeliveryLanes.Receiver receiving = receiver;
            if (receiving == null) {
                throw new UnsupportedOperationException("This subscription has a handler");
            }
            return receiving;
        }

        @Override
        public CompletableFuture<Void> unsubscribe() {
            synchronized (lifecycleLock) {
                if (stopping.get() || retired) {
                    // Discards deliveries still queued here; a running handler finishes.
                    lanes.remove(id);
                    return CompletableFuture.completedFuture(null);
                }

                UnsubscribeAttempt current = unsubscription;
                if (current != null && !current.finished) {
                    // Each observer uses the normal operation/completion budget, but does
                    // not initiate another wire request or affect its rollback on refusal.
                    return submit(false, true, 0, current::observe, null);
                }

                var attempt = new UnsubscribeAttempt(id, false);
                unsubscription = attempt;
                lanes.pause(id);
                return submit(false, true, 0, token -> core.unsubscribe(token, id), attempt);
            }
        }

        @Override
        public void close() {
            synchronized (lifecycleLock) {
                if (locallyClosed) {
                    return;
                }
                locallyClosed = true;
                // Stop locally before scheduling the independently bounded broker removal.
                // A handed-off MANUAL delivery retains its own acknowledgement obligation.
                lanes.remove(id);
                requestClose(this);
            }
        }
    }

    /// One wire exchange, with individually admitted public observers. Guarded by lifecycleLock.
    private final class UnsubscribeAttempt {

        private final long subscriptionId;

        private final boolean background;

        private final List<Long> observers = new ArrayList<>();

        private boolean finished;

        // Set once startClosures hands this background attempt to the core.
        private boolean started;

        private @Nullable Throwable failure;

        UnsubscribeAttempt(long subscriptionId, boolean background) {
            this.subscriptionId = subscriptionId;
            this.background = background;
        }

        void observe(long token) {
            synchronized (lifecycleLock) {
                if (finished) {
                    completeOperation(token, null, failure);
                } else {
                    observers.add(token);
                }
            }
        }

        void finish(@Nullable Throwable cause) {
            synchronized (lifecycleLock) {
                if (cause != null) {
                    lanes.resume(subscriptionId);
                }
                failure = cause;
                finished = true;
                completeObservers(cause);
            }
        }

        /// Caller holds lifecycleLock. Public observations end with their connection; the
        /// background removal itself can survive until a later clean CONNACK confirms it.
        void completeObservers(@Nullable Throwable cause) {
            for (long token : observers) {
                completeOperation(token, null, cause);
            }
            observers.clear();
        }
    }

    private static final class Operation<T> {

        final CompletableFuture<T> future = new CompletableFuture<>();
        volatile boolean notificationReserved;

        final @Nullable UnsubscribeAttempt unsubscribe;

        @Nullable ClientSubscription subscription;

        Operation(boolean notificationReserved, @Nullable UnsubscribeAttempt unsubscribe) {
            this.notificationReserved = notificationReserved;
            this.unsubscribe = unsubscribe;
        }

        @SuppressWarnings("unchecked") // Tokens retain their operation's result type in the core.
        void finish(@Nullable Object result, @Nullable Throwable failure) {
            if (failure == null) {
                future.complete((T) result);
            } else {
                future.completeExceptionally(failure);
            }
        }
    }
}
