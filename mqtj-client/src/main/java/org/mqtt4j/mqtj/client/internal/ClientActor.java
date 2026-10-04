package org.mqtt4j.mqtj.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.mqtj.ConnAck;
import org.mqtt4j.mqtj.ConnectionListener;
import org.mqtt4j.mqtj.Disconnection;
import org.mqtt4j.mqtj.MqttCapacityException;
import org.mqtt4j.mqtj.MqttClient;
import org.mqtt4j.mqtj.MqttConnectException;
import org.mqtt4j.mqtj.MqttConnectionLostException;
import org.mqtt4j.mqtj.MqttDelivery;
import org.mqtt4j.mqtj.MqttMessage;
import org.mqtt4j.mqtj.MqttNotConnectedException;
import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.PublishResult;
import org.mqtt4j.mqtj.Qos;
import org.mqtt4j.mqtj.Subscribe;
import org.mqtt4j.mqtj.Subscription;
import org.mqtt4j.mqtj.TopicFilter;
import org.mqtt4j.mqtj.client.internal.common.io.SendQueue;
import org.mqtt4j.mqtj.client.internal.common.log.Log;
import org.mqtt4j.mqtj.client.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.mqtj.spi.TimerScheduler;
import org.mqtt4j.mqtj.spi.Transport;

import static java.util.Objects.requireNonNull;

/// Serializes protocol decisions, isolating application continuations and listeners from I/O.
/// API methods run on their callers; the core runs on the actor. A separate sender prepares
/// publication payloads and starts writes without blocking protocol decisions.
public final class ClientActor implements MqttClient {

    private static final Log log = Log.of(ClientActor.class);

    private static final int OPERATION_LIMIT = 64;

    private static final int NOTIFICATION_LIMIT = 64;

    private static final int MAILBOX_INCOMING_LIMIT = 256;

    private static final int MAILBOX_CONTROL_LIMIT = 256;

    private static final int MAILBOX_COMMAND_BYTE_LIMIT = 32 * 1024 * 1024;

    private static final int RECEIVE_BYTE_LIMIT = 32 * 1024 * 1024;

    private final URI endpoint;

    private final String clientId;

    private final TimerScheduler scheduler;

    private final ConnectionListener listener;

    private final SessionCore core;

    private final ClientIo io;

    private final Mailbox mailbox;

    private final DeliveryLanes lanes;

    private final Map<Long, PushSubscription> subscriptions = new ConcurrentHashMap<>();

    // Published under lifecycleLock before admitting commands for an established connection.
    private long connectedGeneration;

    private long openedGeneration;

    // Admission and stopping share this lock so shutdown cannot miss a registered operation.
    private final Object lifecycleLock = new Object();

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
            Transport.Factory transportFactory
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
                PacketCodec.of(MqttVersion.V3_1_1)
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
            PacketCodec codec
    ) {
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.scheduler = scheduler;
        this.listener = listener;
        mailbox = createMailbox(maxPacketSize);
        lanes = new DeliveryLanes(
                clientId,
                receiveByteLimit(maxPacketSize),
                this::signalFailure,
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
        core = new SessionCore(new CorePort(), clientId, timeout, keepAlive);
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
        if (message.qos() == Qos.EXACTLY_ONCE) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("QoS 2 publish is not supported")
            );
        }
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
        if (request.qos() == Qos.EXACTLY_ONCE) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("QoS 2 subscribe is not supported")
            );
        }
        return submit(false, true, 0, token -> {
            // The operation token is also the subscription id in the core and lanes.
            subscriptions.put(token, new PushSubscription(token, request, handler));
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
            while ((next = mailbox.take(stopping::get)).isPresent()) {
                Mailbox.Event event = next.orElseThrow();
                try {
                    event.action().run();
                } catch (SpiException cause) {
                    core.failed(cause.generation(), requireNonNull(cause.getCause()));
                }
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
                    lanes.close();
                    subscriptions.clear();
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

    private void deliveryReady(long generation) {
        post(
                generation,
                () -> core.deliveriesReady(generation, lanes.takeReady(generation)),
                Mailbox.Kind.DELIVERY_READY,
                0
        );
    }

    private void post(long generation, Runnable action, Mailbox.Kind kind, int bytes) {
        if (stopping.get()) {
            return;
        }
        Mailbox.OfferResult admitted = mailbox.offer(
                new Mailbox.Event(generation, action, kind, bytes)
        );
        if (admitted == Mailbox.OfferResult.FULL) {
            signalFailure(generation, new MqttCapacityException(
                    (kind == Mailbox.Kind.INCOMING ? "Incoming" : "Control") + " mailbox capacity"
            ));
        }
    }

    private void signalFailure(long generation, Throwable failure) {
        if (!stopping.get()) {
            mailbox.terminate(new Mailbox.Event(
                    generation,
                    () -> core.failed(generation, failure),
                    Mailbox.Kind.CONTROL,
                    0
            ));
        }
    }

    /// Called by an API caller or the actor; futures complete on independent threads.
    private void completeOperation(
            long token,
            @Nullable Object result,
            @Nullable Throwable failure
    ) {
        Operation<?> operation = operations.remove(token);
        if (operation == null) {
            return;
        }
        if (result instanceof ConnAck) {
            synchronized (lifecycleLock) {
                connectedGeneration = openedGeneration;
            }
            log.info("Connected {} to {}", clientId, endpoint);
        }
        if (failure != null) {
            PushSubscription subscription = subscriptions.remove(token);
            if (subscription != null) {
                lanes.remove(token);
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
        return "mqtj-" + clientId + "-" + role;
    }

    private long nanoTime(long generation) {
        try {
            return scheduler.nanoTime();
        } catch (RuntimeException failure) {
            throw new SpiException(generation, failure);
        }
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
            mailbox.activate(generation);
            io.open(generation);
        }

        @Override
        public SendQueue.OfferResult send(long generation, long ticket, Packet packet) {
            return io.send(generation, ticket, packet);
        }

        @Override
        public SendQueue.OfferResult publish(
                long generation,
                long ticket,
                MqttMessage message,
                int packetId
        ) {
            return io.publish(generation, ticket, message, packetId);
        }

        @Override
        public void schedule(long generation, SessionCore.Timer timer, Duration delay) {
            cancel(timer);
            var entry = new TimerEntry();
            timers.put(timer, entry);
            try {
                entry.cancellation = scheduler.schedule(delay, () -> post(
                        generation,
                        () -> {
                            if (timers.get(timer) == entry) {
                                timers.remove(timer);
                                core.timer(generation, timer, nanoTime(generation));
                            }
                        },
                        Mailbox.Kind.CONTROL,
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
        public boolean registerSubscription(long generation, long id) {
            PushSubscription subscription = requireNonNull(subscriptions.get(id));
            return lanes.register(generation, id, subscription.request, subscription.handler);
        }

        @Override
        public Subscription subscribed(long id, Qos granted) {
            PushSubscription subscription = requireNonNull(subscriptions.get(id));
            subscription.granted = granted;
            return subscription;
        }

        @Override
        public boolean deliver(
                long generation,
                long token,
                List<Long> ids,
                MqttMessage message,
                boolean dup,
                int bytes
        ) {
            return lanes.deliver(generation, token, ids, message, dup, bytes);
        }

        @Override
        public void releaseDelivery(long token) {
            lanes.release(token);
        }

        @Override
        public void removeSubscription(long id) {
            lanes.remove(id);
            subscriptions.remove(id);
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
        public void end(long generation, boolean established, Disconnection disconnection) {
            synchronized (lifecycleLock) {
                connectedGeneration = 0;
            }
            lanes.close();
            subscriptions.clear();
            mailbox.discard(generation);
            io.close();
            if (established) {
                log.info("Disconnected {} from {}", clientId, endpoint, disconnection.cause());
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
                        disconnection.cause()
                );
            }
        }
    }

    /// I/O callbacks post events; fatal failures stop the actor independently of its mailbox.
    private final class IoEvents implements ClientIo.Events {

        @Override
        public void connected(long generation) {
            post(generation, () -> core.opened(generation), Mailbox.Kind.CONTROL, 0);
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

    private static final class TimerEntry {

        TimerScheduler.@Nullable Cancellable cancellation;
    }

    private final class PushSubscription implements Subscription {

        private final long id;

        private final Subscribe request;

        private final Consumer<MqttDelivery> handler;

        private volatile Qos granted = Qos.AT_MOST_ONCE;

        // Attempt state belongs to the client, never to a caller-mutable future.
        private @Nullable UnsubscribeAttempt unsubscription;

        PushSubscription(long id, Subscribe request, Consumer<MqttDelivery> handler) {
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
        public CompletableFuture<Void> unsubscribe() {
            synchronized (lifecycleLock) {
                // Clean-session termination removes the handle from this map, and a client
                // shutdown request makes removal local. Older attempts retain their result.
                if (stopping.get() || subscriptions.get(id) != this) {
                    // Discards deliveries still queued here; a running handler finishes.
                    lanes.remove(id);
                    return CompletableFuture.completedFuture(null);
                }

                UnsubscribeAttempt current = unsubscription;
                if (current != null && !current.finished) {
                    // Each observer uses the normal operation/completion budget, but does
                    // not initiate another wire request or affect its rollback on refusal.
                    return submit(false, false, 0, current::observe, null);
                }

                var attempt = new UnsubscribeAttempt(id);
                unsubscription = attempt;
                lanes.pause(id);
                return submit(false, true, 0, token -> core.unsubscribe(token, id), attempt);
            }
        }

        @Override
        public void close() {
            synchronized (lifecycleLock) {
                // A pending removal already covers this handle; another observer would only
                // hold an operation slot until its acknowledgement.
                UnsubscribeAttempt current = unsubscription;
                if (current == null || current.finished) {
                    unsubscribe();
                }
            }
        }
    }

    /// One wire exchange, with individually admitted public observers. Guarded by lifecycleLock.
    private final class UnsubscribeAttempt {

        private final long subscriptionId;

        private final List<Long> observers = new ArrayList<>();

        private boolean finished;

        private @Nullable Throwable failure;

        UnsubscribeAttempt(long subscriptionId) {
            this.subscriptionId = subscriptionId;
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
                for (long token : observers) {
                    completeOperation(token, null, cause);
                }
                observers.clear();
            }
        }
    }

    private static final class Operation<T> {

        final CompletableFuture<T> future = new CompletableFuture<>();
        volatile boolean notificationReserved;

        final @Nullable UnsubscribeAttempt unsubscribe;

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
