package org.mqtt4j.mqtj.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.mqtj.ConnAck;
import org.mqtt4j.mqtj.ConnectionListener;
import org.mqtt4j.mqtj.Disconnection;
import org.mqtt4j.mqtj.MqttCapacityException;
import org.mqtt4j.mqtj.MqttClient;
import org.mqtt4j.mqtj.MqttConnectException;
import org.mqtt4j.mqtj.client.internal.common.log.Log;
import org.mqtt4j.mqtj.client.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.spi.TimerScheduler;
import org.mqtt4j.mqtj.spi.Transport;

import static java.util.Objects.requireNonNull;

/// Serializes protocol decisions, isolating application continuations and listeners from I/O.
/// API methods run on their callers; the core and I/O commands run on the actor thread.
public final class ClientActor implements MqttClient {

    private static final Log log = Log.of(ClientActor.class);

    private static final int OPERATION_LIMIT = 64;

    private static final int NOTIFICATION_LIMIT = 64;

    private static final int MAILBOX_DATA_LIMIT = 256;

    private static final int MAILBOX_CONTROL_LIMIT = 256;

    private static final int MAILBOX_BASE_BYTE_LIMIT = 32 * 1024 * 1024;

    private final URI endpoint;

    private final String clientId;

    private final TimerScheduler scheduler;

    private final ConnectionListener listener;

    private final SessionCore core;

    private final ClientIo io;

    private final Mailbox mailbox;

    // Admission and stopping share this lock so shutdown cannot miss a registered operation.
    private final Object lifecycleLock = new Object();

    private final Semaphore operationSlots = new Semaphore(OPERATION_LIMIT);

    private final Semaphore notificationSlots = new Semaphore(NOTIFICATION_LIMIT);

    private final Map<Long, Operation<?>> operations = new ConcurrentHashMap<>();

    private final AtomicLong sequence = new AtomicLong();

    private final AtomicBoolean stopping = new AtomicBoolean();

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
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.scheduler = scheduler;
        this.listener = listener;
        mailbox = createMailbox(maxPacketSize);
        io = new ClientIo(
                endpoint,
                clientId,
                timeout,
                maxPacketSize,
                transportFactory,
                new IoEvents()
        );
        core = new SessionCore(new CorePort(), clientId, timeout, keepAlive);
        Thread.ofVirtual().name(threadName("actor")).start(this::run);
    }

    @Override
    public CompletableFuture<ConnAck> connect() {
        return submit(true);
    }

    @Override
    public CompletableFuture<Void> disconnect() {
        return submit(false);
    }

    private <T> CompletableFuture<T> submit(boolean connect) {
        synchronized (lifecycleLock) {
            if (stopping.get()) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Client is closed")
                );
            }
            if (!operationSlots.tryAcquire()) {
                return CompletableFuture.failedFuture(
                        new MqttCapacityException("Operation capacity")
                );
            }
            if (connect && !notificationSlots.tryAcquire()) {
                operationSlots.release();
                return CompletableFuture.failedFuture(
                        new MqttCapacityException("Listener capacity")
                );
            }

            long token = sequence.incrementAndGet();
            var operation = new Operation<T>(connect);
            operations.put(token, operation);
            Mailbox.OfferResult admitted = mailbox.offer(new Mailbox.Event(
                    0,
                    () -> {
                        if (connect) {
                            long now;
                            try {
                                now = scheduler.nanoTime();
                            } catch (RuntimeException failure) {
                                completeOperation(token, null, new MqttConnectException(
                                        "Cannot read connection start time",
                                        failure
                                ));
                                return;
                            }
                            core.connect(token, now);
                        } else {
                            core.disconnect(token);
                        }
                    },
                    true,
                    0
            ));
            if (admitted == Mailbox.OfferResult.FULL) {
                completeOperation(
                        token,
                        null,
                        new MqttCapacityException("Mailbox control capacity")
                );
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
            log.error("Actor failed for {} at {}", clientId, endpoint, cause);
        } finally {
            synchronized (lifecycleLock) {
                stopping.set(true);
            }
            try {
                core.close(failure);
            } catch (Throwable cleanupFailure) {
                log.error("Shutdown failed for {} at {}", clientId, endpoint, cleanupFailure);
            } finally {
                io.close();
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
            }
        }
    }

    private void post(long generation, Runnable action, boolean control, int bytes) {
        if (stopping.get()) {
            return;
        }
        Mailbox.OfferResult admitted = mailbox.offer(
                new Mailbox.Event(generation, action, control, bytes)
        );
        if (admitted == Mailbox.OfferResult.FULL) {
            signalFailure(generation, new MqttCapacityException("Mailbox capacity"));
        }
    }

    private void signalFailure(long generation, Throwable failure) {
        if (!stopping.get()) {
            mailbox.terminate(new Mailbox.Event(
                    generation,
                    () -> core.failed(generation, failure),
                    true,
                    0
            ));
        }
    }

    /// Called by an API caller or the actor; futures complete on independent threads.
    private void completeOperation(
            long token,
            @Nullable ConnAck result,
            @Nullable Throwable failure
    ) {
        Operation<?> operation = operations.remove(token);
        if (operation == null) {
            return;
        }
        if (result != null) {
            log.info("Connected {} to {}", clientId, endpoint);
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
        // An empty mailbox must admit one packet up to the configured maximum.
        return new Mailbox(
                MAILBOX_DATA_LIMIT,
                MAILBOX_CONTROL_LIMIT,
                Math.max(MAILBOX_BASE_BYTE_LIMIT, maxPacketSize)
        );
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
            mailbox.activate(generation);
            io.open(generation);
        }

        @Override
        public SendQueue.OfferResult send(long generation, long ticket, Packet packet) {
            return io.send(generation, ticket, packet);
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
                        true,
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
        public void complete(long token, @Nullable ConnAck result, @Nullable Throwable failure) {
            completeOperation(token, result, failure);
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

    /// Called by I/O callbacks, including inline on the actor; only posts mailbox events.
    private final class IoEvents implements ClientIo.Events {

        @Override
        public void connected(long generation) {
            post(generation, () -> core.opened(generation), true, 0);
        }

        @Override
        public void received(long generation, Packet packet, int size) {
            post(
                    generation,
                    () -> core.received(generation, packet, nanoTime(generation)),
                    false,
                    size
            );
        }

        @Override
        public void sent(long generation, long ticket) {
            long sentAt;
            try {
                sentAt = scheduler.nanoTime();
            } catch (RuntimeException failure) {
                signalFailure(generation, failure);
                return;
            }
            post(generation, () -> {
                core.sent(generation, ticket, sentAt);
                io.sendNext(generation);
            }, true, 0);
        }

        @Override
        public void ended(long generation, Throwable cause) {
            // Preserve a final CONNACK refusal that arrived just before EOF.
            if (!stopping.get()) {
                mailbox.offerTermination(new Mailbox.Event(
                        generation,
                        () -> core.failed(generation, cause),
                        true,
                        0
                ));
            }
        }

        @Override
        public void failed(long generation, Throwable cause) {
            signalFailure(generation, cause);
        }
    }

    private static final class TimerEntry {

        TimerScheduler.@Nullable Cancellable cancellation;
    }

    private static final class Operation<T> {

        final CompletableFuture<T> future = new CompletableFuture<>();
        volatile boolean notificationReserved;

        Operation(boolean notificationReserved) {
            this.notificationReserved = notificationReserved;
        }

        @SuppressWarnings("unchecked") // Tokens retain their operation's result type in the core.
        void finish(@Nullable ConnAck result, @Nullable Throwable failure) {
            if (failure == null) {
                future.complete((T) result);
            } else {
                future.completeExceptionally(failure);
            }
        }
    }
}
