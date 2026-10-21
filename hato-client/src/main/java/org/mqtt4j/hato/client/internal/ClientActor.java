package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.AckMode;
import org.mqtt4j.hato.ClientStats;
import org.mqtt4j.hato.ClientStats.CapacityLimit;
import org.mqtt4j.hato.ClientStats.ConnectionState;
import org.mqtt4j.hato.ClientStats.OverloadCause;
import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.ConnectionListener;
import org.mqtt4j.hato.DeliveryOptions;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.FailurePolicy;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttCredentials;
import org.mqtt4j.hato.MqttDelivery;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttNotConnectedException;
import org.mqtt4j.hato.MqttSubscriptionRefusedException;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.PublishResult;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReceiveResult;
import org.mqtt4j.hato.RetainHandling;
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

    private static final int WAITER_LIMIT = 1024;

    // Connection notifications waiting for the listener, excluding the one running. Attempt
    // numbers reveal what a listener that fell this far behind has lost.
    private static final int LISTENER_QUEUE_LIMIT = 64;

    // A burst, such as retained messages on subscribe, can arrive faster than the actor routes
    // it, so most of it may wait here. This count bounds per-packet overhead (about 190 bytes
    // each when measured); the byte limit bounds the packets themselves.
    private static final int MAILBOX_INCOMING_LIMIT = 65_536;

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

    private final ConnectionListener listener;

    // MQTT 3.1.1 cannot carry message properties, so publish() refuses them before admission.
    private final MqttVersion version;

    // A nonpersistent session ends with the client, which confirms its pending removals.
    private final boolean cleanStart;

    // Whether the policy reconnects at all; the log then says when an ending gives up.
    private final boolean reconnects;

    private final TimerScheduler scheduler;

    // Delivery options keep their choice out of the public API; the builder passes their
    // package-private accessor.
    private final Function<DeliveryOptions, HandlerExecution> executionOf;

    private final ClientTermination termination;

    private final ListenerLane listenerLane;

    private final SessionCore core;

    private final ClientIo io;

    // The HttpClient that the client's WebSocket transports share, which ends after them.
    private final @Nullable WebSocketTransports webSockets;

    private final @Nullable CredentialsEvaluator credentials;

    private final Mailbox mailbox;

    private final DeliveryLanes lanes;

    // Published under lifecycleLock before admitting commands for an established connection.
    private long connectedGeneration;

    // Guarded by lifecycleLock. The established connection's CONNACK, published once continued
    // subscriptions are reconciled, and null after the connection ends.
    private @Nullable ConnAck connectedAck;

    // Guarded by lifecycleLock. whenConnected() observations waiting for a connection.
    private final List<CompletableFuture<ConnAck>> connectionWaiters = new ArrayList<>();

    // Guarded by lifecycleLock. stats() observations waiting for the actor's next snapshot.
    private final List<CompletableFuture<ClientStats>> statsWaiters = new ArrayList<>();

    // Guarded by lifecycleLock. Set once the actor has ended: the statistics that stats() returns
    // from then on, or the failure that it returns instead.
    private @Nullable ClientStats lastStats;

    private @Nullable RuntimeException lastStatsFailure;

    // Lifetime counts of operations refused for capacity on the callers' threads, by limit; the
    // core counts its own refusals.
    private final AtomicLongArray refusals = new AtomicLongArray(CapacityLimit.values().length);

    private final Function<ClientSnapshot, ClientStats> statsOf;

    private final ClientLimits limits;

    // Guarded by lifecycleLock. Set when the core accepts a requested disconnect and cleared when
    // the connection ends or a new attempt begins; meanwhile, establishment observations wait
    // for a later connection.
    private boolean disconnecting;

    private long openedGeneration;

    // The attempt being connected or established, and null once its connection ends. Only its
    // credentials results are applied.
    private @Nullable ConnectionAttempt activeAttempt;

    // Admission and stopping share this lock so shutdown cannot miss a registered operation.
    private final Object lifecycleLock = new Object();

    // One durable removal per registration, guarded by lifecycleLock. Observations have their
    // own budget; neither local close nor public unsubscribe needs an operation slot.
    private final Map<Long, UnsubscribeAttempt> closingAttempts = new LinkedHashMap<>();

    private final Semaphore operationSlots;

    private final int commandLimit;

    // connect() and disconnect() keep one slot each, so publications holding every operation
    // slot while they wait for a reconnection cannot refuse the call that hastens or ends it.
    private final Semaphore connectSlot = new Semaphore(1);

    private final Semaphore disconnectSlot = new Semaphore(1);

    private final Semaphore waiterSlots = new Semaphore(WAITER_LIMIT);

    // Handles include registrations still queued for the actor. The core decides admission; the
    // mailbox refuses only registrations beyond its queue bound.
    private final Map<Long, ClientSubscription> subscriptions = new ConcurrentHashMap<>();

    // Handles whose lane may still run, guarded by lifecycleLock. A lane's termination finds its
    // handle here after retirement has removed it from subscriptions.
    private final Map<Long, ClientSubscription> unterminated = new HashMap<>();

    // Guarded by lifecycleLock. Set once the actor's cleanup has retired every subscription.
    private boolean cleanedUp;

    private final Map<Long, Operation<?>> operations = new ConcurrentHashMap<>();

    private final AtomicLong sequence = new AtomicLong();

    private final AtomicBoolean stopping = new AtomicBoolean();

    // Guarded by lifecycleLock and published with stopping. A fatal I/O failure or the actor's
    // own failure; it bypasses mailbox capacity, survives connection generation changes, and
    // decides whether permanent endings report an internal failure.
    private @Nullable Throwable fatalFailure;

    public ClientActor(
            URI endpoint,
            String clientId,
            Duration keepAlive,
            Duration timeout,
            int maxPacketSize,
            TimerScheduler scheduler,
            ConnectionListener listener,
            Transport.Factory transportFactory,
            MqttVersion version,
            Duration sessionExpiry,
            @Nullable Supplier<MqttCredentials> credentials,
            Packet.@Nullable Will will,
            ReconnectBackoff backoff,
            RetryRules rules,
            Duration responseTimeout,
            Function<DeliveryOptions, HandlerExecution> executionOf,
            Function<ClientSnapshot, ClientStats> statsOf,
            ClientLimits limits
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
                PacketCodec.of(version),
                version,
                sessionExpiry,
                credentials,
                will,
                backoff,
                rules,
                responseTimeout,
                executionOf,
                statsOf,
                limits
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
            MqttVersion version,
            Duration sessionExpiry,
            @Nullable Supplier<MqttCredentials> credentials,
            Packet.@Nullable Will will,
            ReconnectBackoff backoff,
            RetryRules rules,
            Duration responseTimeout,
            Function<DeliveryOptions, HandlerExecution> executionOf,
            Function<ClientSnapshot, ClientStats> statsOf,
            ClientLimits limits
    ) {
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.version = version;
        this.scheduler = scheduler;
        this.executionOf = executionOf;
        this.statsOf = statsOf;
        this.limits = limits;
        cleanStart = sessionExpiry.isZero();
        reconnects = backoff.enabled();
        operationSlots = new Semaphore(limits.operations());
        commandLimit = commandLimit(limits);
        termination = new ClientTermination(
                threadName("terminated"),
                waiterSlots,
                WAITER_LIMIT,
                List.of(operationSlots, connectSlot, disconnectSlot),
                commandLimit,
                () -> refused(CapacityLimit.OBSERVATIONS)
        );
        this.listener = listener;
        listenerLane = new ListenerLane(
                endpoint,
                clientId,
                "Connection listener",
                "listener",
                LISTENER_QUEUE_LIMIT,
                termination::check
        );
        mailbox = createMailbox(maxPacketSize, limits);
        lanes = new DeliveryLanes(
                clientId,
                receiveByteLimit(maxPacketSize),
                limits.subscriptions(),
                this::deliveryFailed,
                this::handOffRefused,
                this::deliveryReady,
                this::laneTerminated
        );
        webSockets = transportFactory instanceof WebSocketTransports owner ? owner : null;
        io = new ClientIo(
                endpoint,
                clientId,
                maxPacketSize,
                limits.regularSends(),
                transportFactory,
                new IoEvents(),
                codec
        );
        core = new SessionCore(
                new CorePort(),
                clientId,
                timeout,
                keepAlive,
                version,
                sessionExpiry,
                maxPacketSize,
                will,
                credentials != null,
                backoff,
                rules,
                responseTimeout,
                limits
        );
        this.credentials = credentials == null ? null : new CredentialsEvaluator(
                threadName("credentials"),
                credentials,
                () -> post(0, this::applyCredentials, Mailbox.Kind.CREDENTIALS_READY, 0),
                termination::check
        );
        termination.watch(listenerLane, this.credentials);
        Thread.ofVirtual().name(threadName("actor")).start(this::run);
    }

    @Override
    public CompletableFuture<ConnAck> connect() {
        return submit(Command.CONNECT, 0, token -> {
            long now;
            try {
                now = scheduler.nanoTime();
            } catch (RuntimeException failure) {
                core.schedulerFailed(openedGeneration, failure);
                completeOperation(token, null, new MqttConnectException(
                        "Cannot read connection start time", failure
                ));
                return;
            }
            core.connect(requireNonNull(requireNonNull(operations.get(token)).attempt), now);
        });
    }

    @Override
    public CompletableFuture<ConnAck> whenConnected() {
        synchronized (lifecycleLock) {
            if (stopping.get()) {
                return CompletableFuture.failedFuture(closedFailure());
            }
            ConnAck current = connectedAck;
            if (current != null && !disconnecting) {
                return CompletableFuture.completedFuture(current);
            }
            return observeFuture(connectionWaiters);
        }
    }

    @Override
    public CompletableFuture<Void> disconnect() {
        return submit(Command.DISCONNECT, 0, core::disconnect);
    }

    @Override
    public CompletableFuture<PublishResult> publish(MqttMessage message) {
        requireNonNull(message, "message");
        if (version == MqttVersion.V3_1_1 && MessageProperties.present(message)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "MQTT 3.1.1 cannot carry message properties"
            ));
        }
        // The properties count with the payload: User Properties alone can be large.
        return submit(
                Command.PUBLISH,
                MessageProperties.bodySize(message),
                token -> core.publish(token, message)
        );
    }

    @Override
    public Subscription subscribe(
            Subscribe request,
            Consumer<MqttDelivery> handler
    ) {
        requireNonNull(request, "request");
        requireNonNull(handler, "handler");
        return subscribeInternal(request, new HandlerExecution.Internal(), handler, null);
    }

    @Override
    public Subscription subscribe(
            Subscribe request,
            DeliveryOptions options,
            Consumer<MqttDelivery> handler
    ) {
        requireNonNull(request, "request");
        requireNonNull(options, "options");
        requireNonNull(handler, "handler");
        return subscribeInternal(request, executionOf.apply(options), handler, null);
    }

    @Override
    public Subscription subscribe(Subscribe request) {
        requireNonNull(request, "request");
        if (request.ackMode().orElse(AckMode.ON_RECEIVE) == AckMode.AFTER_HANDLER) {
            throw new IllegalArgumentException(
                    "A pull subscription has no handler to acknowledge"
            );
        }
        if (request.failurePolicy() == FailurePolicy.SKIP) {
            throw new IllegalArgumentException(
                    "A pull subscription has no handler whose failures to skip"
            );
        }
        return subscribeInternal(request, new HandlerExecution.Internal(), null, null);
    }

    @Override
    public Subscription handOff(Subscribe request, Predicate<MqttDelivery> acceptor) {
        requireNonNull(request, "request");
        requireNonNull(acceptor, "acceptor");
        if (request.ackMode().orElse(AckMode.ON_RECEIVE) == AckMode.AFTER_HANDLER) {
            throw new IllegalArgumentException(
                    "A hand-off subscription cannot see when its deliveries are processed"
            );
        }
        return subscribeInternal(request, new HandlerExecution.Internal(), null, acceptor);
    }

    private Subscription subscribeInternal(
            Subscribe request,
            HandlerExecution execution,
            @Nullable Consumer<MqttDelivery> handler,
            @Nullable Predicate<MqttDelivery> acceptor
    ) {
        // MQTT 3.1.1 has no subscription options, so a request that sets one is an argument
        // error, decided by the version of the client.
        boolean setsOptions = request.noLocal()
                || request.retainAsPublished()
                || request.retainHandling() != RetainHandling.SEND;
        if (version == MqttVersion.V3_1_1 && setsOptions) {
            throw new IllegalArgumentException("MQTT 3.1.1 has no subscription options");
        }
        ClientSubscription subscription;
        MqttCapacityException rejection = null;
        synchronized (lifecycleLock) {
            if (stopping.get()) {
                throw new IllegalStateException("Client is closed");
            }
            long id = sequence.incrementAndGet();
            subscription = new ClientSubscription(id, request, execution, handler, acceptor);
            subscriptions.put(id, subscription);
            unterminated.put(id, subscription);
            Mailbox.OfferResult admitted = mailbox.offer(new Mailbox.Event(
                    0,
                    () -> register(subscription),
                    Mailbox.Kind.REGISTRATION,
                    0
            ));
            if (admitted == Mailbox.OfferResult.FULL) {
                refused(CapacityLimit.REGISTRATION_QUEUE);
                rejection = new MqttCapacityException("Registration mailbox capacity");
                retire(subscription, rejection);
            }
        }
        if (rejection != null) {
            logEnding(subscription, rejection);
        }
        return subscription;
    }

    private void register(ClientSubscription subscription) {
        synchronized (lifecycleLock) {
            subscription.registrationStarted = true;
            if (subscription.unsubscription != null) {
                // Removal won before registration reached the broker, so no wire cleanup is due.
                retire(subscription, null);
                completeOperation(subscription.unsubscription.token, null, null);
                return;
            }
        }
        // A removal requested from here on posts its own wake, which starts it after this.
        core.subscribe(subscription.id, subscription.request);
    }

    /// Operators see a registration that ends without the application asking, even when nothing
    /// observes its establishment.
    private void logEnding(ClientSubscription subscription, Throwable cause) {
        log.warn("Subscription {} ended for {} at {}",
                subscription.filter().pattern(), clientId, endpoint, cause);
    }

    private <T> CompletableFuture<T> submit(
            Command kind,
            int bytes,
            LongConsumer command
    ) {
        boolean needsConnection = kind == Command.PUBLISH;
        synchronized (lifecycleLock) {
            Throwable refusal = null;
            Semaphore slot = null;
            if (stopping.get()) {
                refusal = new IllegalStateException("Client is closed");
            } else if (needsConnection && connectedGeneration == 0) {
                refusal = new MqttNotConnectedException();
            } else {
                slot = acquireSlot(kind);
                if (slot == null) {
                    refused(CapacityLimit.OPERATIONS);
                    refusal = new MqttCapacityException("Operation capacity");
                }
            }
            if (refusal != null) {
                return CompletableFuture.failedFuture(refusal);
            }

            long token = sequence.incrementAndGet();
            long generation = connectedGeneration;
            var operation = new Operation<T>(
                    requireNonNull(slot),
                    (kind == Command.CONNECT) ? new ConnectionAttempt(token) : null
            );
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
                refused(CapacityLimit.OPERATION_QUEUE);
                completeOperation(token, null,
                        new MqttCapacityException("Command mailbox capacity"));
            }
            return operation.future;
        }
    }

    /// connect() and disconnect() first take their own slot; a concurrent second call shares the
    /// operation slots.
    private @Nullable Semaphore acquireSlot(Command kind) {
        Semaphore reserved = switch (kind) {
            case CONNECT -> connectSlot;
            case DISCONNECT -> disconnectSlot;
            case PUBLISH -> null;
        };
        if (reserved != null && reserved.tryAcquire()) {
            return reserved;
        }
        return operationSlots.tryAcquire() ? operationSlots : null;
    }

    @Override
    public void close() {
        synchronized (lifecycleLock) {
            stopping.set(true);
            mailbox.connectionRequested(false);
            mailbox.wake();
        }
        // No delivery is handed off after this returns, even while the actor is still busy.
        lanes.closeHandOffs();
    }

    @Override
    public CompletableFuture<Void> terminated() {
        return termination.observe();
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
                    if (cause.schedulerFailure()) {
                        core.schedulerFailed(cause.generation(), requireNonNull(cause.getCause()));
                    } else {
                        core.failed(cause.generation(), requireNonNull(cause.getCause()));
                    }
                }
                busySince = shareCarrier(busySince);
            }
        } catch (Throwable cause) {
            failure = cause;
        } finally {
            synchronized (lifecycleLock) {
                if (fatalFailure != null) {
                    failure = fatalFailure;
                } else {
                    fatalFailure = failure;
                }
                stopping.set(true);
                mailbox.connectionRequested(false);
            }
            if (credentials != null) {
                // Drops a waiting result before the remaining cleanup.
                credentials.close();
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
                    synchronized (lifecycleLock) {
                        Throwable ending = permanentFailure();
                        for (ClientSubscription subscription : subscriptions.values()) {
                            retire(subscription, ending);
                        }
                        finishClosures(ending);
                        completeWaiters(connectionWaiters, null, closedFailure());
                    }
                    lanes.close(failure);
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
                    try {
                        io.shutdown();
                    } finally {
                        recordLastStats();
                        synchronized (lifecycleLock) {
                            cleanedUp = true;
                            checkLanesEnded();
                        }
                        // The actor calls no application code from here on.
                        termination.actorEnded();
                    }
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

    /// A lane runs and hands off nothing more. Called without the lanes' lock, from the lane's
    /// exiting dispatcher or the thread that stopped a pull lane.
    private void laneTerminated(long id) {
        synchronized (lifecycleLock) {
            ClientSubscription subscription = unterminated.get(id);
            if (subscription != null) {
                subscription.laneEnded = true;
                subscription.checkTerminated();
                checkLanesEnded();
            }
        }
    }

    /// Caller holds lifecycleLock. Once the cleanup has retired every subscription, the last one
    /// to terminate ends the lanes for the client's termination.
    private void checkLanesEnded() {
        if (cleanedUp && unterminated.isEmpty()) {
            termination.lanesEnded();
        }
    }

    /// Caller holds lifecycleLock. Public removal and local close share one durable request.
    private UnsubscribeAttempt requestRemoval(ClientSubscription subscription) {
        UnsubscribeAttempt current = subscription.unsubscription;
        if (current != null) {
            return current;
        }
        long token = sequence.incrementAndGet();
        var attempt = new UnsubscribeAttempt(token, subscription);
        subscription.unsubscription = attempt;
        closingAttempts.put(token, attempt);
        subscription.endEstablishment(new IllegalStateException("Subscription is being removed"));
        post(0, this::startClosures, Mailbox.Kind.SUBSCRIPTION_CLOSE, 0);
        return attempt;
    }

    private void startClosures() {
        var starting = new ArrayList<UnsubscribeAttempt>();
        synchronized (lifecycleLock) {
            if (stopping.get()) {
                return;
            }
            for (UnsubscribeAttempt attempt : closingAttempts.values()) {
                if (!attempt.started && attempt.subscription.registrationStarted) {
                    attempt.started = true;
                    starting.add(attempt);
                }
            }
        }
        for (UnsubscribeAttempt attempt : starting) {
            core.unsubscribe(attempt.token, attempt.subscription.id);
        }
        core.sendCapacityAvailable(openedGeneration);
    }

    /// Caller holds lifecycleLock. Completes every removal still pending at shutdown, including
    /// ones whose coalesced wake never reached the core. Closing a nonpersistent session confirms
    /// them; after an ordinary close, a persistent broker may still hold the subscription.
    private void finishClosures(@Nullable Throwable ending) {
        Throwable failure = ending != null || cleanStart
                ? ending
                : new IllegalStateException("Client is closed");
        for (long token : List.copyOf(closingAttempts.keySet())) {
            completeOperation(token, null, failure);
        }
    }

    /// Caller holds lifecycleLock. An internal failure that closes the client is reported with
    /// its cause; an ordinary close reports no failure.
    private @Nullable Throwable permanentFailure() {
        return fatalFailure == null ? null : new MqttConnectionLostException(fatalFailure);
    }

    /// The failure of an observation after stopping: the permanent cause, or an ordinary close.
    private Throwable closedFailure() {
        Throwable ending = permanentFailure();
        return ending != null ? ending : new IllegalStateException("Client is closed");
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
                case REGISTRATION -> "Registration";
                case INCOMING -> "Incoming";
                case KEEP_ALIVE -> "Keep-alive";
                case CONTROL, DELIVERY_READY, SEND_CAPACITY, SUBSCRIPTION_CLOSE,
                        CREDENTIALS_READY, RECONNECT, STATS -> "Control";
            };
            signalOverload(
                    generation,
                    OverloadCause.EVENTS,
                    new MqttCapacityException(admission + " mailbox capacity")
            );
        }
    }

    private void signalFailure(long generation, Throwable failure) {
        terminate(generation, () -> core.failed(generation, failure));
    }

    private void signalOverload(
            long generation,
            OverloadCause cause,
            MqttCapacityException failure
    ) {
        terminate(generation, () -> core.overloaded(generation, cause, failure));
    }

    /// An acceptor refused a QoS 1 or 2 delivery, so the connection that delivered it ends as
    /// overloaded, as with a full buffer. Called by the hand-off worker.
    private void handOffRefused(DeliveryToken arrival) {
        signalOverload(
                arrival.connection(),
                OverloadCause.HAND_OFF,
                new MqttCapacityException("Hand-off capacity")
        );
    }

    /// A result belongs to the attempt, which may span more than one connection. One for an
    /// ended attempt is dropped before it can read the clock on behalf of a later attempt.
    private void applyCredentials() {
        CredentialsEvaluator.Result result = requireNonNull(credentials).take();
        if (result != null && result.attempt() == activeAttempt) {
            core.credentials(
                    result.attempt(),
                    result.value(),
                    result.failure(),
                    nanoTime(openedGeneration)
            );
        }
    }

    private boolean deliveryFailed(DeliveryToken token, Throwable failure) {
        // Reject retired sessions before they can occupy the current request's stop slot.
        // A closing client keeps the failed lane paused until shutdown removes it.
        return mailbox.stopSession(token.session(), () -> core.deliveryFailed(token, failure))
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
            ClientSubscription rejected = null;
            synchronized (lifecycleLock) {
                UnsubscribeAttempt closing = closingAttempts.remove(token);
                if (closing != null) {
                    closing.finish(failure);
                } else if (failure != null) {
                    // The core rejected a registration, whose id is the token.
                    rejected = subscriptions.get(token);
                    if (rejected != null) {
                        retire(rejected, failure);
                    }
                }
            }
            if (rejected != null) {
                logEnding(rejected, requireNonNull(failure));
            }
            return;
        }
        Thread.ofVirtual().name(threadName("complete-" + token)).start(() -> {
            try {
                operation.finish(result, failure);
            } finally {
                operation.slot.release();
                termination.check();
            }
        });
    }

    private String threadName(String role) {
        return "hato-" + clientId + "-" + role;
    }

    private long nanoTime(long generation) {
        try {
            return scheduler.nanoTime();
        } catch (RuntimeException failure) {
            throw SpiException.scheduler(generation, failure);
        }
    }

    @Override
    public CompletableFuture<ClientStats> stats() {
        synchronized (lifecycleLock) {
            if (lastStats != null) {
                return CompletableFuture.completedFuture(lastStats);
            }
            if (lastStatsFailure != null) {
                return CompletableFuture.failedFuture(lastStatsFailure);
            }
            CompletableFuture<ClientStats> observed = observeFuture(statsWaiters);
            if (!observed.isDone()) {
                // The request is a wake, which the mailbox queues once; every observation made
                // before the actor answers it shares its snapshot.
                post(0, this::takeStats, Mailbox.Kind.STATS, 0);
            }
            return observed;
        }
    }

    /// Answers the waiting stats() observations with a snapshot taken now. A clock failure fails
    /// them and then ends the connection as other clock failures do.
    private void takeStats() {
        long now;
        try {
            now = scheduler.nanoTime();
        } catch (RuntimeException failure) {
            answerStats(null, failure);
            throw SpiException.scheduler(0, failure);
        }
        answerStats(statsOf.apply(snapshot(now)), null);
    }

    /// From close() on, the waiting observations get the snapshot collected when the actor ends
    /// instead, so that no call made after close() sees an earlier one.
    private void answerStats(@Nullable ClientStats stats, @Nullable Throwable failure) {
        synchronized (lifecycleLock) {
            if (!stopping.get()) {
                completeWaiters(statsWaiters, stats, failure);
            }
        }
    }

    /// Runs on the actor at its end: keeps what the client counted for later stats() calls, and
    /// answers those that waited, so that termination does not wait for them.
    private void recordLastStats() {
        try {
            long now;
            try {
                now = scheduler.nanoTime();
            } catch (RuntimeException _) {
                // Durations then end at the latest event that brought a time.
                now = core.eventTime();
            }
            ClientStats last = statsOf.apply(snapshot(now));
            synchronized (lifecycleLock) {
                lastStats = last;
                completeWaiters(statsWaiters, last, null);
            }
        } catch (RuntimeException failure) {
            log.error("Final statistics failed for {} at {}", clientId, endpoint, failure);
            synchronized (lifecycleLock) {
                lastStatsFailure = failure;
                completeWaiters(statsWaiters, null, failure);
            }
        }
    }

    /// Called only on the actor. The core's values at one point, then the subscriptions that the
    /// client holds, and the lanes' values read under their own lock.
    private ClientSnapshot snapshot(long now) {
        SessionCore.Values session = core.values(now);
        List<ClientSubscription> listed;
        synchronized (lifecycleLock) {
            // A closed subscription leaves the list at once; its broker removal may continue.
            listed = subscriptions.values().stream()
                    .filter(subscription -> !subscription.ended)
                    .toList();
        }
        DeliveryLanes.Values delivery = lanes.values();
        var held = new ArrayList<ClientSnapshot.SubscriptionValues>();
        for (ClientSubscription subscription : listed) {
            DeliveryLanes.LaneValues lane = delivery.lanes().getOrDefault(
                    subscription.id,
                    DeliveryLanes.LaneValues.UNREGISTERED
            );
            held.add(new ClientSnapshot.SubscriptionValues(
                    subscription,
                    subscription.filter(),
                    lane.waiting(),
                    lane.running(),
                    lane.awaitingAck(),
                    subscription.request.bufferCapacity(),
                    lane.dropped()
            ));
        }
        var refused = new EnumMap<CapacityLimit, Long>(session.refusals());
        for (CapacityLimit limit : CapacityLimit.values()) {
            refused.merge(limit, refusals.get(limit.ordinal()), Long::sum);
        }
        Receptions.Values received = session.receiving();
        Publications.Values published = session.sending();
        Subscriptions.Values registered = session.subscribing();
        return new ClientSnapshot(
                new ClientSnapshot.Connection(
                        connectionState(session.state()),
                        session.attempt(),
                        session.establishedAttempt(),
                        listenerLane.dropped()
                ),
                new ClientSnapshot.Receiving(
                        received.publishes(),
                        received.unmatched(),
                        delivery.admitted(),
                        delivery.dropped(),
                        delivery.deliveries(),
                        delivery.bytes(),
                        received.awaitingConditions(),
                        received.awaitingTurn(),
                        received.awaitingAcknowledgementWrite(),
                        received.awaitingRelease(),
                        received.awaitingCompletionWrite(),
                        received.receptions(),
                        received.oldestAwaitingConditions(),
                        received.oldestAwaitingRelease()
                ),
                new ClientSnapshot.Sending(
                        new ClientStats.Usage(
                                limits.operations() - operationSlots.availablePermits(),
                                limits.operations()
                        ),
                        new ClientStats.Usage(io.sends(), io.sendLimit()),
                        new ClientStats.Usage(io.sendBytes(), io.sendByteLimit()),
                        published.awaitingPubAck(),
                        published.awaitingPubRec(),
                        published.awaitingPubComp(),
                        published.publications(),
                        published.keptBytes(),
                        session.quota(),
                        session.oldestResponseWait()
                ),
                new ClientSnapshot.Subscribing(
                        registered.registrations(),
                        registered.established(),
                        registered.pending(),
                        registered.removing(),
                        delivery.retainedLanes()
                ),
                held,
                refused,
                session.overloads()
        );
    }

    private static ConnectionState connectionState(SessionCore.State state) {
        return switch (state) {
            case DISCONNECTED -> ConnectionState.DISCONNECTED;
            case CONNECTING -> ConnectionState.CONNECTING;
            case CONNECTED -> ConnectionState.CONNECTED;
            case DISCONNECTING -> ConnectionState.DISCONNECTING;
            case RECONNECT_WAIT -> ConnectionState.WAITING_TO_RECONNECT;
            case CLOSED -> ConnectionState.CLOSED;
        };
    }

    /// Counts an operation refused for capacity outside the core; any thread.
    private void refused(CapacityLimit limit) {
        refusals.incrementAndGet(limit.ordinal());
    }

    /// What the client holds against its internal bounds. Tests compare this with the bounds.
    /// The parts are read one after another, not atomically.
    Usage usage() {
        return new Usage(
                commandLimit
                        - operationSlots.availablePermits()
                        - connectSlot.availablePermits()
                        - disconnectSlot.availablePermits(),
                listenerLane.usage(),
                mailbox.size(),
                mailbox.bytes(),
                io.sends(),
                io.sendBytes(),
                lanes.retainedLanes(),
                lanes.retainedDeliveries(),
                lanes.retainedBytes(),
                WAITER_LIMIT - waiterSlots.availablePermits()
        );
    }

    /// Listener notifications dropped because the listener fell behind, for tests until
    /// applications get statistics.
    long droppedNotifications() {
        return listenerLane.dropped();
    }

    /// Handler failures whose report has returned, to the client for a stop or to the log for a
    /// skip, for tests that order later events after it until applications get statistics.
    long reportedHandlerFailures() {
        return lanes.reportedFailures();
    }

    /// Deliveries that dispatchers moved to an executor, for tests that order later events after
    /// the move until applications get statistics.
    long placedDeliveries() {
        return lanes.placedDeliveries();
    }

    /// Connection ends that I/O has posted to the actor, for tests that order later events after
    /// an end until applications get statistics.
    long postedConnectionEnds() {
        return io.postedEnds();
    }

    /// What the client's termination still waits for, for tests that hold one part until
    /// applications get statistics.
    Set<ClientTermination.Pending> terminationPending() {
        return termination.pending();
    }

    static Mailbox createMailbox(int maxPacketSize, ClientLimits limits) {
        // Registrations have their own admission class within the shared FIFO.
        return new Mailbox(
                commandLimit(limits),
                MAILBOX_COMMAND_BYTE_LIMIT,
                limits.subscriptions(),
                MAILBOX_INCOMING_LIMIT,
                receiveByteLimit(maxPacketSize),
                limits.controlEvents()
        );
    }

    // The shared operation slots plus the connect and disconnect slots. Every admitted operation
    // holds one of them and posts one command, so the command count never refuses one.
    private static int commandLimit(ClientLimits limits) {
        return limits.operations() + 2;
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
        public void beginAttempt(long generation, ConnectionAttempt attempt, long attemptNumber) {
            synchronized (lifecycleLock) {
                disconnecting = false;
            }
            activeAttempt = attempt;
            openedGeneration = generation;
            mailbox.activateSession(core.sessionNumber());
            mailbox.activate(generation);
        }

        @Override
        public void continueAttempt(long generation) {
            // The previous connection wrote its DISCONNECT and ends without a notification.
            mailbox.discard(openedGeneration);
            io.close();
            openedGeneration = generation;
            mailbox.activate(generation);
        }

        @Override
        public void connectionRequested(boolean requested) {
            synchronized (lifecycleLock) {
                mailbox.connectionRequested(requested && !stopping.get());
            }
        }

        @Override
        public void evaluateCredentials(ConnectionAttempt attempt) {
            requireNonNull(credentials).evaluate(attempt);
        }

        @Override
        public void cancelCredentials(ConnectionAttempt attempt) {
            requireNonNull(credentials).cancel(attempt);
        }

        @Override
        public void openTransport(long generation, Duration timeout) {
            io.open(generation, timeout);
        }

        @Override
        public void admit(long generation) {
            // Admission opens before onConnected is queued, so the callback can always use it.
            synchronized (lifecycleLock) {
                connectedGeneration = generation;
            }
        }

        @Override
        public void connected(long generation, ConnAck ack) {
            synchronized (lifecycleLock) {
                connectedAck = ack;
                // A requested disconnect lets observers wait for a later connection.
                if (!disconnecting) {
                    completeWaiters(connectionWaiters, ack, null);
                }
            }
            listenerLane.submit(() -> listener.onConnected(ack));
            log.info("Connected {} to {}", clientId, endpoint);
        }

        @Override
        public void disconnecting() {
            synchronized (lifecycleLock) {
                disconnecting = true;
            }
        }

        @Override
        public void stopDiscarded() {
            lanes.stopDiscarded();
        }

        @Override
        public void resubscribeFailed(
                Subscription handle,
                MqttSubscriptionRefusedException failure
        ) {
            var subscription = (ClientSubscription) handle;
            synchronized (lifecycleLock) {
                if (subscription.retired || subscription.removalRequested()) {
                    // The first ending decides; a requested removal only waited for this answer.
                    retire(subscription, null, !subscription.locallyClosed);
                    return;
                }
                retire(subscription, failure);
            }
            listenerLane.submit(() -> listener.onResubscribeFailed(subscription, failure));
            // Operators see the refusal even when the listener does not override the callback.
            log.warn("Re-creation of subscription {} for {} at {} was refused",
                    subscription.filter().pattern(), clientId, endpoint, failure);
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
                @Nullable SendReceipt receipt,
                TopicAlias alias
        ) {
            return io.publish(generation, ticket, message, packetId, receipt, alias);
        }

        @Override
        public void schedule(long generation, SessionCore.Timer timer, Duration delay) {
            cancel(timer);
            var entry = new TimerEntry();
            timers.put(timer, entry);
            Mailbox.Kind kind = switch (timer) {
                case KEEP_ALIVE, PING -> Mailbox.Kind.KEEP_ALIVE;
                case CONNECT, DISCONNECT, STABLE, RESPONSE -> Mailbox.Kind.CONTROL;
                case RECONNECT -> Mailbox.Kind.RECONNECT;
            };
            Runnable fired = (timer == SessionCore.Timer.RECONNECT)
                    ? () -> {
                        // The only way back to a connection must survive a full control queue,
                        // such as the one that just ended the connection when the delay is
                        // zero and the scheduler runs the task at once. A wake is never refused.
                        entry.due = true;
                        post(0, this::reconnect, kind, 0);
                    }
                    : () -> post(
                            generation,
                            () -> {
                                if (timers.get(timer) == entry) {
                                    timers.remove(timer);
                                    core.timer(generation, timer, nanoTime(generation));
                                }
                            },
                            kind,
                            0
                    );
            try {
                entry.cancellation = scheduler.schedule(delay, fired);
            } catch (RuntimeException failure) {
                timers.remove(timer);
                throw SpiException.scheduler(generation, failure);
            }
        }

        // Wakes are coalesced, so the actor checks the current timer. A late callback of a
        // timer that the actor has replaced marks only its own entry.
        private void reconnect() {
            TimerEntry entry = timers.get(SessionCore.Timer.RECONNECT);
            if (entry != null && entry.due) {
                timers.remove(SessionCore.Timer.RECONNECT);
                core.timer(0, SessionCore.Timer.RECONNECT, nanoTime(0));
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
            synchronized (lifecycleLock) {
                ClientSubscription subscription = requireNonNull(subscriptions.get(id));
                if (subscription.locallyClosed) {
                    return Optional.of(subscription);
                }
                boolean admitted = subscription.admitLane();
                if (admitted && subscription.receiver == null) {
                    // Termination now waits for this push or hand-off lane to end.
                    subscription.laneStarted = true;
                }
                if (subscription.unsubscription != null) {
                    lanes.pause(id);
                }
                return admitted ? Optional.of(subscription) : Optional.empty();
            }
        }

        @Override
        public void subscribed(Subscription handle, Qos granted) {
            var subscription = (ClientSubscription) handle;
            synchronized (lifecycleLock) {
                subscription.granted = granted;
                subscription.establishedGeneration = connectedGeneration;
                if (!disconnecting && subscription.establishmentFailure == null) {
                    completeWaiters(subscription.establishmentWaiters, granted, null);
                }
            }
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
            var subscription = (ClientSubscription) handle;
            boolean unrequested;
            synchronized (lifecycleLock) {
                boolean closed = core.state() == SessionCore.State.CLOSED;
                Throwable ending;
                if (closed) {
                    // The core's cause cannot tell an ordinary close from an internal failure.
                    ending = permanentFailure();
                } else if (subscription.removalRequested()) {
                    // A requested removal decides the ending; a refusal only confirms it.
                    ending = null;
                } else {
                    ending = cause;
                }
                unrequested = !closed && ending != null && !subscription.retired;
                // Confirming unsubscribe() lets the deliveries it accepted finish; local closure
                // has already discarded them.
                boolean drain = !closed && ending == null && !subscription.locallyClosed;
                retire(subscription, ending, drain);
            }
            if (unrequested) {
                logEnding(subscription, requireNonNull(cause));
            }
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
                connectedAck = null;
                disconnecting = false;
            }
            mailbox.discard(generation);
            // Reject old failures before releasing their lane pause; otherwise a late failure
            // can pause the lane again with a terminal notification that discard would erase.
            Map<Long, Boolean> handed = lanes.connectionEnded(preserveSession, stopped);
            io.close();
            if (established) {
                listenerLane.submit(() -> listener.onDisconnected(disconnection));
                log.info("Disconnected {} from {}", clientId, endpoint, cause);
            } else {
                listenerLane.submit(() -> listener.onConnectFailed(disconnection));
                if (activeAttempt != null) {
                    log.info("Connection failed for {} at {}", clientId, endpoint, cause);
                } else {
                    // A handler, clock, or internal failure ends a wait that has no attempt.
                    log.info("Stopped reconnecting {} to {}", clientId, endpoint, cause);
                }
            }
            Duration delay = disconnection.reconnectDelay().orElse(null);
            if (delay != null) {
                log.info("Reconnecting {} to {} in {}", clientId, endpoint, delay);
            } else if (reconnects && (established || activeAttempt != null)
                    && !disconnection.userRequested()) {
                // The policy reconnects, but this ending gives the connection request up.
                log.info("Stopped reconnecting {} to {}", clientId, endpoint);
            }
            activeAttempt = null;
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
                return () -> terminate(generation, () -> core.schedulerFailed(generation, failure));
            }
            return () -> post(
                    generation,
                    () -> core.sent(generation, ticket, sentAt, nanoTime(generation)),
                    Mailbox.Kind.CONTROL,
                    0
            );
        }

        @Override
        public void written(long generation, long ticket) {
            post(
                    generation,
                    () -> core.written(generation, ticket, nanoTime(generation)),
                    Mailbox.Kind.CONTROL,
                    0
            );
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
                mailbox.connectionRequested(false);
                mailbox.wake();
            }
        }

        @Override
        public void closed() {
            // The HttpClient of WebSocket transports is released once none uses it, and
            // termination waits for its threads too.
            WebSocketTransports shared = webSockets;
            if (shared == null) {
                termination.transportsClosed();
            } else {
                shared.release(termination::transportsClosed);
            }
        }
    }

    /// Operation slots, including those of connect() and disconnect(), stay held until application
    /// continuations return, and notifications until the listener returns or the lane drops them;
    /// lanes and deliveries until handlers return and acknowledgements are released.
    record Usage(
            int operations,
            int notifications,
            int mailboxEvents,
            long mailboxBytes,
            int sends,
            long sendBytes,
            int lanes,
            int deliveries,
            long deliveryBytes,
            int waiters
    ) {
    }

    private static final class TimerEntry {

        TimerScheduler.@Nullable Cancellable cancellation;

        // Set by the scheduler when a reconnect timer fires; the actor reads it.
        volatile boolean due;
    }

    private void retire(ClientSubscription subscription, @Nullable Throwable cause) {
        retire(subscription, cause, false);
    }

    /// With `drain`, deliveries the subscription had already accepted still run or can still be
    /// received; otherwise they are discarded.
    private void retire(ClientSubscription subscription, @Nullable Throwable cause, boolean drain) {
        synchronized (lifecycleLock) {
            if (subscription.retired) {
                return;
            }
            subscription.retired = true;
            subscriptions.remove(subscription.id);
            subscription.endLocally(cause, drain);
        }
    }

    private final class ClientSubscription implements Subscription {

        private final long id;

        private final Subscribe request;

        private final HandlerExecution execution;

        // Exactly one of handler, acceptor, and receiver decides how deliveries reach the
        // application: a push, hand-off, or pull subscription.
        private final @Nullable Consumer<MqttDelivery> handler;

        private final @Nullable Predicate<MqttDelivery> acceptor;

        private final DeliveryLanes.@Nullable Receiver receiver;

        private volatile @Nullable Qos granted;

        private long establishedGeneration;

        private final List<CompletableFuture<Qos>> establishmentWaiters = new ArrayList<>();

        private @Nullable Throwable establishmentFailure;

        private boolean registrationStarted;

        // Attempt state belongs to the client, never to a caller-mutable future.
        private @Nullable UnsubscribeAttempt unsubscription;

        private boolean locallyClosed;

        private boolean retired;

        // Termination follows the local end once a started lane has ended: a push lane starts
        // on admission, while a pull lane exists from creation.
        private boolean ended;

        private boolean laneStarted;

        private boolean laneEnded;

        private boolean terminated;

        private final List<CompletableFuture<Void>> terminationWaiters = new ArrayList<>();

        ClientSubscription(
                long id,
                Subscribe request,
                HandlerExecution execution,
                @Nullable Consumer<MqttDelivery> handler,
                @Nullable Predicate<MqttDelivery> acceptor
        ) {
            this.id = id;
            this.request = request;
            this.execution = execution;
            this.handler = handler;
            this.acceptor = acceptor;
            receiver = handler == null && acceptor == null ? lanes.receiver(id, request) : null;
            laneStarted = receiver != null;
        }

        /// Caller holds lifecycleLock. Admits this subscription's routing lane, of the kind that
        /// its registration chose. False when the lane limit is full.
        private boolean admitLane() {
            if (receiver != null) {
                return lanes.register(receiver);
            }
            if (acceptor != null) {
                return lanes.registerHandOff(id, request, acceptor);
            }
            return lanes.register(id, request, execution, requireNonNull(handler));
        }

        @Override
        public TopicFilter filter() {
            return request.filter();
        }

        @Override
        public Qos grantedQos() {
            Qos value = granted;
            if (value == null) {
                throw new IllegalStateException("Subscription has not been established");
            }
            return value;
        }

        @Override
        public CompletableFuture<Qos> established() {
            synchronized (lifecycleLock) {
                if (establishmentFailure != null) {
                    return CompletableFuture.failedFuture(establishmentFailure);
                }
                if (stopping.get()) {
                    endEstablishment(closedFailure());
                    return CompletableFuture.failedFuture(requireNonNull(establishmentFailure));
                }
                if (!disconnecting && connectedGeneration != 0
                        && establishedGeneration == connectedGeneration) {
                    return CompletableFuture.completedFuture(requireNonNull(granted));
                }
                return observeFuture(establishmentWaiters);
            }
        }

        private void endEstablishment(Throwable failure) {
            if (establishmentFailure == null) {
                establishmentFailure = failure;
                completeWaiters(establishmentWaiters, null, failure);
            }
        }

        /// Caller holds lifecycleLock. Ends local delivery before establishment, so a caller that
        /// sees establishment fail finds no further delivery admitted. Without `drain`, receiving
        /// also has ended; with it, the deliveries accepted before the end still run or can still
        /// be received. Without a cause the ending is ordinary.
        private void endLocally(@Nullable Throwable cause, boolean drain) {
            ended = true;
            if (receiver != null) {
                if (drain) {
                    receiver.finish();
                } else {
                    receiver.end(cause);
                }
            } else if (drain) {
                lanes.finish(id);
            } else {
                lanes.remove(id, cause);
            }
            endEstablishment(
                    cause != null ? cause : new IllegalStateException("Subscription is closed")
            );
            checkTerminated();
        }

        /// Caller holds lifecycleLock. Termination follows the local end once a started lane has
        /// ended, so no handler of this subscription runs and no delivery remains for it.
        private void checkTerminated() {
            if (!terminated && ended && (!laneStarted || laneEnded)) {
                terminated = true;
                unterminated.remove(id);
                completeWaiters(terminationWaiters, null, null);
            }
        }

        @Override
        public CompletableFuture<Void> terminated() {
            synchronized (lifecycleLock) {
                if (terminated) {
                    return CompletableFuture.completedFuture(null);
                }
                return observeFuture(terminationWaiters);
            }
        }

        /// Caller holds lifecycleLock. A requested removal decides how this subscription ends.
        private boolean removalRequested() {
            return locallyClosed || unsubscription != null;
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
                throw new UnsupportedOperationException("Only a pull subscription receives");
            }
            return receiving;
        }

        @Override
        public CompletableFuture<Void> unsubscribe() {
            synchronized (lifecycleLock) {
                if (stopping.get()) {
                    // Discards deliveries still queued here; a running handler finishes.
                    endLocally(permanentFailure(), false);
                    return CompletableFuture.completedFuture(null);
                }
                if (retired) {
                    // Deliveries an earlier confirmed removal accepted may still be finishing.
                    return CompletableFuture.completedFuture(null);
                }

                lanes.pause(id);
                return requestRemoval(this).observe();
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
                endLocally(permanentFailure(), false);
                if (!stopping.get() && !retired) {
                    requestRemoval(this);
                }
            }
        }
    }

    /// One durable removal, with independently bounded observations. Guarded by lifecycleLock.
    private final class UnsubscribeAttempt {

        private final long token;
        private final ClientSubscription subscription;
        private final List<CompletableFuture<Void>> observers = new ArrayList<>();
        private boolean started;

        UnsubscribeAttempt(long token, ClientSubscription subscription) {
            this.token = token;
            this.subscription = subscription;
        }

        /// A finished attempt has no new observers: its handle is already retired, and later
        /// calls complete locally.
        CompletableFuture<Void> observe() {
            return observeFuture(observers);
        }

        void finish(@Nullable Throwable cause) {
            completeWaiters(observers, null, cause);
        }
    }

    /// Caller holds lifecycleLock. The budget includes completion through synchronous callbacks.
    private <T> CompletableFuture<T> observeFuture(List<CompletableFuture<T>> observers) {
        if (!waiterSlots.tryAcquire()) {
            refused(CapacityLimit.OBSERVATIONS);
            return CompletableFuture.failedFuture(new MqttCapacityException("Waiter capacity"));
        }
        var future = new CompletableFuture<T>();
        observers.add(future);
        return future;
    }

    /// Caller holds lifecycleLock, but only for one thread start: a starter outside the lock gives
    /// each observation its own thread, so a slow continuation delays no other observation.
    private <T> void completeWaiters(
            List<CompletableFuture<T>> observers,
            @Nullable T value,
            @Nullable Throwable failure
    ) {
        if (observers.isEmpty()) {
            return;
        }
        List<CompletableFuture<T>> completing = List.copyOf(observers);
        observers.clear();
        Thread.ofVirtual().name(threadName("observations")).start(() -> {
            for (CompletableFuture<T> future : completing) {
                Thread.ofVirtual().name(threadName("observation-" + sequence.incrementAndGet()))
                        .start(() -> {
                            try {
                                if (failure == null) {
                                    future.complete(value);
                                } else {
                                    future.completeExceptionally(failure);
                                }
                            } finally {
                                waiterSlots.release();
                                termination.check();
                            }
                        });
            }
        });
    }

    private enum Command {
        CONNECT,
        DISCONNECT,
        PUBLISH
    }

    private static final class Operation<T> {

        final CompletableFuture<T> future = new CompletableFuture<>();

        // Returned once the future's synchronous continuations have run.
        final Semaphore slot;

        // Present for connect(). The core starts it unless the call joins the current attempt or
        // completes at once.
        final @Nullable ConnectionAttempt attempt;

        Operation(Semaphore slot, @Nullable ConnectionAttempt attempt) {
            this.slot = slot;
            this.attempt = attempt;
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
