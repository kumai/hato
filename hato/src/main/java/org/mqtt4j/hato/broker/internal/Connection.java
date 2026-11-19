package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.SocketAddress;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.broker.BrokerListener.DisconnectReason;
import org.mqtt4j.hato.internal.concurrent.Timer;
import org.mqtt4j.hato.internal.io.SendQueue;
import org.mqtt4j.hato.internal.log.Log;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.PacketDecodeException;
import org.mqtt4j.hato.internal.protocol.Properties;
import org.mqtt4j.hato.internal.protocol.UnsupportedProtocolVersionException;

import static java.util.Objects.requireNonNull;

// One connection of a client: the protocol and the state of the connection, over a wire that
// carries its packets (see Wire). The selector that it is assigned to reads and writes it; where
// a comment speaks of the connection's reader or writer, it means that selector at the one work
// or the other, and no thread of the connection's own. The reader hands the packets that arrive
// to the session that the CONNECT attached, whose actor handles them.
final class Connection {

    private static final Log log = Log.of(Connection.class);

    static final Set<MqttVersion> SUPPORTED_VERSIONS = EnumSet.of(
            MqttVersion.V3_1_1,
            MqttVersion.V5
    );

    static final int SEND_COUNT = 128;

    // Sends that wait together go out in one write, up to this many bytes, rather than one write
    // each: every write is a call into the operating system, which costs more than a small packet
    // itself. A larger send goes alone.
    static final int BATCH_BYTES = 64 * 1024;

    // Responses to a client's requests queue apart from deliveries and PUBRELs, up to the 128
    // the regular queue allowed them before sessions. While they are all waiting, the reader
    // stops reading requests, so a client that does not read its responses is held back by TCP.
    private static final int RESERVED_COUNT = 128;

    private static final long RESERVED_BYTES = 1024 * 1024;

    // How many bytes of packets a connection reads at most for a CONNECT that waits to take its
    // session over (see readBeforeTakeOver): a client that goes on sending there holds the
    // takeover up only while its session handles this much.
    private static final long TAKE_OVER_INPUT = 1024 * 1024;

    // The largest packet that the framing format allows. The broker's own limits are at most
    // this.
    static final int MAX_PACKET_SIZE = 268_435_460;

    private final Routing routing;

    private final BrokerSettings settings;

    private final Timer scheduler;

    private final Wire wire;

    final Loop loop;

    private final SendQueue<Outbound> outgoing;

    final Client client;

    // The identifier is assigned once on acceptance, before the session attaches the connection,
    // and read by timers, logs, and the test controls.
    volatile String clientId = "";

    final AtomicBoolean scheduled = new AtomicBoolean();

    final Reactor.Task readiness = new Reactor.Task(this, this::ready);

    // I/O state belongs to the selector. Actor callbacks publish acceptance through its queue.

    private boolean connecting;

    private boolean connected;

    // The identifier of the client whose CONNECT waits for the application's authenticator,
    // while it waits; the connection's deadline is then the decision's. Guarded by this.
    private @Nullable String authenticating;

    // Whether the connection counts among those that wait for the broker to accept a CONNECT on
    // them, as every connection does from its registration.
    private boolean awaitingConnect = true;

    private boolean readClosed;

    private boolean ioFailed;

    // Why a write failed, while the connection still hands the input that had arrived by then
    // to its session, and null otherwise.
    private @Nullable String writeFailure;

    private boolean disposed;

    // Whether the broker shuts down, which has the connection read the input that has arrived,
    // and whether it has: once a read found nothing more, or once it ended. The selector sets
    // both; the thread that shuts the broker down reads the second.
    private boolean catchingUp;

    private volatile boolean caughtUp;

    // Whether a CONNECT waits for the connection to read the input that has arrived before it
    // takes the session over, and the bytes of the packets that the connection has read since a
    // CONNECT last came for that; the selector's.
    private boolean takeOverWaits;

    private long readForTakeOver;

    // Whether the connection waits, after its last packet, for the client to close it, which
    // only a shutdown of the broker has a connection do; guarded by this. Then whether its output
    // has ended, after which it drops what the client still sends (see endOutput), and whether
    // the write in progress is the last packet of such a connection; the selector's.
    private boolean lingering;

    private boolean outputEnded;

    private boolean writingLastLingers;

    private @Nullable Packet pending;

    private boolean pendingAnswered;

    private int pendingBytes;

    // When the reader took the pending packet, by the broker's clock: the time at which the
    // broker received it, from which the lifetime of a publication counts down.
    private long pendingReceived;

    private boolean pendingReserved;

    private List<SendQueue.Send<Outbound>> writing = List.of();

    // What the deliveries of the write in progress hold on the heap.
    private long writingMemory;

    // The deliveries of the write in progress, by QoS level, and whether the write leaves them
    // out: they count as delivered once the write completes with them.
    private final int[] writingDeliveries = new int[3];

    private boolean writingSkips;

    // Where the loop counts the PUBLISH packets that it reads and writes.
    private final Traffic traffic;

    private boolean writingLast;

    // Remaining state is guarded by this connection's monitor.

    // Responses that a test released, waiting for room in the reserved quota in order. Later
    // responses queue behind them, so that none overtakes a released one.
    private final ArrayDeque<Packet> released = new ArrayDeque<>();

    // The version of the connection's CONNECT. Until CONNECT selects one, a refusal of an
    // unsupported protocol level uses the MQTT 3.1.1 form, which every client can read.
    private MqttVersion version = MqttVersion.V3_1_1;

    // Whether the connection has received its CONNECT, which a shutdown that comes before the
    // broker accepts the client answers.
    private boolean connectReceived;

    boolean ended;

    // Whether the end of the connection has gone to its session, or found none attached.
    private boolean endHandled;

    // The responses that the reader has reserved room for and the session has yet to queue: the
    // reader hands the requests to the session, which answers them on its actor.
    private int owed;

    // Why the connection ended first; later failures are consequences of that end.
    private String endReason = "";

    // The same, as the application's listener hears of it. It is null while the connection goes
    // on, and stays null for one that ends before the broker accepts its client, or as the
    // broker closes, of which the listener hears nothing.
    private @Nullable DisconnectReason endKind;

    private long ticket;

    // What the deliveries in the send queue hold on the heap, those of the write in progress
    // included. The queue bounds the bytes of the packets that wait in it, but a delivery of
    // many small properties holds many times its packet's bytes, and the queue holds the
    // delivery until it is written. So what the deliveries hold stays within the same limit.
    private long queuedMemory;

    // Set once the connection is to end with this packet, such as a refusal CONNACK or a
    // DISCONNECT. The queue then admits nothing more; the writer writes what waits, except
    // deliveries, then this packet, and closes.
    private @Nullable Outbound lastPacket;

    // A delivery or PUBREL found its queue full; the writer's next completion resumes it.
    private boolean awaitingSpace;

    // Set once a write has failed. Nothing goes out any more, so the connection takes no
    // deliveries, drops the responses that its session still makes, and reserves no room for
    // them.
    private boolean outputFailed;

    private long lastReceived;

    private long receiveTimeout;

    private long timerGeneration;

    private Timer.@Nullable Cancellable cancellation;

    Connection(Wire wire, Routing routing) {
        this.wire = wire;
        this.routing = routing;
        settings = routing.settings;
        scheduler = settings.scheduler();
        outgoing = new SendQueue<>(
                SEND_COUNT,
                settings.sendBytes(),
                RESERVED_COUNT,
                RESERVED_BYTES
        );
        lastReceived = scheduler.nanoTime();
        receiveTimeout = settings.connectTimeout();
        client = new Client(this, routing);
        loop = wire.loop();
        traffic = loop.traffic();
    }

    @Nullable SocketAddress remoteAddress() {
        return wire.remoteAddress();
    }

    // Whether the client runs in the same JVM.
    boolean inProcess() {
        return wire.inProcess();
    }

    // Whether the client connected to the TLS listener.
    boolean overTls() {
        return wire.overTls();
    }

    // The certificates that the client presented in its TLS handshake, its own first, or none.
    // Called on the selector once the client has sent its CONNECT.
    List<X509Certificate> clientCertificates() {
        return wire.clientCertificates();
    }

    // Refuses the packets after this point that are larger than `maximumPacketSize`: the
    // broker's limit for an accepted client, which an MQTT 5.0 CONNACK announced. Acceptance
    // sets it before the selector resumes reads.
    void limitPacketSize(int maximumPacketSize) {
        wire.limit(maximumPacketSize);
    }

    synchronized boolean start() {
        if (!wire.register(this)) {
            return false;
        }
        armTimer(receiveTimeout - (scheduler.nanoTime() - lastReceived));
        return true;
    }

    // Has the selector look at the connection: at what waits to be written, and at the input
    // that it can take again. A thread of the actor pool keeps the request until its task ends
    // (see Worker).
    void wake() {
        loop.wake(this);
    }

    // Has the selector look at the connection without waiting for the end of a pool thread's
    // task, as wake() does there.
    void wakeNow() {
        loop.schedule(this);
    }

    void ready() {
        scheduled.set(false);
        if (disposed) {
            return;
        }
        if (terminated()) {
            dispose();
            return;
        }
        if (outputEnded) {
            discardInput();
            return;
        }
        if (writeFailure != null) {
            handOverLastInput();
        } else if (!ioFailed) {
            try {
                writeReady();
            } catch (IOException failure) {
                writeFailed(failure);
            }
        }
        if (outputEnded) {
            // The last packet has just gone out: from now on, the connection reads only to drop
            // what the client still sends (see endOutput).
            return;
        }
        try {
            if (!ioFailed && !isEnded() && !connecting && !readClosed && resumePending()
                    && wire.inputWaiting()) {
                readReady();
            }
        } catch (IOException failure) {
            endRead(failureReason("I/O", failure), true);
        } catch (PacketDecodeException failure) {
            decodeFailed(failure);
        }
        wire.interest(
                !isEnded() && !connecting && !readClosed && pending == null,
                !ioFailed && wire.writing()
        );
        if (catchingUp && !caughtUp) {
            catchUpOrReadOn();
        }
        if (takeOverWaits) {
            readOnForTakeOver();
        }
    }

    // Has the connection read the input that has arrived by now, as the broker begins to shut
    // down: the broker delivers what that input asks for before it ends (see
    // BrokerServer.shutdown). Runs on the loop.
    void catchUp() {
        catchingUp = true;
        // The loop may not have seen the input arrive yet.
        wire.expectInput();
        ready();
    }

    // Notes that the connection has read the input that had arrived as the shutdown began, once
    // a read finds nothing more or the connection has ended, and otherwise has the loop come
    // back to read on: a read takes a part of what waits. A packet that waits for its session or
    // for the CONNECT's acceptance keeps the connection from reading until that is done, which
    // brings the loop back here. Records that wait to go out to a client that does not read are
    // no input: the loop would come back for them without end.
    private void catchUpOrReadOn() {
        if (isEnded() || readClosed || ioFailed) {
            caughtUp = true;
        } else if (!connecting && pending == null) {
            if (wire.unreadInput()) {
                loop.schedule(this);
            } else {
                caughtUp = true;
            }
        }
    }

    // Whether the connection has read the input that had arrived as the broker began to shut
    // down; see catchUp().
    boolean caughtUp() {
        return caughtUp;
    }

    // Has the connection read the input that has arrived, as a CONNECT comes to take its session
    // over, and then tells the session's actor, which handles that input before the CONNECT
    // (see Session.takeOverAfterInput). Runs on the loop.
    void readBeforeTakeOver() {
        takeOverWaits = true;
        readForTakeOver = 0;
        // The loop may not have seen the input arrive yet.
        wire.expectInput();
        ready();
    }

    // Tells the session's actor once the connection has read the input that had arrived when a
    // CONNECT came to take the session over, and otherwise has the loop come back to read on,
    // unless the failed write, the acceptance, or the actor brings it back. Nothing more comes
    // to the session once the connection ends or reads no more, and the reading stops once a
    // read finds nothing more or the connection has read TAKE_OVER_INPUT bytes since. It stops
    // too at a packet that waits for room for its response, which a client that does not read
    // its responses never makes.
    // Ceiling: a client with more requests unanswered than their responses have room for has what
    // it sent after them come after the takeover, which leaves it unhandled with the old
    // connection. Waiting for that room until a deadline would lift that.
    private void readOnForTakeOver() {
        if (!isEnded() && !readClosed && readForTakeOver < TAKE_OVER_INPUT) {
            boolean broughtBack = writeFailure != null
                    || connecting
                    || (pending != null && (pendingReserved || !pendingAnswered));
            if (broughtBack) {
                return;
            }
            if (pending == null && wire.unreadInput()) {
                loop.schedule(this);
                return;
            }
        }
        takeOverWaits = false;
        Session owner = requireNonNull(client.session);
        owner.post(() -> owner.takeOversGoOn(this), Session.ELSEWHERE);
    }

    // Whether the connection is still to take deliveries: it has not ended, is not about to,
    // and can still write.
    synchronized boolean takesDeliveries() {
        return !ended && lastPacket == null && !outputFailed;
    }

    // Whether everything that the connection had to send has been written: nothing waits in
    // its send queue, nothing is being written, and no response that a test released waits.
    synchronized boolean sentAll() {
        return writing.isEmpty() && !outgoing.hasReady() && released.isEmpty();
    }

    // Ends the connection as the broker shuts down. The broker gives no notice of it, as it
    // closes: it is to end the connection after this. A client whose CONNECT the broker has yet
    // to accept, such as one that waits for the authenticator's decision, receives CONNACK with
    // Server unavailable, as one that connects during the shutdown does, and a connection
    // without CONNECT ends at once. Otherwise an MQTT 5.0 client receives DISCONNECT with Server
    // shutting down after what waits to be written, deliveries aside, as for any end with a last
    // packet, and an MQTT 3.1.1 client the end of the output. Over a network, the connection
    // then waits for the client to close it (see endOutput).
    synchronized void shutDown() {
        if (ended || lastPacket != null) {
            return;
        }
        lingering = !wire.inProcess();
        if (client.session == null) {
            if (connectReceived) {
                refuse(ReasonCode.SERVER_UNAVAILABLE, "a client while the broker shuts down");
            } else {
                terminate("broker shut down", null);
            }
            return;
        }
        switch (version) {
            case V5 -> closeAfter(
                    new Packet.Disconnect(ReasonCode.SERVER_SHUTTING_DOWN, Properties.EMPTY),
                    "broker shut down",
                    null
            );
            case V3_1_1 -> {
                if (lingering) {
                    closeAfter(new Outbound.End(), "broker shut down", null);
                } else {
                    terminate("broker shut down", null);
                }
            }
        }
    }

    // Ends the output once the last packet is written, and waits for the client to close the
    // connection, taking in and dropping what it still sends. A socket that closes while input
    // that has arrived lies unread resets the connection, which can lose what the client has
    // yet to read: the deliveries before the last packet, and that packet. The end of the
    // broker closes the connection if the client does not.
    private void endOutput() {
        outputEnded = true;
        try {
            wire.endOutput();
        } catch (IOException failure) {
            close(failureReason("I/O", failure), null);
            return;
        }
        discardInput();
    }

    // Takes in and drops what the client sends after the end of the output, until it closes the
    // connection.
    private void discardInput() {
        try {
            int read;
            do {
                read = wire.discard();
            } while (read > 0);
            if (read < 0) {
                close("closed by the client after the end", null);
                return;
            }
        } catch (IOException failure) {
            close(failureReason("I/O", failure), null);
            return;
        }
        wire.interest(true, false);
    }

    private synchronized boolean terminated() {
        return ended;
    }

    private boolean resumePending() {
        if (pending == null) {
            return true;
        }
        synchronized (this) {
            if (pendingAnswered && !pendingReserved) {
                if (!outputFailed && responseRoomTaken()) {
                    return false;
                }
                owed++;
                pendingReserved = true;
            }
        }
        if (!requireNonNull(client.session).receive(
                this,
                pending,
                pendingAnswered,
                pendingBytes,
                pendingReceived
        )) {
            return false;
        }
        pending = null;
        pendingReserved = false;
        return true;
    }

    private void readReady() throws IOException {
        // A hot publisher gets at most one read of input per turn. Input that has arrived
        // resumes first after backpressure, without waiting for another readiness edge.
        if (consume()) {
            return;
        }
        if (wire.read() < 0) {
            endRead("connection closed by the peer", false);
            return;
        }
        consume();
    }

    // A peer that goes away can leave packets behind, such as the DISCONNECT that discards its
    // Will, and a write can meet the failure before a read finds them. So a failed write ends
    // the output at once, and the connection itself only after the input that has arrived.
    private void writeFailed(IOException failure) {
        writeFailure = failureReason("I/O", failure);
        ioFailed = true;
        synchronized (this) {
            outputFailed = true;
        }
        handOverLastInput();
    }

    // Hands the input that has arrived to the session, then ends the connection after it, for
    // the write that failed. A CONNECT that its actor has yet to answer, or a packet that waits
    // for room in the session's inbox, keeps the connection until then: this returns, and the
    // selector comes back here as the actor goes on. A response needs no room any more.
    private void handOverLastInput() {
        String reason = requireNonNull(writeFailure);
        try {
            while (!isEnded() && !readClosed) {
                if (connecting || !resumePending()) {
                    return;
                }
                if (!consume() && wire.read() <= 0) {
                    break;
                }
            }
        } catch (IOException | PacketDecodeException _) {
            // Whatever the input holds, the connection ends for the failure of the write.
        }
        writeFailure = null;
        endRead(reason, true);
    }

    // Ends the connection for a packet that cannot be read, after the packets before it, which
    // the session has already: a DISCONNECT among them discards the Will, as it would without
    // the bytes behind it. The reader reads no more, because what follows a broken packet has no
    // packet boundary. Before a session, nothing waits ahead of the failure.
    private void decodeFailed(PacketDecodeException failure) {
        readClosed = true;
        Session owner = client.session;
        if (owner == null) {
            failed("decoding", failure);
        } else {
            owner.post(() -> failed("decoding", failure), Session.ELSEWHERE);
        }
    }

    // End the session's connection after the packets it has already received. A FIN still
    // allows a final response to be written; an I/O error stops both directions.
    private void endRead(String reason, boolean failed) {
        boolean alreadyPosted = readClosed;
        readClosed = true;
        ioFailed = failed;
        Session owner = client.session;
        if (owner == null) {
            close(reason, DisconnectReason.CONNECTION_LOST);
        } else if (failed || !alreadyPosted) {
            owner.post(() -> {
                if (failed || !endsWithLastPacket()) {
                    close(reason, DisconnectReason.CONNECTION_LOST);
                }
            }, Session.ELSEWHERE);
        }
    }

    // Returns true while a CONNECT or one complete packet waits for its actor or response room.
    private boolean consume() {
        while (true) {
            Wire.Arrival arrival;
            try {
                arrival = wire.next(connected, version);
            } catch (UnsupportedProtocolVersionException failure) {
                // A CONNECT after the one accepted breaks the protocol whatever its version
                // [MQTT-3.1.0-2], and the client has had its CONNACK.
                if (connected) {
                    throw new PacketDecodeException(
                            ReasonCode.PROTOCOL_ERROR,
                            "A second CONNECT",
                            failure
                    );
                }
                received();
                refuse(
                        ReasonCode.UNSUPPORTED_PROTOCOL_VERSION,
                        "unsupported version " + failure.protocolLevel()
                );
                return true;
            }
            if (arrival == null) {
                return false;
            }
            PacketChecks.check(arrival.packet());
            long receivedAt = received();
            if (!connected) {
                var request = (Packet.Connect) arrival.packet();
                // The client's Maximum Packet Size holds from the moment that the CONNECT counts as
                // received, for a shutdown that refuses the client before the CONNECT is checked.
                synchronized (this) {
                    version = request.version();
                    connectReceived = true;
                    client.sendLimit = Client.sendLimitOf(request);
                }
                settings.beforeProcess().accept(request);
                connecting = true;
                client.connect(request, accepted -> {
                    // The callback can be on an actor worker; only the selector resumes I/O.
                    loop.execute(this, () -> {
                        connecting = false;
                        connected = accepted;
                        if (accepted) {
                            leaveAwaitingConnect();
                        }
                        if (!accepted && !endsWithLastPacket()) {
                            close("CONNECT refused", null);
                        } else {
                            wake();
                        }
                    });
                });
                return true;
            }
            pending = arrival.packet();
            if (pending instanceof Packet.Publish publish) {
                traffic.received(publish.qos());
            } else if (pending instanceof Packet.AliasPublish publish) {
                traffic.received(publish.qos());
            }
            settings.beforeProcess().accept(pending);
            log.debug("Broker received {} from {}",
                    pending.getClass().getSimpleName(), wire.peer());
            pendingBytes = arrival.size();
            readForTakeOver += pendingBytes;
            pendingAnswered = answered(pending);
            pendingReceived = receivedAt;
            if (!resumePending() || isEnded()) {
                return true;
            }
        }
    }

    void refuse(ReasonCode reason, String request) {
        refuse(new Packet.ConnAck(false, reason, Properties.EMPTY), request);
    }

    // Answers the CONNECT with `refusal` and closes the connection. MQTT 3.1.1 has return codes
    // for five reasons only, and a broker with none of them to give closes the connection
    // without CONNACK (MQTT 3.1.1 [MQTT-3.2.2-6]).
    void refuse(Packet.ConnAck refusal, String request) {
        ReasonCode reason = refusal.reasonCode();
        log.info("Broker refused {} from {} on port {}: {}",
                request, wire.peer(), routing.port, reason);
        synchronized (this) {
            boolean answered = switch (version) {
                case V3_1_1 -> switch (reason) {
                    case UNSUPPORTED_PROTOCOL_VERSION,
                         CLIENT_IDENTIFIER_NOT_VALID,
                         SERVER_UNAVAILABLE,
                         BAD_USER_NAME_OR_PASSWORD,
                         NOT_AUTHORIZED -> true;
                    default -> false;
                };
                case V5 -> true;
            };
            if (answered) {
                closeAfter(refusal, "refusal sent", null);
            } else {
                terminate("refusal without an MQTT 3.1.1 return code", null);
            }
        }
    }

    synchronized SendQueue.OfferResult enqueue(Outgoing message) {
        if (lastPacket != null || outputFailed) {
            return SendQueue.OfferResult.CLOSED;
        }
        // PUBLISH and PUBREL share the regular queue, whose window always fits a resumed
        // session, and leave the reserved queue to responses to the client's requests.
        SendQueue.OfferResult result = switch (message) {
            // The writer prepares a delivery as it writes it. A session queues a delivery here
            // only once the connection that had it before has ended, so whether that connection
            // started to send it no longer changes.
            case Delivery delivery -> {
                // The queue holds the delivery until it is written, so it is full for one
                // whose memory does not fit, as for one whose packet does not.
                if (delivery.memoryBytes > settings.sendBytes() - queuedMemory) {
                    yield SendQueue.OfferResult.FULL;
                }
                SendQueue.OfferResult offered = outgoing.offer(
                        ++ticket,
                        new Outbound.Publish(delivery, delivery.sent),
                        delivery.byteCount,
                        false
                );
                if (offered == SendQueue.OfferResult.ACCEPTED) {
                    queuedMemory += delivery.memoryBytes;
                }
                yield offered;
            }
            case Release release -> {
                Outbound queued = wire.queued(release.packet(), version);
                yield outgoing.offer(
                        ++ticket,
                        queued,
                        queued.size(),
                        false
                );
            }
        };
        if (result == SendQueue.OfferResult.ACCEPTED) {
            wake();
        } else {
            awaitingSpace = true;
            // Only the writer makes room, and what a pool thread still queues waits or is
            // dropped meanwhile. So a pool thread that has filled the queue hands the
            // connection over now. One that found it full already has nothing to hand over:
            // the selector knows, and a peer that does not read gains nothing from a wake.
            if (Thread.currentThread() instanceof Worker worker) {
                worker.handOver(this);
            }
        }
        return result;
    }

    /// Whether the broker answers `packet` with one response in the reserved quota.
    private static boolean answered(Packet packet) {
        return switch (packet) {
            case Packet.Publish publish -> publish.qos() != Qos.AT_MOST_ONCE;
            case Packet.AliasPublish publish -> publish.qos() != Qos.AT_MOST_ONCE;
            case Packet.Subscribe _,
                 Packet.Unsubscribe _,
                 Packet.PubRel _,
                 Packet.PingReq _ -> true;
            case Packet.Connect _,
                 Packet.ConnAck _,
                 Packet.PubAck _,
                 Packet.PubRec _,
                 Packet.PubComp _,
                 Packet.SubAck _,
                 Packet.UnsubAck _,
                 Packet.PingResp _,
                 Packet.Disconnect _,
                 Packet.Auth _ -> false;
        };
    }

    // Whether a response now would find no room: the reserved quota is full with the responses
    // owed, or responses that a test released wait for it first. Called under the connection
    // monitor.
    private boolean responseRoomTaken() {
        return !released.isEmpty()
                || outgoing.checkCapacity(0, true, owed) == SendQueue.OfferResult.FULL;
    }

    // Returns the room that the reader reserved for the response to a request, which the
    // session has answered or passed over.
    synchronized void responseSettled() {
        owed--;
        wake();
    }

    // Queues a response to the client's request in the reserved quota, behind the responses
    // that a test released, and ends the connection when the quota is full. Called on the
    // session's actor, which holds no monitor, so that closing can take the session's inbox.
    void respond(Packet packet) {
        boolean accepted;
        synchronized (this) {
            // A connection that ends with a last packet drops later responses, and so does one
            // whose write has failed.
            if (lastPacket != null || outputFailed) {
                return;
            }
            if (!released.isEmpty()) {
                released.addLast(packet);
                return;
            }
            accepted = enqueue(packet);
        }
        if (!accepted) {
            close("send queue full", DisconnectReason.BROKER_ERROR);
        }
    }

    // Queues `responses`, which a test held back and now releases, behind any released before,
    // and sends what fits. Called by the test controls.
    synchronized void release(List<Packet> responses) {
        released.addAll(responses);
        sendReleased();
    }

    // Sends released responses while the reserved quota has room; the writer calls this
    // again each time it frees room. A connection that is ending drops them. Called under
    // the connection monitor.
    private void sendReleased() {
        Packet next;
        while ((next = released.peekFirst()) != null) {
            if (ended || lastPacket != null) {
                released.clear();
                return;
            }
            Outbound queued = wire.queued(next, version);
            if (outgoing.checkCapacity(queued.size(), true) != SendQueue.OfferResult.ACCEPTED) {
                return;
            }
            SendQueue.OfferResult result = outgoing.offer(
                    ++ticket,
                    queued,
                    queued.size(),
                    true
            );
            if (result != SendQueue.OfferResult.ACCEPTED) {
                return;
            }
            released.removeFirst();
            wake();
        }
    }

    // Queues `packet`, a packet other than a delivery, and returns false if the send queue is full
    // or the connection ends anyway. One that the client's Maximum Packet Size does not take ends
    // the connection instead, with DISCONNECT Packet too large if that fits [MQTT-3.1.2-24], and
    // counts as queued, since its sender goes on as for one that will go out. The client checks
    // the answers that can grow before it carries out their requests, so this is a last guard.
    // Called under the connection monitor; no I/O occurs on a session's actor.
    boolean enqueue(Packet packet) {
        if (ended || lastPacket != null) {
            return false;
        }
        Outbound queued = wire.queued(packet, version);
        if (queued.size() > client.sendLimit) {
            closeAfter(
                    new Packet.Disconnect(ReasonCode.PACKET_TOO_LARGE, Properties.EMPTY),
                    "a " + packet.getClass().getSimpleName() + " of " + queued.size()
                            + " bytes that the client's Maximum Packet Size of "
                            + client.sendLimit + " bytes does not take",
                    DisconnectReason.PROTOCOL_ERROR
            );
            return true;
        }
        boolean enqueueFailed = outgoing.offer(
                ++ticket,
                queued,
                queued.size(),
                true
        ) != SendQueue.OfferResult.ACCEPTED;
        if (enqueueFailed) {
            return false;
        }
        wake();
        return true;
    }

    private void writeReady() throws IOException {
        // One batch per turn bounds the time a writable peer keeps the selector. Completed
        // batches requeue the connection; zero-byte writes wait only for OP_WRITE.
        if (!wire.writing()) {
            Outbound last;
            boolean lingers;
            synchronized (this) {
                // A write that a test keeps back still has the sends that it took.
                if (writing.isEmpty()) {
                    writing = outgoing.pollBatch(BATCH_BYTES);
                    writingMemory = 0;
                    for (SendQueue.Send<Outbound> send : writing) {
                        if (send.value() instanceof Outbound.Publish publish) {
                            writingMemory += publish.delivery().memoryBytes;
                            writingDeliveries[publish.delivery().qos.level()]++;
                        }
                    }
                }
                if (ended) {
                    return;
                }
                last = lastPacket;
                lingers = lingering;
            }
            if (writing.isEmpty()) {
                if (last == null) {
                    return;
                }
                wire.prepareLast(last);
                writingLast = true;
                writingLastLingers = lingers;
            } else {
                // Every delivery holds something.
                boolean delivers = writingMemory > 0;
                boolean skip = false;
                if (delivers) {
                    // While a test keeps the write back, the connection goes on with its
                    // input. The test has the selector come back here.
                    if (settings.deliveryWriteHeld().test(this)) {
                        return;
                    }
                    synchronized (this) {
                        if (ended) {
                            return;
                        }
                        skip = lastPacket != null;
                        if (!skip) {
                            for (SendQueue.Send<Outbound> send : writing) {
                                if (send.value() instanceof Outbound.Publish publish) {
                                    publish.delivery().sent = true;
                                }
                            }
                        }
                    }
                }
                writingSkips = skip;
                wire.prepare(writing, skip, scheduler.nanoTime());
            }
        }
        if (!wire.write()) {
            return;
        }
        if (writingLast) {
            if (writingLastLingers) {
                endOutput();
            } else {
                // The connection's end has its reason since the last packet was queued.
                close("last packet written", null);
            }
            return;
        }
        if (!writingSkips) {
            traffic.delivered(writingDeliveries);
        }
        Arrays.fill(writingDeliveries, 0);
        boolean resume;
        boolean more;
        Session owner;
        synchronized (this) {
            outgoing.completed(writing.getLast().ticket());
            writing = List.of();
            queuedMemory -= writingMemory;
            writingMemory = 0;
            sendReleased();
            resume = awaitingSpace;
            awaitingSpace = false;
            owner = client.session;
            more = outgoing.hasReady() || lastPacket != null;
        }
        if (resume && owner != null) {
            owner.requestDrain();
        }
        if (more) {
            wake();
        }
    }

    // Peer I/O and protocol errors end only this connection. Anything else is a broker
    // defect, such as an OutOfMemoryError, and keeps its stack trace at WARNING.
    private String failureReason(String loop, Throwable failure) {
        if (failure instanceof IOException || failure instanceof PacketDecodeException) {
            if (!isEnded()) {
                log.debug("Broker {} for {} failed", loop, wire.peer(), failure);
            }
        } else {
            log.warn("Broker {} for {} failed", loop, wire.peer(), failure);
        }
        return failure.getClass().getSimpleName() + ": " + failure.getMessage();
    }

    private void logEnd() {
        String id;
        String reason;
        synchronized (this) {
            id = clientId;
            reason = endReason;
        }
        if (id.isEmpty()) {
            log.debug("Broker closed {} on port {} before accepting a client: {}",
                    wire.peer(), routing.port, reason);
        } else {
            log.info("Broker disconnected client {} from {} on port {}: {}",
                    id, wire.peer(), routing.port, reason);
        }
    }

    // Whether sessions must leave this connection: it has ended or ends with its last packet.
    synchronized boolean isEnded() {
        return ended || lastPacket != null;
    }

    private synchronized boolean endsWithLastPacket() {
        return lastPacket != null;
    }

    // Ends the connection for `kind`, first telling an MQTT 5.0 client why with DISCONNECT. MQTT
    // 3.1.1 has no DISCONNECT from a server.
    synchronized void disconnect(ReasonCode reasonCode, String reason, DisconnectReason kind) {
        disconnect(new Packet.Disconnect(reasonCode, Properties.EMPTY), reason, kind);
    }

    // Ends the connection as above with `disconnect`, and returns false if it was ending already.
    synchronized boolean disconnect(
            Packet.Disconnect disconnect,
            String reason,
            DisconnectReason kind
    ) {
        if (ended || lastPacket != null) {
            return false;
        }
        switch (version) {
            case V3_1_1 -> terminate(reason, kind);
            case V5 -> closeAfter(disconnect, reason, kind);
        }
        return true;
    }

    // Ends the connection with `packet` after the sends that wait, for `kind`, or without it when
    // the client's Maximum Packet Size does not take it [MQTT-3.1.2-24]. Called under the
    // connection monitor.
    private void closeAfter(Packet packet, String reason, @Nullable DisconnectReason kind) {
        if (ended || lastPacket != null) {
            return;
        }
        Outbound last = wire.queued(packet, version);
        closeAfter((last.size() <= client.sendLimit) ? last : new Outbound.End(), reason, kind);
    }

    // Ends the connection with `last`, a packet as it waits to be written or the end of the
    // output without one, after the sends that wait. Called under the connection monitor.
    private void closeAfter(Outbound last, String reason, @Nullable DisconnectReason kind) {
        if (ended || lastPacket != null) {
            return;
        }
        client.letGoOfWaitingConnect();
        // The connect timeout bounds the wait for a writer that a slow peer holds up.
        cancelTimer();
        try {
            armTimer(settings.connectTimeout());
        } catch (RuntimeException failure) {
            log.warn("Broker timer for {} failed", wire.peer(), failure);
            terminate(reason, kind);
            return;
        }
        lastPacket = last;
        endReason = reason;
        endKind = kind;
        wake();
    }

    // Notes that a packet has arrived, and returns when, by the broker's clock.
    private synchronized long received() {
        lastReceived = scheduler.nanoTime();
        return lastReceived;
    }

    // Has the connection wait for the authenticator's decision on the client `clientId`, which
    // may take `timeout` nanoseconds from the CONNECT. Returns false if the connection has ended.
    synchronized boolean awaitDecision(String clientId, long timeout) {
        if (ended || lastPacket != null) {
            return false;
        }
        authenticating = clientId;
        cancelTimer();
        receiveTimeout = timeout;
        armTimer(receiveTimeout - (scheduler.nanoTime() - lastReceived));
        return true;
    }

    // Whether the connection still waits for the authenticator's decision in time: it has not
    // ended, and the deadline has not passed. A false answer stays false.
    synchronized boolean awaitsDecision() {
        return authenticating != null
                && !ended
                && lastPacket == null
                && scheduler.nanoTime() - lastReceived < receiveTimeout;
    }

    // Ends the wait for the authenticator's decision. Returns false if the decision came too
    // late: the connection has ended, its deadline has refused the client already, or the
    // deadline has passed though its timer has not come yet, which refuses the client here.
    // A decision in time has the client's keep-alive count from now, as the connection read
    // nothing from the client while it waited.
    synchronized boolean decided() {
        String deciding = authenticating;
        authenticating = null;
        if (deciding == null || ended || lastPacket != null) {
            return false;
        }
        long now = scheduler.nanoTime();
        if (now - lastReceived >= receiveTimeout) {
            refuseUndecided(deciding);
            return false;
        }
        lastReceived = now;
        return true;
    }

    // Refuses the client `deciding`, whose authentication did not end by its deadline. Called
    // under this.
    private void refuseUndecided(String deciding) {
        refuse(
                ReasonCode.SERVER_UNAVAILABLE,
                "client '" + deciding + "', whose authentication took too long"
        );
    }

    synchronized void keepAlive(int seconds) {
        cancelTimer();
        receiveTimeout = seconds * 1_500_000_000L;
        if (receiveTimeout != 0) {
            armTimer(receiveTimeout - (scheduler.nanoTime() - lastReceived));
        }
    }

    private void armTimer(long delay) {
        long generation = ++timerGeneration;
        Timer.Cancellable scheduled = scheduler.schedule(
                Duration.ofNanos(Math.max(0, delay)), () -> checkDeadline(generation)
        );
        // An already-due callback may close or rearm the connection before schedule returns.
        if (ended || generation != timerGeneration) {
            cancel(scheduled);
        } else {
            cancellation = scheduled;
        }
    }

    private void cancelTimer() {
        // Cancellation cannot stop a callback already waiting for this monitor.
        timerGeneration++;
        Timer.Cancellable previous = cancellation;
        cancellation = null;
        if (previous != null) {
            cancel(previous);
        }
    }

    private void cancel(Timer.Cancellable task) {
        try {
            task.cancel();
        } catch (Throwable failure) {
            // A failing injected timer must not prevent the cleanup of the wire and the queue.
            log.warn(
                    "Cancelling broker timer for {} failed",
                    wire.peer(),
                    failure
            );
        }
    }

    private void checkDeadline(long generation) {
        try {
            synchronized (this) {
                if (ended || generation != timerGeneration) {
                    return;
                }
                if (lastPacket != null) {
                    // The writer did not get the last packet out within the connect timeout.
                    terminate(endReason, endKind);
                    return;
                }
                long remaining = receiveTimeout - (scheduler.nanoTime() - lastReceived);
                if (remaining > 0) {
                    armTimer(remaining);
                    return;
                }
                String deciding = authenticating;
                if (deciding != null) {
                    // The decision that comes later finds that it came too late.
                    authenticating = null;
                    refuseUndecided(deciding);
                    return;
                }
                // Mark the deadline under the same lock as receipt; a late frame cannot
                // revive this connection while its end goes to the session.
                if (clientId.isEmpty()) {
                    terminate("CONNECT timeout", null);
                } else {
                    disconnect(
                            ReasonCode.KEEP_ALIVE_TIMEOUT,
                            "keep-alive timeout",
                            DisconnectReason.KEEP_ALIVE_TIMEOUT
                    );
                }
            }
        } catch (Throwable failure) {
            log.warn("Broker timer for {} failed", wire.peer(), failure);
            synchronized (this) {
                terminate("timer failure", DisconnectReason.BROKER_ERROR);
            }
        }
        // Ending reserves selector work to hand the end to the session. The shared timer waits
        // for no actor before checking another peer.
    }

    // Stops I/O for `kind` and reserves selector work for any remaining session cleanup.
    private void terminate(String reason, @Nullable DisconnectReason kind) {
        if (ended) {
            return;
        }
        ended = true;
        client.letGoOfWaitingConnect();
        // A connection that ends with a last packet already holds its reason.
        if (endReason.isEmpty()) {
            endReason = reason;
            endKind = kind;
        }
        cancelTimer();
        outgoing.close();
        wake();
    }

    // Why the connection ended first, as the application's listener hears of it, or null if it
    // goes on or ended before the broker accepted its client.
    synchronized @Nullable DisconnectReason endKind() {
        return endKind;
    }

    // Ends the connection for `kind` and hands its end, once, to the session that it is attached
    // to.
    void close(String reason, @Nullable DisconnectReason kind) {
        Session owner;
        synchronized (this) {
            terminate(reason, kind);
            if (endHandled) {
                return;
            }
            endHandled = true;
            owner = client.session;
        }
        if (owner != null && !routing.closed) {
            owner.post(client::connectionEnded, Session.ELSEWHERE);
        }
    }

    // Only the selector lets go of the wire and releases its write batch.
    void dispose() {
        if (disposed) {
            return;
        }
        disposed = true;
        close("broker closed", null);
        // Before the wire closes, so that a peer that sees the close finds the place free.
        leaveAwaitingConnect();
        try {
            wire.close();
        } catch (IOException failure) {
            log.warn("Closing broker peer {} failed", wire.peer(), failure);
        } finally {
            outgoing.ended();
            routing.connections.remove(this);
        }
        logEnd();
    }

    // Takes the connection out of those that wait for their CONNECT, once: as the broker
    // accepts its client, or as it ends without that. Runs on the selector.
    private void leaveAwaitingConnect() {
        if (awaitingConnect) {
            awaitingConnect = false;
            routing.leftAwaitingConnect(wire.inProcess());
        }
    }

    // Has the connection's client belong to `session`, whose actor accepted the CONNECT, and
    // queues `connAck`. Returns false if the connection ends before its CONNACK: it has ended
    // already, or ends with another last packet, such as the refusal of a shutdown that came
    // meanwhile. The caller then hands the end to the session at once, so the end itself hands
    // over nothing when it comes. Called under the session's inbox.
    synchronized boolean attach(Session session, Packet.ConnAck connAck) {
        client.session = session;
        if (enqueue(connAck)) {
            return true;
        }
        endHandled = true;
        return false;
    }

    // Ends the connection for a failure in handling one of its packets, as the reader ends it for
    // one in reading: a packet that breaks the protocol with DISCONNECT, after which the writer
    // closes it.
    void failed(String operation, Throwable failure) {
        String reason = failureReason(operation, failure);
        DisconnectReason kind = switch (failure) {
            case PacketDecodeException _ -> DisconnectReason.PROTOCOL_ERROR;
            case IOException _ -> DisconnectReason.CONNECTION_LOST;
            default -> DisconnectReason.BROKER_ERROR;
        };
        if (failure instanceof PacketDecodeException violation) {
            disconnect(violation.reasonCode(), reason, kind);
        }
        if (!endsWithLastPacket()) {
            close(reason, kind);
        }
    }
}
