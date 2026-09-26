package org.mqtt4j.mqtj.client.internal;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.mqtj.ConnAck;
import org.mqtt4j.mqtj.Disconnection;
import org.mqtt4j.mqtj.MqttCapacityException;
import org.mqtt4j.mqtj.MqttConnectException;
import org.mqtt4j.mqtj.MqttConnectionLostException;
import org.mqtt4j.mqtj.MqttNotConnectedException;
import org.mqtt4j.mqtj.MqttProtocolException;
import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.ReasonCode;
import org.mqtt4j.mqtj.client.internal.common.io.SendQueue;
import org.mqtt4j.mqtj.client.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.client.internal.common.protocol.PacketDecodeException;

import static java.util.Objects.requireNonNull;
import static org.mqtt4j.mqtj.client.internal.common.protocol.Properties.EMPTY;

/// A single-owner state machine. Time is input; outputs are nonblocking commands.
/// Operation tokens belong to the shell, which alone owns and completes futures.
final class SessionCore {

    enum State {
        DISCONNECTED,
        CONNECTING,
        CONNECTED,
        CLOSED
    }

    enum Timer {
        CONNECT,
        KEEP_ALIVE,
        PING,
        DISCONNECT
    }

    interface Port {

        void open(long generation, long operation);

        /// Reports queue admission, not write completion. The core handles refusal per operation.
        SendQueue.OfferResult send(long generation, long ticket, Packet packet);

        void schedule(long generation, Timer timer, Duration delay);

        void cancel(Timer timer);

        void ignoredPingResponse();

        void complete(long operation, @Nullable ConnAck result, @Nullable Throwable failure);

        void end(long generation, boolean established, Disconnection disconnection);
    }

    private final Port port;

    private final String clientId;

    private final Duration timeout;

    private final Duration keepAlive;

    private State state = State.DISCONNECTED;

    private long generation;

    private long connectOperation;

    private long disconnectOperation;

    private long nextTicket;

    private long pingTicket;

    private long disconnectTicket;

    private long lastSent;

    private boolean transportConnected;

    private boolean awaitingPing;

    SessionCore(Port port, String clientId, Duration timeout, Duration keepAlive) {
        this.port = port;
        this.clientId = clientId;
        this.timeout = timeout;
        this.keepAlive = keepAlive;
    }

    State state() {
        return state;
    }

    void connect(long operation, long now) {
        if (state != State.DISCONNECTED) {
            port.complete(operation, null, new IllegalStateException("Client is " + state));
            return;
        }
        state = State.CONNECTING;
        generation++;
        connectOperation = operation;
        lastSent = now;
        transportConnected = false;
        port.open(generation, operation);
        port.schedule(generation, Timer.CONNECT, timeout);
    }

    void opened(long connection) {
        if (connection == generation && state == State.CONNECTING) {
            transportConnected = true;
            sendRequired(++nextTicket, new Packet.Connect(
                    clientId,
                    MqttVersion.V3_1_1,
                    true,
                    (int) keepAlive.toSeconds(),
                    null,
                    null,
                    null,
                    EMPTY
            ));
        }
    }

    void received(long connection, Packet packet, long now) {
        if (connection != generation || state == State.DISCONNECTED || state == State.CLOSED) {
            return;
        }
        if (state == State.CONNECTING) {
            if (!(packet instanceof Packet.ConnAck ack)) {
                failed(connection, protocolError("Expected CONNACK"));
            } else if (ack.reasonCode() != ReasonCode.SUCCESS) {
                failed(connection, new MqttConnectException(
                        "Broker refused connection",
                        ack.reasonCode()
                ));
            } else if (ack.sessionPresent()) {
                failed(connection, protocolError("Session Present with Clean Session"));
            } else {
                state = State.CONNECTED;
                port.cancel(Timer.CONNECT);
                long operation = connectOperation;
                connectOperation = 0;
                port.complete(operation, new ConnAck(false), null);
                if (!keepAlive.isZero()) {
                    scheduleKeepAlive(now);
                }
            }
        } else if (disconnectOperation != 0) {
            // Input already in flight cannot change a local shutdown's write/close sequence.
            return;
        } else if (packet instanceof Packet.PingResp) {
            if (awaitingPing) {
                awaitingPing = false;
                port.cancel(Timer.PING);
            } else {
                port.ignoredPingResponse();
            }
        } else {
            failed(connection, protocolError("Unexpected " + packet.getClass().getSimpleName()));
        }
    }

    void sent(long connection, long ticket, long now) {
        if (connection != generation || state == State.DISCONNECTED || state == State.CLOSED) {
            return;
        }
        lastSent = now;
        if (ticket == disconnectTicket) {
            long operation = disconnectOperation;
            disconnectOperation = 0;
            end(null, true, false);
            port.complete(operation, null, null);
        }
    }

    void timer(long connection, Timer timer, long now) {
        if (connection != generation) {
            return;
        }
        switch (timer) {
            case CONNECT -> {
                if (state == State.CONNECTING) {
                    String phase = transportConnected ? "CONNACK" : "Transport connection";
                    failed(connection, new TimeoutException(phase + " deadline expired"));
                }
            }
            case KEEP_ALIVE -> {
                if (state == State.CONNECTED && disconnectOperation == 0) {
                    long remaining = keepAlive.toNanos() - (now - lastSent);
                    if (awaitingPing) {
                        port.schedule(generation, Timer.KEEP_ALIVE, keepAlive);
                    } else if (remaining > 0) {
                        port.schedule(generation, Timer.KEEP_ALIVE, Duration.ofNanos(remaining));
                    } else {
                        awaitingPing = true;
                        pingTicket = ++nextTicket;
                        // Include a stalled write in the deadline; otherwise it can wait forever.
                        port.schedule(generation, Timer.PING, keepAlive);
                        if (sendRequired(pingTicket, new Packet.PingReq())) {
                            port.schedule(generation, Timer.KEEP_ALIVE, keepAlive);
                        }
                    }
                }
            }
            case PING -> {
                if (state == State.CONNECTED && awaitingPing) {
                    failed(connection, new TimeoutException("PINGRESP deadline expired"));
                }
            }
            case DISCONNECT -> {
                if (disconnectOperation != 0) {
                    failed(connection, new TimeoutException("DISCONNECT write deadline expired"));
                }
            }
        }
    }

    private void scheduleKeepAlive(long now) {
        port.schedule(
                generation,
                Timer.KEEP_ALIVE,
                Duration.ofNanos(Math.max(0, keepAlive.toNanos() - (now - lastSent)))
        );
    }

    void disconnect(long operation) {
        if (state == State.CLOSED) {
            port.complete(operation, null, new IllegalStateException("Client is closed"));
        } else if (state != State.CONNECTED || disconnectOperation != 0) {
            port.complete(operation, null, new MqttNotConnectedException());
        } else {
            disconnectOperation = operation;
            port.cancel(Timer.KEEP_ALIVE);
            port.cancel(Timer.PING);
            awaitingPing = false;
            disconnectTicket = ++nextTicket;
            port.schedule(generation, Timer.DISCONNECT, timeout);
            sendRequired(
                    disconnectTicket,
                    new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY)
            );
        }
    }

    private boolean sendRequired(long ticket, Packet packet) {
        return switch (port.send(generation, ticket, packet)) {
            case ACCEPTED -> true;
            case FULL -> {
                failed(generation, new MqttCapacityException("Reserved send queue capacity"));
                yield false;
            }
            // Wait for the transport's terminal callback to preserve its original cause.
            case CLOSED -> false;
        };
    }

    void failed(long connection, Throwable cause) {
        if (connection == generation && state != State.DISCONNECTED && state != State.CLOSED) {
            end(cause, disconnectOperation != 0, false);
        }
    }

    void close(@Nullable Throwable cause) {
        end(cause, cause == null || disconnectOperation != 0, true);
    }

    private void end(@Nullable Throwable cause, boolean requested, boolean permanent) {
        if (state == State.CLOSED) {
            return;
        }
        if (cause instanceof PacketDecodeException failure) {
            // Both listener notifications and pending operations expose the same public cause.
            cause = new MqttProtocolException(
                    requireNonNull(failure.getMessage()),
                    failure.reasonCode(),
                    failure
            );
        }
        boolean established = state == State.CONNECTED;
        boolean active = state != State.DISCONNECTED;
        state = permanent ? State.CLOSED : State.DISCONNECTED;
        for (var timer : Timer.values()) {
            port.cancel(timer);
        }
        awaitingPing = false;
        pingTicket = 0;
        disconnectTicket = 0;
        if (active) {
            port.end(generation, established, new Disconnection(cause, requested));
        }
        if (connectOperation != 0) {
            Throwable failure = switch (cause) {
                case null -> new MqttConnectException("Connection failed");
                case MqttConnectException refusal -> refusal;
                default -> new MqttConnectException("Connection failed", cause);
            };
            long operation = connectOperation;
            connectOperation = 0;
            port.complete(operation, null, failure);
        }
        if (disconnectOperation != 0) {
            long operation = disconnectOperation;
            disconnectOperation = 0;
            port.complete(
                    operation,
                    null,
                    (cause == null)
                            ? new MqttConnectionLostException()
                            : new MqttConnectionLostException(cause)
            );
        }
    }

    private static PacketDecodeException protocolError(String message) {
        return new PacketDecodeException(ReasonCode.PROTOCOL_ERROR, message);
    }
}
