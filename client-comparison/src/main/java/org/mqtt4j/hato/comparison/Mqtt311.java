package org.mqtt4j.hato.comparison;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/// The few MQTT 3.1.1 packets that the comparison's stand-ins exchange, encoded and decoded by
/// hand.
///
/// The comparison keeps a codec of its own so that none of the clients under comparison, Hato
/// included, shapes the traffic that all of them receive. It handles only well-formed packets from
/// the clients and checks no more than it needs to.
final class Mqtt311 {

    static final int CONNECT = 1;

    static final int CONNACK = 2;

    static final int PUBACK = 4;

    static final int SUBSCRIBE = 8;

    static final int PINGREQ = 12;

    static final int PINGRESP = 13;

    static final int DISCONNECT = 14;

    private static final byte[] CONNACK_ACCEPTED = {0x20, 0x02, 0x00, 0x00};

    private static final byte[] PINGRESP_PACKET = {(byte) 0xd0, 0x00};

    private static final byte[] DISCONNECT_PACKET = {(byte) 0xe0, 0x00};

    // The variable header of a CONNECT for MQTT 3.1.1 with a clean session and keep-alive off: the
    // protocol name, level 4, the clean session flag, and a keep-alive of 0.
    private static final byte[] CONNECT_HEADER = {0, 4, 'M', 'Q', 'T', 'T', 4, 0x02, 0, 0};

    private Mqtt311() {
    }

    /// A packet as it arrived: its type, the flags of its first byte, and its body after the
    /// remaining length.
    record Packet(int type, int flags, byte[] body) {}

    /// A topic filter that a SUBSCRIBE asks for, and the QoS that it requests.
    record Subscription(String filter, int qos) {}

    /// A SUBSCRIBE packet's identifier and the subscriptions that it asks for, in order.
    record Subscribe(int packetId, List<Subscription> subscriptions) {

        Subscribe {
            subscriptions = List.copyOf(subscriptions);
        }
    }

    /// Reads the next packet. The end of the stream throws [java.io.EOFException].
    static Packet read(DataInputStream in) throws IOException {
        int first = in.readUnsignedByte();
        var body = new byte[readRemainingLength(in)];
        in.readFully(body);
        return new Packet(first >>> 4, first & 0x0f, body);
    }

    static Subscribe subscribe(byte[] body) {
        ByteBuffer buffer = ByteBuffer.wrap(body);
        int packetId = Short.toUnsignedInt(buffer.getShort());
        var subscriptions = new ArrayList<Subscription>();
        while (buffer.hasRemaining()) {
            var filter = new byte[Short.toUnsignedInt(buffer.getShort())];
            buffer.get(filter);
            subscriptions.add(new Subscription(new String(filter, UTF_8), buffer.get() & 0x03));
        }
        return new Subscribe(packetId, subscriptions);
    }

    static void writeConnAck(OutputStream out) throws IOException {
        out.write(CONNACK_ACCEPTED);
    }

    /// Grants every subscription of `subscribe` at the QoS that it requests.
    static void writeSubAck(OutputStream out, Subscribe subscribe) throws IOException {
        List<Subscription> subscriptions = subscribe.subscriptions();
        out.write(0x90);
        out.write(remainingLength(2 + subscriptions.size()));
        out.write(subscribe.packetId() >>> 8);
        out.write(subscribe.packetId());
        for (Subscription subscription : subscriptions) {
            out.write(subscription.qos());
        }
    }

    static void writePingResp(OutputStream out) throws IOException {
        out.write(PINGRESP_PACKET);
    }

    /// Asks for a clean session with keep-alive off, so that a sender never has to ping.
    static void writeConnect(OutputStream out, String clientId) throws IOException {
        byte[] id = clientId.getBytes(UTF_8);
        out.write(0x10);
        out.write(remainingLength(CONNECT_HEADER.length + 2 + id.length));
        out.write(CONNECT_HEADER);
        out.write(id.length >>> 8);
        out.write(id.length);
        out.write(id);
    }

    /// Fails unless `packet` is a CONNACK that accepts the connection.
    static void checkConnAck(Packet packet) throws IOException {
        if (packet.type() != CONNACK || packet.body().length < 2 || packet.body()[1] != 0) {
            throw new IOException(
                    "Connection refused: packet type " + packet.type()
                            + ", body " + Arrays.toString(packet.body())
            );
        }
    }

    static void writeDisconnect(OutputStream out) throws IOException {
        out.write(DISCONNECT_PACKET);
    }

    /// The encoded remaining length: seven bits per byte, least significant first, with the high
    /// bit set on every byte but the last.
    private static byte[] remainingLength(int length) {
        var encoded = new byte[4];
        int size = 0;
        int rest = length;
        do {
            int digit = rest & 0x7f;
            rest >>>= 7;
            encoded[size++] = (byte) (rest > 0 ? digit | 0x80 : digit);
        } while (rest > 0);
        return Arrays.copyOf(encoded, size);
    }

    private static int readRemainingLength(DataInputStream in) throws IOException {
        int length = 0;
        for (int shift = 0; shift < 28; shift += 7) {
            int digit = in.readUnsignedByte();
            length |= (digit & 0x7f) << shift;
            if ((digit & 0x80) == 0) {
                return length;
            }
        }
        throw new IOException("Remaining length longer than four bytes");
    }

    /// A PUBLISH packet that is encoded once and then rewritten in place for each message: only
    /// its packet identifier and its payload header change, so that a sender writes every message
    /// from the same array. A frame is for one thread at a time.
    static final class PublishFrame {

        private final byte[] bytes;

        private final ByteBuffer view;

        // The offset of the packet identifier, which a QoS 0 packet does not have.
        private final int packetIdOffset;

        private final int payloadOffset;

        PublishFrame(String topic, int qos, int payloadSize) {
            if (payloadSize < Payloads.HEADER_SIZE) {
                throw new IllegalArgumentException(
                        "Payload shorter than its " + Payloads.HEADER_SIZE + "-byte header: "
                                + payloadSize
                );
            }
            byte[] topicBytes = topic.getBytes(UTF_8);
            int packetIdSize = qos > 0 ? 2 : 0;
            byte[] length = remainingLength(2 + topicBytes.length + packetIdSize + payloadSize);
            int topicOffset = 1 + length.length;
            packetIdOffset = qos > 0 ? topicOffset + 2 + topicBytes.length : -1;
            payloadOffset = topicOffset + 2 + topicBytes.length + packetIdSize;
            bytes = new byte[payloadOffset + payloadSize];
            view = ByteBuffer.wrap(bytes);
            bytes[0] = (byte) (0x30 | qos << 1);
            System.arraycopy(length, 0, bytes, 1, length.length);
            view.putShort(topicOffset, (short) topicBytes.length);
            System.arraycopy(topicBytes, 0, bytes, topicOffset + 2, topicBytes.length);
        }

        /// Writes the packet for one message. `packetId` is ignored at QoS 0.
        void write(OutputStream out, int packetId, long sequence, long sentMicros)
                throws IOException {
            if (packetIdOffset >= 0) {
                view.putShort(packetIdOffset, (short) packetId);
            }
            Payloads.writeHeader(view, payloadOffset, sequence, sentMicros);
            out.write(bytes);
        }
    }
}
