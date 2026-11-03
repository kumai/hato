package org.mqtt4j.hato.client.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Optional;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.common.log.Log;

import static java.nio.charset.StandardCharsets.UTF_8;

/// The content of a session record for one client identifier and MQTT version. It is either the
/// mark that a client object owns the session, which holds nothing for the next one, or a
/// [SessionState] that a client object saved for the next one. A client object reads a state only
/// if it saved for the same client identifier and MQTT version.
final class SessionCodec {

    private static final Log log = Log.of(SessionCodec.class);

    // "HSST", which starts the content of every session record.
    private static final int MAGIC = 0x48535354;

    // The content's format: the first one.
    private static final byte FORMAT = 1;

    // The kinds of content.
    private static final byte OWNED = 0;

    private static final byte SAVED = 1;

    private final String clientId;

    private final MqttVersion version;

    SessionCodec(String clientId, MqttVersion version) {
        this.clientId = clientId;
        this.version = version;
    }

    /// The mark that a client object owns the session. A client object that finds it starts a
    /// new session, because the owner may have changed the broker's session since.
    byte[] owned() {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeByte(FORMAT);
            out.writeByte(OWNED);
        } catch (IOException impossible) {
            // Writing to a byte array does not fail.
            throw new UncheckedIOException(impossible);
        }
        return bytes.toByteArray();
    }

    /// The content that saves `state` for the next client object of this client identifier and
    /// MQTT version.
    byte[] encode(SessionState state) {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeByte(FORMAT);
            out.writeByte(SAVED);
            writeString(out, clientId);
            out.writeByte(version.protocolLevel());
            out.writeInt(state.nextIdentifier());
            out.writeInt(state.registrations().size());
            for (SessionState.Registration registration : state.registrations()) {
                writeString(out, registration.filter().pattern());
                out.writeInt(registration.identifier());
                out.writeBoolean(registration.heldWithoutIdentifier());
            }
            out.writeInt(state.receptions().size());
            for (SessionState.Reception reception : state.receptions()) {
                out.writeShort(reception.packetId());
                out.writeByte(reception.qos().level());
            }
        } catch (IOException impossible) {
            // Writing to a byte array does not fail.
            throw new UncheckedIOException(impossible);
        }
        return bytes.toByteArray();
    }

    /// The state that `content` saved for this client, or empty when it holds none for it: when
    /// it marks the session as owned, or when this client cannot use it, which is logged.
    Optional<SessionState> decode(byte[] content) {
        try (var in = new DataInputStream(new ByteArrayInputStream(content))) {
            if (in.readInt() != MAGIC || in.readByte() != FORMAT) {
                return unusable("content in an unknown format");
            }
            byte kind = in.readByte();
            if (kind == OWNED && in.available() == 0) {
                return Optional.empty();
            }
            if (kind != SAVED) {
                return unusable("content of an unknown kind");
            }
            String savedClientId = readString(in);
            if (!savedClientId.equals(clientId)) {
                return unusable("the session of client identifier " + savedClientId);
            }
            int level = in.readUnsignedByte();
            if (level != version.protocolLevel()) {
                return unusable("a session of MQTT protocol level " + level);
            }
            int nextIdentifier = in.readInt();
            int registrationCount = count(in);
            var registrations = new ArrayList<SessionState.Registration>(registrationCount);
            for (int i = 0; i < registrationCount; i++) {
                registrations.add(new SessionState.Registration(
                        new TopicFilter(readString(in)),
                        in.readInt(),
                        in.readBoolean()
                ));
            }
            int receptionCount = count(in);
            var receptions = new ArrayList<SessionState.Reception>(receptionCount);
            for (int i = 0; i < receptionCount; i++) {
                receptions.add(new SessionState.Reception(
                        in.readUnsignedShort(),
                        Qos.of(in.readUnsignedByte())
                ));
            }
            if (in.available() != 0) {
                return unusable("content with bytes after the state");
            }
            return Optional.of(new SessionState(registrations, nextIdentifier, receptions));
        } catch (IOException | IllegalArgumentException malformed) {
            return unusable("malformed content (" + malformed.getMessage() + ")");
        }
    }

    private Optional<SessionState> unusable(String reason) {
        log.warn("Ignoring the saved session for {}: it holds {}", clientId, reason);
        return Optional.empty();
    }

    /// A count of elements that follow, each of which takes at least a byte.
    private static int count(DataInputStream in) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > in.available()) {
            throw new EOFException("Count " + count + " exceeds the content");
        }
        return count;
    }

    /// Writes `value` as MQTT does, as its UTF-8 bytes after their two-byte length.
    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(UTF_8);
        out.writeShort(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readUnsignedShort();
        byte[] bytes = in.readNBytes(length);
        if (bytes.length != length) {
            throw new EOFException("String of " + length + " bytes exceeds the content");
        }
        return new String(bytes, UTF_8);
    }
}
