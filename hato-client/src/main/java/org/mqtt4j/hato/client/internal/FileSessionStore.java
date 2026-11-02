package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.client.internal.common.log.Log;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;

/// A [SessionStore] in one file.
///
/// A replacement writes a new file beside the old one, forces it to the device, and moves it over
/// the old one atomically, so that a reader finds either record whole and a crash leaves one of
/// them. The file carries the record's version and a checksum; a file that fails the checksum,
/// as storage corruption leaves it, counts as no record.
///
/// A store holds the file with a lock on a second file beside it, which keeps the stores of
/// other processes out until it releases the lock or its process ends, and as the only holder of
/// that lock file within this JVM, which may hold only one lock on a file, whatever path names it.
/// The file locks work on a local file system; a network file system may not honour them, and
/// nothing guards a lock file that someone deletes or replaces while it is held.
final class FileSessionStore implements SessionStore {

    private static final Log log = Log.of(FileSessionStore.class);

    // "HSES", which starts every session file.
    private static final int MAGIC = 0x48534553;

    // The layout of the file around the record's content: the first one.
    private static final int LAYOUT = 1;

    // The magic number, layout, version, and content length before the content.
    private static final int HEADER_BYTES = 4 + 4 + 8 + 4;

    // The CRC-32 of everything before it, after the content.
    private static final int CHECKSUM_BYTES = 8;

    // The store that holds each lock file within this JVM, by the lock file's identity rather than
    // a path, which a symbolic link or another mount can alias. Closing any channel to a lock file
    // would release the JVM's lock on it, so a store opens the lock file only once it is the
    // holder here.
    private static final Map<Object, FileSessionStore> holders = new ConcurrentHashMap<>();

    private final Path file;

    private final Path directory;

    private final Path lockFile;

    // The lock file that this store holds, or null while it does not hold it.
    private @Nullable Holding holding;

    FileSessionStore(Path file) {
        this.file = file.toAbsolutePath().normalize();
        directory = requireNonNull(this.file.getParent(), "A session file needs a directory");
        lockFile = this.file.resolveSibling(this.file.getFileName() + ".lock");
    }

    @Override
    public synchronized boolean hold() throws IOException {
        if (holding != null) {
            return true;
        }
        Object identity = lockFileIdentity();
        if (holders.putIfAbsent(identity, this) != null) {
            return false;
        }
        boolean held = false;
        try {
            FileChannel channel = FileChannel.open(lockFile, WRITE);
            try {
                // No lock while the store of another process holds the file.
                if (channel.tryLock() != null) {
                    holding = new Holding(identity, channel);
                    held = true;
                }
            } finally {
                if (!held) {
                    channel.close();
                }
            }
            return held;
        } finally {
            if (!held) {
                holders.remove(identity, this);
            }
        }
    }

    @Override
    public synchronized void release() {
        Holding held = holding;
        if (held == null) {
            return;
        }
        holding = null;
        try {
            held.channel().close();
        } catch (IOException failure) {
            log.warn("Could not release the session file {}", file, failure);
        } finally {
            holders.remove(held.identity(), this);
        }
    }

    /// The identity of the lock file, which this creates if it is missing: its file key, such as
    /// the device and inode of a POSIX file, or else its real path. Neither opens a lock file
    /// that exists, so the lock that this JVM may hold on it stays; a new one carries no lock.
    private Object lockFileIdentity() throws IOException {
        try {
            Files.createFile(lockFile);
        } catch (FileAlreadyExistsException _) {
            // A store created it earlier.
        }
        Object key = Files.readAttributes(lockFile, BasicFileAttributes.class).fileKey();
        return key != null ? key : lockFile.toRealPath();
    }

    @Override
    public Optional<Stored> read() throws IOException {
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(file);
        } catch (NoSuchFileException _) {
            return Optional.empty();
        }
        Optional<Stored> stored = decode(bytes);
        if (stored.isEmpty()) {
            log.warn("Ignoring the unreadable session file {}", file);
        }
        return stored;
    }

    @Override
    public synchronized boolean replace(long expected, long next, byte[] content)
            throws IOException {
        if (holding == null) {
            throw new IllegalStateException("Only the holder replaces the session file");
        }
        long current = read().map(Stored::version).orElse(0L);
        if (current != expected) {
            return false;
        }
        write(encode(next, content));
        return true;
    }

    /// Writes `bytes` to a new file beside the record's file and moves it over that file.
    private void write(byte[] bytes) throws IOException {
        Path written = Files.createTempFile(directory, file.getFileName() + ".", ".tmp");
        try {
            try (FileChannel channel = FileChannel.open(written, WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            Files.move(written, file, ATOMIC_MOVE, REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(written);
        }
        forceDirectory();
    }

    /// Forces the directory's entries to the device, so that the move survives a crash of the
    /// machine. A platform that cannot open a directory, such as Windows, keeps the move atomic,
    /// but such a crash may undo it.
    private void forceDirectory() {
        try (FileChannel channel = FileChannel.open(directory, READ)) {
            channel.force(true);
        } catch (IOException _) {
            // The record stays whole either way; only the move's durability is weaker.
        }
    }

    private static byte[] encode(long version, byte[] content) {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_BYTES + content.length + CHECKSUM_BYTES);
        buffer.putInt(MAGIC)
                .putInt(LAYOUT)
                .putLong(version)
                .putInt(content.length)
                .put(content);
        buffer.putLong(checksum(buffer.array(), HEADER_BYTES + content.length));
        return buffer.array();
    }

    /// The record that `bytes` hold, or empty when they are not a whole record in this layout.
    private static Optional<Stored> decode(byte[] bytes) {
        if (bytes.length < HEADER_BYTES + CHECKSUM_BYTES) {
            return Optional.empty();
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if (buffer.getInt() != MAGIC || buffer.getInt() != LAYOUT) {
            return Optional.empty();
        }
        long version = buffer.getLong();
        int length = buffer.getInt();
        if (version == 0 || length != bytes.length - HEADER_BYTES - CHECKSUM_BYTES) {
            return Optional.empty();
        }
        var content = new byte[length];
        buffer.get(content);
        if (buffer.getLong() != checksum(bytes, HEADER_BYTES + length)) {
            return Optional.empty();
        }
        return Optional.of(new Stored(version, content));
    }

    /// The CRC-32 of the first `length` bytes of `bytes`.
    private static long checksum(byte[] bytes, int length) {
        var crc = new CRC32();
        crc.update(bytes, 0, length);
        return crc.getValue();
    }

    /// A lock file that a store holds: its identity in [#holders], and the open channel whose
    /// lock holds it until the channel closes.
    private record Holding(Object identity, FileChannel channel) {}
}
