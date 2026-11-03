package org.mqtt4j.hato.client.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSessionStoreTest {

    @TempDir
    Path directory;

    private final List<FileSessionStore> stores = new ArrayList<>();

    @AfterEach
    void releaseStores() {
        stores.forEach(FileSessionStore::release);
    }

    @Test
    void a_missing_file_holds_no_record() throws IOException {
        FileSessionStore sut = store(directory.resolve("session"));

        assertThat(sut.read()).isEmpty();
    }

    @Test
    void a_replacement_stores_the_version_that_it_names() throws IOException {
        // Arrange: versions are picked at random, so they may be negative.
        FileSessionStore sut = held(directory.resolve("session"));

        // Act
        boolean replaced = sut.replace(0, -7, new byte[] {1, 2});

        // Assert
        assertThat(replaced).isTrue();
        assertThat(sut.read()).hasValueSatisfying(stored -> {
            assertThat(stored.version()).isEqualTo(-7);
            assertThat(stored.content()).containsExactly(1, 2);
        });
    }

    @Test
    void a_replacement_of_another_version_changes_nothing() throws IOException {
        // Arrange
        FileSessionStore sut = held(directory.resolve("session"));
        boolean _ = sut.replace(0, 1, new byte[] {1});

        // Act
        boolean stale = sut.replace(0, 2, new byte[] {2});
        boolean ahead = sut.replace(2, 3, new byte[] {3});

        // Assert
        assertThat(stale).isFalse();
        assertThat(ahead).isFalse();
        assertThat(sut.read()).hasValueSatisfying(stored -> {
            assertThat(stored.version()).isEqualTo(1);
            assertThat(stored.content()).containsExactly(1);
        });
    }

    @Test
    void a_store_of_the_same_file_sees_the_latest_record() throws IOException {
        // Arrange: the first store replaces the record and gives the file up.
        Path file = directory.resolve("session");
        FileSessionStore first = held(file);
        boolean _ = first.replace(0, 1, new byte[] {1});
        first.release();
        FileSessionStore second = held(file);

        // Act
        boolean replaced = second.replace(1, 2, new byte[] {2});

        // Assert
        assertThat(replaced).isTrue();
        assertThat(first.read()).hasValueSatisfying(stored -> {
            assertThat(stored.version()).isEqualTo(2);
            assertThat(stored.content()).containsExactly(2);
        });
    }

    @Test
    void a_corrupted_file_holds_no_record_and_can_be_replaced() throws IOException {
        // Arrange: one bit of the content flips on the device.
        Path file = directory.resolve("session");
        FileSessionStore sut = held(file);
        boolean _ = sut.replace(0, 1, new byte[] {1, 2, 3});
        byte[] bytes = Files.readAllBytes(file);
        bytes[21] ^= 1;
        Files.write(file, bytes);

        // Act & Assert
        assertThat(sut.read()).isEmpty();
        assertThat(sut.replace(0, 2, new byte[] {4})).isTrue();
        assertThat(sut.read()).hasValueSatisfying(stored -> {
            assertThat(stored.version()).isEqualTo(2);
            assertThat(stored.content()).containsExactly(4);
        });
    }

    @Test
    void replacements_leave_only_the_record_and_its_lock_file() throws IOException {
        // Arrange
        FileSessionStore sut = held(directory.resolve("session"));

        // Act
        boolean _ = sut.replace(0, 1, new byte[] {1});
        boolean _ = sut.replace(1, 2, new byte[] {2});

        // Assert
        try (Stream<Path> files = Files.list(directory)) {
            assertThat(files.map(path -> path.getFileName().toString()))
                    .containsExactlyInAnyOrder("session", "session.lock");
        }
    }

    @Test
    void one_store_at_a_time_holds_a_file() throws IOException {
        // Arrange
        Path file = directory.resolve("session");
        FileSessionStore first = held(file);
        FileSessionStore second = store(file);

        // Act & Assert: the second store holds the file only once the first gives it up.
        assertThat(second.hold()).isFalse();
        first.release();
        assertThat(second.hold()).isTrue();
        assertThat(first.hold()).isFalse();
    }

    @Test
    void a_store_holds_the_file_only_once_another_process_that_held_it_has_ended()
            throws Exception {
        // Arrange: another process holds the file.
        Path file = directory.resolve("session");
        Process holder = holder(file);
        try {
            assertThat(firstLine(holder)).isEqualTo("held");
            FileSessionStore sut = store(file);
            assertThat(sut.hold()).isFalse();

            // Act: the other process ends without giving the file up, as a crash would.
            holder.destroyForcibly();
            assertThat(holder.waitFor(5, SECONDS)).isTrue();

            // Assert
            assertThat(sut.hold()).isTrue();
        } finally {
            holder.destroyForcibly();
        }
    }

    @Test
    void a_store_that_names_the_file_by_another_path_is_refused_without_loosening_the_hold()
            throws Exception {
        // Arrange: the second store reaches the file through a symbolic link to its directory.
        Path real = Files.createDirectory(directory.resolve("real"));
        Path alias = Files.createSymbolicLink(directory.resolve("alias"), real);
        FileSessionStore first = held(real.resolve("session"));
        FileSessionStore second = store(alias.resolve("session"));

        // Act
        boolean taken = second.hold();

        // Assert: the first store still keeps another process out, and the second one holds the
        // file once the first one gives it up.
        assertThat(taken).isFalse();
        Process other = holder(real.resolve("session"));
        try {
            assertThat(firstLine(other)).isEqualTo("refused");
        } finally {
            other.destroyForcibly();
        }
        first.release();
        assertThat(second.hold()).isTrue();
    }

    @Test
    void a_store_that_does_not_hold_the_file_cannot_replace_it() {
        FileSessionStore sut = store(directory.resolve("session"));

        assertThatThrownBy(() -> sut.replace(0, 1, new byte[] {1}))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void a_file_in_a_missing_directory_cannot_be_held() {
        FileSessionStore sut = store(directory.resolve("missing").resolve("session"));

        assertThatThrownBy(sut::hold).isInstanceOf(IOException.class);
    }

    @Test
    void the_record_content_reads_back_whatever_its_bytes() throws IOException {
        // Arrange: every byte value, including those of the file's own magic number.
        FileSessionStore sut = held(directory.resolve("session"));
        var content = new byte[1024];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) i;
        }

        // Act
        boolean _ = sut.replace(0, 1, content);

        // Assert
        assertThat(sut.read().orElseThrow().content()).isEqualTo(content);
    }

    private FileSessionStore store(Path file) {
        var store = new FileSessionStore(file);
        stores.add(store);
        return store;
    }

    /// A store of `file` that holds it.
    private FileSessionStore held(Path file) throws IOException {
        FileSessionStore store = store(file);
        assertThat(store.hold()).isTrue();
        return store;
    }

    /// Starts a process that holds `file` until it ends, or that ends at once when it cannot hold
    /// the file. Its first line tells which.
    private static Process holder(Path file) throws IOException {
        return new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                Holder.class.getName(),
                file.toString()
        ).redirectErrorStream(true).start();
    }

    private static @Nullable String firstLine(Process process) throws IOException {
        var output = new BufferedReader(new InputStreamReader(process.getInputStream(), UTF_8));
        return output.readLine();
    }

    /// Holds the session file that the first argument names, in a process of its own, until the
    /// process ends, and says "held", or says "refused" when another store holds the file.
    static final class Holder {

        void main(String[] args) throws Exception {
            var store = new FileSessionStore(Path.of(args[0]));
            if (!store.hold()) {
                IO.println("refused");
                return;
            }
            IO.println("held");
            Thread.sleep(Long.MAX_VALUE);
        }
    }
}
