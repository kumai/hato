package org.mqtt4j.hato.common.log;

import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

class LogTest {

    @Test
    void a_statement_does_not_wait_for_a_backend_that_holds_its_caller() throws Exception {
        // Arrange
        var logger = new HoldingLogger();
        var sut = Log.of(logger);
        try {
            // Act: both calls return while the backend holds the first statement.
            sut.info("first");
            sut.info("second");

            // Assert
            assertThat(logger.entered.await(10, SECONDS)).isTrue();
            assertThat(Log.awaitWritten(Duration.ofMillis(50))).isFalse();
        } finally {
            logger.released.countDown();
        }

        // Assert: both reach the backend once it lets go.
        awaitWritten();
        assertThat(logger.messages).containsExactly("first", "second");
    }

    @Test
    void statements_that_find_no_room_are_dropped_and_later_ones_are_written() throws Exception {
        // Arrange: the backend holds one statement while far more text arrives than may wait.
        var logger = new HoldingLogger();
        var sut = Log.of(logger);
        String text = "x".repeat(1024);
        int made = 20_000;
        try {
            sut.info("first");
            assertThat(logger.entered.await(10, SECONDS)).isTrue();

            // Act
            for (int i = 0; i < made; i++) {
                sut.info("{} {}", i, text);
            }
        } finally {
            logger.released.countDown();
        }
        awaitWritten();
        sut.info("last");
        awaitWritten();

        // Assert: what waited is the start of what was made, in its order, and logging goes on.
        List<String> messages = logger.messages;
        assertThat(messages).hasSizeBetween(3, made);
        assertThat(messages.getFirst()).isEqualTo("first");
        assertThat(messages.getLast()).isEqualTo("last");
        for (int i = 1; i < messages.size() - 1; i++) {
            assertThat(messages.get(i)).startsWith((i - 1) + " ");
        }
    }

    @Test
    void the_messages_of_waiting_exceptions_count_against_the_room() throws Exception {
        // Arrange: the backend holds one statement while statements with short text arrive,
        // each with an exception whose message is one mebi of characters.
        var logger = new HoldingLogger();
        var sut = Log.of(logger);
        String message = "x".repeat(1 << 20);
        try {
            sut.info("first");
            assertThat(logger.entered.await(10, SECONDS)).isTrue();

            // Act
            for (int i = 0; i < 40; i++) {
                sut.error("failure {}", i, new IllegalStateException(message));
            }
        } finally {
            logger.released.countDown();
        }
        awaitWritten();

        // Assert: no more of them waited than the room of four mebi characters holds.
        assertThat(logger.messages).hasSizeBetween(2, 5);
        assertThat(logger.messages.getFirst()).isEqualTo("first");
    }

    @Test
    void the_backend_sees_which_thread_made_a_statement() throws Exception {
        // Arrange
        var logger = new RecordingLogger(Level.TRACE);
        var sut = Log.of(logger);

        // Act
        Thread.ofPlatform().name("hato-client-1-actor").start(() -> sut.info("named")).join();
        Thread.ofVirtual().start(() -> sut.info("unnamed")).join();
        awaitWritten();

        // Assert: the name of the writing thread ends with the name of the thread that made
        // the statement, without being a second thread of that name. A thread without a name
        // adds nothing to it.
        assertThat(logger.records).extracting(Record::thread).satisfiesExactly(
                thread -> assertThat(thread)
                        .endsWith(" hato-client-1-actor")
                        .doesNotStartWith("hato-client-1-actor"),
                thread -> assertThat(thread).isNotEmpty().doesNotContain("hato-client-1-actor")
        );
    }

    @Test
    void jul_prints_the_name_of_the_log_in_place_of_a_caller() {
        // Arrange
        var jul = Logger.getLogger(LogTest.class.getName());
        var sut = Log.of(LogTest.class);
        var lines = new ArrayList<String>();
        var handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                // Formats as the console handler does, on the thread that the caller is
                // inferred from.
                lines.add(new SimpleFormatter().format(record));
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        jul.addHandler(handler);
        try {
            // Act
            sut.info("caller {}", "test");
            awaitWritten();

            // Assert: the first line ends with the source, which is the name of the log alone.
            assertThat(lines).singleElement().asString()
                    .contains(" " + LogTest.class.getName() + System.lineSeparator());
        } finally {
            jul.removeHandler(handler);
        }
    }

    @Test
    void substitutes_placeholders_in_order() {
        var logger = new RecordingLogger(Level.TRACE);
        var sut = Log.of(logger);

        sut.info("connected to {} as {}", "broker:1883", "client-1");
        awaitWritten();

        assertThat(logger.records).singleElement().satisfies(record -> {
            assertThat(record.level()).isEqualTo(Level.INFO);
            assertThat(record.message()).isEqualTo("connected to broker:1883 as client-1");
            assertThat(record.thrown()).isNull();
        });
    }

    @Test
    void a_trailing_throwable_becomes_the_cause() {
        var logger = new RecordingLogger(Level.TRACE);
        var sut = Log.of(logger);
        var failure = new IllegalStateException("boom");

        sut.warn("write to {} failed", "broker:1883", failure);
        awaitWritten();

        assertThat(logger.records).singleElement().satisfies(record -> {
            assertThat(record.message()).isEqualTo("write to broker:1883 failed");
            assertThat(record.thrown()).isSameAs(failure);
        });
    }

    @Test
    void a_throwable_consumed_by_a_placeholder_is_rendered_as_text() {
        var logger = new RecordingLogger(Level.TRACE);
        var sut = Log.of(logger);

        sut.warn("rejected: {}", new IllegalStateException("boom"));
        awaitWritten();

        assertThat(logger.records).singleElement().satisfies(record -> {
            assertThat(record.message())
                    .isEqualTo("rejected: java.lang.IllegalStateException: boom");
            assertThat(record.thrown()).isNull();
        });
    }

    @Test
    void escaped_and_unmatched_placeholders_stay_as_text() {
        var logger = new RecordingLogger(Level.TRACE);
        var sut = Log.of(logger);

        sut.info("payload \\{} first {} then {}", "one");
        awaitWritten();

        assertThat(logger.records.getFirst().message()).isEqualTo("payload {} first one then {}");
    }

    @Test
    void arrays_render_their_elements_and_byte_arrays_render_as_hex() {
        var logger = new RecordingLogger(Level.TRACE);
        var sut = Log.of(logger);

        sut.debug(
                "bytes {} names {} nothing {}",
                new byte[] {0x30, (byte) 0xff},
                new String[] {"a"},
                null
        );
        awaitWritten();

        assertThat(logger.records.getFirst().message())
                .isEqualTo("bytes 30ff names [a] nothing null");
    }

    @Test
    void does_not_render_arguments_when_the_level_is_disabled() {
        var logger = new RecordingLogger(Level.INFO);
        var sut = Log.of(logger);
        var argument = new CountingArgument();

        sut.debug("state {}", argument);
        awaitWritten();

        assertThat(argument.renderings).isZero();
        assertThat(logger.records).isEmpty();
    }

    @Test
    void a_failing_to_string_does_not_break_the_log_statement() {
        var logger = new RecordingLogger(Level.TRACE);
        var sut = Log.of(logger);
        Object broken = new Object() {
            @Override
            public String toString() {
                throw new IllegalStateException("no text");
            }
        };

        sut.info("value {}", broken);
        awaitWritten();

        assertThat(logger.records.getFirst().message())
                .startsWith("value [toString failed: java.lang.IllegalStateException: no text");
    }

    @Test
    void a_failing_backend_does_not_reach_the_caller() {
        // Arrange
        var logger = new FailingLogger();
        var sut = Log.of(logger);

        // Act
        sut.warn("value {}", 1);
        sut.error("text", new IllegalStateException("reported failure"));
        awaitWritten();

        // Assert: both statements reached the backend, and the calls returned normally.
        assertThat(logger.calls).isEqualTo(2);
    }

    @Test
    void class_based_logs_are_named_after_the_class() {
        assertThat(Log.of(LogTest.class).getName()).isEqualTo(LogTest.class.getName());
        assertThat(Log.of("org.mqtt4j.hato.test").getName()).isEqualTo("org.mqtt4j.hato.test");
    }

    /// Waits for the thread that writes. What the backend recorded until then is safe to read.
    private static void awaitWritten() {
        assertThat(Log.awaitWritten(Duration.ofSeconds(10)))
                .as("the backend took the statements made so far")
                .isTrue();
    }

    private record Record(
            Level level,
            String message,
            @Nullable Throwable thrown,
            String thread
    ) {
    }

    private static final class CountingArgument {

        int renderings;

        @Override
        public String toString() {
            renderings++;
            return "rendered";
        }
    }

    /// Records what reaches System.Logger. The facade must pass finished text, never parameters.
    private static final class RecordingLogger implements System.Logger {

        final List<Record> records = new ArrayList<>();
        private final Level minimum;

        RecordingLogger(Level minimum) {
            this.minimum = minimum;
        }

        @Override
        public String getName() {
            return "recording";
        }

        @Override
        public boolean isLoggable(Level level) {
            return level.getSeverity() >= minimum.getSeverity();
        }

        @Override
        public void log(Level level, ResourceBundle bundle, String message, Throwable thrown) {
            records.add(new Record(level, message, thrown, Thread.currentThread().getName()));
        }

        @Override
        public void log(Level level, ResourceBundle bundle, String format, Object... params) {
            throw new AssertionError("the facade must not delegate formatting to System.Logger");
        }
    }

    /// A backend that keeps its caller until the test lets it go. It gives up by itself, once
    /// and for all its callers, so that a facade that waited for it would fail its test instead
    /// of stopping the run.
    private static final class HoldingLogger implements System.Logger {

        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch released = new CountDownLatch(1);
        final List<String> messages = new ArrayList<>();
        private volatile boolean gaveUp;

        @Override
        public String getName() {
            return "holding";
        }

        @Override
        public boolean isLoggable(Level level) {
            return true;
        }

        @Override
        public void log(Level level, ResourceBundle bundle, String message, Throwable thrown) {
            entered.countDown();
            try {
                if (!gaveUp && !released.await(10, SECONDS)) {
                    gaveUp = true;
                }
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
            messages.add(message);
        }

        @Override
        public void log(Level level, ResourceBundle bundle, String format, Object... params) {
            throw new AssertionError("the facade must not delegate formatting to System.Logger");
        }
    }

    /// A broken handler: every statement that reaches it throws.
    private static final class FailingLogger implements System.Logger {

        int calls;

        @Override
        public String getName() {
            return "failing";
        }

        @Override
        public boolean isLoggable(Level level) {
            return true;
        }

        @Override
        public void log(Level level, ResourceBundle bundle, String message, Throwable thrown) {
            calls++;
            throw new AssertionError("Logging backend failed");
        }

        @Override
        public void log(Level level, ResourceBundle bundle, String format, Object... params) {
            calls++;
            throw new AssertionError("Logging backend failed");
        }
    }
}
