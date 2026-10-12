package org.mqtt4j.hato.common.log;

import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LogTest {

    @Test
    void jul_infers_the_class_and_method_calling_the_facade() {
        // Arrange
        var jul = Logger.getLogger(LogTest.class.getName());
        var sut = Log.of(LogTest.class);
        var records = new ArrayList<LogRecord>();
        var handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                // Resolve lazily inferred caller information while the caller is on the stack.
                record.getSourceClassName();
                records.add(record);
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

            // Assert
            assertThat(records).singleElement().satisfies(record -> {
                assertThat(record.getSourceClassName()).isEqualTo(LogTest.class.getName());
                assertThat(record.getSourceMethodName())
                        .isEqualTo("jul_infers_the_class_and_method_calling_the_facade");
            });
        } finally {
            jul.removeHandler(handler);
        }
    }

    @Test
    void system_logger_parameter_overloads_use_the_same_placeholder_syntax() {
        // Arrange
        var recording = new RecordingLogger(Level.TRACE);
        System.Logger sut = Log.of(recording);

        // Act
        sut.log(Level.INFO, "value {}", "one");
        sut.log(Level.INFO, (ResourceBundle) null, "value {}", "two");

        // Assert
        assertThat(recording.records).extracting(Record::message)
                .containsExactly("value one", "value two");
    }

    @Test
    void substitutes_placeholders_in_order() {
        var logger = new RecordingLogger(Level.TRACE);
        var sut = Log.of(logger);

        sut.info("connected to {} as {}", "broker:1883", "client-1");

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

        assertThat(logger.records.getFirst().message())
                .isEqualTo("bytes 30ff names [a] nothing null");
    }

    @Test
    void does_not_render_arguments_when_the_level_is_disabled() {
        var logger = new RecordingLogger(Level.INFO);
        var sut = Log.of(logger);
        var argument = new CountingArgument();

        sut.debug("state {}", argument);

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

        assertThat(logger.records.getFirst().message())
                .startsWith("value [toString failed: java.lang.IllegalStateException: no text");
    }

    @Test
    void class_based_logs_are_named_after_the_class() {
        assertThat(Log.of(LogTest.class).getName()).isEqualTo(LogTest.class.getName());
        assertThat(Log.of("org.mqtt4j.hato.test").getName()).isEqualTo("org.mqtt4j.hato.test");
    }

    private record Record(Level level, String message, @Nullable Throwable thrown) {
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
            records.add(new Record(level, message, thrown));
        }

        @Override
        public void log(Level level, ResourceBundle bundle, String format, Object... params) {
            assertThat(params)
                    .as("the facade must not delegate formatting to System.Logger")
                    .isNull();
            records.add(new Record(level, format, null));
        }
    }
}
