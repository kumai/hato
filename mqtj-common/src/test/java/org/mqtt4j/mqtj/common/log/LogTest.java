package org.mqtt4j.mqtj.common.log;

import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LogTest {

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
    void arrays_are_rendered_element_by_element() {
        var logger = new RecordingLogger(Level.TRACE);
        var sut = Log.of(logger);

        sut.debug("bytes {} names {} nothing {}", new byte[] {1, 2}, new String[] {"a"}, null);

        assertThat(logger.records.getFirst().message())
                .isEqualTo("bytes [1, 2] names [a] nothing null");
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
    void the_prefix_goes_in_front_of_every_message() {
        var logger = new RecordingLogger(Level.TRACE);
        var sut = Log.of(logger).withPrefix("client-1");

        sut.info("reconnecting in {}", "5s");
        sut.warnOnce("key", "slow");

        assertThat(logger.records).extracting(Record::message)
                .containsExactly("[client-1] reconnecting in 5s", "[client-1] slow");
    }

    @Test
    void warn_once_logs_the_first_occurrence_of_a_key_only() {
        var logger = new RecordingLogger(Level.TRACE);
        var sut = Log.of(logger);

        sut.warnOnce("puback-for-qos2", "PUBACK for QoS 2 packet {}", 1);
        sut.warnOnce("puback-for-qos2", "PUBACK for QoS 2 packet {}", 2);
        sut.warnOnce("other", "other {}", 3);

        assertThat(logger.records).extracting(Record::message)
                .containsExactly("PUBACK for QoS 2 packet 1", "other 3");
    }

    @Test
    void class_based_logs_are_named_after_the_class() {
        assertThat(Log.of(LogTest.class).name()).isEqualTo(LogTest.class.getName());
        assertThat(Log.of("org.mqtt4j.mqtj.test").name()).isEqualTo("org.mqtt4j.mqtj.test");
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
