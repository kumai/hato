package org.mqtt4j.hato.client.internal;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.TopicFilter;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.assertj.core.api.Assertions.assertThat;

class SessionCodecTest {

    private static final SessionState STATE = new SessionState(
            List.of(
                    new SessionState.Registration(new TopicFilter("sensors/+/temp"), 7, false),
                    new SessionState.Registration(new TopicFilter("$share/group/jobs/#"), 3, true),
                    new SessionState.Registration(new TopicFilter("ü/日本"), 268_435_455, false)
            ),
            12,
            List.of(
                    new SessionState.Reception(1, Qos.AT_LEAST_ONCE),
                    new SessionState.Reception(65_535, Qos.EXACTLY_ONCE)
            )
    );

    private final SessionCodec sut = new SessionCodec("sensor-17", MqttVersion.V5);

    @Test
    void a_saved_state_reads_back_unchanged() {
        assertThat(sut.decode(sut.encode(STATE))).hasValue(STATE);
    }

    @Test
    void the_owned_mark_holds_no_state() {
        assertThat(sut.decode(sut.owned())).isEmpty();
    }

    @Test
    void a_state_of_another_client_identifier_holds_none_for_this_client() {
        byte[] content = new SessionCodec("sensor-18", MqttVersion.V5).encode(STATE);

        assertThat(sut.decode(content)).isEmpty();
    }

    @Test
    void a_state_of_another_mqtt_version_holds_none_for_this_client() {
        byte[] content = new SessionCodec("sensor-17", MqttVersion.V3_1_1).encode(STATE);

        assertThat(sut.decode(content)).isEmpty();
    }

    static Stream<Arguments> unreadable() {
        byte[] content = new SessionCodec("sensor-17", MqttVersion.V5).encode(STATE);
        byte[] longer = Arrays.copyOf(content, content.length + 1);
        // The reception count, the four bytes before the two receptions of three bytes each,
        // claims one reception more than the content holds.
        byte[] miscounted = content.clone();
        miscounted[content.length - 7] = 3;
        // The shared subscription "$share/group/jobs/#" turns into "$share//roup/jobs/#", a valid
        // topic filter whose share name is empty, which no subscription can have.
        byte[] nameless = content.clone();
        int shareName = new String(content, ISO_8859_1).indexOf("$share/group")
                + "$share/".length();
        nameless[shareName] = '/';
        return Stream.of(
                Arguments.of("empty content", new byte[0]),
                Arguments.of("another format", new byte[] {0x48, 0x53, 0x53, 0x54, 2, 1}),
                Arguments.of("truncated content", Arrays.copyOf(content, content.length - 1)),
                Arguments.of("bytes after the state", longer),
                Arguments.of("a count beyond the content", miscounted),
                Arguments.of("a shared subscription without a share name", nameless)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unreadable")
    void unreadable_content_holds_no_state(String description, byte[] content) {
        assertThat(sut.decode(content)).isEmpty();
    }
}
