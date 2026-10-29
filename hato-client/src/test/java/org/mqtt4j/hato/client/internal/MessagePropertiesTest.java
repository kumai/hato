package org.mqtt4j.hato.client.internal;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.UserProperty;
import org.mqtt4j.hato.client.internal.common.protocol.MqttString;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.client.internal.common.protocol.RemainingLength;

import static org.assertj.core.api.Assertions.assertThat;

class MessagePropertiesTest {

    @Test
    void the_size_of_the_properties_is_the_length_that_their_publish_declares() {
        // Arrange: every property that a message carries, with text of one to four UTF-8 bytes
        // per character, in the PUBLISH that the codec writes for it.
        MqttMessage message = MqttMessage.of(new Topic("t"), new byte[0], Qos.AT_MOST_ONCE, false)
                .withPayloadUtf8(true)
                .withMessageExpiry(Duration.ofSeconds(90))
                .withContentType("text/é")
                .withResponseTopic(new Topic("replies/あ"))
                .withCorrelationData(new byte[] {1, 2, 3})
                .withUserProperties(List.of(
                        new UserProperty("emoji", "😀"),
                        new UserProperty("", "")
                ));
        byte[] header = PacketCodec.of(MqttVersion.V5).encodePublishHeader(
                message.topic(),
                message.qos(),
                false,
                false,
                0,
                MessageProperties.of(message),
                0
        );
        // The first byte and the Remaining Length, then the topic name; the length of the
        // properties follows.
        var input = ByteBuffer.wrap(header);
        input.get();
        RemainingLength.read(input);
        MqttString.read(input);

        // Act
        int size = MessageProperties.size(message);

        // Assert
        assertThat(size).isEqualTo(RemainingLength.read(input));
    }
}
