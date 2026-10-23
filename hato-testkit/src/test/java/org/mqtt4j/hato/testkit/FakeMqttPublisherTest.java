package org.mqtt4j.hato.testkit;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttNotConnectedException;
import org.mqtt4j.hato.PublishResult;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Topic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FakeMqttPublisherTest {

    private static final MqttMessage FIRST = message("sensors/1");
    private static final MqttMessage SECOND = message("sensors/2");

    @Test
    void records_messages_in_order_and_completes_them_at_once_by_default() {
        // Arrange
        var sut = new FakeMqttPublisher();

        // Act
        CompletableFuture<PublishResult> first = sut.publish(FIRST);
        CompletableFuture<PublishResult> second = sut.publish(SECOND);

        // Assert
        assertThat(sut.published()).containsExactly(FIRST, SECOND);
        assertThat(first).isCompletedWithValue(PublishResult.of());
        assertThat(second).isCompletedWithValue(PublishResult.of());
        assertThat(sut.heldPublications()).isEmpty();
    }

    @Test
    void later_publications_take_the_result_or_the_failure_that_the_test_chose() {
        // Arrange
        var sut = new FakeMqttPublisher();
        var failure = new MqttNotConnectedException();
        PublishResult unmatched = PublishResult.of(ReasonCode.NO_MATCHING_SUBSCRIBERS);

        // Act
        sut.failPublications(failure);
        CompletableFuture<PublishResult> failed = sut.publish(FIRST);
        sut.completePublications(unmatched);
        CompletableFuture<PublishResult> completed = sut.publish(SECOND);

        // Assert
        assertThatThrownBy(failed::join).hasCause(failure);
        assertThat(completed).isCompletedWithValue(unmatched);
        assertThat(sut.published()).containsExactly(FIRST, SECOND);
    }

    @Test
    void held_publications_wait_until_the_test_completes_or_fails_each_one() {
        // Arrange
        var sut = new FakeMqttPublisher();
        var failure = new MqttNotConnectedException();
        sut.holdPublications();
        CompletableFuture<PublishResult> first = sut.publish(FIRST);
        CompletableFuture<PublishResult> second = sut.publish(SECOND);
        List<FakeMqttPublisher.HeldPublication> held = sut.heldPublications();

        // Act
        held.get(1).fail(failure);
        held.get(0).complete(PublishResult.of());

        // Assert
        assertThat(held)
                .extracting(FakeMqttPublisher.HeldPublication::message)
                .containsExactly(FIRST, SECOND);
        assertThat(first).isCompletedWithValue(PublishResult.of());
        assertThatThrownBy(second::join).hasCause(failure);
        assertThat(sut.heldPublications()).isEmpty();
        assertThatThrownBy(() -> held.get(0).complete(PublishResult.of()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void changing_the_result_leaves_publications_held_before_held() {
        // Arrange
        var sut = new FakeMqttPublisher();
        sut.holdPublications();
        CompletableFuture<PublishResult> held = sut.publish(FIRST);

        // Act
        sut.completePublications(PublishResult.of());
        CompletableFuture<PublishResult> later = sut.publish(SECOND);

        // Assert
        assertThat(held).isNotDone();
        assertThat(later).isCompletedWithValue(PublishResult.of());
        assertThat(sut.heldPublications())
                .extracting(FakeMqttPublisher.HeldPublication::message)
                .containsExactly(FIRST);
    }

    private static MqttMessage message(String topic) {
        return MqttMessage.of(new Topic(topic), new byte[] {1}, Qos.AT_LEAST_ONCE, false);
    }
}
