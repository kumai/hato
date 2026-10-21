package org.mqtt4j.hato.spi;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.spi.MetricsListener.DeliveryStart;
import org.mqtt4j.hato.spi.MetricsListener.PublishCompletion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class MetricsListenerTest {

    @Test
    void a_successful_completion_preserves_its_values_and_compares_by_value() {
        // Act
        var sut = PublishCompletion.of(Qos.AT_LEAST_ONCE, Duration.ofMillis(5));
        var same = PublishCompletion.of(Qos.AT_LEAST_ONCE, Duration.ofMillis(5));

        // Assert
        assertThat(sut.qos()).isEqualTo(Qos.AT_LEAST_ONCE);
        assertThat(sut.failure()).isEmpty();
        assertThat(sut.duration()).isEqualTo(Duration.ofMillis(5));
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same)
                .isNotEqualTo(PublishCompletion.of(Qos.EXACTLY_ONCE, Duration.ofMillis(5)))
                .isNotEqualTo(PublishCompletion.of(Qos.AT_LEAST_ONCE, Duration.ofMillis(6)));
        assertThat(sut.toString())
                .contains("qos=AT_LEAST_ONCE", "failure=null", "duration=PT0.005S");
    }

    @Test
    void a_failed_completion_preserves_its_failure_and_compares_it_by_identity() {
        // Arrange
        var failure = new MqttConnectionLostException();

        // Act
        var sut = PublishCompletion.of(Qos.AT_MOST_ONCE, failure, Duration.ZERO);
        var same = PublishCompletion.of(Qos.AT_MOST_ONCE, failure, Duration.ZERO);

        // Assert
        assertThat(sut.failure()).containsSame(failure);
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same)
                .isNotEqualTo(PublishCompletion.of(
                        Qos.AT_MOST_ONCE,
                        new MqttConnectionLostException(),
                        Duration.ZERO
                ))
                .isNotEqualTo(PublishCompletion.of(Qos.AT_MOST_ONCE, Duration.ZERO));
        assertThat(sut.toString()).contains("failure=" + failure);
    }

    @Test
    void a_delivery_start_preserves_its_subscription_and_wait_and_compares_by_value() {
        // Arrange
        var subscription = new TestSubscription("readings/#");

        // Act
        var sut = DeliveryStart.of(subscription, Duration.ofMillis(7));
        var same = DeliveryStart.of(subscription, Duration.ofMillis(7));

        // Assert
        assertThat(sut.subscription()).isSameAs(subscription);
        assertThat(sut.waited()).isEqualTo(Duration.ofMillis(7));
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same)
                .isNotEqualTo(DeliveryStart.of(
                        new TestSubscription("readings/#"),
                        Duration.ofMillis(7)
                ))
                .isNotEqualTo(DeliveryStart.of(subscription, Duration.ofMillis(8)));
        assertThat(sut.toString()).contains("subscription=readings/#", "waited=PT0.007S");
    }

    @Test
    void negative_durations_are_rejected() {
        // Arrange
        var subscription = new TestSubscription("readings");
        var negative = Duration.ofNanos(-1);

        // Act & Assert
        assertThatIllegalArgumentException()
                .isThrownBy(() -> PublishCompletion.of(Qos.AT_LEAST_ONCE, negative));
        assertThatIllegalArgumentException().isThrownBy(() -> PublishCompletion.of(
                Qos.AT_LEAST_ONCE,
                new MqttConnectionLostException(),
                negative
        ));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> DeliveryStart.of(subscription, negative));
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the non-null contracts.
    void missing_values_are_rejected() {
        // Arrange
        var subscription = new TestSubscription("readings");

        // Act & Assert
        assertThatNullPointerException()
                .isThrownBy(() -> PublishCompletion.of(Qos.AT_LEAST_ONCE, null, Duration.ZERO));
        assertThatNullPointerException()
                .isThrownBy(() -> PublishCompletion.of(null, Duration.ZERO));
        assertThatNullPointerException()
                .isThrownBy(() -> DeliveryStart.of(subscription, null));
        assertThatNullPointerException()
                .isThrownBy(() -> DeliveryStart.of(null, Duration.ZERO));
    }

    /// A subscription handle that only knows its filter.
    private static final class TestSubscription implements Subscription {

        private final TopicFilter filter;

        TestSubscription(String filter) {
            this.filter = new TopicFilter(filter);
        }

        @Override
        public TopicFilter filter() {
            return filter;
        }

        @Override
        public Qos grantedQos() {
            return Qos.AT_LEAST_ONCE;
        }

        @Override
        public CompletableFuture<Qos> established() {
            return CompletableFuture.completedFuture(grantedQos());
        }

        @Override
        public CompletableFuture<Void> unsubscribe() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }
    }
}
