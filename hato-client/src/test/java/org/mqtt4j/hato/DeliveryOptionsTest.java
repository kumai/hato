package org.mqtt4j.hato;

import java.util.concurrent.Executor;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.client.internal.HandlerExecution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeliveryOptionsTest {

    @Test
    void an_executor_replaces_the_internal_thread_without_changing_serial() {
        // Arrange
        Executor first = Runnable::run;
        Executor second = Runnable::run;

        // Act
        DeliveryOptions sut = DeliveryOptions.serial().withExecutor(first).withExecutor(second);

        // Assert: the later executor wins, and serial() still runs on an internal thread.
        assertThat(sut.execution()).isEqualTo(new HandlerExecution.OnExecutor(second));
        assertThat(DeliveryOptions.serial().execution()).isEqualTo(new HandlerExecution.Internal());
    }

    @Test
    void inline_options_run_handlers_as_part_of_receiving_and_refuse_an_executor() {
        // Act
        DeliveryOptions sut = DeliveryOptions.inline();

        // Assert
        assertThat(sut.execution()).isEqualTo(new HandlerExecution.Inline());
        assertThatThrownBy(() -> sut.withExecutor(Runnable::run))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void options_with_the_same_executor_are_not_equal() {
        Executor executor = Runnable::run;

        assertThat(DeliveryOptions.serial().withExecutor(executor))
                .isNotEqualTo(DeliveryOptions.serial().withExecutor(executor));
    }

    @Test
    void lanes_take_their_executors_from_the_selector_and_refuse_another_executor() {
        // Arrange
        Function<MqttMessage, Executor> selector = _ -> Runnable::run;

        // Act
        DeliveryOptions sut = DeliveryOptions.lanes(selector);

        // Assert
        assertThat(sut.execution()).isEqualTo(new HandlerExecution.Lanes(selector));
        assertThatThrownBy(() -> sut.withExecutor(Runnable::run))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void striped_options_key_on_the_topic_and_keep_their_stripes_on_an_executor() {
        // Arrange
        Executor executor = Runnable::run;
        var message = MqttMessage.of(new Topic("devices/1"), new byte[0], Qos.AT_MOST_ONCE, false);

        // Act
        DeliveryOptions sut = DeliveryOptions.striped(4);
        DeliveryOptions onExecutor = sut.withExecutor(executor);

        // Assert: the default key is the topic, and the executor replaces the internal threads.
        var striped = (HandlerExecution.Striped) sut.execution();
        assertThat(striped.stripes()).isEqualTo(4);
        assertThat(striped.key().apply(message)).isEqualTo(new Topic("devices/1"));
        assertThat(striped.executor()).isNull();
        assertThat(onExecutor.execution())
                .isEqualTo(new HandlerExecution.Striped(4, striped.key(), executor));
    }

    @Test
    void parallel_options_keep_their_parallelism_on_an_executor() {
        // Arrange
        Executor executor = Runnable::run;

        // Act
        DeliveryOptions sut = DeliveryOptions.parallel(8);

        // Assert
        assertThat(sut.execution()).isEqualTo(new HandlerExecution.Parallel(8, null));
        assertThat(sut.withExecutor(executor).execution())
                .isEqualTo(new HandlerExecution.Parallel(8, executor));
    }

    @ParameterizedTest
    @ValueSource(ints = {Integer.MIN_VALUE, -1, 0, 257})
    void stripes_and_parallelism_must_be_from_1_to_256(int value) {
        assertThatThrownBy(() -> DeliveryOptions.striped(value))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DeliveryOptions.striped(value, MqttMessage::topic))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DeliveryOptions.parallel(value))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 256})
    void the_bounds_of_the_range_are_accepted(int value) {
        Function<MqttMessage, ?> key = MqttMessage::topic;

        assertThat(DeliveryOptions.striped(value, key).execution())
                .isEqualTo(new HandlerExecution.Striped(value, key, null));
        assertThat(DeliveryOptions.parallel(value).execution())
                .isEqualTo(new HandlerExecution.Parallel(value, null));
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the non-null contracts.
    void executors_selectors_and_keys_cannot_be_null() {
        assertThatThrownBy(() -> DeliveryOptions.serial().withExecutor(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> DeliveryOptions.lanes(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> DeliveryOptions.striped(4, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> DeliveryOptions.striped(4).withExecutor(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> DeliveryOptions.parallel(4).withExecutor(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void a_subscriber_that_only_provides_handlers_rejects_delivery_options() {
        // Arrange
        MqttSubscriber sut = (_, _) -> {
            throw new AssertionError("Only the delivery options overload is called");
        };
        var request = Subscribe.of(new TopicFilter("events"), Qos.AT_MOST_ONCE);

        // Act & Assert
        assertThatThrownBy(() -> sut.subscribe(request, DeliveryOptions.serial(), _ -> { }))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
