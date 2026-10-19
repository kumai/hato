package org.mqtt4j.hato;

import java.util.concurrent.Executor;

import org.junit.jupiter.api.Test;

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
    void options_with_the_same_executor_are_not_equal() {
        Executor executor = Runnable::run;

        assertThat(DeliveryOptions.serial().withExecutor(executor))
                .isNotEqualTo(DeliveryOptions.serial().withExecutor(executor));
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the non-null contract.
    void an_executor_cannot_be_null() {
        assertThatThrownBy(() -> DeliveryOptions.serial().withExecutor(null))
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
