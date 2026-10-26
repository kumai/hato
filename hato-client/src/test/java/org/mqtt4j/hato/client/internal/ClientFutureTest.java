package org.mqtt4j.hato.client.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// A wait that the client fails to refuse would block the test, and join() ignores interrupts,
// so each test runs on a thread of its own that the timeout gives up on.
@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ClientFutureTest {

    private final InlineHandlers client = new InlineHandlers();

    /// A way to wait for a future.
    @FunctionalInterface
    private interface Wait {

        Object on(CompletableFuture<String> future) throws Exception;
    }

    static Stream<Named<Wait>> waits() {
        return Stream.of(
                Named.of("join", CompletableFuture::join),
                Named.of("get", Future::get),
                Named.of("get with a timeout", future -> future.get(1, SECONDS))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("waits")
    void waiting_inside_an_inline_handler_of_its_client_throws(Wait wait) {
        // Arrange
        var sut = new ClientFuture<String>(client);
        InlineHandlers previous = client.enter();
        try {
            // Act & Assert
            assertThatThrownBy(() -> wait.on(sut))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("inline handler");
        } finally {
            InlineHandlers.exit(previous);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("waits")
    void a_completed_future_refuses_the_wait_too_while_its_value_stays_readable(Wait wait) {
        // Arrange
        ClientFuture<String> sut = ClientFuture.completed(client, "done");
        InlineHandlers previous = client.enter();
        try {
            // Act & Assert: the outcome does not depend on how soon the answer came.
            assertThatThrownBy(() -> wait.on(sut)).isInstanceOf(IllegalStateException.class);
            assertThat(sut.getNow("absent")).isEqualTo("done");
            assertThat(sut.resultNow()).isEqualTo("done");
        } finally {
            InlineHandlers.exit(previous);
        }
    }

    @Test
    void a_future_made_from_it_refuses_the_wait_too() {
        // Arrange
        CompletableFuture<Integer> sut = ClientFuture.completed(client, "done")
                .thenApply(String::length);
        InlineHandlers previous = client.enter();
        try {
            // Act & Assert
            assertThatThrownBy(sut::join).isInstanceOf(IllegalStateException.class);
        } finally {
            InlineHandlers.exit(previous);
        }
    }

    @Test
    void a_future_of_another_client_can_be_waited_for_inside_an_inline_handler() {
        // Arrange: an inline handler of one client waits for another client, as a bridge does.
        ClientFuture<String> sut = ClientFuture.completed(new InlineHandlers(), "done");
        InlineHandlers previous = client.enter();
        try {
            // Act & Assert
            assertThat(sut.join()).isEqualTo("done");
        } finally {
            InlineHandlers.exit(previous);
        }
    }

    @Test
    void a_future_can_be_waited_for_outside_inline_handlers() throws Exception {
        // Arrange
        ClientFuture<String> sut = ClientFuture.completed(client, "done");
        InlineHandlers previous = client.enter();
        InlineHandlers.exit(previous);

        // Act & Assert: the mark ends with the handler.
        assertThat(sut.join()).isEqualTo("done");
        assertThat(sut.get()).isEqualTo("done");
    }
}
