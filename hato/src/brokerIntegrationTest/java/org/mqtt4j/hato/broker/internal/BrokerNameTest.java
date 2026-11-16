package org.mqtt4j.hato.broker.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.broker.MqttBroker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mqtt4j.hato.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;

/// A broker holds its name from its start until it ends, as it holds its port.
class BrokerNameTest {

    private static final Duration WAIT = Duration.ofSeconds(5);

    @Test
    void a_broker_without_a_name_takes_one_that_no_running_broker_has() throws Exception {
        // Act
        try (MqttBroker first = MqttBroker.start(); MqttBroker second = MqttBroker.start()) {
            // Assert
            assertThat(first.name()).startsWith("hato-");
            assertThat(second.name()).startsWith("hato-").isNotEqualTo(first.name());
        }
    }

    @Test
    void a_broker_keeps_the_name_that_it_is_given() throws Exception {
        // Act
        try (MqttBroker sut = MqttBroker.builder().name("given.name-test").start()) {
            // Assert
            assertThat(sut.name()).isEqualTo("given.name-test");
        }
    }

    @Test
    void no_broker_starts_with_the_name_of_a_running_one() throws Exception {
        // Arrange
        try (MqttBroker running = MqttBroker.builder().name("taken-test").start()) {
            // Act & Assert
            assertThatThrownBy(() -> MqttBroker.builder().name("taken-test").start())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("taken-test");
            try (RawSession client = connected(running.port(), "still-served")) {
                client.ping();
            }
        }
    }

    @Test
    void a_closed_broker_gives_its_name_back() throws Exception {
        // Arrange
        MqttBroker first = MqttBroker.builder().name("reused-test").start();
        // Act
        first.close();
        // Assert
        try (MqttBroker second = MqttBroker.builder().name("reused-test").start()) {
            assertThat(second.name()).isEqualTo("reused-test");
        }
    }

    @Test
    void a_broker_that_a_failure_stops_gives_its_name_back() throws Exception {
        // Arrange: a broker whose selector fails once a publication wakes it.
        var failing = new AtomicBoolean();
        try (
                BrokerServer failed = BrokerServer.builder()
                        .name("failed-test")
                        .beforeSelect(() -> {
                            if (failing.compareAndSet(true, false)) {
                                throw new AssertionError("injected selector failure");
                            }
                        })
                        .start(loopback());
                RawSession client = connected(failed.port(), "trigger")
        ) {
            // Act
            failing.set(true);
            client.send(publication("unheard", "", AT_MOST_ONCE, 0));
            // Assert
            assertThat(failed.terminated()).succeedsWithin(WAIT);
            try (BrokerServer next = BrokerServer.builder().name("failed-test").start(loopback())) {
                assertThat(next.name()).isEqualTo("failed-test");
            }
        }
    }

    @Test
    void a_broker_whose_start_fails_on_the_way_gives_its_name_back() throws Exception {
        // Arrange
        var failure = new IllegalStateException("injected start failure");
        BrokerServer.Builder failing = BrokerServer.builder()
                .name("unstarted-test")
                .beforeAcceptingStarts(() -> {
                    throw failure;
                });
        // Act
        assertThatThrownBy(() -> failing.start(loopback())).isSameAs(failure);
        // Assert
        try (BrokerServer next = BrokerServer.builder().name("unstarted-test").start(loopback())) {
            assertThat(next.name()).isEqualTo("unstarted-test");
        }
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }
}
