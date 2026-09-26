package org.mqtt4j.mqtj.testkit;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import org.mqtt4j.mqtj.MqttCapacityException;
import org.mqtt4j.mqtj.spi.Transport;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.mqtj.spi.Transport.SendResult.ACCEPTED;
import static org.mqtt4j.mqtj.spi.Transport.SendResult.CLOSED;

class FaultyTransportTest {

    @Test
    void reducing_delay_does_not_reorder_chunks_and_cancellation_releases_them() throws Exception {
        // Arrange
        var clock = new ManualTimerScheduler();
        var delegate = new Delegate();
        try (var sut = new FaultyTransport(delegate, clock)) {
            var receiver = new Receiver();
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);

            // Act
            sut.receiveDelay(Duration.ofSeconds(10));
            delegate.listener.join().onBytes(new byte[] {1});
            sut.receiveDelay(Duration.ZERO);
            delegate.listener.join().onBytes(new byte[] {2});
            clock.advance(Duration.ofSeconds(9));

            // Assert
            assertThat(receiver.received).isEmpty();
            clock.advance(Duration.ofSeconds(1));
            assertThat(receiver.received).containsExactly((byte) 1, (byte) 2);
            sut.receiveDelay(Duration.ofSeconds(10));
            delegate.listener.join().onBytes(new byte[] {3});
            var failure = new IOException("injected disconnect");

            // Act
            sut.disconnect(failure);

            // Assert
            assertThat(receiver.end.get(5, SECONDS)).isSameAs(failure);
            assertThat(clock.pendingTaskCount()).isZero();
            clock.advance(Duration.ofSeconds(10));
            assertThat(receiver.received).containsExactly((byte) 1, (byte) 2);
        }
    }

    @Test
    void pending_send_ownership_is_released_by_delegate_termination_not_close_returning()
            throws Exception {
        // Arrange: closing requests termination; the delegate reports completion separately.
        var delegate = new Delegate();
        delegate.terminateOnClose = false;
        try (var sut = new FaultyTransport(delegate, new ManualTimerScheduler())) {
            var receiver = new Receiver();
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);
            assertThat(sut.send(1, new byte[] {42})).isEqualTo(ACCEPTED);
            var failure = new IOException("injected disconnect");

            // Act
            sut.disconnect(failure);
            Thread closeWorker = delegate.closingThread.get(5, SECONDS);
            assertThat(closeWorker.join(Duration.ofSeconds(5))).isTrue();

            // Assert
            assertThat(receiver.end).isNotDone();
            assertThat(sut.send(2, new byte[] {43})).isEqualTo(CLOSED);
            delegate.listener.join().onClosed(new IOException("underlying close completed"));

            // Assert
            assertThat(receiver.end.get(5, SECONDS)).isSameAs(failure);
            assertThat(sut.send(3, new byte[] {44})).isEqualTo(CLOSED);
        }
    }

    @Test
    void delayed_receive_overload_ends_once_and_cancels_the_timer() throws Exception {
        // Arrange
        var clock = new ManualTimerScheduler();
        var delegate = new Delegate();
        try (var sut = new FaultyTransport(delegate, clock)) {
            var receiver = new Receiver();
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);
            sut.receiveDelay(Duration.ofSeconds(10));

            // Act: exceed the delayed-input budget without advancing the clock.
            for (int i = 0; i <= 256; i++) {
                delegate.listener.join().onBytes(new byte[] {1});
            }

            // Assert
            assertThat(receiver.end.get(5, SECONDS)).isInstanceOf(MqttCapacityException.class);
            assertThat(clock.pendingTaskCount()).isZero();
            assertThat(receiver.received).isEmpty();
        }
    }

    private static final class Delegate implements Transport {

        final CompletableFuture<Listener> listener = new CompletableFuture<>();
        final CompletableFuture<Thread> closingThread = new CompletableFuture<>();
        boolean terminateOnClose = true;

        @Override
        public void start(URI endpoint, Duration timeout, Listener receiver) {
            listener.complete(receiver);
            receiver.onConnected();
        }

        @Override
        public SendResult send(long ticket, byte[] bytes) {
            return SendResult.ACCEPTED;
        }

        @Override
        public void close() {
            closingThread.complete(Thread.currentThread());
            if (terminateOnClose) {
                listener.join().onClosed(new IOException("Delegate closed"));
            }
        }
    }

    private static final class Receiver implements Transport.Listener {

        final List<Byte> received = new ArrayList<>();
        final CompletableFuture<Throwable> end = new CompletableFuture<>();

        @Override
        public void onConnected() {
        }

        @Override
        public void onBytes(byte[] bytes) {
            for (byte value : bytes) {
                received.add(value);
            }
        }

        @Override
        public void onSent(long ticket) {
        }

        @Override
        public void onClosed(Throwable cause) {
            assertThat(end.complete(cause)).isTrue();
        }
    }
}
