package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.internal.inproc.InprocBrokers;
import org.mqtt4j.hato.internal.inproc.InprocLink;
import org.mqtt4j.hato.internal.protocol.Packet;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/// The thread that reads an in-process transport follows the rules of a TCP transport's reader:
/// a platform reader while the JVM has one to spare, the core's own thread for a core on platform
/// threads, and moves between the two kinds. A broker of the test's own stands at the other end
/// of each link.
class InprocTransportTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private static final AtomicInteger brokers = new AtomicInteger();

    @Test
    @SuppressWarnings("try") // The first transport closes before the third starts.
    void connections_read_on_platform_threads_of_their_own_while_one_is_to_spare()
            throws Exception {
        // Arrange: one platform thread to spare for reading.
        var slots = new PlatformReaders(1);
        var firstReader = new Reader();
        var secondReader = new Reader();
        var thirdReader = new Reader();
        try (
                var broker = new Broker();
                var first = new InprocTransport("first", CoreThreads.virtual(), slots);
                var second = new InprocTransport("second", CoreThreads.virtual(), slots);
                var third = new InprocTransport("third", CoreThreads.virtual(), slots)
        ) {
            // Act: the second connects while the first reads, and the third once the first has
            // closed.
            Thread firstThread = threadOf(broker.connect(first, firstReader), firstReader);
            Thread secondThread = threadOf(broker.connect(second, secondReader), secondReader);
            first.close();
            firstReader.closed.get(5, SECONDS);
            Thread thirdThread = threadOf(broker.connect(third, thirdReader), thirdReader);

            // Assert: the first takes the platform thread and gives it back before it reports
            // its close; the second reads on the virtual thread that connected it.
            assertThat(firstThread.isVirtual()).isFalse();
            assertThat(firstThread.getName()).isEqualTo("hato-first-reader");
            assertThat(secondThread.isVirtual()).isTrue();
            assertThat(secondThread.getName()).isEqualTo("hato-second-reader");
            assertThat(thirdThread.isVirtual()).isFalse();
        }
    }

    @Test
    void a_core_on_platform_threads_reads_on_its_own_thread_and_leaves_platform_readers()
            throws Exception {
        // Arrange: one platform reader to spare, and a transport of a client whose core runs on
        // platform threads.
        var slots = new PlatformReaders(1);
        var reader = new Reader();
        try (
                var broker = new Broker();
                var sut = new InprocTransport("core", new CoreThreads.Places(1).take(), slots)
        ) {
            // Act
            Thread thread = threadOf(broker.connect(sut, reader), reader);

            // Assert: a platform thread of the transport reads, and the platform reader stays
            // free for another connection.
            assertThat(thread.isVirtual()).isFalse();
            assertThat(thread.getName()).isEqualTo("hato-core-reader");
            assertThat(slots.take()).isTrue();
        }
    }

    @Test
    void a_busy_connection_takes_the_platform_thread_of_a_quiet_one() throws Exception {
        // Arrange: one platform thread to spare, which a connection that receives nothing takes
        // first, and a second connection that reads on a virtual thread.
        PlatformReaders slots = moving(1);
        var quietReader = new Reader();
        var busyReader = new Reader();
        try (
                var broker = new Broker();
                var quiet = new InprocTransport("quiet", CoreThreads.virtual(), slots);
                var busy = new InprocTransport("busy", CoreThreads.virtual(), slots)
        ) {
            InprocLink quietLink = broker.connect(quiet, quietReader);
            InprocLink busyLink = broker.connect(busy, busyReader);

            // Act: the second receives a packet about every millisecond.
            var feeding = new CompletableFuture<Void>();
            Thread feeder = Thread.ofPlatform().start(() -> {
                while (!feeding.isDone()) {
                    boolean _ = busyLink.deliver(packet());
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
            });
            try {
                await().atMost(5, SECONDS).until(() -> {
                    Thread reading = busyReader.last;
                    return reading != null && !reading.isVirtual();
                });
            } finally {
                feeding.complete(null);
                feeder.join();
            }

            // Assert: the busy connection reads on a platform thread of the same name, the quiet
            // one reads its next packet on a virtual thread, and neither connection ends.
            assertThat(requireNonNull(busyReader.last).getName()).isEqualTo("hato-busy-reader");
            Thread quietThread = threadOf(quietLink, quietReader);
            assertThat(quietThread.isVirtual()).isTrue();
            assertThat(quietThread.getName()).isEqualTo("hato-quiet-reader");
            assertThat(quietReader.closed).isNotDone();
            assertThat(busyReader.closed).isNotDone();
        }
    }

    // Has the broker hand the client of `link` a packet, and returns the thread that read it.
    private static Thread threadOf(InprocLink link, Reader reader) {
        int before = reader.received.get();
        assertThat(link.deliver(packet())).isTrue();
        await().atMost(5, SECONDS).until(() -> reader.received.get() > before);
        return requireNonNull(reader.last);
    }

    private static InprocLink.Item packet() {
        return new InprocLink.Item(new Packet.PingResp(), 2);
    }

    // Platform readers that move as the production ones do on a machine with more processors,
    // quicker: each reader counts its reads over 50 ms, five of which make a connection busy and
    // fewer than two quiet, and a platform reader's waits end after 10 ms.
    private static PlatformReaders moving(int slots) {
        return new PlatformReaders(
                slots,
                new PlatformReaders.Moves(
                        Duration.ofMillis(50),
                        5,
                        2,
                        Duration.ofMillis(10),
                        true
                )
        );
    }

    // A broker of the test's own: it accepts every link, which the test then serves.
    private static final class Broker implements AutoCloseable {

        private final InprocBrokers.Registration registration =
                InprocBrokers.take("transport-test-" + brokers.incrementAndGet());

        private final LinkedBlockingQueue<InprocLink> links = new LinkedBlockingQueue<>();

        Broker() {
            registration.open(link -> {
                links.add(link);
                link.accept(() -> {
                });
            });
        }

        // Starts `transport` with `reader`, and returns its link once it is connected.
        InprocLink connect(InprocTransport transport, Reader reader) throws Exception {
            transport.start(
                    URI.create("inproc://" + registration.name()),
                    TIMEOUT,
                    reader
            );
            InprocLink link = requireNonNull(links.poll(5, SECONDS));
            reader.connected.get(5, SECONDS);
            return link;
        }

        @Override
        public void close() {
            registration.release();
        }
    }

    // Records the thread that read last and what the reads took.
    private static final class Reader implements PacketListener {

        final CompletableFuture<Void> connected = new CompletableFuture<>();

        final CompletableFuture<Throwable> closed = new CompletableFuture<>();

        final AtomicInteger received = new AtomicInteger();

        volatile @Nullable Thread last;

        @Override
        public void onConnected() {
            connected.complete(null);
        }

        @Override
        public void onPackets(List<InprocLink.Item> packets, boolean filled) {
            last = Thread.currentThread();
            received.addAndGet(packets.size());
        }

        @Override
        public void onBytes(byte[] bytes) {
            throw new AssertionError("An in-process transport passes packets");
        }

        @Override
        public void onBorrowedBytes(byte[] buffer, int offset, int length) {
            throw new AssertionError("An in-process transport passes packets");
        }

        @Override
        public void afterBorrowedBytes() {
        }

        @Override
        public boolean runsHandlersAfterReads() {
            return true;
        }

        @Override
        public void onSent(long ticket) {
        }

        @Override
        public void onClosed(Throwable cause) {
            closed.complete(cause);
        }
    }
}
