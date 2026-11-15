package org.mqtt4j.hato.broker.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.broker.BrokerListener;

import static org.assertj.core.api.Assertions.assertThat;

/// How the reports of dropped deliveries take turns while the listener is behind: those of the
/// clients that wait with their identifiers, and the one count of the clients that found no
/// place among them.
class DropReportsTest {

    /// What a call without a client is recorded as.
    private static final String NO_CLIENT = "";

    @Test
    void a_client_that_waits_gets_its_report_while_other_clients_keep_finding_no_place()
            throws Exception {
        // Arrange: one client may wait. The listener is busy with a first client while a second
        // comes to wait and a third finds no place. From then on, another client drops a
        // delivery during each report without a client, five times.
        var listener = new Listener();
        var sut = new DropReports(listener, 0, 1, new Termination(1, 0));
        var others = new AtomicInteger();
        listener.duringReportWithoutClient = () -> {
            if (others.getAndIncrement() < 5) {
                sut.add("other-" + others.get());
            }
        };
        sut.start();
        try {
            sut.add("first");
            assertThat(listener.busy.await(5, TimeUnit.SECONDS)).isTrue();
            sut.add("waiting");
            sut.add("without a place");

            // Act
            listener.release.countDown();

            // Assert: the client does not wait until no client is left without a place.
            assertThat(listener.nextCalls(3)).contains("waiting");
        } finally {
            listener.release.countDown();
            sut.close();
        }
    }

    @Test
    void the_clients_without_a_place_get_their_report_while_other_clients_keep_coming_to_wait()
            throws Exception {
        // Arrange: one client may wait. The listener is busy with a first client while a second
        // comes to wait and a third finds no place. From then on, a new client comes to wait
        // during each report of a client, five times.
        var listener = new Listener();
        var sut = new DropReports(listener, 0, 1, new Termination(1, 0));
        var others = new AtomicInteger();
        listener.duringReportOfClient = () -> {
            if (others.getAndIncrement() < 5) {
                sut.add("other-" + others.get());
            }
        };
        sut.start();
        try {
            sut.add("first");
            assertThat(listener.busy.await(5, TimeUnit.SECONDS)).isTrue();
            sut.add("waiting");
            sut.add("without a place");

            // Act
            listener.release.countDown();

            // Assert: the count without a client does not wait until no client is left.
            assertThat(listener.nextCalls(3)).contains(NO_CLIENT);
        } finally {
            listener.release.countDown();
            sut.close();
        }
    }

    /// Records its calls. The first one does not return before `release` opens; each later one
    /// first runs what the test has set for its kind.
    private static final class Listener implements BrokerListener {

        final CountDownLatch busy = new CountDownLatch(1);

        final CountDownLatch release = new CountDownLatch(1);

        volatile Runnable duringReportOfClient = () -> { };

        volatile Runnable duringReportWithoutClient = () -> { };

        private final BlockingQueue<String> calls = new LinkedBlockingQueue<>();

        @Override
        public void onDropped(String clientId, long count) {
            heard(clientId, duringReportOfClient);
        }

        @Override
        public void onDroppedUnattributed(long count) {
            heard(NO_CLIENT, duringReportWithoutClient);
        }

        private void heard(String clientId, Runnable during) {
            calls.add(clientId);
            if (busy.getCount() == 0) {
                during.run();
                return;
            }
            busy.countDown();
            try {
                release.await();
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        }

        /// The next `count` calls, in their order.
        List<String> nextCalls(int count) throws InterruptedException {
            var next = new ArrayList<String>();
            for (int taken = 0; taken < count; taken++) {
                String call = calls.poll(5, TimeUnit.SECONDS);
                assertThat(call).as("call %d of the listener", taken + 1).isNotNull();
                next.add(call);
            }
            return next;
        }
    }
}
