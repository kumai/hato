package org.mqtt4j.hato.broker.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.broker.BrokerListener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.broker.BrokerListener.DisconnectReason.CLIENT_DISCONNECT;

/// How the notices reach a listener that is behind: in the order in which they came, the reports
/// of dropped deliveries taking turns with the one count of the clients that found no place to
/// wait, and the oldest notices of connections giving way to newer ones beyond their limit.
class NoticesTest {

    /// What a call without a client is recorded as.
    private static final String NO_CLIENT = "";

    /// A limit on notices of connections that the tests of dropped deliveries never reach.
    private static final int NOTICE_LIMIT = 100;

    @Test
    void a_client_that_waits_gets_its_report_while_other_clients_keep_finding_no_place()
            throws Exception {
        // Arrange: one client may wait. The listener is busy with a first client while a second
        // comes to wait and a third finds no place. From then on, another client drops a
        // delivery during each report without a client, five times.
        var listener = new Listener();
        var sut = new Notices(listener, 0, 1, NOTICE_LIMIT, new Termination(1, 0));
        var others = new AtomicInteger();
        listener.duringReportWithoutClient = () -> {
            if (others.getAndIncrement() < 5) {
                sut.add("other-" + others.get(), 1);
            }
        };
        sut.start();
        try {
            sut.add("first", 1);
            assertThat(listener.busy.await(5, TimeUnit.SECONDS)).isTrue();
            sut.add("waiting", 1);
            sut.add("without a place", 1);

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
        var sut = new Notices(listener, 0, 1, NOTICE_LIMIT, new Termination(1, 0));
        var others = new AtomicInteger();
        listener.duringReportOfClient = () -> {
            if (others.getAndIncrement() < 5) {
                sut.add("other-" + others.get(), 1);
            }
        };
        sut.start();
        try {
            sut.add("first", 1);
            assertThat(listener.busy.await(5, TimeUnit.SECONDS)).isTrue();
            sut.add("waiting", 1);
            sut.add("without a place", 1);

            // Act
            listener.release.countDown();

            // Assert: the count without a client does not wait until no client is left.
            assertThat(listener.nextCalls(3)).contains(NO_CLIENT);
        } finally {
            listener.release.countDown();
            sut.close();
        }
    }

    @Test
    void notices_and_reports_reach_the_listener_in_the_order_in_which_they_came() throws Exception {
        // Arrange: the listener is busy with a first report.
        var listener = new Listener();
        var sut = new Notices(listener, 0, NOTICE_LIMIT, NOTICE_LIMIT, new Termination(1, 0));
        sut.start();
        try {
            sut.add("first", 1);
            assertThat(listener.busy.await(5, TimeUnit.SECONDS)).isTrue();
            ClientConnection robot = connection("robot");
            sut.connected(robot);
            sut.add("other", 1);
            sut.disconnected(robot, CLIENT_DISCONNECT);

            // Act
            listener.release.countDown();

            // Assert
            assertThat(listener.nextCalls(4)).containsExactly(
                    "first",
                    "connected robot",
                    "other",
                    "disconnected robot CLIENT_DISCONNECT"
            );
        } finally {
            listener.release.countDown();
            sut.close();
        }
    }

    @Test
    void notices_beyond_their_limit_drop_the_oldest_and_are_counted() throws Exception {
        // Arrange: two notices of connections may wait, while the listener is busy with a first
        // one.
        var listener = new Listener();
        var sut = new Notices(listener, 0, NOTICE_LIMIT, 2, new Termination(1, 0));
        sut.start();
        try {
            sut.connected(connection("first"));
            assertThat(listener.busy.await(5, TimeUnit.SECONDS)).isTrue();

            // Act: one more than may wait.
            sut.connected(connection("oldest"));
            sut.connected(connection("second"));
            sut.connected(connection("newest"));
            listener.release.countDown();

            // Assert
            assertThat(listener.nextCalls(3)).containsExactly(
                    "connected first",
                    "connected second",
                    "connected newest"
            );
            assertThat(sut.droppedNotices()).isEqualTo(1);
        } finally {
            listener.release.countDown();
            sut.close();
        }
    }

    /// A connection of the client `clientId` from the same JVM, without a user name or a Will.
    private static ClientConnection connection(String clientId) {
        return new ClientConnection(
                clientId,
                null,
                null,
                MqttVersion.V5,
                null,
                false,
                List.of()
        );
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
        public void onConnected(Connection connection) {
            heard("connected " + connection.clientId(), () -> { });
        }

        @Override
        public void onDisconnected(Connection connection, DisconnectReason reason) {
            heard("disconnected " + connection.clientId() + " " + reason, () -> { });
        }

        @Override
        public void onDropped(String clientId, long count) {
            heard(clientId, duringReportOfClient);
        }

        @Override
        public void onDroppedUnattributed(long count) {
            heard(NO_CLIENT, duringReportWithoutClient);
        }

        private void heard(String call, Runnable during) {
            calls.add(call);
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
