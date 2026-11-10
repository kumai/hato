package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.client.internal.common.concurrent.TimerService;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class CompletionsTest {

    // The test's own platform thread, which the clients of other tests cannot hold.
    private final Semaphore platformThreads = new Semaphore(1);

    // A short idle limit lets a test see a thread end for idleness.
    private final Completions sut = completions(
            "completions-test",
            Duration.ofMillis(10),
            Duration.ofMillis(50)
    );

    @Test
    void completions_queued_behind_a_blocked_one_still_complete() throws Exception {
        // Arrange
        var release = new CountDownLatch(1);
        var entered = new CountDownLatch(1);
        var completed = new CountDownLatch(2);
        try {
            // Act: two completions wait behind one whose continuation blocks.
            sut.submit(() -> {
                entered.countDown();
                waitFor(release);
            });
            sut.submit(completed::countDown);
            sut.submit(completed::countDown);

            // Assert
            assertThat(entered.await(5, SECONDS)).isTrue();
            assertThat(completed.await(5, SECONDS)).isTrue();
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_completion_submitted_while_another_blocks_completes() throws Exception {
        // Arrange
        var release = new CountDownLatch(1);
        var entered = new CountDownLatch(1);
        var completed = new CountDownLatch(1);
        try {
            sut.submit(() -> {
                entered.countDown();
                waitFor(release);
            });
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act
            sut.submit(completed::countDown);

            // Assert
            assertThat(completed.await(5, SECONDS)).isTrue();
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_failing_completion_leaves_the_next_one_to_run() throws Exception {
        // Arrange
        var completed = new CountDownLatch(1);

        // Act
        sut.submit(() -> {
            throw new IllegalStateException("Continuation failed");
        });
        sut.submit(completed::countDown);

        // Assert
        assertThat(completed.await(5, SECONDS)).isTrue();
    }

    @Test
    void a_completion_that_leaves_its_thread_interrupted_does_not_interrupt_the_next()
            throws Exception {
        // Arrange
        var interrupted = new LinkedBlockingQueue<Boolean>();

        // Act
        sut.submit(() -> Thread.currentThread().interrupt());
        sut.submit(() -> interrupted.add(Thread.currentThread().isInterrupted()));

        // Assert
        assertThat(interrupted.poll(5, SECONDS)).isFalse();
    }

    @Test
    void completions_still_run_after_an_idle_thread_has_ended() throws Exception {
        // Arrange
        var threads = new LinkedBlockingQueue<Thread>();
        sut.submit(() -> threads.add(Thread.currentThread()));
        Thread first = requireNonNull(threads.poll(5, SECONDS));

        // Act: the thread ends after the idle limit, and the next completion starts another.
        assertThat(first.join(Duration.ofSeconds(5))).isTrue();
        sut.submit(() -> threads.add(Thread.currentThread()));

        // Assert
        assertThat(threads.poll(5, SECONDS)).isNotNull().isNotSameAs(first);
    }

    @Test
    void thread_state_left_by_one_clients_continuation_never_reaches_another_clients()
            throws Exception {
        // Arrange: a continuation of the first client sets a thread-local value and keeps it.
        var tenant = new ThreadLocal<String>();
        var kept = new CountDownLatch(1);
        sut.submit(() -> {
            tenant.set("first client");
            kept.countDown();
        });
        assertThat(kept.await(5, SECONDS)).isTrue();
        Completions other = completions(
                "other-client",
                Duration.ofMillis(10),
                Duration.ofMillis(50)
        );
        var seen = new CompletableFuture<String>();

        // Act
        other.submit(() -> seen.complete(tenant.get()));

        // Assert
        assertThat(seen.get(5, SECONDS)).isNull();
    }

    @Test
    void continuations_see_the_context_class_loader_of_the_thread_that_created_the_client()
            throws Exception {
        // Arrange: two clients created under class loaders of their own.
        var firstLoader = new URLClassLoader(new URL[0], getClass().getClassLoader());
        var secondLoader = new URLClassLoader(new URL[0], getClass().getClassLoader());
        Completions first = createdUnder(firstLoader, "first-client");
        Completions second = createdUnder(secondLoader, "second-client");
        var firstSeen = new CompletableFuture<ClassLoader>();
        var secondSeen = new CompletableFuture<ClassLoader>();

        // Act
        first.submit(() -> firstSeen.complete(Thread.currentThread().getContextClassLoader()));
        second.submit(() -> secondSeen.complete(Thread.currentThread().getContextClassLoader()));

        // Assert
        assertThat(firstSeen.get(5, SECONDS)).isSameAs(firstLoader);
        assertThat(secondSeen.get(5, SECONDS)).isSameAs(secondLoader);
    }

    @Test
    void completions_that_pile_up_move_in_order_to_a_platform_thread_of_the_client()
            throws Exception {
        // Arrange: a long takeover limit keeps the completions waiting behind the first one
        // until it returns.
        Completions sut = completions(
                "piling-client",
                Duration.ofSeconds(5),
                Duration.ofSeconds(5)
        );
        var release = new CountDownLatch(1);
        var indices = new LinkedBlockingQueue<Integer>();
        var threads = new LinkedBlockingQueue<Thread>();
        int piled = 2 * Completions.STREAMING;
        sut.submit(() -> {
            threads.add(Thread.currentThread());
            waitFor(release);
        });
        Thread first = requireNonNull(threads.poll(5, SECONDS));
        for (int i = 0; i < piled; i++) {
            int index = i;
            sut.submit(() -> {
                indices.add(index);
                threads.add(Thread.currentThread());
            });
        }

        // Act
        release.countDown();

        // Assert: the client's virtual thread handed them all to one platform thread of the
        // client, which ran them in order.
        var ran = new ArrayList<Integer>();
        var later = new ArrayList<Thread>();
        for (int i = 0; i < piled; i++) {
            ran.add(requireNonNull(indices.poll(5, SECONDS)));
            later.add(requireNonNull(threads.poll(5, SECONDS)));
        }
        List<Integer> expected = IntStream.range(0, piled).boxed().toList();
        assertThat(ran).containsExactlyElementsOf(expected);
        assertThat(first.isVirtual()).isTrue();
        Thread platform = later.getFirst();
        assertThat(platform.isVirtual()).isFalse();
        assertThat(platform.getName()).startsWith("hato-piling-client-complete-");
        assertThat(later).allMatch(thread -> thread == platform);
    }

    @Test
    void a_client_goes_back_to_a_virtual_thread_once_its_platform_thread_has_idled()
            throws Exception {
        // Arrange: a long takeover limit keeps the piled-up completions behind the first one.
        Completions sut = completions(
                "idling-client",
                Duration.ofSeconds(5),
                Duration.ofMillis(50)
        );
        Thread platform = pileUp(sut);
        var threads = new LinkedBlockingQueue<Thread>();

        // Act: the platform thread ends after the idle limit.
        assertThat(platform.join(Duration.ofSeconds(5))).isTrue();
        sut.submit(() -> threads.add(Thread.currentThread()));

        // Assert: and the client has given it back.
        assertThat(platform.isVirtual()).isFalse();
        assertThat(requireNonNull(threads.poll(5, SECONDS)).isVirtual()).isTrue();
        assertThat(platformThreads.availablePermits()).isEqualTo(1);
    }

    @Test
    void completions_that_pile_up_stay_in_order_on_the_virtual_thread_if_none_is_free()
            throws Exception {
        // Arrange: another client holds the only platform thread.
        assertThat(platformThreads.tryAcquire()).isTrue();
        Completions sut = completions(
                "waiting-client",
                Duration.ofSeconds(5),
                Duration.ofSeconds(5)
        );
        var release = new CountDownLatch(1);
        var entered = new CountDownLatch(1);
        var indices = new LinkedBlockingQueue<Integer>();
        var threads = new LinkedBlockingQueue<Thread>();
        int piled = 2 * Completions.STREAMING;
        sut.submit(() -> {
            entered.countDown();
            waitFor(release);
        });
        assertThat(entered.await(5, SECONDS)).isTrue();
        for (int i = 0; i < piled; i++) {
            int index = i;
            sut.submit(() -> {
                indices.add(index);
                threads.add(Thread.currentThread());
            });
        }

        // Act
        release.countDown();

        // Assert
        var ran = new ArrayList<Integer>();
        for (int i = 0; i < piled; i++) {
            ran.add(requireNonNull(indices.poll(5, SECONDS)));
            assertThat(requireNonNull(threads.poll(5, SECONDS)).isVirtual()).isTrue();
        }
        List<Integer> expected = IntStream.range(0, piled).boxed().toList();
        assertThat(ran).containsExactlyElementsOf(expected);
    }

    @ParameterizedTest
    @EnumSource(PlatformThread.class)
    void a_held_platform_thread_hands_completions_to_a_virtual_thread_until_it_returns(
            PlatformThread kind
    ) throws Exception {
        // Arrange: the client takes its completions on a platform thread, which a continuation
        // then holds. The continuation spins rather than waits, so that the thread waits only
        // for completions, and only once the continuation has returned.
        Completions sut = onPlatformThread(kind);
        var released = new AtomicBoolean();
        var threads = new LinkedBlockingQueue<Thread>();
        sut.submit(() -> {
            threads.add(Thread.currentThread());
            while (!released.get()) {
                Thread.onSpinWait();
            }
        });
        Thread platform = requireNonNull(threads.poll(5, SECONDS));
        try {
            // Act: a completion waits behind the held thread past the takeover limit; then the
            // continuation returns, and the platform thread waits for completions again.
            sut.submit(() -> threads.add(Thread.currentThread()));
            Thread replacing = requireNonNull(threads.poll(5, SECONDS));
            released.set(true);
            await().until(() -> LockSupport.getBlocker(platform) == sut);
            sut.submit(() -> threads.add(Thread.currentThread()));

            // Assert: a virtual thread took over rather than a second platform thread, and the
            // platform thread then took the completions back, while the virtual thread ended.
            assertThat(platform.isVirtual()).isFalse();
            assertThat(replacing.isVirtual()).isTrue();
            assertThat(threads.poll(5, SECONDS)).isSameAs(platform);
            assertThat(replacing.join(Duration.ofSeconds(5))).isTrue();
        } finally {
            released.set(true);
        }
    }

    @Test
    void concurrent_submissions_complete_exactly_once() throws Exception {
        // Arrange
        var completed = new CountDownLatch(2000);
        var counts = new AtomicIntegerArray(2000);
        try (ExecutorService submitters = Executors.newFixedThreadPool(4)) {
            // Act
            for (int i = 0; i < counts.length(); i++) {
                int index = i;
                submitters.execute(() -> sut.submit(() -> {
                    counts.incrementAndGet(index);
                    completed.countDown();
                }));
            }

            // Assert
            assertThat(completed.await(5, SECONDS)).isTrue();
            for (int i = 0; i < counts.length(); i++) {
                assertThat(counts.get(i)).isEqualTo(1);
            }
        }
    }

    @Test
    void a_replaced_thread_ends_once_its_continuation_returns() throws Exception {
        // Arrange: a takeover limit long enough for the test to queue a third completion
        // behind the second before a further takeover.
        Completions sut = completions(
                "replaced-client",
                Duration.ofMillis(200),
                Duration.ofSeconds(5)
        );
        var releaseFirst = new CountDownLatch(1);
        var releaseSecond = new CountDownLatch(1);
        var threads = new LinkedBlockingQueue<Thread>();
        var completed = new CountDownLatch(1);
        try {
            sut.submit(() -> {
                threads.add(Thread.currentThread());
                waitFor(releaseFirst);
            });
            sut.submit(() -> {
                threads.add(Thread.currentThread());
                waitFor(releaseSecond);
            });
            Thread first = requireNonNull(threads.poll(5, SECONDS));
            Thread second = requireNonNull(threads.poll(5, SECONDS));
            sut.submit(() -> {
                threads.add(Thread.currentThread());
                completed.countDown();
            });

            // Act: the first continuation returns while the third completion waits.
            releaseFirst.countDown();

            // Assert: the replaced thread ends rather than taking it, and a further takeover
            // runs it.
            assertThat(first.join(Duration.ofSeconds(5))).isTrue();
            assertThat(completed.await(5, SECONDS)).isTrue();
            assertThat(threads.poll(5, SECONDS)).isNotNull().isNotIn(first, second);
        } finally {
            releaseFirst.countDown();
            releaseSecond.countDown();
        }
    }

    @Test
    void an_interrupt_while_waiting_for_completions_leaves_the_thread_waiting() throws Exception {
        // Arrange: the client's thread waits for a completion.
        Completions sut = completions(
                "interrupted-client",
                Duration.ofMillis(10),
                Duration.ofSeconds(5)
        );
        var threads = new LinkedBlockingQueue<Thread>();
        sut.submit(() -> threads.add(Thread.currentThread()));
        Thread thread = requireNonNull(threads.poll(5, SECONDS));
        await().until(() -> thread.getState() == Thread.State.WAITING);

        // Act
        thread.interrupt();

        // Assert: it waits again rather than spinning, and still takes the next completion.
        await().during(Duration.ofMillis(100))
                .atMost(Duration.ofSeconds(5))
                .until(() -> thread.getState() == Thread.State.WAITING);
        sut.submit(() -> threads.add(Thread.currentThread()));
        assertThat(threads.poll(5, SECONDS)).isSameAs(thread);
    }

    // Completions of the client `clientId` with the given limits, which draw on the test's own
    // platform thread.
    private Completions completions(String clientId, Duration takeover, Duration idle) {
        return completions(clientId, takeover, idle, CoreThreads.virtual());
    }

    // Completions like those above of a client whose core runs on the threads that `cores` gives.
    private Completions completions(
            String clientId,
            Duration takeover,
            Duration idle,
            CoreThreads cores
    ) {
        return new Completions(
                URI.create("tcp://localhost:1883"),
                clientId,
                "hato-" + clientId + "-complete-",
                TimerService.instance,
                takeover,
                idle,
                cores,
                platformThreads
        );
    }

    // Completions that run on a platform thread of the client, which they took as `kind` says.
    // The takeover limit leaves the test the time to pile completions up behind a blocked one.
    private Completions onPlatformThread(PlatformThread kind) throws InterruptedException {
        return switch (kind) {
            case CORE -> completions(
                    "core-client",
                    Duration.ofMillis(200),
                    Duration.ofSeconds(5),
                    new CoreThreads.Places(1).take()
            );
            case STREAMING -> {
                Completions streaming = completions(
                        "streaming-client",
                        Duration.ofMillis(200),
                        Duration.ofSeconds(5)
                );
                Thread platform = pileUp(streaming);
                assertThat(platform.isVirtual()).isFalse();
                yield streaming;
            }
        };
    }

    // Creates completions the way a client does, on the thread that creates the client, while
    // that thread's context class loader is `loader`.
    private Completions createdUnder(ClassLoader loader, String clientId) {
        Thread thread = Thread.currentThread();
        ClassLoader original = thread.getContextClassLoader();
        thread.setContextClassLoader(loader);
        try {
            return completions(clientId, Duration.ofMillis(10), Duration.ofMillis(50));
        } finally {
            thread.setContextClassLoader(original);
        }
    }

    // Piles completions up behind a blocked one until a platform thread of the client takes
    // them, and returns that thread.
    private static Thread pileUp(Completions completions) throws InterruptedException {
        var release = new CountDownLatch(1);
        var entered = new CountDownLatch(1);
        var threads = new LinkedBlockingQueue<Thread>();
        completions.submit(() -> {
            entered.countDown();
            waitFor(release);
        });
        assertThat(entered.await(5, SECONDS)).isTrue();
        for (int i = 0; i <= Completions.STREAMING; i++) {
            completions.submit(() -> threads.add(Thread.currentThread()));
        }
        release.countDown();
        Thread last = requireNonNull(threads.poll(5, SECONDS));
        for (int i = 0; i < Completions.STREAMING; i++) {
            last = requireNonNull(threads.poll(5, SECONDS));
        }
        return last;
    }

    // How a client comes to take its completions on a platform thread.
    enum PlatformThread {

        // Its core runs on platform threads.
        CORE,

        // Its completions piled up behind its virtual thread.
        STREAMING
    }

    // Blocks until the test releases it, which each test does last, so that a completion that
    // merely waited for its turn cannot pass for one that ran beside the blocked one.
    private static void waitFor(CountDownLatch release) {
        try {
            release.await();
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }
}
