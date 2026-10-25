package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.client.internal.common.concurrent.TimerService;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

class CompletionsTest {

    // A short idle limit lets a test see a thread end for idleness.
    private final Completions sut = new Completions(
            URI.create("tcp://localhost:1883"),
            "completions-test",
            "hato-completions-test-complete-",
            TimerService.instance,
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
