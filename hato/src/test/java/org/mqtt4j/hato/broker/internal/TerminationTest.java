package org.mqtt4j.hato.broker.internal;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class TerminationTest {

    private static final Duration WAIT = Duration.ofSeconds(5);

    @Test
    void a_waiting_continuation_holds_up_no_part_when_no_thread_can_be_started() throws Exception {
        // Arrange: the end of one part, whose observations cannot have threads of their own, and
        // an observation whose continuation waits until the test lets it go.
        var sut = new Termination(1, 0, _ -> {
            throw new OutOfMemoryError("unable to create native thread");
        });
        sut.start();
        var release = new CountDownLatch(1);
        var waitingOn = new AtomicReference<String>();
        CompletableFuture<Void> continued = sut.observe().thenRun(() -> {
            waitingOn.set(Thread.currentThread().getName());
            try {
                release.await();
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        });
        // Act: the part ends on a thread of its own.
        var part = Thread.ofPlatform().name("part").start(sut::partEnded);
        // Assert: the part ends at once, and the continuation waits on the end's own thread.
        try {
            assertThat(part.join(WAIT)).isTrue();
            await().atMost(WAIT).until(() -> waitingOn.get() != null);
            assertThat(waitingOn.get()).endsWith("-end");
        } finally {
            release.countDown();
        }
        assertThat(continued).succeedsWithin(WAIT);
    }
}
