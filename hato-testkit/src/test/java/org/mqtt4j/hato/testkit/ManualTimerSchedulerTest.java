package org.mqtt4j.hato.testkit;

import java.time.Duration;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.TimerScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ManualTimerSchedulerTest {

    @Test
    void advancing_runs_in_deadline_then_registration_order_including_nested_tasks() {
        // Arrange
        var sut = new ManualTimerScheduler();
        var calls = new ArrayList<String>();
        sut.schedule(Duration.ofSeconds(2), () -> calls.add("last"));
        sut.schedule(Duration.ofSeconds(1), () -> {
            calls.add("first");
            sut.schedule(Duration.ZERO, () -> calls.add("nested"));
        });
        sut.schedule(Duration.ofSeconds(1), () -> calls.add("second"));

        // Act
        sut.advance(Duration.ofSeconds(3));

        // Assert
        assertThat(calls).containsExactly("first", "second", "nested", "last");
        assertThat(sut.nanoTime()).isEqualTo(Duration.ofSeconds(3).toNanos());
        assertThat(sut.pendingTaskCount()).isZero();
    }

    @Test
    void cancellation_releases_the_task_immediately_and_is_idempotent() {
        // Arrange
        var sut = new ManualTimerScheduler();
        TimerScheduler.Cancellable task = sut.schedule(Duration.ofDays(1), () -> {
            throw new AssertionError("cancelled task ran");
        });

        // Act
        task.cancel();
        task.cancel();

        // Assert
        assertThat(sut.pendingTaskCount()).isZero();
        sut.advance(Duration.ofDays(2));
        assertThatThrownBy(() -> sut.advance(Duration.ofNanos(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
