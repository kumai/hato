package org.mqtt4j.hato.broker.internal;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// The room that the broker's memory for deliveries hands out and takes back, directly and
/// through the room that a thread of the actor pool keeps to itself.
class MemoryBudgetTest {

    @Test
    void room_beyond_the_limit_is_refused_until_some_comes_back() {
        // Arrange
        var sut = new MemoryBudget(100, 0, 0);
        boolean first = sut.take(60);

        // Act
        boolean tooMuch = sut.take(41);
        boolean rest = sut.take(40);
        sut.give(60);
        boolean again = sut.take(60);

        // Assert
        assertThat(List.of(first, tooMuch, rest, again)).containsExactly(true, false, true, true);
        assertThat(sut.lent()).isEqualTo(100);
    }

    @Test
    void room_that_must_be_had_is_taken_beyond_the_limit_and_holds_others_off_until_it_is_back() {
        // Arrange
        var sut = new MemoryBudget(100, 0, 0);
        boolean first = sut.take(80);

        // Act
        sut.takeAnyway(50);
        boolean whileOver = sut.take(1);
        sut.give(50);
        boolean afterwards = sut.take(20);

        // Assert
        assertThat(List.of(first, whileOver, afterwards)).containsExactly(true, false, true);
        assertThat(sut.lent()).isEqualTo(100);
    }

    @Test
    void a_pool_thread_fetches_a_unit_and_then_takes_and_gives_within_it() throws Exception {
        // Arrange
        var sut = new MemoryBudget(1000, 100, 0);
        var lent = new ArrayList<Long>();

        // Act: the first take fetches a unit, and what follows fits in it.
        onPoolThread(() -> {
            sut.take(10);
            lent.add(sut.lent());
            sut.give(10);
            sut.take(90);
            sut.give(90);
            lent.add(sut.lent());
        });

        // Assert
        assertThat(lent).containsExactly(100L, 100L);
    }

    @Test
    void a_pool_thread_returns_what_it_keeps_beyond_two_units() throws Exception {
        // Arrange
        var sut = new MemoryBudget(1000, 100, 0);
        var lent = new ArrayList<Long>();

        // Act: the thread takes more than a unit at once, and then has all of it to spare.
        onPoolThread(() -> {
            sut.take(250);
            lent.add(sut.lent());
            sut.give(250);
            lent.add(sut.lent());
        });

        // Assert: it keeps one unit.
        assertThat(lent).containsExactly(250L, 100L);
    }

    @Test
    void a_pool_thread_fetches_only_what_it_needs_when_a_unit_does_not_fit() throws Exception {
        // Arrange: a budget of one and a half units.
        var sut = new MemoryBudget(150, 100, 0);
        var taken = new ArrayList<Boolean>();

        // Act: the second take needs 40 more than the thread keeps, and a unit is too much.
        onPoolThread(() -> {
            taken.add(sut.take(60));
            taken.add(sut.take(80));
            taken.add(sut.take(11));
            taken.add(sut.take(10));
        });

        // Assert
        assertThat(taken).containsExactly(true, true, false, true);
        assertThat(sut.lent()).isEqualTo(150);
    }

    @Test
    void what_a_pool_thread_gives_back_without_a_unit_to_keep_is_there_for_others()
            throws Exception {
        // Arrange: a budget whose threads keep nothing.
        var sut = new MemoryBudget(100, 0, 0);
        var whileTaken = new ArrayList<Boolean>();

        // Act
        onPoolThread(() -> {
            sut.take(100);
            whileTaken.add(sut.lent() == 100);
            sut.give(100);
        });
        boolean afterwards = sut.take(100);

        // Assert
        assertThat(whileTaken).containsExactly(true);
        assertThat(afterwards).isTrue();
    }

    @Test
    void a_publication_takes_its_room_once_for_all_its_holders() {
        // Arrange
        var budget = new MemoryBudget(100, 0, 0);
        var sut = new PublicationMemory(60);

        // Act: two holders come, of which the second would not fit on its own, and go.
        boolean first = sut.hold(budget);
        boolean second = sut.hold(budget);
        long held = budget.lent();
        sut.release(budget);
        long stillHeld = budget.lent();
        sut.release(budget);

        // Assert
        assertThat(List.of(first, second)).containsExactly(true, true);
        assertThat(List.of(held, stillHeld, budget.lent())).containsExactly(60L, 60L, 0L);
    }

    @Test
    void a_publication_without_room_gets_no_holder() {
        // Arrange
        var budget = new MemoryBudget(50, 0, 0);
        var sut = new PublicationMemory(60);

        // Act
        boolean held = sut.hold(budget);

        // Assert: nothing is taken, and a later holder asks for the room again.
        assertThat(held).isFalse();
        assertThat(budget.lent()).isZero();
        assertThat(sut.hold(new MemoryBudget(60, 0, 0))).isTrue();
    }

    /// Runs `body` on a thread of the kind that the actor pool has, and waits for it.
    private static void onPoolThread(Runnable body) throws InterruptedException {
        var failure = new ArrayList<Throwable>();
        var thread = new Worker(body, "memory-budget-test");
        thread.setUncaughtExceptionHandler((_, thrown) -> failure.add(thrown));
        thread.start();
        thread.join();
        assertThat(failure).isEmpty();
    }
}
