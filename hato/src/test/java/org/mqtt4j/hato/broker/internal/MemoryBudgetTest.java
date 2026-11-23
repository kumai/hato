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
    void room_that_a_pool_thread_keeps_for_its_next_take_is_not_there_for_others_until_it_settles()
            throws Exception {
        // Arrange: a budget whose threads keep nothing otherwise.
        var sut = new MemoryBudget(100, 0, 0);
        var taken = new ArrayList<Boolean>();

        // Act: a delivery that took all the room gives it to its thread alone, which takes
        // part of it for the next delivery and then settles.
        onPoolThread(() -> {
            sut.take(100);
            sut.keep(100);
            taken.add(takenElsewhere(sut, 1));
            taken.add(sut.take(60));
            taken.add(takenElsewhere(sut, 1));
            sut.settle();
            taken.add(takenElsewhere(sut, 40));
        });

        // Assert: another thread finds room only once the thread has settled.
        assertThat(taken).containsExactly(false, true, false, true);
        assertThat(sut.lent()).isEqualTo(100);
    }

    @Test
    void room_that_a_pool_thread_gives_to_the_shared_count_is_not_its_own_to_take()
            throws Exception {
        // Arrange: a budget of 100 bytes whose threads keep up to two units of 50, which has
        // handed out 80.
        var sut = new MemoryBudget(100, 50, 0);
        sut.take(80);
        var taken = new ArrayList<Boolean>();

        // Act: a pool thread takes 50 bytes whatever the budget has left, for what cannot go
        // without them, gives them back to the shared count, and then asks for some of them.
        onPoolThread(() -> {
            sut.takeAnyway(50);
            sut.giveShared(50);
            taken.add(sut.take(21));
            taken.add(sut.take(20));
        });

        // Assert: the thread kept none of the 50 bytes, and took only what the limit allows.
        assertThat(taken).containsExactly(false, true);
        assertThat(sut.lent()).isEqualTo(100);
    }

    @Test
    void what_is_missing_for_room_is_what_has_to_come_back_first() {
        // Arrange: 40 of 100 bytes are left.
        var sut = new MemoryBudget(100, 0, 0);
        sut.take(60);

        // Act
        long fits = sut.shortOf(40);
        long tooMuch = sut.shortOf(55);

        // Assert: nothing is missing for the first, and 15 bytes for the second.
        assertThat(fits).isNotPositive();
        assertThat(tooMuch).isEqualTo(15);
    }

    @Test
    void what_a_pool_thread_keeps_is_not_missing_for_it() throws Exception {
        // Arrange: a thread that keeps 90 bytes of the unit that it fetched, which leaves the
        // other threads 50.
        var sut = new MemoryBudget(150, 100, 0);
        var missing = new ArrayList<Long>();

        // Act
        onPoolThread(() -> {
            sut.take(10);
            missing.add(sut.shortOf(140));
            missing.add(sut.shortOf(150));
        });

        // Assert: the thread counts what it keeps, and another thread only what is left.
        assertThat(missing).containsExactly(0L, 10L);
        assertThat(sut.shortOf(60)).isEqualTo(10);
    }

    @Test
    void a_publication_tells_whether_a_holder_would_take_or_give_back_its_room() {
        // Arrange
        var budget = new MemoryBudget(100, 0, 0);
        var sut = new PublicationMemory(60);

        // Act: no holder, one, two, and one again. Each time: whether the publication has its
        // room, and whether a holder that let go would give it back.
        List<Boolean> without = List.of(sut.held(), sut.heldOnce());
        boolean first = sut.hold(budget);
        List<Boolean> withOne = List.of(sut.held(), sut.heldOnce());
        boolean second = sut.hold(budget);
        List<Boolean> withTwo = List.of(sut.held(), sut.heldOnce());
        sut.release(budget);
        List<Boolean> withOneAgain = List.of(sut.held(), sut.heldOnce());

        // Assert: the first holder takes the room, and only the last one gives it back.
        assertThat(List.of(first, second)).containsOnly(true);
        assertThat(without).containsExactly(false, false);
        assertThat(withOne).containsExactly(true, true);
        assertThat(withTwo).containsExactly(true, false);
        assertThat(withOneAgain).containsExactly(true, true);
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
    void a_publication_counts_once_for_its_holders_within_the_limit_and_once_for_the_others() {
        // Arrange
        var budget = new MemoryBudget(100, 0, 0);
        var sut = new PublicationMemory(60);

        // Act: a holder within the limit comes, then two that take the room whatever the
        // budget has left, and they go in the same order.
        boolean within = sut.hold(budget);
        sut.holdAnyway(budget);
        sut.holdAnyway(budget);
        long heldByAll = budget.lent();
        sut.releaseAnyway(budget);
        long heldByTwo = budget.lent();
        sut.releaseAnyway(budget);
        long heldWithin = budget.lent();
        sut.release(budget);

        // Assert
        assertThat(within).isTrue();
        assertThat(List.of(heldByAll, heldByTwo, heldWithin, budget.lent()))
                .containsExactly(120L, 120L, 60L, 0L);
    }

    @Test
    void a_holder_within_the_limit_finds_no_room_in_what_another_took_past_it() {
        // Arrange: a budget that has handed out 50 of its 100 bytes, and a publication of 60
        // bytes that a holder holds with room taken whatever the budget had left.
        var budget = new MemoryBudget(100, 0, 0);
        var sut = new PublicationMemory(60);
        budget.take(50);
        sut.holdAnyway(budget);

        // Act
        boolean held = sut.hold(budget);

        // Assert: the holder would need room of its own, which the limit does not allow.
        assertThat(held).isFalse();
        assertThat(budget.lent()).isEqualTo(110);
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

    @Test
    void a_holder_joins_a_publication_that_has_its_room_and_takes_none() {
        // Arrange: a publication whose one holder took all the room that there is.
        var budget = new MemoryBudget(60, 0, 0);
        var sut = new PublicationMemory(60);
        boolean first = sut.hold(budget);

        // Act
        boolean joined = sut.holdIfHeld();
        sut.release(budget);
        long heldByOne = budget.lent();
        sut.release(budget);

        // Assert: the room stays until both have let go.
        assertThat(List.of(first, joined)).containsExactly(true, true);
        assertThat(List.of(heldByOne, budget.lent())).containsExactly(60L, 0L);
    }

    @Test
    void nothing_joins_a_publication_that_has_no_holder() {
        // Arrange
        var sut = new PublicationMemory(60);

        // Act
        boolean joined = sut.holdIfHeld();

        // Assert
        assertThat(joined).isFalse();
        assertThat(sut.held()).isFalse();
    }

    @Test
    void the_last_holder_of_a_publication_lets_go_and_keeps_the_room() {
        // Arrange
        var budget = new MemoryBudget(100, 0, 0);
        var sut = new PublicationMemory(60);
        boolean held = sut.hold(budget);

        // Act
        boolean letGo = sut.letGoAsLast();

        // Assert: the publication has no holder, and its room has not gone back.
        assertThat(List.of(held, letGo)).containsExactly(true, true);
        assertThat(sut.held()).isFalse();
        assertThat(budget.lent()).isEqualTo(60);
    }

    @Test
    void a_holder_among_others_does_not_let_go_as_the_last() {
        // Arrange
        var budget = new MemoryBudget(100, 0, 0);
        var sut = new PublicationMemory(60);
        boolean first = sut.hold(budget);
        boolean second = sut.hold(budget);

        // Act
        boolean letGo = sut.letGoAsLast();

        // Assert: both still hold it, so that one of them is the last once the other goes.
        assertThat(List.of(first, second, letGo)).containsExactly(true, true, false);
        assertThat(sut.heldOnce()).isFalse();
        sut.release(budget);
        assertThat(sut.heldOnce()).isTrue();
    }

    @Test
    void a_holder_that_comes_back_with_the_room_leaves_it_with_its_publication() {
        // Arrange: a holder that let go as the last one and kept the room.
        var budget = new MemoryBudget(60, 0, 0);
        var sut = new PublicationMemory(60);
        boolean held = sut.hold(budget);
        boolean letGo = sut.letGoAsLast();

        // Act
        sut.holdWithRoom(budget);

        // Assert: the publication has its room again, and gives it back with its holder.
        assertThat(List.of(held, letGo)).containsExactly(true, true);
        assertThat(sut.heldOnce()).isTrue();
        assertThat(budget.lent()).isEqualTo(60);
        sut.release(budget);
        assertThat(budget.lent()).isZero();
    }

    @Test
    void a_holder_that_comes_back_with_the_room_gives_it_up_if_another_holder_came() {
        // Arrange: a holder that let go as the last one and kept the room, and another that
        // then took room of its own for the publication.
        var budget = new MemoryBudget(120, 0, 0);
        var sut = new PublicationMemory(60);
        boolean held = sut.hold(budget);
        boolean letGo = sut.letGoAsLast();
        boolean other = sut.hold(budget);
        long withBoth = budget.lent();

        // Act
        sut.holdWithRoom(budget);

        // Assert: the publication counts once again, for its two holders.
        assertThat(List.of(held, letGo, other)).containsExactly(true, true, true);
        assertThat(List.of(withBoth, budget.lent())).containsExactly(120L, 60L);
        assertThat(sut.heldOnce()).isFalse();
    }

    /// Whether another thread, outside the actor pool, takes `bytes` of room from `budget`.
    private static boolean takenElsewhere(MemoryBudget budget, long bytes) {
        var taken = new ArrayList<Boolean>();
        Thread other = Thread.ofPlatform().start(() -> taken.add(budget.take(bytes)));
        try {
            other.join();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
        return taken.getFirst();
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
