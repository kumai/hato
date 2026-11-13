package org.mqtt4j.hato.broker.internal;

import java.util.concurrent.atomic.AtomicInteger;

// The room that one publication takes in the broker's memory for deliveries: its size, once,
// while at least one of its deliveries has room there. The deliveries of a publication to many
// sessions share its payload, so counting it for each of them would refuse deliveries for
// memory that nobody uses. The sessions that hold them run on different threads, so the
// number of holders is the one count here that they share.
final class PublicationMemory {

    private final int bytes;

    private final AtomicInteger holders = new AtomicInteger();

    // `bytes` is what the publication takes: about the size of the packet that brought it.
    PublicationMemory(int bytes) {
        this.bytes = bytes;
    }

    // Adds a holder, which takes the publication's room from `budget` if it is the only one.
    // Returns false, without adding it, when the budget has no room for the publication.
    boolean hold(MemoryBudget budget) {
        return hold(budget, false);
    }

    // Adds a holder as hold() does, but takes the publication's room whatever the budget has
    // left.
    void holdAnyway(MemoryBudget budget) {
        boolean _ = hold(budget, true);
    }

    private boolean hold(MemoryBudget budget, boolean anyway) {
        while (true) {
            int held = holders.get();
            if (held > 0) {
                if (holders.compareAndSet(held, held + 1)) {
                    return true;
                }
                continue;
            }
            // The room comes first, so that no other holder ever counts on room that this one
            // then fails to get.
            if (anyway) {
                budget.takeAnyway(bytes);
            } else if (!budget.take(bytes)) {
                return false;
            }
            if (holders.compareAndSet(0, 1)) {
                return true;
            }
            budget.give(bytes);
        }
    }

    // Removes a holder, which gives the publication's room back to `budget` if it was the last.
    void release(MemoryBudget budget) {
        if (holders.decrementAndGet() == 0) {
            budget.give(bytes);
        }
    }
}
