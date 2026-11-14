package org.mqtt4j.hato.broker.internal;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;

// What one publication holds on the heap, and its room in the broker's memory for deliveries:
// that size, once, while at least one of its deliveries has room there. The deliveries of a
// publication to many sessions share it, so counting it for each of them would refuse
// deliveries for memory that nobody uses. The sessions that hold them run on different
// threads, so the number of holders is the one count here that they share.
//
// The size is an estimate of the decoded publication, not the bytes of the packet that brought
// it. A property is an object of its own, with objects for its strings, so a packet of many
// small properties holds many times its bytes: a User Property of two strings of one character
// takes 7 bytes in the packet and about 120 on the heap. The limits that bound memory count
// this size, and the bytes of a packet serve only where the protocol or a socket measures
// packets. The estimate takes the larger of the object layouts that a JVM uses, with
// references of 8 bytes, and two bytes for each character of a string, so that it errs
// towards counting more than is held.
final class PublicationMemory {

    static final int REFERENCE_BYTES = 8;

    // A small object: its header and up to two references, or a number.
    static final int OBJECT_BYTES = 32;

    // An array, before its elements.
    static final int ARRAY_BYTES = 24;

    // A string and its array, before its characters.
    private static final int STRING_BYTES = OBJECT_BYTES + ARRAY_BYTES;

    // The publication and its topic, before the topic's name.
    private static final int PUBLICATION_BYTES = 80;

    // A sequence of properties and its list, before the references to the properties.
    private static final int LIST_BYTES = 80;

    private final long bytes;

    private final AtomicInteger holders = new AtomicInteger();

    PublicationMemory(Packet.Publish publication) {
        this(sizeOf(publication));
    }

    // For a publication that holds `bytes`.
    PublicationMemory(long bytes) {
        this.bytes = bytes;
    }

    // What `publication` holds on the heap: itself, its topic, its payload, and its properties
    // with their values.
    static long sizeOf(Packet.Publish publication) {
        long size = PUBLICATION_BYTES
                + sizeOf(publication.topic().name())
                + ARRAY_BYTES
                + publication.payload().length;
        List<Property> entries = publication.properties().entries();
        // A publication without properties, as most are, shares the empty sequence.
        if (entries.isEmpty()) {
            return size;
        }
        size += LIST_BYTES;
        for (int i = 0; i < entries.size(); i++) {
            size += REFERENCE_BYTES + sizeOf(entries.get(i));
        }
        return size;
    }

    private static long sizeOf(Property property) {
        return switch (property) {
            case Property.ByteValue _,
                 Property.TwoByteValue _,
                 Property.FourByteValue _,
                 Property.VariableByteValue _ -> OBJECT_BYTES;
            case Property.BinaryValue binary ->
                    OBJECT_BYTES + ARRAY_BYTES + binary.value().length;
            case Property.StringValue string -> OBJECT_BYTES + sizeOf(string.value());
            case Property.UserProperty pair ->
                    OBJECT_BYTES + sizeOf(pair.name()) + sizeOf(pair.value());
        };
    }

    // What a string holds on the heap, with two bytes for each of its characters.
    static long sizeOf(String text) {
        return STRING_BYTES + 2L * text.length();
    }

    // What a sequence of `count` properties holds beside the properties that it shares with
    // another sequence: its list, and `added` properties of its own without strings.
    static long sequenceSize(int count, int added) {
        return LIST_BYTES + (long) REFERENCE_BYTES * count + (long) OBJECT_BYTES * added;
    }

    // What the publication holds on the heap, as the limits that bound memory count it.
    long bytes() {
        return bytes;
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

    // Whether the publication has its room, so that another holder would take none for it.
    // Other threads add and remove holders meanwhile, so the answer is of this moment.
    boolean held() {
        return holders.get() > 0;
    }

    // Whether the publication has one holder, which then gives its room back as it lets go:
    // with more holders, the room stays taken until the last of them has. The answer is of
    // this moment as well.
    boolean heldOnce() {
        return holders.get() == 1;
    }
}
