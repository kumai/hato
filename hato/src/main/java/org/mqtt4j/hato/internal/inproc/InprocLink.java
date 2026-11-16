package org.mqtt4j.hato.internal.inproc;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.internal.protocol.Packet;

/// One in-process connection between a client and a broker of the same JVM: a queue of packets
/// each way, bounded as the buffers of a socket are, and the ends of both sides.
///
/// The two sides share no lock: the broker's threads are platform threads, and a client's may be
/// virtual, which must never hold up a broker's thread that waits for a lock. Each queue has one
/// thread that offers and one that takes at a time. The client's threads wait for packets or for
/// room by parking, and the broker's side unparks them; the broker's side never waits, and the
/// client's side wakes it with the action that the broker gave as it accepted the link.
public final class InprocLink {

    /// A packet and the bytes that it takes on the wire, which every limit counts.
    public record Item(Packet packet, int size) {}

    // A queue holds at most this many packets, or this many bytes of them, but always takes one
    // packet when it holds none, as the buffers of a socket bound what waits in them.
    private static final int MAX_ITEMS = 128;

    private static final long MAX_BYTES = 64 * 1024;

    private final ConcurrentLinkedQueue<Item> toBroker = new ConcurrentLinkedQueue<>();

    private final AtomicInteger toBrokerCount = new AtomicInteger();

    private final AtomicLong toBrokerBytes = new AtomicLong();

    private final ConcurrentLinkedQueue<Item> toClient = new ConcurrentLinkedQueue<>();

    private final AtomicInteger toClientCount = new AtomicInteger();

    private final AtomicLong toClientBytes = new AtomicLong();

    // How a client's thread wakes the broker's side, from the broker's acceptance on.
    private volatile @Nullable Runnable brokerWake;

    // The broker's answer: 0 while none has come, 1 once it accepted the link, -1 once it
    // refused it.
    private volatile int answer;

    private volatile boolean clientEnded;

    private volatile boolean brokerEnded;

    // Whether the broker's side waits for room in the queue to the client.
    private volatile boolean brokerWantsRoom;

    // The client's thread that waits for the broker's answer or for packets, and the one that
    // waits for room in the queue to the broker, while they wait.
    private volatile @Nullable Thread clientReader;

    private volatile @Nullable Thread clientSender;

    // ---- The client's side.

    /// Waits until the broker answers, at most `nanos` nanoseconds, and returns whether it
    /// accepted the link; false also when the wait ran out or the link has ended.
    public boolean awaitAnswer(long nanos) {
        long deadline = System.nanoTime() + nanos;
        clientReader = Thread.currentThread();
        try {
            while (answer == 0 && !clientEnded) {
                long left = deadline - System.nanoTime();
                if (left <= 0) {
                    return false;
                }
                LockSupport.parkNanos(this, left);
            }
            return answer > 0 && !clientEnded;
        } finally {
            clientReader = null;
        }
    }

    /// Offers `item` to the broker, and wakes the broker's side. Returns false while the queue to
    /// the broker has no room for it; [#awaitRoom(int)] then waits for room.
    public boolean send(Item item) {
        if (!offer(toBroker, toBrokerCount, toBrokerBytes, item)) {
            return false;
        }
        wakeBroker();
        return true;
    }

    /// Waits until the queue to the broker has room for a packet of `size` bytes, or either side
    /// has ended. Called by the client's thread that sends.
    public void awaitRoom(int size) {
        clientSender = Thread.currentThread();
        try {
            while (!room(toBrokerCount, toBrokerBytes, size) && !clientEnded && !brokerEnded) {
                LockSupport.park(this);
            }
        } finally {
            clientSender = null;
        }
    }

    /// The next packet from the broker, or null while none waits. Taking one makes room, which
    /// wakes the broker's side if it waits for it.
    public @Nullable Item receive() {
        Item item = take(toClient, toClientCount, toClientBytes);
        if (item != null && brokerWantsRoom) {
            brokerWantsRoom = false;
            wakeBroker();
        }
        return item;
    }

    /// Waits until a packet from the broker waits, either side has ended, or another thread
    /// unparks the calling one, which reads for the client.
    public void awaitPackets() {
        clientReader = Thread.currentThread();
        try {
            // An end of the client's side before the reader was known to it woke nobody, so the
            // reader looks for it after making itself known.
            if (toClient.isEmpty() && !brokerEnded && !clientEnded) {
                LockSupport.park(this);
            }
        } finally {
            clientReader = null;
        }
    }

    /// Whether the broker's side has ended: no packet comes after those that wait.
    public boolean brokerEnded() {
        return brokerEnded;
    }

    /// Ends the client's side: the broker reads what waits and then the end of the input. Wakes
    /// the broker's side, and the client's threads that wait on the link.
    public void endClient() {
        clientEnded = true;
        wakeBroker();
        unpark(clientReader);
        unpark(clientSender);
    }

    // ---- The broker's side.

    /// Accepts the link: from now on `wake` has the broker look at it, from any thread and
    /// without waiting.
    public void accept(Runnable wake) {
        brokerWake = wake;
        answer = 1;
        unpark(clientReader);
    }

    /// Refuses the link, which the client takes as a connection that could not be made.
    public void refuse() {
        answer = -1;
        brokerEnded = true;
        unpark(clientReader);
    }

    /// The next packet from the client, or null while none waits. Taking one makes room, which
    /// wakes the client's thread that waits for it.
    public @Nullable Item take() {
        Item item = take(toBroker, toBrokerCount, toBrokerBytes);
        if (item != null) {
            unpark(clientSender);
        }
        return item;
    }

    /// Whether a packet from the client waits.
    public boolean input() {
        return !toBroker.isEmpty();
    }

    /// Whether the client's side has ended: no packet comes after those that wait.
    public boolean clientEnded() {
        return clientEnded;
    }

    /// Offers `item` to the client and wakes the client's thread that reads. Returns false while
    /// the queue to the client has no room for it; the broker's side is then woken as the client
    /// takes a packet.
    public boolean deliver(Item item) {
        if (offer(toClient, toClientCount, toClientBytes, item)) {
            unpark(clientReader);
            return true;
        }
        brokerWantsRoom = true;
        // The client may have taken a packet before it could see that the broker waits.
        if (offer(toClient, toClientCount, toClientBytes, item)) {
            unpark(clientReader);
            return true;
        }
        return false;
    }

    /// Ends the broker's side: the client reads what waits and then the end of the input. Wakes
    /// the client's threads that wait on the link.
    public void endBroker() {
        brokerEnded = true;
        unpark(clientReader);
        unpark(clientSender);
    }

    // ---- Shared.

    private void wakeBroker() {
        Runnable wake = brokerWake;
        if (wake != null) {
            wake.run();
        }
    }

    private static boolean offer(
            ConcurrentLinkedQueue<Item> queue,
            AtomicInteger count,
            AtomicLong bytes,
            Item item
    ) {
        // Only one thread offers to a queue at a time, so what it found stays room until it adds.
        if (!room(count, bytes, item.size())) {
            return false;
        }
        count.incrementAndGet();
        bytes.addAndGet(item.size());
        queue.add(item);
        return true;
    }

    private static boolean room(AtomicInteger count, AtomicLong bytes, int size) {
        int waiting = count.get();
        return waiting == 0 || (waiting < MAX_ITEMS && bytes.get() + size <= MAX_BYTES);
    }

    private static @Nullable Item take(
            ConcurrentLinkedQueue<Item> queue,
            AtomicInteger count,
            AtomicLong bytes
    ) {
        Item item = queue.poll();
        if (item != null) {
            bytes.addAndGet(-item.size());
            count.decrementAndGet();
        }
        return item;
    }

    private static void unpark(@Nullable Thread thread) {
        if (thread != null) {
            LockSupport.unpark(thread);
        }
    }
}
