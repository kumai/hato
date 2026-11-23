package org.mqtt4j.hato.broker.internal;

import java.util.Arrays;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.SharedTopicFilter;

// One shared subscription of the broker, `$share/{group}/{filter}`, with the sessions that
// subscribe to it: its members. It hands each publication that its filter selects to one member,
// which takes it as a delivery of its own, independently of every other subscription (MQTT 5.0
// section 4.8.2). It belongs to the broker rather than to a session: sessions join it with a
// SUBSCRIBE and leave it with an UNSUBSCRIBE or as they end, and it ends as its last member
// leaves, never to take a member again. A later SUBSCRIBE to the same name makes a new one.
//
// The routing's index holds it under its filter, and the index's write monitor orders the
// changes of its members (see Routing.joined and Routing.left), while publishers read the members
// without a lock. Its deliveries hold it too, to hand themselves to another member, and may
// outlive it: so it keeps no more than its name of what the client wrote, which they count (see
// bytes).
final class SharedSubscription implements Subscriber {

    private static final String SHARE_PREFIX = "$share/";

    // How many rounds one call of deliver() makes at most, a round being one look through the
    // members as they are. A member that leaves as another joins takes a second round, and a few
    // such changes at once a few more. A publication that finds the members changed after each of
    // them waits for a later turn of the actor that hands it over (see Session.share), so that
    // sessions which keep changing the members cannot hold that actor's turn for as long as they
    // do.
    static final int ROUNDS = 4;

    // The subscription as clients write it, which names it in the broker.
    final String name;

    // What each of its deliveries counts for it in the limits that bound memory: itself, a small
    // object with two references more, the array of its members, which is empty once it has
    // ended, and its name. Once it has ended, no subscription of a session counts its name any
    // more, while deliveries that wait still hold it.
    final long bytes;

    // What a test runs with the name before the last step of each round of deliver().
    private final Consumer<String> beforeLastStep;

    // The members, replaced as a whole under the index's write monitor. Empty until the first
    // member joins, which it does before the index holds the shared subscription, and once the
    // shared subscription has ended.
    private volatile Session[] members = new Session[0];

    // Where the next look for a member starts: after the member that took the last publication,
    // so that members which take none, as those without a connection, do not tilt the turns
    // towards the member after them. Publishers on different actors write it without a lock;
    // one that overwrites another's only moves the start, which spreads the publications a
    // little less evenly.
    private volatile int next;

    // The shared subscription `name`, which has yet to take its first member.
    SharedSubscription(String name, Consumer<String> beforeLastStep) {
        this.name = name;
        this.beforeLastStep = beforeLastStep;
        bytes = PublicationMemory.OBJECT_BYTES
                + 2 * PublicationMemory.REFERENCE_BYTES
                + PublicationMemory.ARRAY_BYTES
                + PublicationMemory.sizeOf(name);
    }

    // Its group, the share name, which its name holds between `$share/` and the next '/'.
    String group() {
        return name.substring(SHARE_PREFIX.length(), name.indexOf('/', SHARE_PREFIX.length()));
    }

    // The filter after its group, which selects the publications, as its name holds it.
    TopicFilter filter() {
        return SharedTopicFilter.parse(new TopicFilter(name)).orElseThrow().filter();
    }

    // Takes `session` in as a member, unless it is one already: a session that subscribes again
    // stays one member (MQTT 5.0 section 4.8.2). Under the index's write monitor.
    // Ceiling: each member that joins or leaves copies the members, which takes time in their
    // number; a shared subscription is meant for a few members, and hundreds would want them in
    // a structure that changes in place.
    void join(Session session) {
        Session[] current = members;
        for (Session member : current) {
            if (member == session) {
                return;
            }
        }
        Session[] joined = Arrays.copyOf(current, current.length + 1);
        joined[current.length] = session;
        members = joined;
    }

    // Lets `session` go as a member, and returns whether it was the last one, which ends the
    // shared subscription. Under the index's write monitor.
    boolean leave(Session session) {
        Session[] current = members;
        for (int at = 0; at < current.length; at++) {
            if (current[at] == session) {
                var left = new Session[current.length - 1];
                System.arraycopy(current, 0, left, 0, at);
                System.arraycopy(current, at + 1, left, at, left.length - at);
                members = left;
                return left.length == 0;
            }
        }
        return false;
    }

    // Hands `publication`, which the broker received at `received` by its clock, to one member
    // that takes it, and returns what became of it. A member takes it once the delivery waits in
    // its queues, also for a connection to come, and declines it before that, as it does when it
    // has left the shared subscription meanwhile (see Session.deliverShared). The members are
    // asked in turn from after the one that took the last publication, in this order:
    //
    // 1. A member with a connection that has not ended and nothing waiting, which has sent
    //    everything so far (see connected). That nothing waits is an observation that a moment
    //    may change, and no promise of room in the member's window, which the last delivery
    //    may have filled: it serves as the sign of a member that keeps up.
    // 2. The member with a connection that has the fewest deliveries waiting, so that a member
    //    which falls behind takes no more while others have fewer, rather than the next one in
    //    turn.
    // 3. For QoS 1 and QoS 2, every member not asked yet, whatever its connection: one without
    //    a connection whose session outlives it takes the publication for its next connection,
    //    and one that has connected since the steps before looked takes it as well.
    //
    // The members are read once for these steps, while sessions may join and leave meanwhile.
    // If all that were asked declined and the members changed, as when one joined and then the
    // one asked left, the members as they are now are asked in the same way, so that the
    // publication reaches a member that joined while another was still there. A later shared
    // subscription of the same name never gets it. A call makes ROUNDS such rounds at most: if
    // the members have changed again after the last of them, it returns LATER, and the caller
    // offers the publication again in a later turn, rather than looking on while the members
    // keep changing.
    //
    // A publication that no member takes, as at QoS 0 while no member has a connection, goes to
    // none, as for an ordinary subscription whose session has no connection. Called by the actor
    // that hands the publication over, as a publisher's actor calls Session.deliverMatching.
    Outcome deliver(Packet.Publish publication, PublicationMemory room, long received) {
        Session[] current = members;
        for (int round = 1; current.length > 0; round++) {
            if (deliverAmong(current, publication, room, received)) {
                return Outcome.TAKEN;
            }
            Session[] now = members;
            if (now == current) {
                return Outcome.NONE;
            }
            if (round == ROUNDS && now.length > 0) {
                return Outcome.LATER;
            }
            current = now;
        }
        return Outcome.NONE;
    }

    // Asks `current`, the members as read once, for one that takes `publication`, in the steps
    // that deliver() describes, and returns whether one took it.
    // Ceiling: while every member declines, as when none can take a packet that large, the
    // second step looks the members through for each one that it asks, which takes time in the
    // square of their number; a shared subscription is meant for a few members.
    private boolean deliverAmong(
            Session[] current,
            Packet.Publish publication,
            PublicationMemory room,
            long received
    ) {
        int count = current.length;
        int start = Math.floorMod(next, count);
        // The members that declined the publication, by their place; made at the first decline.
        boolean[] declined = null;
        for (int step = 0; step < count; step++) {
            int place = (start + step) % count;
            Session member = current[place];
            if (!connected(member) || member.queued() != 0) {
                continue;
            }
            if (member.deliverShared(publication, room, received, this)) {
                next = place + 1;
                return true;
            }
            declined = declinedBy(declined, count, place);
        }
        while (true) {
            int fewest = -1;
            int fewestWaiting = Integer.MAX_VALUE;
            for (int step = 0; step < count; step++) {
                int place = (start + step) % count;
                Session member = current[place];
                if ((declined != null && declined[place]) || !connected(member)) {
                    continue;
                }
                int waiting = member.queued();
                if (waiting < fewestWaiting) {
                    fewest = place;
                    fewestWaiting = waiting;
                }
            }
            if (fewest < 0) {
                break;
            }
            if (current[fewest].deliverShared(publication, room, received, this)) {
                next = fewest + 1;
                return true;
            }
            declined = declinedBy(declined, count, fewest);
        }
        if (publication.qos() == Qos.AT_MOST_ONCE) {
            return false;
        }
        beforeLastStep.accept(name);
        for (int step = 0; step < count; step++) {
            int place = (start + step) % count;
            if (declined != null && declined[place]) {
                continue;
            }
            if (current[place].deliverShared(publication, room, received, this)) {
                next = place + 1;
                return true;
            }
        }
        return false;
    }

    // Whether `member` has a connection that can still send, as its session judges as it takes a
    // delivery (see Session.admit): one that has ended, whose end has yet to reach the session,
    // counts as none, so that a publication waits there only in the last step.
    private static boolean connected(Session member) {
        Connection peer = member.connection;
        return peer != null && !peer.isEnded();
    }

    // `declined`, made for `count` members if it is null, with the member at `place` marked.
    private static boolean[] declinedBy(boolean @Nullable [] declined, int count, int place) {
        boolean[] marked = (declined == null) ? new boolean[count] : declined;
        marked[place] = true;
        return marked;
    }

    // What became of a publication that deliver() offered to the members.
    enum Outcome {

        // A member took it.
        TAKEN,

        // No member took it, and it goes to none.
        NONE,

        // No member took it while the members kept changing: it is to be offered again in a
        // later turn.
        LATER
    }
}
