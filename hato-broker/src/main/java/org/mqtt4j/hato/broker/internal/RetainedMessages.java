package org.mqtt4j.hato.broker.internal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.mqtt4j.hato.broker.internal.common.log.Log;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.Topic;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

// The retained message of each topic, in the order they were stored, within the retained limits.
// The monitor of this object is the topic lock, which every caller holds. A retained publication
// is stored and delivered under it, and a new subscription takes the retained messages that it
// matches and starts to receive deliveries under it, so the subscription receives the
// publication once, either as a retained message or as a delivery, and its later deliveries
// follow its retained messages.
//
// A new subscription scans them all, which the retained limits keep short; an index by topic
// level would replace the scan if productization needs it.
final class RetainedMessages {

    private static final Log log = Log.of(RetainedMessages.class);

    private final BrokerSettings settings;

    // The port that the broker listens on, for logs.
    private final int port;

    private final Map<Topic, Message> messages = new LinkedHashMap<>();

    private long bytes;

    // Whether the retained limits have refused a message yet; only the first refusal warns.
    private boolean limitReached;

    RetainedMessages(BrokerSettings settings, int port) {
        this.settings = settings;
        this.port = port;
    }

    // Makes `publication` the retained message of its topic in place of the previous one
    // [MQTT-3.3.1-5]. An empty payload only removes the previous one [MQTT-3.3.1-6]
    // [MQTT-3.3.1-7]. A message beyond the retained limits is not kept either, which leaves its
    // topic without a retained message rather than with an outdated one.
    void store(Packet.Publish publication, long received) {
        Message previous = messages.remove(publication.topic());
        if (previous != null) {
            bytes -= previous.byteCount();
        }
        if (publication.payload().length == 0) {
            return;
        }
        // A retained message counts as the MQTT 5.0 PUBLISH that carries it with its properties.
        byte[] header = PacketCodec.of(MqttVersion.V5).encodePublishHeader(
                publication.topic(),
                publication.qos(),
                true,
                false,
                (publication.qos() == Qos.AT_MOST_ONCE) ? 0 : 1,
                publication.properties(),
                publication.payload().length
        );
        var message = new Message(
                publication,
                header.length + publication.payload().length,
                received
        );
        if (messages.size() >= settings.maxRetained()
                || message.byteCount() > settings.maxRetainedBytes() - bytes) {
            if (!limitReached) {
                limitReached = true;
                log.warn(
                        "Broker on port {} reached its retained message limits and keeps no"
                                + " retained message for topic {}; later refusals log at DEBUG",
                        port,
                        publication.topic().name()
                );
            } else {
                log.debug(
                        "Broker on port {} keeps no retained message for topic {}: limits reached",
                        port,
                        publication.topic().name()
                );
            }
            return;
        }
        messages.put(publication.topic(), message);
        bytes += message.byteCount();
    }

    // The retained messages whose topics `filter` matches, in the order they were stored, after
    // discarding those whose Message Expiry Interval has passed (MQTT 5.0 section 3.3.1.3). They
    // are looked for here rather than with a timer each, so an expired one keeps its share of the
    // retained limits until a subscription looks; a timer would release it at once, if
    // productization needs that.
    List<Message> matching(TopicFilter filter) {
        long now = settings.scheduler().nanoTime();
        messages.values().removeIf(message -> {
            if (!message.expired(now)) {
                return false;
            }
            bytes -= message.byteCount();
            return true;
        });
        var matched = new ArrayList<Message>();
        for (Message message : messages.values()) {
            if (filter.matches(message.publication().topic())) {
                matched.add(message);
            }
        }
        return matched;
    }

    // Forgets every retained message, as the broker closes.
    void clear() {
        messages.clear();
        bytes = 0;
    }

    // A retained message, its share of the retained size limit, and when the broker received it.
    record Message(Packet.Publish publication, long byteCount, long received) {

        boolean expired(long now) {
            return Delivery.expired(Delivery.lifetimeOf(publication), received, now);
        }
    }
}
