package org.mqtt4j.hato.broker.internal;

import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.Properties;

record Release(int packetId) implements Outgoing {

    Packet.PubRel packet() {
        return new Packet.PubRel(packetId, ReasonCode.SUCCESS, Properties.EMPTY);
    }
}
