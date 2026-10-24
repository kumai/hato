package org.mqtt4j.hato.broker.internal;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;

record Release(int packetId) implements Outgoing {

    @Override
    public Packet.PubRel packet() {
        return new Packet.PubRel(packetId, ReasonCode.SUCCESS, Properties.EMPTY);
    }
}
