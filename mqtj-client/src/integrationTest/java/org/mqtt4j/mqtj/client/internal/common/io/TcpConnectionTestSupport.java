package org.mqtt4j.mqtj.client.internal.common.io;

import java.io.IOException;
import java.net.Socket;

/// Creates connections around test-controlled sockets.
public final class TcpConnectionTestSupport {

    private TcpConnectionTestSupport() { }

    public static TcpConnection wrap(Socket socket) throws IOException {
        return new TcpConnection(socket);
    }
}
