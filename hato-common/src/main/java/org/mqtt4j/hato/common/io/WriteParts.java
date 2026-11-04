package org.mqtt4j.hato.common.io;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/// The parts of one gathering write, for [FrameWriter#writeParts(ByteBuffer[])], made from the
/// arrays that the write carries one after another, such as complete packets and the headers and
/// payloads of PUBLISH packets. An array of at least [#JOIN_BYTES] is a part of its own, which
/// leaves from where it lies, so that it is not copied on its way out. The smaller arrays between
/// such ones are joined into one part: copying a small array costs less than another buffer in
/// the gathering write. Message properties can make a PUBLISH header as large as any payload, so
/// only the size of an array decides.
public final class WriteParts {

    /// The size from which an array is a part of its own.
    public static final int JOIN_BYTES = 4 * 1024;

    private final List<ByteBuffer> parts = new ArrayList<>();

    // The arrays under JOIN_BYTES added since the last part, which join into the next one.
    private final List<byte[]> run = new ArrayList<>();

    /// Adds `array`, which the write carries after the arrays added before. Do not modify it
    /// until the write is over.
    public void add(byte[] array) {
        if (array.length < JOIN_BYTES) {
            run.add(array);
            return;
        }
        endRun();
        parts.add(ByteBuffer.wrap(array));
    }

    /// The parts of the arrays added so far, in order.
    public ByteBuffer[] toArray() {
        endRun();
        return parts.toArray(ByteBuffer[]::new);
    }

    private void endRun() {
        if (run.isEmpty()) {
            return;
        }
        parts.add(ByteBuffer.wrap(joined(run)));
        run.clear();
    }

    // `arrays` one after another in one array, or the only one itself.
    private static byte[] joined(List<byte[]> arrays) {
        if (arrays.size() == 1) {
            return arrays.getFirst();
        }
        int length = 0;
        for (byte[] array : arrays) {
            length += array.length;
        }
        var joined = new byte[length];
        int at = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, joined, at, array.length);
            at += array.length;
        }
        return joined;
    }
}
