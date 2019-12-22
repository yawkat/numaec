package at.yawk.numaec;

import java.io.Closeable;

public interface BufferBasedCollection extends Closeable {
    /**
     * Close the buffers associated with this collection. This collection may be in an invalid state after this
     * operation.
     */
    @Override
    void close();
}
