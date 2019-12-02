package at.yawk.numaec;

import java.io.Closeable;
import java.nio.ReadOnlyBufferException;

/**
 * A long-indexed byte buffer.
 *
 * Accessors may require positions to be aligned to their data type size. Endianness is not specified.
 */
public interface LargeByteBuffer extends Closeable {
    byte getByte(long position) throws IndexOutOfBoundsException;

    short getShort(long position) throws IndexOutOfBoundsException;

    int getInt(long position) throws IndexOutOfBoundsException;

    long getLong(long position) throws IndexOutOfBoundsException;

    default char getChar(long position) throws IndexOutOfBoundsException {
        return (char) getShort(position);
    }

    default float getFloat(long position) throws IndexOutOfBoundsException {
        return Float.intBitsToFloat(getInt(position));
    }

    default double getDouble(long position) throws IndexOutOfBoundsException {
        return Double.longBitsToDouble(getLong(position));
    }

    void setByte(long position, byte value) throws IndexOutOfBoundsException, ReadOnlyBufferException;

    void setShort(long position, short value) throws IndexOutOfBoundsException, ReadOnlyBufferException;

    void setInt(long position, int value) throws IndexOutOfBoundsException, ReadOnlyBufferException;

    void setLong(long position, long value) throws IndexOutOfBoundsException, ReadOnlyBufferException;

    default void setChar(long position, char value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        setShort(position, (short) value);
    }

    default void setFloat(long position, float value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        setInt(position, Float.floatToRawIntBits(value));
    }

    default void setDouble(long position, double value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        setLong(position, Double.doubleToRawLongBits(value));
    }

    /**
     * Total size of this buffer in bytes.
     */
    long size();

    /**
     * Copy `length` bytes from `from`'s `fromIndex` to this buffer's `toIndex`.
     *
     * Implementations only have to support this for buffers created from the same {@link LargeByteBufferAllocator},
     * but may optionally support this for other buffers as well.
     *
     * The from and to buffers may be the same buffer.
     */
    void copyFrom(LargeByteBuffer from, long fromIndex, long toIndex, long length)
            throws ReadOnlyBufferException, UnsupportedOperationException, IndexOutOfBoundsException;

    /**
     * Unmap this buffer. Queries to this buffer <i>may</i> fail after this call. Optional operation.
     */
    @Override
    default void close() {
    }

    /**
     * Attempt to allocate a new buffer that contains this buffer's data. If a new buffer is created, the old buffer
     * becomes invalid (as if close was called).
     *
     * May return null to indicate reallocation was unsuccessful. If null is returned, this buffer remains valid.
     */
    default LargeByteBuffer reallocate(long newSize) {
        return null;
    }

    @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" })
    LargeByteBuffer EMPTY = new LargeByteBuffer() {
        @Override
        public byte getByte(long position) {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public short getShort(long position) {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public int getInt(long position) {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public long getLong(long position) {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public void setByte(long position, byte value) throws ReadOnlyBufferException {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public void setShort(long position, short value) throws ReadOnlyBufferException {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public void setInt(long position, int value) throws ReadOnlyBufferException {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public void setLong(long position, long value) throws ReadOnlyBufferException {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public void copyFrom(LargeByteBuffer from, long fromIndex, long toIndex, long length)
                throws UnsupportedOperationException {
            if (length != 0) {
                throw new IndexOutOfBoundsException();
            }
        }

        @Override
        public long size() {
            return 0;
        }
    };
}
