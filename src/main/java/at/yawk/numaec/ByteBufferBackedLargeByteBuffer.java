package at.yawk.numaec;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

public class ByteBufferBackedLargeByteBuffer implements LargeByteBuffer {
    private final ByteBuffer[] buffers;
    private final int componentSize;

    private int offset(long position) {
        return (int) (position % componentSize);
    }

    private ByteBuffer buffer(long position) throws IndexOutOfBoundsException {
        long bufferI = position / componentSize;
        return buffers[(int) bufferI];
    }

    public ByteBufferBackedLargeByteBuffer(ByteBuffer[] buffers, int componentSize) {
        if (Integer.bitCount(componentSize) != 1) {
            throw new IllegalArgumentException("componentSize must be power of 2");
        }
        this.buffers = buffers;
        this.componentSize = componentSize;
    }

    @Override
    public byte getByte(long position) throws IndexOutOfBoundsException {
        return buffer(position).get(offset(position));
    }

    @Override
    public short getShort(long position) throws IndexOutOfBoundsException {
        return buffer(position).getShort(offset(position));
    }

    @Override
    public int getInt(long position) throws IndexOutOfBoundsException {
        return buffer(position).getInt(offset(position));
    }

    @Override
    public long getLong(long position) throws IndexOutOfBoundsException {
        return buffer(position).getLong(offset(position));
    }

    @Override
    public void setByte(long position, byte value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        buffer(position).put(offset(position), value);
    }

    @Override
    public void setShort(long position, short value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        buffer(position).putShort(offset(position), value);
    }

    @Override
    public void setInt(long position, int value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        buffer(position).putInt(offset(position), value);
    }

    @Override
    public void setLong(long position, long value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        buffer(position).putLong(offset(position), value);
    }

    @Override
    public long size() {
        return (long) (buffers.length - 1) * componentSize + buffers[buffers.length - 1].limit();
    }

    @Override
    public void copyFrom(LargeByteBuffer from, long fromIndex, long toIndex, long length)
            throws ReadOnlyBufferException, UnsupportedOperationException, IndexOutOfBoundsException {
        if (length < 0) {
            throw new IllegalArgumentException("length < 0");
        }
        if (length == 0) {
            return;
        }
        if (!(from instanceof ByteBufferBackedLargeByteBuffer)) {
            throw new UnsupportedOperationException();
        }
        ByteBufferBackedLargeByteBuffer other = (ByteBufferBackedLargeByteBuffer) from;
        if (other.componentSize != this.componentSize) {
            throw new UnsupportedOperationException();
        }
        if (fromIndex + length > other.size() ||
                toIndex + length > this.size()) {
            throw new IndexOutOfBoundsException();
        }
        if (fromIndex >= toIndex) {
            while (length > 0) {
                ByteBuffer src = other.buffer(fromIndex);
                ByteBuffer dest = this.buffer(toIndex);
                if (dest == src) {
                    src = src.duplicate();
                }

                dest.position(this.offset(toIndex));
                src.position(other.offset(fromIndex));
                try {
                    int toCopy = (int) Math.min(src.remaining(), Math.min(dest.remaining(), length));
                    if (toCopy == 0) {
                        throw new IndexOutOfBoundsException();
                    }

                    int oldLimit = src.limit();
                    src.limit(src.position() + toCopy);
                    dest.put(src);
                    src.limit(oldLimit);

                    fromIndex += toCopy;
                    toIndex += toCopy;
                    length -= toCopy;
                } finally {
                    dest.position(0);
                    src.position(0);
                }
            }
        } else {
            while (length > 0) {
                ByteBuffer src = other.buffer(fromIndex + length - 1);
                ByteBuffer dest = this.buffer(toIndex + length - 1);
                if (dest == src) {
                    src = src.duplicate();
                }
                int toCopy = Math.min(
                        offset(toIndex + length - 1) + 1,
                        (int) Math.min(length, offset(fromIndex + length - 1) + 1)
                );
                if (toCopy == 0) {
                    throw new IndexOutOfBoundsException();
                }
                dest.position(this.offset(toIndex + length - toCopy));
                src.position(other.offset(fromIndex + length - toCopy));
                try {
                    int oldLimit = src.limit();
                    src.limit(src.position() + toCopy);
                    dest.put(src);
                    src.limit(oldLimit);
                } finally {
                    dest.position(0);
                    src.position(0);
                }

                length -= toCopy;
            }
        }
    }
}
