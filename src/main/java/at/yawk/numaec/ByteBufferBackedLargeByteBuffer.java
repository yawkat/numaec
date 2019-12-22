package at.yawk.numaec;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

public class ByteBufferBackedLargeByteBuffer extends GenericJoinedBuffer<ByteBuffer> implements LargeByteBuffer {
    private final ByteBuffer[] buffers;
    private final int componentSize;

    public ByteBufferBackedLargeByteBuffer(ByteBuffer[] buffers, int componentSize) {
        if (Integer.bitCount(componentSize) != 1) {
            throw new IllegalArgumentException("componentSize must be power of 2");
        }
        this.buffers = buffers;
        this.componentSize = componentSize;
    }

    @Override
    long offset(long position) {
        return offsetInt(position);
    }

    private int offsetInt(long position) {
        return (int) (position % componentSize);
    }

    @Override
    ByteBuffer component(long position) throws IndexOutOfBoundsException {
        long bufferI = position / componentSize;
        return buffers[(int) bufferI];
    }

    @Override
    public byte getByte(long position) throws IndexOutOfBoundsException {
        return component(position).get(offsetInt(position));
    }

    @Override
    public short getShort(long position) throws IndexOutOfBoundsException {
        return component(position).getShort(offsetInt(position));
    }

    @Override
    public int getInt(long position) throws IndexOutOfBoundsException {
        return component(position).getInt(offsetInt(position));
    }

    @Override
    public long getLong(long position) throws IndexOutOfBoundsException {
        return component(position).getLong(offsetInt(position));
    }

    @Override
    public void setByte(long position, byte value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        component(position).put(offsetInt(position), value);
    }

    @Override
    public void setShort(long position, short value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        component(position).putShort(offsetInt(position), value);
    }

    @Override
    public void setInt(long position, int value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        component(position).putInt(offsetInt(position), value);
    }

    @Override
    public void setLong(long position, long value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        component(position).putLong(offsetInt(position), value);
    }

    @Override
    public long size() {
        return (long) (buffers.length - 1) * componentSize + buffers[buffers.length - 1].limit();
    }

    @Override
    long nextRegionStart(long position) {
        return currentRegionStart(position) + componentSize;
    }

    @Override
    void copyLargeToComponent(
            ByteBuffer dest,
            long toIndex,
            LargeByteBuffer src,
            long fromIndex,
            long length
    ) {
        throw new UnsupportedOperationException("Incompatible buffers");
    }

    @Override
    void copyBetweenComponents(ByteBuffer dest, long toIndex, ByteBuffer src, long fromIndex, long length) {
        if (length < 0 || length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("length");
        }
        if (src.limit() < fromIndex + length || dest.limit() < toIndex + length) {
            throw new IndexOutOfBoundsException();
        }
        if (dest == src) {
            dest = dest.duplicate();
        }

        int oldDestLimit = dest.limit();
        int oldSrcLimit = src.limit();

        // casts are necessary for java 8 compat
        ((Buffer) src).position(Math.toIntExact(fromIndex));
        ((Buffer) dest).position(Math.toIntExact(toIndex));
        ((Buffer) src).limit(Math.toIntExact(src.position() + length));
        ((Buffer) dest).limit(Math.toIntExact(dest.position() + length));

        dest.put(src);

        ((Buffer) dest).position(0);
        ((Buffer) src).position(0);
        ((Buffer) dest).limit(oldDestLimit);
        ((Buffer) src).limit(oldSrcLimit);
    }

    @Override
    GenericJoinedBuffer<ByteBuffer> convertToCompatible(LargeByteBuffer other) {
        if (other instanceof ByteBufferBackedLargeByteBuffer) {
            ByteBufferBackedLargeByteBuffer compatible = (ByteBufferBackedLargeByteBuffer) other;
            // right now only same component size is supported
            if (compatible.componentSize == this.componentSize) {
                return compatible;
            }
        }
        return null;
    }
}
