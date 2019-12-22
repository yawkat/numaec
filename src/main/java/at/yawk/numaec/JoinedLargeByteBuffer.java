package at.yawk.numaec;

import java.nio.ReadOnlyBufferException;

abstract class JoinedLargeByteBuffer extends GenericJoinedBuffer<LargeByteBuffer> {
    @Override
    void copyBetweenComponents(
            LargeByteBuffer dest,
            long toIndex,
            LargeByteBuffer src,
            long fromIndex,
            long length
    )
            throws IndexOutOfBoundsException {
        dest.copyFrom(src, fromIndex, toIndex, length);
    }

    @Override
    void copyLargeToComponent(
            LargeByteBuffer dest,
            long toIndex,
            LargeByteBuffer src,
            long fromIndex,
            long length
    )
            throws IndexOutOfBoundsException, UnsupportedOperationException {
        copyBetweenComponents(dest, toIndex, src, fromIndex, length);
    }

    @Override
    public byte getByte(long position) throws IndexOutOfBoundsException {
        return component(position).getByte(offset(position));
    }

    @Override
    public short getShort(long position) throws IndexOutOfBoundsException {
        return component(position).getShort(offset(position));
    }

    @Override
    public int getInt(long position) throws IndexOutOfBoundsException {
        return component(position).getInt(offset(position));
    }

    @Override
    public long getLong(long position) throws IndexOutOfBoundsException {
        return component(position).getLong(offset(position));
    }

    @Override
    public void setByte(long position, byte value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        component(position).setByte(offset(position), value);
    }

    @Override
    public void setShort(long position, short value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        component(position).setShort(offset(position), value);
    }

    @Override
    public void setInt(long position, int value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        component(position).setInt(offset(position), value);
    }

    @Override
    public void setLong(long position, long value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        component(position).setLong(offset(position), value);
    }

    @Override
    public abstract long size();

    @Override
    GenericJoinedBuffer<LargeByteBuffer> convertToCompatible(LargeByteBuffer other) {
        if (other instanceof JoinedLargeByteBuffer) {
            return (JoinedLargeByteBuffer) other;
        } else {
            return null;
        }
    }
}
