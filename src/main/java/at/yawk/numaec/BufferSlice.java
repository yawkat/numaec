package at.yawk.numaec;

import java.nio.ReadOnlyBufferException;

class BufferSlice implements LargeByteBuffer {
    private final LargeByteBuffer delegate;
    private final long start;
    private final long size;

    BufferSlice(LargeByteBuffer delegate, long start, long size) {
        this.delegate = delegate;
        this.start = start;
        this.size = size;

        // check for overflow or negative size
        if (size < 0 || start + size < start) { throw new IllegalArgumentException(); }
    }

    @Override
    public byte getByte(long position) throws IndexOutOfBoundsException {
        if (position < 0 || position > size - 1) { throw new IndexOutOfBoundsException(); }
        return delegate.getByte(position + start);
    }

    @Override
    public short getShort(long position) throws IndexOutOfBoundsException {
        if (position < 0 || position > size - 2) { throw new IndexOutOfBoundsException(); }
        return delegate.getShort(position + start);
    }

    @Override
    public int getInt(long position) throws IndexOutOfBoundsException {
        if (position < 0 || position > size - 4) { throw new IndexOutOfBoundsException(); }
        return delegate.getInt(position + start);
    }

    @Override
    public long getLong(long position) throws IndexOutOfBoundsException {
        if (position < 0 || position > size - 8) { throw new IndexOutOfBoundsException(); }
        return delegate.getLong(position + start);
    }

    @Override
    public void setByte(long position, byte value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        if (position < 0 || position > size - 1) { throw new IndexOutOfBoundsException(); }
        delegate.setByte(position + start, value);
    }

    @Override
    public void setShort(long position, short value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        if (position < 0 || position > size - 2) { throw new IndexOutOfBoundsException(); }
        delegate.setShort(position + start, value);
    }

    @Override
    public void setInt(long position, int value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        if (position < 0 || position > size - 4) { throw new IndexOutOfBoundsException(); }
        delegate.setInt(position + start, value);
    }

    @Override
    public void setLong(long position, long value) throws IndexOutOfBoundsException, ReadOnlyBufferException {
        if (position < 0 || position > size - 8) { throw new IndexOutOfBoundsException(); }
        delegate.setLong(position + start, value);
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public void copyFrom(LargeByteBuffer from, long fromIndex, long toIndex, long length)
            throws ReadOnlyBufferException, UnsupportedOperationException, IndexOutOfBoundsException {
        if (toIndex < 0 || toIndex + length > size) { throw new IndexOutOfBoundsException(); }
        if (length == 0) { return; }

        if (from instanceof BufferSlice) {
            BufferSlice other = (BufferSlice) from;
            if (fromIndex < 0 || fromIndex + length > other.size) { throw new IndexOutOfBoundsException(); }
            this.delegate.copyFrom(other.delegate, fromIndex + other.start, toIndex + this.start, length);
        } else {
            this.delegate.copyFrom(from, fromIndex, toIndex + this.start, length);
        }
    }
}
