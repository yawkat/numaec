package at.yawk.numaec;

import java.nio.ReadOnlyBufferException;

abstract class GenericJoinedBuffer<C> implements LargeByteBuffer {
    abstract C component(long position);

    abstract long offset(long position);

    long currentRegionStart(long position) {
        return position - offset(position);
    }

    abstract long nextRegionStart(long position);

    abstract void copyLargeToComponent(C dest, long toIndex, LargeByteBuffer src, long fromIndex, long length)
            throws IndexOutOfBoundsException, UnsupportedOperationException;

    abstract void copyBetweenComponents(C dest, long toIndex, C src, long fromIndex, long length)
            throws IndexOutOfBoundsException;

    abstract GenericJoinedBuffer<C> convertToCompatible(LargeByteBuffer other);

    @Override
    public void copyFrom(LargeByteBuffer from, long fromIndex, long toIndex, long length)
            throws ReadOnlyBufferException, UnsupportedOperationException, IndexOutOfBoundsException {
        if (length < 0) {
            throw new IllegalArgumentException("length < 0");
        }
        GenericJoinedBuffer<C> joinedFrom = convertToCompatible(from);
        if (fromIndex >= toIndex) {
            // copy left to right
            while (length > 0) {
                C component = this.component(toIndex);
                long end = this.nextRegionStart(toIndex);
                long toCopy = Math.min(end - toIndex, length);
                long componentToIndex = offset(toIndex);
                if (joinedFrom != null) {
                    copyToComponentLtr(component, componentToIndex, joinedFrom, fromIndex, toCopy);
                } else {
                    copyLargeToComponent(component, componentToIndex, from, fromIndex, length);
                }
                fromIndex += toCopy;
                toIndex += toCopy;
                length -= toCopy;
            }
        } else {
            // copy right to left
            while (length > 0) {
                long toEnd = toIndex + length;
                C component = this.component(toEnd - 1);
                long regionStart = this.currentRegionStart(toEnd - 1);
                long toCopy = Math.min(toEnd - regionStart, length);
                long copyFromIndex = fromIndex + length - toCopy;
                long componentToIndex = offset(toEnd - toCopy);
                if (joinedFrom != null) {
                    copyToComponentRtl(component, componentToIndex, joinedFrom, copyFromIndex, toCopy);
                } else {
                    copyLargeToComponent(component, componentToIndex, from, copyFromIndex, toCopy);
                }
                length -= toCopy;
            }
        }
    }

    private void copyToComponentLtr(C dest, long toIndex, GenericJoinedBuffer<C> src, long fromIndex, long length) {
        // copy left to right
        while (length > 0) {
            C component = src.component(fromIndex);
            long end = src.nextRegionStart(fromIndex);
            long toCopy = Math.min(end - fromIndex, length);
            copyBetweenComponents(dest, toIndex, component, src.offset(fromIndex), toCopy);
            fromIndex += toCopy;
            toIndex += toCopy;
            length -= toCopy;
        }
    }

    private void copyToComponentRtl(C dest, long toIndex, GenericJoinedBuffer<C> src, long fromIndex, long length) {
        // copy right to left
        while (length > 0) {
            long fromEnd = fromIndex + length;
            C component = src.component(fromEnd - 1);
            long regionStart = src.currentRegionStart(fromEnd - 1);
            long toCopy = Math.min(fromEnd - regionStart, length);
            copyBetweenComponents(
                    dest, toIndex + length - toCopy, component, src.offset(fromEnd - toCopy), toCopy);
            length -= toCopy;
        }
    }
}
