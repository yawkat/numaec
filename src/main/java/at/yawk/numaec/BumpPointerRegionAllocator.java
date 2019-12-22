package at.yawk.numaec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * {@link LargeByteBufferAllocator} that allocates fixed-size regions from a delegate
 * {@link LargeByteBufferAllocator} and returns small chunks of those regions. Useful as a wrapper around allocators
 * with expensive allocate functions.
 */
public final class BumpPointerRegionAllocator implements LargeByteBufferAllocator {
    private static final Comparator<Region> REMAINING_COMPARATOR = Comparator.comparingLong(Region::remaining);

    private final LargeByteBufferAllocator delegate;
    private final long regionSize;
    private final long align;
    private final int maxRegionCache;

    private final List<Region> regions;

    private static long alignUp(long value, long align) {
        return value == 0 ? 0 : ((value - 1) / align + 1) * align;
    }

    private BumpPointerRegionAllocator(Builder builder) {
        this.delegate = builder.delegate;
        this.regionSize = builder.regionSize;
        this.align = builder.align;
        this.maxRegionCache = builder.maxRegionCache;

        this.regions = new ArrayList<>(maxRegionCache + 1);
    }

    private void insertRegion(Region r) {
        int searchResult = Collections.binarySearch(regions, r, REMAINING_COMPARATOR);
        int insertionIndex = searchResult < 0 ? ~searchResult : searchResult;
        regions.add(insertionIndex, r);
    }

    @Override
    public LargeByteBuffer allocate(long size) {
        if (size >= regionSize) {
            return delegate.allocate(alignUp(size, align));
        } else {
            for (int i = 0; i < regions.size(); i++) {
                Region region = regions.get(i);
                LargeByteBuffer allocated = region.allocate(size, align);
                if (allocated != null) {
                    regions.remove(i);
                    insertRegion(region);
                    return allocated;
                }
            }
            // no room :(
            Region region = new Region(delegate.allocate(regionSize));
            LargeByteBuffer buffer = region.allocate(size, align);
            if (buffer == null) { throw new AssertionError(); }
            insertRegion(region);
            if (regions.size() > maxRegionCache) {
                regions.remove(0); // remove smallest
            }
            return buffer;
        }
    }

    public static Builder builder(LargeByteBufferAllocator delegate) {
        return new Builder(delegate);
    }

    public static final class Builder {
        private final LargeByteBufferAllocator delegate;
        private long regionSize = 1024 * 1024;
        private long align = 8;
        private int maxRegionCache = 4;

        private Builder(LargeByteBufferAllocator delegate) {
            this.delegate = delegate;
        }

        public Builder regionSize(long regionSize) {
            this.regionSize = regionSize;
            return this;
        }

        public Builder align(long align) {
            this.align = align;
            return this;
        }

        public Builder maxRegionCache(int maxRegionCache) {
            this.maxRegionCache = maxRegionCache;
            return this;
        }

        public BumpPointerRegionAllocator build() {
            return new BumpPointerRegionAllocator(this);
        }
    }

    private static class Region {
        private final LargeByteBuffer buffer;
        private long position = 0;
        private int openCount = 0;

        Region(LargeByteBuffer buffer) {
            this.buffer = buffer;
        }

        long remaining() {
            return buffer.size() - position;
        }

        LargeByteBuffer allocate(long size, long alignment) {
            // jump to next aligned position
            long start = alignUp(position, alignment);
            long end = start + size;
            if (end > buffer.size()) {
                return null;
            } else {
                openCount++;
                position = end;
                return new BufferSlice(this.buffer, start, size) {
                    boolean closed = false;

                    @Override
                    public void close() {
                        if (!closed) {
                            closed = true;
                            if (--openCount == 0) {
                                buffer.close();
                            }
                        }
                    }
                };
            }
        }
    }
}
