package at.yawk.numaec;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

final class PageAllocator implements Closeable {
    private final LargeByteBufferAllocator allocator;
    private final List<LargeByteBuffer> regions = new ArrayList<>();
    /**
     * Size of each region in pages.
     */
    private final int regionSize;
    /**
     * Size of each region in bytes.
     */
    private final int regionSizeBytes;

    private final LargeByteBuffer bufferView = new Buf();

    private final BitSet occupied = new BitSet();

    /**
     * @param regionSize Region size in <i>pages</i>.
     */
    PageAllocator(LargeByteBufferAllocator allocator, int regionSize, int pageSize) {
        this.allocator = allocator;
        this.regionSize = regionSize;
        this.regionSizeBytes = regionSize * pageSize;
    }

    public LargeByteBuffer getBufferView() {
        return bufferView;
    }

    public int allocatePage() {
        int nextClear = occupied.nextClearBit(0);
        int maxPage = regionSize * regions.size();
        if (nextClear > maxPage) { throw new AssertionError(); }
        if (nextClear == maxPage) {
            regions.add(allocator.allocate(regionSizeBytes));
        }
        occupied.set(nextClear);
        return nextClear;
    }

    public void freePage(int page) {
        if (!occupied.get(page)) {
            throw new IllegalStateException("Page not allocated (double-free?)");
        }
        occupied.clear(page);
    }

    public void freeAllPages() {
        occupied.clear();
    }

    @Override
    public void close() {
        RuntimeException re = null;
        for (LargeByteBuffer region : regions) {
            try {
                region.close();
            } catch (RuntimeException e) {
                if (re == null) {
                    re = e;
                } else {
                    re.addSuppressed(e);
                }
            }
        }
        regions.clear();
        if (re != null) {
            throw re;
        }
    }

    private class Buf extends JoinedLargeByteBuffer {
        @Override
        protected LargeByteBuffer component(long position) {
            return regions.get(Math.toIntExact(position / regionSizeBytes));
        }

        @Override
        protected long offset(long position) {
            return position % regionSizeBytes;
        }

        @Override
        protected long nextRegionStart(long position) {
            return currentRegionStart(position) + regionSizeBytes;
        }

        @Override
        public long size() {
            return (long) regionSizeBytes * regions.size();
        }

        @Override
        public void close() {
        }
    }
}
