package at.yawk.numaec;

import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;

abstract class LinearHashTable implements AutoCloseable {
    /*
     * hash: xxxxxxxxxxxxxxxx
     *       |--| depth bits
     *
     * bucket = flip(hash >> (64 - depth))
     */

    private static final long NULL = -1;

    private final int bucketSize;
    private final int pointerSize;
    private final long maxBucket;
    private final int entrySize;
    private final int maxBucketEntryCount;
    private final int bucketEntryCountBytes;

    private final PageAllocator allocator;
    private final LargeByteBuffer buf;

    private final MutableLongList mainBuckets = LongLists.mutable.empty();

    private final AtomicReference<Cursor> reuseCursor = new AtomicReference<>();

    LinearHashTable(
            LargeByteBufferAllocator allocator,
            LinearHashMapConfig config,
            int entrySize
    ) {
        this.allocator = new PageAllocator(allocator, config.regionSize, config.bucketSize);
        this.buf = this.allocator.getBufferView();
        this.bucketSize = config.bucketSize;
        this.pointerSize = config.pointerSize;
        this.entrySize = entrySize;
        this.bucketEntryCountBytes = BTree.requiredCountBytes((bucketSize - pointerSize) / entrySize);
        this.maxBucketEntryCount = (bucketSize - pointerSize - bucketEntryCountBytes) / entrySize;

        mainBuckets.add(NULL);

        long maxBucket = (1L << (8 * pointerSize)) - 2; // -2 so we have a NULL pointer
        if (maxBucket < 0) {
            maxBucket = Long.MAX_VALUE;
        }
        this.maxBucket = maxBucket;
    }

    private int splitIndex = 0;
    private int lowDepth = 0;

    public void clear() {
        splitIndex = 0;
        lowDepth = 0;
        mainBuckets.clear();
        allocator.freeAllPages();

        mainBuckets.add(NULL);
    }

    public void expandToFullLoadCapacity(long entryCount) {
        long requiredBuckets = (entryCount - 1) / maxBucketEntryCount + 1;
        if (requiredBuckets > mainBuckets.size()) {
            if (mainBuckets.isEmpty()) {
                // shortcut for initial sizing
                while (requiredBuckets > 0) {
                    mainBuckets.add(NULL);
                    requiredBuckets--;
                }
            } else {
                try (Cursor cursor = allocateCursor()) {
                    while (requiredBuckets > mainBuckets.size()) {
                        cursor.seekMainBucketByBucketIndex(splitIndex);
                        cursor.splitBucket();
                    }
                }
            }
        }
    }

    @SuppressWarnings("resource")
    public Cursor allocateCursor() {
        Cursor cursor = reuseCursor.getAndSet(null);
        if (cursor == null) {
            cursor = new Cursor();
            cursor.init();
        }
        return cursor;
    }

    @DoNotMutate
    void checkInvariants() {
        if (mainBuckets.size() < (1 << lowDepth)) { throw new AssertionError(); }
        if (mainBuckets.size() >= (1 << (lowDepth + 1))) { throw new AssertionError(); }
        if (!mainBuckets.isEmpty() && splitIndex > mainBuckets.size()) { throw new AssertionError(); }
        for (int i = 0; i < mainBuckets.size(); i++) {
            int depth = i < splitIndex || i >= (1 << lowDepth)
                    ? lowDepth + 1 : lowDepth;
            long hashMask = Long.reverse((1 << depth) - 1);
            long hashPrefix = Long.reverse(i);

            long nextHash = hashPrefix;
            long nextKey = 0;

            long block = mainBuckets.get(i);
            while (block != NULL) {
                for (int j = 0; j < getBucketEntryCount(block); j++) {
                    long hash = getHash0(block, j);
                    long key = getKey0(block, j);
                    getValue0(block, j);
                    if ((hash & hashMask) != hashPrefix) { throw new AssertionError("block prefix"); }
                    if (Long.compareUnsigned(hash, nextHash) < 0) { throw new AssertionError("block order"); }
                    if (hash == nextHash && Long.compareUnsigned(key, nextKey) < 0) {
                        throw new AssertionError("block order");
                    }
                    if (key == -1L) {
                        nextHash = hash + 1;
                        nextKey = 0;
                    } else {
                        nextHash = hash;
                        nextKey = key + 1;
                    }
                }
                if (getNextPointer(block) != NULL && getBucketEntryCount(block) != maxBucketEntryCount) {
                    throw new AssertionError("block still has room but also a next block");
                }
                block = getNextPointer(block);
            }
        }
    }

    private static int bucketIndex(int depth, long hash) {
        if (depth >= 32) { throw new IllegalArgumentException(); }
        return (int) (Long.reverse(hash) & ((1 << depth) - 1));
    }

    private long baseAddress(long bucketPtr) {
        if (bucketPtr == NULL) { throw new IllegalArgumentException(); }
        return bucketSize * bucketPtr;
    }

    private long getNextPointer(long bucketPtr) {
        long v = BTree.uget(buf, baseAddress(bucketPtr + 1) - pointerSize, pointerSize);
        if (v == maxBucket + 1) {
            return NULL;
        } else {
            return v;
        }
    }

    private void setNextPointer(long bucketPtr, long nextPtr) {
        if (nextPtr > maxBucket) { throw new IllegalArgumentException(); }
        if (nextPtr < NULL && pointerSize != 8) { throw new IllegalArgumentException(); }
        if (nextPtr == NULL) { nextPtr = maxBucket + 1; }
        BTree.uset(buf, baseAddress(bucketPtr + 1) - pointerSize, pointerSize, nextPtr);
    }

    private long getBucketEntryCount(long bucketPtr) {
        return BTree.uget(buf, baseAddress(bucketPtr + 1) - pointerSize - bucketEntryCountBytes, bucketEntryCountBytes);
    }

    private void setBucketEntryCount(long bucketPtr, long count) {
        BTree.uset(buf, baseAddress(bucketPtr + 1) - pointerSize - bucketEntryCountBytes, bucketEntryCountBytes, count);
    }

    private long getEntryAddress(long bucket, long indexInBucket) {
        return baseAddress(bucket) + indexInBucket * entrySize;
    }

    private long getHash0(long bucket, long indexInBucket) {
        return readHash(buf, getEntryAddress(bucket, indexInBucket));
    }

    private long getKey0(long bucket, long indexInBucket) {
        return readKey(buf, getEntryAddress(bucket, indexInBucket));
    }

    private long getValue0(long bucket, long indexInBucket) {
        return readValue(buf, getEntryAddress(bucket, indexInBucket));
    }

    protected abstract void write(LargeByteBuffer lbb, long address, long hash, long key, long value);

    protected abstract long readHash(LargeByteBuffer lbb, long address);

    protected abstract long readKey(LargeByteBuffer lbb, long address);

    protected abstract long readValue(LargeByteBuffer lbb, long address);

    @Override
    public void close() {
        allocator.close();
    }

    @DoNotMutate
    String toStringFlat() {
        try (LinearHashTable.Cursor iterator = allocateCursor()) {
            return BTree.toString(iterator);
        }
    }

    @DoNotMutate
    String[] toStringBlocks() {
        return mainBuckets.primitiveStream()
                .mapToObj(b -> {
                    StringBuilder builder = new StringBuilder();
                    long block = b;
                    while (true) {
                        if (block == NULL) {
                            builder.append("NULL");
                            break;
                        } else {
                            builder.append(block)
                                    .append(":{");
                            for (int i = 0; i < getBucketEntryCount(block); i++) {
                                if (i != 0) { builder.append(", "); }
                                builder.append(getKey0(block, i))
                                        .append("(0x").append(Long.toHexString(getHash0(block, i)))
                                        .append("): ").append(getValue0(block, 0));
                            }
                            builder.append("}->");
                            block = getNextPointer(block);
                        }
                    }
                    return builder.toString();
                })
                .toArray(String[]::new);
    }

    public class Cursor implements MapStoreCursor {
        private long bucket;
        private long prevBucket;
        private int bucketIndex;
        private long indexInBucket;

        void init() {
            bucket = NULL;
            prevBucket = NULL;
            bucketIndex = -1;
            indexInBucket = -1;
        }

        public void seek(long hash, long key) {
            int bucketIndex = bucketIndex(lowDepth, hash);
            if (bucketIndex < splitIndex) {
                bucketIndex = bucketIndex(lowDepth + 1, hash);
            }
            seekMainBucketByBucketIndex(bucketIndex);
            seekInChain(hash, key);
        }

        @Override
        public boolean elementFound() {
            return indexInBucket >= 0;
        }

        private void seekMainBucketByBucketIndex(int bucketIndex) {
            this.bucketIndex = bucketIndex;
            bucket = mainBuckets.get(bucketIndex);
            prevBucket = NULL;
        }

        private void jumpToNextBucket() {
            prevBucket = bucket;
            bucket = getNextPointer(bucket);
        }

        private void seekInChain(long hash, long key) {
            while (true) {
                if (bucket == NULL) {
                    indexInBucket = ~0;
                    break;
                }

                binarySearch(hash, key);
                long entryCount = getEntryCount();
                if (entryCount >= maxBucketEntryCount && indexInBucket == ~entryCount) {
                    jumpToNextBucket();
                } else {
                    // either found the entry, or the insertion index is in this bucket
                    break;
                }
            }
        }

        private void checkElementFound() {
            if (bucket == NULL) { throw new IllegalStateException(); }
            if (indexInBucket < 0) { throw new IllegalStateException(); }
            if (indexInBucket >= getEntryCount()) { throw new IllegalStateException(); }
        }

        @Override
        public long getKey() {
            checkElementFound();
            return getKey0(bucket, indexInBucket);
        }

        @Override
        public long getValue() {
            checkElementFound();
            return getValue0(bucket, indexInBucket);
        }

        public void setValue(long value) {
            checkElementFound();
            replace0(getHash0(bucket, indexInBucket), getKey0(bucket, indexInBucket), value);
        }

        @Override
        public boolean next() {
            while (true) {
                indexInBucket++;
                if (bucket == NULL) {
                    // either we are just starting, or we just finished a certain main bucket and its followers.
                    // go to next main bucket
                    if (bucketIndex + 1 >= mainBuckets.size()) {
                        return false;
                    } else {
                        seekMainBucketByBucketIndex(bucketIndex + 1);
                        indexInBucket = -1;
                    }
                } else if (indexInBucket >= getEntryCount()) {
                    jumpToNextBucket();
                    indexInBucket = -1;
                } else {
                    // entry found \o/
                    return true;
                }
            }
        }

        private void allocateBucket() {
            if (bucket != NULL) { throw new IllegalStateException(); }
            bucket = allocator.allocatePage();
            if (prevBucket == NULL) {
                mainBuckets.set(bucketIndex, bucket);
            } else {
                setNextPointer(prevBucket, bucket);
            }
            setNextPointer(bucket, NULL);
            setBucketEntryCount(bucket, 0);
        }

        public void insert(long hash, long key, long value) {
            while (true) {
                if (indexInBucket >= 0) { throw new IllegalStateException(); }
                long insertionIndex = ~indexInBucket;
                long oldEntryCount = getEntryCount();
                if (oldEntryCount < maxBucketEntryCount) {
                    if (bucket == NULL) {
                        allocateBucket();
                    }

                    // shift items
                    buf.copyFrom(
                            buf,
                            getEntryAddress(bucket, insertionIndex),
                            getEntryAddress(bucket, insertionIndex + 1),
                            getEntryAddress(bucket, oldEntryCount) - getEntryAddress(bucket, insertionIndex)
                    );
                    setBucketEntryCount(bucket, oldEntryCount + 1);
                    indexInBucket = insertionIndex;
                    replace0(hash, key, value);
                    break;
                } else {
                    if (insertionIndex == oldEntryCount) { throw new AssertionError(); }
                    long trailingHash = getHash0(bucket, oldEntryCount - 1);
                    long trailingKey = getKey0(bucket, oldEntryCount - 1);
                    long trailingValue = getValue0(bucket, oldEntryCount - 1);
                    // shift items
                    buf.copyFrom(
                            buf,
                            getEntryAddress(bucket, insertionIndex),
                            getEntryAddress(bucket, insertionIndex + 1),
                            getEntryAddress(bucket, oldEntryCount - 1) - getEntryAddress(bucket, insertionIndex)
                    );
                    // place item
                    indexInBucket = insertionIndex;
                    replace0(hash, key, value);
                    // proceed insert the trailing item at the start of the next block
                    jumpToNextBucket();
                    hash = trailingHash;
                    key = trailingKey;
                    value = trailingValue;
                    indexInBucket = ~0;
                }
            }
        }

        /**
         * Replace any pointers that are pointing to the bucket this cursor is at with a pointer to the given bucket.
         */
        private void replaceBucketWith(long bucket) {
            if (prevBucket == NULL) {
                mainBuckets.set(bucketIndex, bucket);
            } else {
                setNextPointer(prevBucket, bucket);
            }
        }

        private void freeBucket() {
            long next = getNextPointer(bucket);
            replaceBucketWith(next);
            allocator.freePage(Math.toIntExact(bucket));
            bucket = next;
        }

        /**
         * Backfill items into the previous bucket, if there is space.
         */
        private void backfill() {
            while (true) {
                if (prevBucket == NULL) { throw new IllegalStateException(); }
                if (bucket == NULL) { break; } // done
                long oldPrevEntryCount = getBucketEntryCount(prevBucket);
                long oldEntryCount = getEntryCount();
                long toShift = Math.min(
                        oldEntryCount,
                        maxBucketEntryCount - oldPrevEntryCount
                );
                if (toShift == 0) { return; }
                // copy to previous
                buf.copyFrom(
                        buf,
                        getEntryAddress(bucket, 0),
                        getEntryAddress(prevBucket, oldPrevEntryCount),
                        getEntryAddress(bucket, toShift) - getEntryAddress(bucket, 0)
                );
                setBucketEntryCount(prevBucket, oldPrevEntryCount + toShift);
                // shift remaining items to the left
                buf.copyFrom(
                        buf,
                        getEntryAddress(bucket, toShift),
                        getEntryAddress(bucket, 0),
                        getEntryAddress(bucket, oldEntryCount) - getEntryAddress(bucket, toShift)
                );
                if (oldEntryCount == toShift) {
                    freeBucket();
                } else {
                    setBucketEntryCount(bucket, oldEntryCount - toShift);
                    prevBucket = bucket;
                    bucket = getNextPointer(bucket);
                }
            }
        }

        public void remove() {
            checkElementFound();
            long oldCount = getEntryCount();
            if (oldCount == 1) {
                freeBucket();
            } else {
                // shift other elements left
                buf.copyFrom(
                        buf,
                        getEntryAddress(bucket, indexInBucket + 1),
                        getEntryAddress(bucket, indexInBucket),
                        getEntryAddress(bucket, oldCount) - getEntryAddress(bucket, indexInBucket + 1)
                );
                setBucketEntryCount(bucket, oldCount - 1);
                jumpToNextBucket();
                backfill();
            }
        }

        private void replace0(long hash, long key, long value) {
            checkElementFound();
            write(buf, getEntryAddress(bucket, indexInBucket), hash, key, value);
        }

        private long getEntryCount() {
            return bucket == NULL ? 0 : getBucketEntryCount(bucket);
        }

        private void binarySearch(long hash, long key) {
            if (bucket == NULL) { throw new IllegalStateException(); }
            long low = 0;
            long high = getEntryCount() - 1;
            while (low <= high) {
                long mid = (low + high) / 2;
                // compare by hash, then by key
                int cmp = Long.compareUnsigned(getHash0(bucket, mid), hash);
                if (cmp == 0) {
                    cmp = Long.compareUnsigned(getKey0(bucket, mid), key);
                }
                if (cmp < 0) { // pivot < hash
                    low = mid + 1;
                } else if (cmp > 0) { // pivot > hash
                    high = mid - 1;
                } else {
                    indexInBucket = mid;
                    return;
                }
            }
            indexInBucket = ~low;
        }

        private void appendItemsFromBucket(long sourceBucket, long startIndex) {
            long toCopy = getBucketEntryCount(sourceBucket) - startIndex;
            while (toCopy > 0) {
                if (bucket == NULL) {
                    allocateBucket();
                }

                long oldEntryCount = getEntryCount();
                long copyHere = Math.min(maxBucketEntryCount - oldEntryCount, toCopy);
                long newEntryCount = oldEntryCount + copyHere;
                buf.copyFrom(
                        buf,
                        getEntryAddress(sourceBucket, startIndex),
                        getEntryAddress(bucket, oldEntryCount),
                        getEntryAddress(bucket, newEntryCount) - getEntryAddress(bucket, oldEntryCount)
                );
                setBucketEntryCount(bucket, newEntryCount);
                toCopy -= copyHere;
                startIndex += copyHere;
                if (newEntryCount >= maxBucketEntryCount) {
                    prevBucket = bucket;
                    bucket = getNextPointer(bucket);
                }
            }
        }

        private void splitBucket() {
            if (splitIndex != bucketIndex) { throw new IllegalStateException(); }
            int daughterBucketIndex = this.bucketIndex | (1 << lowDepth);
            if (daughterBucketIndex != mainBuckets.size()) { throw new AssertionError(); }
            mainBuckets.add(NULL);
            splitIndex++;
            if (lowDepth == -1 || splitIndex == 1 << lowDepth) {
                splitIndex = 0;
                lowDepth++;
            }

            if (bucket != NULL) {
                long firstToDaughter = Long.reverse(Integer.toUnsignedLong(daughterBucketIndex));
                seekInChain(firstToDaughter, 0); // 0 is the lowest key for the hash
                if (indexInBucket == ~maxBucketEntryCount) { throw new AssertionError(); }

                long pivotBucket = this.bucket;
                long startIndex = indexInBucket < 0 ? ~indexInBucket : indexInBucket;
                if (startIndex == 0) {
                    // move everything starting at this block to daughter
                    replaceBucketWith(NULL);
                    mainBuckets.set(daughterBucketIndex, pivotBucket);
                } else if (startIndex < getEntryCount()) {
                    // copy only some items
                    seekMainBucketByBucketIndex(daughterBucketIndex);
                    appendItemsFromBucket(pivotBucket, startIndex);
                    setBucketEntryCount(pivotBucket, startIndex);
                    // relink buckets after the pivot bucket to the new daughter chain
                    setNextPointer(bucket, getNextPointer(pivotBucket));
                    setNextPointer(pivotBucket, NULL);
                    jumpToNextBucket();
                    backfill();
                } else {
                    // copy nothing
                    if (startIndex > getEntryCount()) { throw new AssertionError(); }
                }
            }
        }

        @Override
        public void close() {
            init();
            reuseCursor.set(this);
        }
    }
}
