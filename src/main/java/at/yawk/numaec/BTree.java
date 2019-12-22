package at.yawk.numaec;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;

abstract class BTree {

    /*
     * Branch node layout:
     * [ptr (key value? ptr)+ size]
     *
     * Leaf node layout:
     * [(key value)+ nextPtr? size]
     */

    // exposed for testing
    static final String MESSAGE_POINTER_TOO_SMALL = "BTree pointer size too small";

    private static final long NULL = -1;

    private static final int INITIAL_BLOCK_COUNT = 16;

    private final int blockSize;
    private final int pointerSize;
    private final int leafEntrySize;
    private final int branchEntrySize;
    private final boolean storeNextPointer;
    private final boolean entryMustBeInLeaf;

    private final LargeByteBuffer buf;
    // visible for testing
    int levelCount = 0;
    private long rootPtr = NULL;

    private final long maxPage;
    private final PageAllocator allocator;

    private final AtomicReference<Cursor> reuseCursor = new AtomicReference<>();

    @SuppressWarnings({ "UnnecessaryLocalVariable", "TooBroadScope" })
    BTree(LargeByteBufferAllocator allocator, BTreeConfig config, int branchEntrySize, int leafEntrySize) {
        this.allocator = new PageAllocator(allocator, config.regionSize, config.blockSize);
        this.buf = this.allocator.getBufferView();
        this.blockSize = config.blockSize;
        this.pointerSize = config.pointerSize;
        this.leafEntrySize = leafEntrySize;
        this.branchEntrySize = branchEntrySize;
        this.storeNextPointer = config.storeNextPointer;
        this.entryMustBeInLeaf = config.entryMustBeInLeaf;

        leafItemCountSize = requiredCountBytes(blockSize / leafEntrySize);
        branchItemCountSize = requiredCountBytes(blockSize / (branchEntrySize + pointerSize));
        int leafHeaderSize = leafItemCountSize + (storeNextPointer ? pointerSize : 0);
        int branchHeaderSize = branchItemCountSize;
        leafCapacity = (blockSize - leafHeaderSize) / leafEntrySize - 1;
        branchCapacity = (blockSize - branchHeaderSize - pointerSize) / (branchEntrySize + pointerSize) - 1;

        long maxPage = (1L << (8 * pointerSize)) - 2; // -2 so we have a NULL pointer
        if (maxPage < 0) {
            maxPage = Long.MAX_VALUE;
        }
        this.maxPage = maxPage;
    }

    private final int leafItemCountSize;
    private final int branchItemCountSize;

    private final int leafCapacity;

    private final int branchCapacity;

    private static int requiredCountBytes(long maxCount) {
        if (maxCount < 0x100) { return 1; }
        if (maxCount < 0x10000) { return 2; }
        if (maxCount < 0x100000000L) { return 4; }
        return 8;
    }

    private long mapPointer(long ptr) {
        if (ptr >= 0 && ptr <= maxPage) {
            return ptr;
        } else if (ptr == maxPage + 1 || ptr == -1L) {
            return NULL;
        } else {
            throw new IllegalArgumentException();
        }
    }

    private long baseAddress(long blockPtr) {
        if (blockPtr == NULL) { throw new IllegalArgumentException(); }
        return blockSize * blockPtr;
    }

    /**
     * @return The page pointer
     */
    private long allocatePage() {
        int ptr = allocator.allocatePage();
        if (ptr > maxPage) {
            allocator.freePage(ptr);
            throw new IllegalStateException(MESSAGE_POINTER_TOO_SMALL);
        }
        return ptr;
    }

    private void freePage(long page) {
        allocator.freePage(Math.toIntExact(page));
    }

    public Cursor allocateCursor() {
        Cursor cursor = reuseCursor.getAndSet(null);
        if (cursor == null) {
            return new Cursor();
        } else {
            return cursor;
        }
    }

    private long uget(long address, int length) {
        if (length == 1) { return buf.getByte(address) & 0xffL; }
        if (length == 2) { return buf.getShort(address) & 0xffffL; }
        if (length == 4) { return buf.getInt(address) & 0xffffffffL; }
        return buf.getLong(address);
    }

    private long getPtr(long address) {
        return mapPointer(uget(address, pointerSize));
    }

    private void uset(long address, int length, long value) {
        if (length == 1) {
            if (value > 0xff) { throw new IllegalArgumentException(); }
            buf.setByte(address, (byte) value);
            return;
        }
        if (length == 2) {
            if (value > 0xffff) { throw new IllegalArgumentException(); }
            buf.setShort(address, (short) value);
            return;
        }
        if (length == 4) {
            if (value > 0xffffffffL) { throw new IllegalArgumentException(); }
            buf.setInt(address, (int) value);
            return;
        }
        buf.setLong(address, value);
    }

    private long getLeafItemCount(long blockPtr) {
        long baseAddress = baseAddress(blockPtr);
        return uget(baseAddress + blockSize - leafItemCountSize, leafItemCountSize);
    }

    private void setLeafItemCount(long blockPtr, long leafItemCount) {
        long baseAddress = baseAddress(blockPtr);
        uset(baseAddress + blockSize - leafItemCountSize, leafItemCountSize, leafItemCount);
    }

    private long getBranchItemCount(long blockPtr) {
        long baseAddress = baseAddress(blockPtr);
        return uget(baseAddress + blockSize - branchItemCountSize, branchItemCountSize);
    }

    private void setBranchItemCount(long blockPtr, long branchItemCount) {
        long baseAddress = baseAddress(blockPtr);
        uset(baseAddress + blockSize - branchItemCountSize, branchItemCountSize, branchItemCount);
    }

    private long getNextLeafPtr(long blockPtr) {
        if (!storeNextPointer) { throw new UnsupportedOperationException(); }
        long baseAddress = baseAddress(blockPtr);
        return getPtr(baseAddress + blockSize - leafItemCountSize - pointerSize);
    }

    private void setNextLeafPtr(long blockPtr, long nextLeafPtr) {
        if (!storeNextPointer) { throw new UnsupportedOperationException(); }
        long baseAddress = baseAddress(blockPtr);
        uset(baseAddress + blockSize - leafItemCountSize - pointerSize, pointerSize, nextLeafPtr);
    }

    /**
     * Search for a key in a block.
     *
     * @param leaf     {@code true} iff this is a leaf block.
     * @param blockPtr The ID of this block.
     * @param key      The key to search for.
     * @return The binary search result as returned by {@link java.util.Arrays#binarySearch}
     */
    private long blockSearch(boolean leaf, long blockPtr, long key) {
        long itemCount = leaf ? getLeafItemCount(blockPtr) : getBranchItemCount(blockPtr);
        long low = 0;
        long high = itemCount - 1;
        while (low <= high) {
            long mid = (low + high) / 2;
            long pivot = leaf ? readLeafKey(blockPtr, mid) : readBranchKey(blockPtr, mid);
            int cmp = compare(pivot, key);
            if (cmp < 0) { // pivot < key
                low = mid + 1;
            } else if (cmp > 0) { // pivot > key
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return ~low;
    }

    private void simpleBranchInsert(
            long branchPtr, long insertionIndex,
            long key, long value,
            long prevPtr, long nextPtr
    ) {
        long oldSize = getBranchItemCount(branchPtr);
        if (insertionIndex < 0 || insertionIndex > oldSize) {
            throw new IllegalArgumentException();
        }
        if (oldSize > branchCapacity) { // must split - allow == for tentative insert
            throw new UnsupportedOperationException();
        }
        // shift items after insertion point
        long start = branchValueIndex(branchPtr, insertionIndex);
        buf.copyFrom(buf,
                     start,
                     branchValueIndex(branchPtr, insertionIndex + 1),
                     branchValueIndex(branchPtr, oldSize) - start);
        writeBranchEntry(branchPtr, insertionIndex, key, value);
        writeBranchPrevPointer(branchPtr, insertionIndex, prevPtr);
        writeBranchPrevPointer(branchPtr, insertionIndex + 1, nextPtr);
        setBranchItemCount(branchPtr, oldSize + 1);
    }

    private void simpleLeafInsert(long leafPtr, long insertionIndex, long key, long value) {
        long oldSize = getLeafItemCount(leafPtr);
        if (insertionIndex < 0 || insertionIndex > oldSize) {
            throw new IllegalArgumentException();
        }
        if (oldSize > leafCapacity) { // must split - allow == for tentative insert
            throw new UnsupportedOperationException();
        }
        // shift items after insertion point
        copyLeafEntries(
                leafPtr, insertionIndex,
                leafPtr, insertionIndex + 1,
                oldSize - insertionIndex
        );

        writeLeafEntry(leafPtr, insertionIndex, key, value);
        setLeafItemCount(leafPtr, oldSize + 1);
    }

    private void simpleLeafRemoveAt(long leafPtr, long index) {
        long oldSize = getLeafItemCount(leafPtr);
        if (index < 0 || index >= oldSize) {
            throw new IllegalArgumentException();
        }
        copyLeafEntries(
                leafPtr, index + 1,
                leafPtr, index,
                oldSize - index - 1
        );
        setLeafItemCount(leafPtr, oldSize - 1);
    }

    private void copyLeafEntries(
            long fromBlock,
            long fromIndex,
            long toBlock,
            long toIndex,
            long count
    ) {
        // short-circuit to bypass exception checks
        if (count == 0) { return; }
        // allow +1 for overflow handling
        if (fromIndex < 0 || fromIndex + count > leafCapacity + 1) { throw new IllegalArgumentException(); }
        if (toIndex < 0 || toIndex + count > leafCapacity + 1) { throw new IllegalArgumentException(); }
        long copyStart = leafValueIndex(fromBlock, fromIndex);
        long copyEnd = leafValueIndex(fromBlock, fromIndex + count);
        long copyDest = leafValueIndex(toBlock, toIndex);
        buf.copyFrom(buf, copyStart, copyDest, copyEnd - copyStart);
    }

    private long splitLeafBlock(long leftPtr, long pivotIndex) {
        long rightPtr = allocatePage();
        long copyStart = pivotIndex + (entryMustBeInLeaf ? 0 : 1);
        long rightCount = getLeafItemCount(leftPtr) - copyStart;
        copyLeafEntries(
                leftPtr, copyStart,
                rightPtr, 0,
                rightCount
        );
        setLeafItemCount(rightPtr, rightCount);
        setLeafItemCount(leftPtr, pivotIndex); // truncate left block
        if (storeNextPointer) {
            setNextLeafPtr(rightPtr, getNextLeafPtr(leftPtr));
            setNextLeafPtr(leftPtr, rightPtr);
        }
        return rightPtr;
    }

    /**
     * Copy branch block entries, including the <i>next</i> leaf pointers.
     *
     * @param copyPrev Whether to copy the prev pointer of the first entry.
     */
    private void copyBranchEntries(
            long fromBlock,
            long fromIndex,
            long toBlock,
            long toIndex,
            long count,
            boolean copyPrev
    ) {
        // allow +1 for overflow handling
        if (fromIndex < 0 || fromIndex + count > branchCapacity + 1) { throw new IllegalArgumentException(); }
        if (toIndex < 0 || toIndex + count > branchCapacity + 1) { throw new IllegalArgumentException(); }
        long copyStart = branchValueIndex(fromBlock, fromIndex);
        long copyEnd = branchValueIndex(fromBlock, fromIndex + count);
        long copyDest = branchValueIndex(toBlock, toIndex);
        if (copyPrev) {
            copyStart -= pointerSize;
            copyDest -= pointerSize;
        }
        buf.copyFrom(buf, copyStart, copyDest, copyEnd - copyStart);
    }

    private long splitBranchBlock(long leftPtr, long pivotIndex) {
        long rightPtr = allocatePage();
        long rightCount = getBranchItemCount(leftPtr) - pivotIndex - 1;
        copyBranchEntries(
                leftPtr, pivotIndex + 1,
                rightPtr, 0,
                rightCount,
                true // copy prev
        );
        setBranchItemCount(rightPtr, rightCount);
        setBranchItemCount(leftPtr, pivotIndex); // truncate left block
        return rightPtr;
    }

    public void clear() {
        rootPtr = NULL;
        levelCount = 0;
        allocator.freeAllPages();
    }

    public void close() {
        levelCount = -1;
        allocator.close();
    }

    /**
     * Check invariants of this tree. Assumes {@link #compare} is consistent with {@link Long#compare}.
     */
    @DoNotMutate
    void checkInvariants() {
        if (rootPtr != NULL) {
            checkInvariants(Long.MAX_VALUE, rootPtr, 0, LongLists.mutable.empty(), LongSets.mutable.empty());
        }
        @SuppressWarnings("resource")
        Cursor cursor = reuseCursor.get();
        if (cursor != null) {
            cursor.checkCursorInvariants();
        }
    }

    @DoNotMutate
    private long checkInvariants(
            long max,
            long node,
            int level,
            MutableLongList visitedLeaves,
            MutableLongSet visitedNodes
    ) {
        if (node == NULL) { throw new IllegalArgumentException(); }
        if (!visitedNodes.add(node)) { throw new AssertionError("Node referenced twice"); }
        if (level < 0 || level >= levelCount) { throw new IllegalArgumentException(); }
        if (level == levelCount - 1) {
            long count = getLeafItemCount(node);
            if (count <= 0) { throw new AssertionError("Invalid leaf item count"); }
            if (count > leafCapacity) { throw new AssertionError("Leaf over capacity"); }
            for (long i = count - 1; i >= 0; i--) {
                long key = readLeafKey(node, i);
                if (key > max) { throw new AssertionError("Sort order not maintained"); }
                max = key - 1;
                readLeafValue(node, i);
            }
            if (storeNextPointer) {
                long nextPtr = getNextLeafPtr(node);
                if (visitedLeaves.isEmpty()) {
                    if (nextPtr != NULL) { throw new AssertionError("Non-null next ptr for last leaf"); }
                } else {
                    if (nextPtr != visitedLeaves.getLast()) { throw new AssertionError("Wrong next ptr for leaf"); }
                }
            }
            visitedLeaves.add(node);
        } else {
            long count = getBranchItemCount(node);
            if (count <= 0) { throw new AssertionError("Invalid branch item count"); }
            if (count > branchCapacity) { throw new AssertionError("Branch over capacity"); }
            for (long i = count - 1; i >= 0; i--) {
                max = checkInvariants(
                        max,
                        readBranchNextPointer(node, i),
                        level + 1,
                        visitedLeaves,
                        visitedNodes
                );

                long key = readBranchKey(node, i);
                if (key > (entryMustBeInLeaf ? max + 1 : max)) {
                    throw new AssertionError("Sort order not maintained");
                }
                max = key - 1;
                if (!entryMustBeInLeaf) {
                    readBranchValue(node, i);
                }
            }
            max = checkInvariants(
                    max,
                    readBranchPrevPointer(node, 0),
                    level + 1,
                    visitedLeaves,
                    visitedNodes
            );
        }
        return max;
    }

    @DoNotMutate
    String toStringFlat() {
        StringBuilder builder = new StringBuilder("[");
        try (Cursor iterator = new Cursor()) {
            iterator.descendToStart();
            boolean first = true;
            while (iterator.next()) {
                if (first) {
                    first = false;
                } else {
                    builder.append(", ");
                }
                builder.append(iterator.getKey()).append("->").append(iterator.getValue());
            }
            return builder.append(']').toString();
        }
    }

    @DoNotMutate
    String[] toStringBlocksHex() {
        String[] array = new String[Math.toIntExact(buf.size() / blockSize)];
        for (int i = 0; i < array.length; i++) {
            StringBuilder builder = new StringBuilder(blockSize * 2);
            for (int j = 0; j < blockSize; j++) {
                int b = buf.getByte((long) i * blockSize + j) & 0xff;
                if (b <= 0xf) { builder.append('0'); }
                builder.append(Integer.toHexString(b));
                if (j % 4 == 3) { builder.append(' '); }
            }
            array[i] = builder.toString();
        }
        return array;
    }

    @SuppressWarnings("unused")
    @DoNotMutate
    String[] toStringBlocks() {
        String[] array = new String[Math.toIntExact(buf.size() / blockSize)];
        if (rootPtr != NULL) {
            toStringBlocks(array, rootPtr, 0);
        }
        return array;
    }

    @DoNotMutate
    private void toStringBlocks(String[] array, long block, int level) {
        if (block >= array.length || block < 0) { return; }
        StringBuilder sb = new StringBuilder();
        if (level == levelCount - 1) {
            sb.append("Leaf [");
            for (long i = 0; i < getLeafItemCount(block); i++) {
                if (i != 0) { sb.append(", "); }
                sb.append(readLeafKey(block, i)).append("->").append(readLeafValue(block, i));
            }
            sb.append(']');
            if (storeNextPointer) {
                sb.append(" next: &").append(getNextLeafPtr(block));
            }
        } else {
            sb.append(level == 0 ? "Root" : "Branch").append(" [");
            for (long i = 0; i < getBranchItemCount(block); i++) {
                if (i == 0) {
                    long prev = readBranchPrevPointer(block, 0);
                    toStringBlocks(array, prev, level + 1);
                    sb.append('&').append(prev);
                }
                sb.append(' ').append(readBranchKey(block, i));
                if (!entryMustBeInLeaf) {
                    sb.append("->").append(readBranchValue(block, i));
                }
                long next = readBranchNextPointer(block, i);
                toStringBlocks(array, next, level + 1);
                sb.append(' ').append('&').append(next);
            }
            sb.append(']');
        }
        array[Math.toIntExact(block)] = sb.toString();
    }

    // accessors

    private void checkLeafIndex(long index) {
        // allow + 1
        if (index < 0 || index > leafCapacity) { throw new IllegalArgumentException(); }
    }

    private void checkBranchIndex(long index) {
        // allow + 1
        if (index < 0 || index > branchCapacity) { throw new IllegalArgumentException(); }
    }

    private long branchValueIndex(long blockPtr, long index) {
        return baseAddress(blockPtr) + pointerSize + index * (branchEntrySize + pointerSize);
    }

    private long readBranchKey(long blockPtr, long index) {
        checkBranchIndex(index);
        return readBranchKey(buf, branchValueIndex(blockPtr, index));
    }

    private long readBranchValue(long blockPtr, long index) {
        checkBranchIndex(index);
        return readBranchValue(buf, branchValueIndex(blockPtr, index));
    }

    private void writeBranchEntry(long blockPtr, long index, long key, long value) {
        checkBranchIndex(index);
        writeBranchEntry(buf, branchValueIndex(blockPtr, index), key, value);
    }

    private long readBranchNextPointer(long blockPtr, long index) {
        return readBranchPrevPointer(blockPtr, index + 1);
    }

    private long readBranchPrevPointer(long blockPtr, long index) {
        return getPtr(branchValueIndex(blockPtr, index) - pointerSize);
    }

    private void writeBranchPrevPointer(long blockPtr, long index, long ptr) {
        uset(branchValueIndex(blockPtr, index) - pointerSize, pointerSize, ptr);
    }

    private long leafValueIndex(long blockPtr, long index) {
        return baseAddress(blockPtr) + index * leafEntrySize;
    }

    private long readLeafKey(long blockPtr, long index) {
        checkLeafIndex(index);
        return readLeafKey(buf, leafValueIndex(blockPtr, index));
    }

    private long readLeafValue(long blockPtr, long index) {
        checkLeafIndex(index);
        return readLeafValue(buf, leafValueIndex(blockPtr, index));
    }

    private void writeLeafEntry(long blockPtr, long index, long key, long value) {
        checkLeafIndex(index);
        writeLeafEntry(buf, leafValueIndex(blockPtr, index), key, value);
    }

    // entry-specific methods

    protected int compare(long lhsKey, long rhsKey) {
        return Long.compare(lhsKey, rhsKey);
    }

    protected abstract void writeBranchEntry(LargeByteBuffer lbb, long address, long key, long value);

    protected abstract void writeLeafEntry(LargeByteBuffer lbb, long address, long key, long value);

    protected abstract long readBranchKey(LargeByteBuffer lbb, long address);

    protected abstract long readBranchValue(LargeByteBuffer lbb, long address);

    protected abstract long readLeafKey(LargeByteBuffer lbb, long address);

    protected abstract long readLeafValue(LargeByteBuffer lbb, long address);

    public class Cursor implements Closeable {
        long[] trace;
        long[] trace2;
        long[] traceIndex;
        long[] traceIndex2;
        int level;
        int level2;

        boolean marked;

        private Cursor() {
            init();
        }

        private void init() {
            if (marked) { throw new IllegalStateException(); }
            if (trace == null || trace.length != levelCount) {
                trace = new long[levelCount];
                trace2 = new long[levelCount];
                traceIndex = new long[levelCount];
                traceIndex2 = new long[levelCount];
            }
            level = -1;
        }

        private void mark() {
            if (marked) { throw new IllegalStateException(); }
            System.arraycopy(trace, 0, trace2, 0, levelCount);
            System.arraycopy(traceIndex, 0, traceIndex2, 0, levelCount);
            level2 = level;
            marked = true;
        }

        /**
         * Does not discard the mark
         */
        private void resetToMark() {
            if (!marked) { throw new IllegalStateException(); }
            System.arraycopy(trace2, 0, trace, 0, levelCount);
            System.arraycopy(traceIndex2, 0, traceIndex, 0, levelCount);
            level = level2;
        }

        private void discardMark() {
            if (!marked) { throw new IllegalStateException(); }
            marked = false;
        }

        private boolean inLeaf() {
            return level == levelCount - 1;
        }

        private long getItemCount() {
            return inLeaf() ? getLeafItemCount(trace[level]) : getBranchItemCount(trace[level]);
        }

        private long getCapacity() {
            return inLeaf() ? leafCapacity : branchCapacity;
        }

        private long getPreviousBlockPtr() {
            if (inLeaf()) { throw new IllegalStateException(); }
            long ix = this.traceIndex[level];
            if (ix < 0) { throw new IllegalStateException(); }
            return readBranchPrevPointer(trace[level], ix);
        }

        private long getNextBlockPtr() {
            if (inLeaf()) { throw new IllegalStateException(); }
            long ix = this.traceIndex[level];
            if (ix < -1) { throw new IllegalStateException(); }
            return readBranchNextPointer(trace[level], ix);
        }

        public void descendToKey(long key) {
            while (!inLeaf() && (level == -1 || traceIndex[level] < 0 || entryMustBeInLeaf)) {
                if (level == -1) {
                    level = 0;
                    trace[0] = rootPtr;
                } else {
                    level++;
                    long ix = traceIndex[level - 1];
                    if (ix < 0) {
                        trace[level] = readBranchPrevPointer(trace[level - 1], ~ix);
                    } else {
                        if (!entryMustBeInLeaf) { throw new AssertionError(); }
                        trace[level] = readBranchNextPointer(trace[level - 1], ix);
                    }
                }
                traceIndex[level] = blockSearch(inLeaf(), trace[level], key);
            }
        }

        private void descendToImmediateLeftLeaf() {
            if (!elementFound()) { throw new IllegalStateException(); }
            while (!inLeaf()) {
                trace[level + 1] = getPreviousBlockPtr();
                traceIndex[level] = ~traceIndex[level];
                level++;
                traceIndex[level] = getItemCount();
            }
            // insertion index for last element of leaf
            traceIndex[level] = ~traceIndex[level];
        }

        private void descendToImmediateRightLeaf() {
            if (level < -1) { throw new IllegalStateException(); }
            while (!inLeaf()) {
                trace[level + 1] = level == -1 ? rootPtr : getNextBlockPtr();
                if (level >= 0) { traceIndex[level] = ~(traceIndex[level] + 1); }
                level++;
                traceIndex[level] = -1;
            }
        }

        /**
         * Descend to before the start of the first leaf. After this call the tree can be iterated using
         * {@link #next()}.
         */
        public void descendToStart() {
            if (level != -1) { throw new IllegalStateException(); }
            descendToImmediateRightLeaf();
        }

        public boolean elementFound() {
            return level >= 0 && traceIndex[level] >= 0;
        }

        public void simpleInsert(long key, long value) {
            if (level == -1) {
                if (levelCount != 0) { throw new IllegalStateException(); }
                // first element in the tree
                rootPtr = allocatePage();
                if (storeNextPointer) {
                    setNextLeafPtr(rootPtr, NULL);
                }
                setLeafItemCount(rootPtr, 1);
                writeLeafEntry(rootPtr, 0, key, value);
                levelCount = 1;

                init();
                this.level = 0;
                this.trace[0] = rootPtr;
                this.traceIndex[0] = 0;
                return;
            }

            if (!inLeaf()) { throw new IllegalStateException(); }
            if (elementFound()) { throw new IllegalStateException(); }
            simpleLeafInsert(trace[level], ~traceIndex[level], key, value);
        }

        public void update(long key, long value) {
            if (!elementFound()) { throw new IllegalStateException(); }
            if (inLeaf()) {
                writeLeafEntry(trace[level], traceIndex[level], key, value);
            } else {
                writeBranchEntry(trace[level], traceIndex[level], key, value);
            }
        }

        public long getKey() {
            if (traceIndex[level] < 0 || traceIndex[level] >= getItemCount()) { throw new IllegalStateException(); }
            return inLeaf() ? readLeafKey(trace[level], traceIndex[level]) :
                    readBranchKey(trace[level], traceIndex[level]);
        }

        public long getValue() {
            if (traceIndex[level] < 0 || traceIndex[level] >= getItemCount()) { throw new IllegalStateException(); }
            return inLeaf() ?
                    readLeafValue(trace[level], traceIndex[level]) :
                    readBranchValue(trace[level], traceIndex[level]);
        }

        private long getValueOr0() {
            return entryMustBeInLeaf ? 0 : getValue();
        }

        public void setValue(long value) {
            long key = getKey();
            if (inLeaf()) {
                writeLeafEntry(trace[level], traceIndex[level], key, value);
            } else {
                writeBranchEntry(trace[level], traceIndex[level], key, value);
            }
        }

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        private boolean ascendToNextParent() {
            level--;
            if (level < 0) { return false; }
            if (this.traceIndex[level] < 0) {
                this.traceIndex[level] = ~this.traceIndex[level];
            } else {
                if (!entryMustBeInLeaf) { throw new IllegalStateException(); }
                this.traceIndex[level]++;
            }
            return true;
        }

        private boolean jumpToNextLeaf() {
            if (!inLeaf()) { throw new IllegalStateException(); }
            if (storeNextPointer) {
                long nextLeafPtr = getNextLeafPtr(trace[level]);
                if (nextLeafPtr == NULL) {
                    return false;
                } else {
                    trace[level] = nextLeafPtr;
                    traceIndex[level] = -1;
                    return true;
                }
            } else {
                do {
                    if (!ascendToNextParent()) { return false; }
                } while (this.traceIndex[level] >= getItemCount());
                descendToImmediateRightLeaf();
                return true;
            }
        }

        private boolean jumpToPreviousLeaf() {
            if (!inLeaf()) { throw new IllegalStateException(); }
            do {
                if (!ascendToNextParent()) { return false; }
            } while (this.traceIndex[level] == 0);
            this.traceIndex[level]--;
            descendToImmediateLeftLeaf();
            return true;
        }

        public boolean next() {
            if (levelCount == 0) {
                return false;
            }

            if (inLeaf()) {
                traceIndex[level]++;
                if (traceIndex[level] < getItemCount()) {
                    if (!elementFound()) { throw new AssertionError(); }
                    return true;
                }
                if (entryMustBeInLeaf) {
                    boolean more = jumpToNextLeaf();
                    if (more) {
                        // select first item in new leaf
                        traceIndex[level]++;
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    // ascend to next pivot
                    do {
                        if (level <= 0) {
                            return false;
                        }
                        level--;
                        if (traceIndex[level] >= 0) { throw new IllegalStateException(); }
                    } while (~traceIndex[level] >= getItemCount());
                    traceIndex[level] = ~traceIndex[level];
                    if (!elementFound()) { throw new AssertionError(); }
                    return true;
                }
            } else {
                if (traceIndex[level] < -1) { throw new IllegalStateException(); }
                // either just visited a pivot, or we're in some odd initial state
                if (traceIndex[level] >= getItemCount()) {
                    return false;
                } else {
                    descendToImmediateRightLeaf();
                    if (!inLeaf()) { throw new AssertionError(); }
                    traceIndex[level]++; // select first item in leaf
                    if (!elementFound()) { throw new AssertionError(); }
                    return true;
                }
            }
        }

        public void simpleRemove() {
            if (inLeaf()) {
                simpleLeafRemoveAt(trace[level], traceIndex[level]);
                if (entryMustBeInLeaf && traceIndex[level] == 0) {
                    if (getItemCount() == 0) {
                        // underflow, can't replace pivot!
                        return;
                    }
                    // update parents
                    for (int i = 0; i < level; i++) {
                        if (traceIndex[i] >= 0) {
                            traceIndex[i] = ~(traceIndex[i] + 1);
                        }
                    }
                }
            } else {
                if (entryMustBeInLeaf) {
                    throw new UnsupportedOperationException();
                }

                int removalLevel = level;
                long removalIndex = traceIndex[level];

                descendToImmediateLeftLeaf();
                long leftCount = getItemCount();

                // go back up
                mark();
                level = removalLevel;
                traceIndex[level] = removalIndex;

                descendToImmediateRightLeaf();
                long rightCount = getItemCount();

                boolean pickLeft = rightCount < leftCount;
                if (pickLeft) {
                    // go back to left
                    resetToMark();
                    // select last element in leaf
                    traceIndex[level] = getItemCount() - 1;
                } else {
                    // stay on right, select first element in leaf
                    traceIndex[level] = 0;
                }
                discardMark();

                // overwrite pivot
                writeBranchEntry(trace[removalLevel], removalIndex, getKey(), getValue());
                // remove leaf item
                simpleRemove();

                // update index of branch depending on which leaf we are now in
                if (pickLeft) {
                    traceIndex[removalLevel] = ~removalIndex;
                } else {
                    traceIndex[removalLevel] = ~(removalIndex + 1);
                }
            }
        }

        public void balance() {
            while (level >= 0) {
                // for debugging
                assert checkCursorInvariants();

                long itemCount = getItemCount();
                if (itemCount > getCapacity()) {
                    if (itemCount > getCapacity() + 1) { throw new AssertionError(); }
                    splitBlock();
                } else if (itemCount == 0) {
                    mergeBlock();
                } else {
                    level--;
                }
            }
        }

        private void splitBlock() {
            // select pivot element
            traceIndex[level] = getCapacity() / 2;
            long pivotKey = getKey();
            long pivotValue = getValueOr0();
            long left = trace[level];
            long right = inLeaf() ?
                    splitLeafBlock(left, traceIndex[level]) :
                    splitBranchBlock(left, traceIndex[level]);
            if (level == 0) {
                rootPtr = allocatePage();
                setBranchItemCount(rootPtr, 1);
                writeBranchEntry(rootPtr, 0, pivotKey, pivotValue);
                writeBranchPrevPointer(rootPtr, 0, left);
                writeBranchPrevPointer(rootPtr, 1, right);
                levelCount++;
                init();
            } else {
                long index;
                if (traceIndex[level - 1] < 0) {
                    index = ~traceIndex[level - 1];
                } else {
                    index = traceIndex[level - 1] + 1;
                }
                simpleBranchInsert(trace[level - 1], index, pivotKey, pivotValue, left, right);
                level--;
            }
        }

        private void mergeBlock() {
            if (getItemCount() != 0) { throw new UnsupportedOperationException(); }

            if (level == 0) {
                if (inLeaf()) {
                    clear();
                    init();
                } else {
                    traceIndex[0] = 0;
                    long onlyChild = getPreviousBlockPtr();
                    freePage(rootPtr);
                    rootPtr = onlyChild;
                    levelCount--;
                    level--;
                }
                return;
            }

            if (inLeaf() && storeNextPointer) {
                long next = getNextLeafPtr(trace[level]);
                mark();
                if (jumpToPreviousLeaf()) {
                    setNextLeafPtr(trace[level], next);
                }
                resetToMark();
                discardMark();
            }

            // child ptr to be added to one of the neighbour branches
            long remainingChild;
            if (inLeaf()) {
                remainingChild = NULL;
            } else {
                traceIndex[level] = 0;
                remainingChild = getPreviousBlockPtr();
            }
            freePage(trace[level]);
            level--;
            if (traceIndex[level] < 0) {
                traceIndex[level] = (~traceIndex[level]) - 1;
            }
            long pivotIndex = traceIndex[level];
            // pivotIndex is the index of the pivot before the child that is being removed.
            // pivotIndex may be -1 if it's the first child of the parent.

            boolean deletedFirstChild = pivotIndex == -1;
            boolean deletedLastChild = pivotIndex == getItemCount() - 1;
            if (level >= levelCount - 2) {
                // the deleted child is a leaf.
                if (remainingChild != NULL) { throw new AssertionError(); }

                if (deletedFirstChild) {
                    traceIndex[level] = pivotIndex = 0;
                }

                // save the pivot
                long pivotKey;
                long pivotValue;
                if (entryMustBeInLeaf) {
                    pivotKey = pivotValue = -1;
                } else {
                    pivotKey = getKey();
                    pivotValue = getValue();
                }
                // overwrite our entry
                copyBranchEntries(
                        trace[level],
                        pivotIndex + 1,
                        trace[level],
                        pivotIndex,
                        getItemCount() - pivotIndex - 1,
                        deletedFirstChild // possibly overwrite first pointer in block
                );
                setBranchItemCount(trace[level], getItemCount() - 1);

                if (entryMustBeInLeaf) {
                    traceIndex[level] = ~pivotIndex;
                } else {
                    // add pivot to next leaf
                    if (deletedFirstChild) {
                        traceIndex[level]--;
                        // add to the leftmost leaf as first child
                        descendToImmediateRightLeaf();
                        traceIndex[level] = ~0; // insertion index for first item
                        simpleInsert(pivotKey, pivotValue);
                    } else if (pivotIndex == getItemCount()) {
                        // deleted last child, add to new rightmost leaf as last child.
                        // pivotIndex is after the last child, so descend to left and then as right as possible.
                        descendToImmediateLeftLeaf();
                        traceIndex[level] = ~getItemCount(); // insertion index for last item
                        simpleInsert(pivotKey, pivotValue);
                    } else {
                        int pivotLevel = level;

                        descendToImmediateLeftLeaf();
                        long leftCount = getItemCount();

                        mark();
                        level = pivotLevel;
                        traceIndex[level] = pivotIndex;
                        descendToImmediateRightLeaf();

                        long rightCount = getItemCount();
                        if (rightCount <= leftCount) {
                            // swap with new pivot
                            discardMark();
                            // mark right leaf
                            mark();
                            level = pivotLevel;
                            traceIndex[level] = pivotIndex;
                            long newPivotKey = getKey();
                            long newPivotValue = getValue();
                            writeBranchEntry(trace[pivotLevel], pivotIndex, pivotKey, pivotValue);
                            pivotKey = newPivotKey;
                            pivotValue = newPivotValue;
                        }
                        // go back to leaf (right if we entered the if, left if we didn't)
                        resetToMark();
                        discardMark();

                        simpleInsert(pivotKey, pivotValue);
                        // selected leaf might now overflow, checked in loop
                    }
                }
            } else {
                // the deleted child is a branch.
                if (remainingChild == NULL) { throw new AssertionError(); }

                boolean mergeWithLeftNeighbour;
                if (deletedFirstChild) {
                    mergeWithLeftNeighbour = false;
                } else if (deletedLastChild) {
                    mergeWithLeftNeighbour = true;
                } else {
                    long leftCount = getBranchItemCount(getPreviousBlockPtr());
                    traceIndex[level]++;
                    long rightCount = getBranchItemCount(getNextBlockPtr());
                    traceIndex[level]--;
                    mergeWithLeftNeighbour = leftCount <= rightCount;
                }

                // select the pivot we're deleting
                if (!mergeWithLeftNeighbour) {
                    traceIndex[level]++;
                }
                // save pivot
                long pivotKey = getKey();
                long pivotValue = getValueOr0();
                // overwrite pivot and reference to deleted child
                copyBranchEntries(
                        trace[level],
                        traceIndex[level] + 1,
                        trace[level],
                        traceIndex[level],
                        getItemCount() - traceIndex[level] - 1,
                        // replace prev pointer of the pivot iff we're merging with the right side
                        !mergeWithLeftNeighbour
                );
                setBranchItemCount(trace[level], getItemCount() - 1);
                // branch we're merging with is now left of us.
                // enter that branch
                trace[level + 1] = getPreviousBlockPtr();
                // set our index to be the insertion index
                traceIndex[level] = ~traceIndex[level];
                level++;

                if (mergeWithLeftNeighbour) {
                    // add to end of left neighbour
                    traceIndex[level] = getItemCount(); // insertion index for last item
                } else {
                    // add to start of right neighbour
                    traceIndex[level] = 0; // insertion index for first item
                }
                simpleBranchInsert(
                        trace[level],
                        traceIndex[level],
                        pivotKey,
                        pivotValue,
                        mergeWithLeftNeighbour ? getPreviousBlockPtr() : remainingChild,
                        mergeWithLeftNeighbour ? remainingChild : getPreviousBlockPtr()
                );
                if (mergeWithLeftNeighbour) {
                    traceIndex[level] = ~getItemCount(); // insertion index for last item
                } else {
                    traceIndex[level] = ~0; // insertion index for first item
                }
            }
        }

        @DoNotMutate
        private boolean checkCursorInvariants() {
            boolean hasMatchedKey = false;
            long key = 0;
            for (int i = 0; i < level; i++) {
                if (traceIndex[i] >= 0) {
                    long matchedKey = readBranchKey(trace[i], traceIndex[i]);
                    if (hasMatchedKey) {
                        throw new AssertionError();
                    } else {
                        key = matchedKey;
                        hasMatchedKey = true;
                    }
                }
            }
            if (hasMatchedKey) {
                if (entryMustBeInLeaf) {
                    if (elementFound() && traceIndex[level] < getItemCount() && getKey() != key) {
                        throw new AssertionError();
                    }
                } else {
                    if (elementFound()) { throw new AssertionError(); }
                }
            }
            if (levelCount != trace.length || levelCount != traceIndex.length ||
                levelCount != trace2.length || levelCount != traceIndex2.length) {
                throw new AssertionError();
            }
            if (level >= levelCount || level < -1) {
                throw new AssertionError();
            }
            return true;
        }

        /**
         * Marks this cursor for possible reuse.
         */
        @Override
        public void close() {
            init();
            reuseCursor.set(this);
        }
    }
}
