package at.yawk.numaec;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public final class BTreeTest {
    private static final LargeByteBufferAllocator SIMPLE_ALLOCATOR = size -> {
        int sizeInt = Math.toIntExact(size);
        return new ByteBufferBackedLargeByteBuffer(new ByteBuffer[]{ ByteBuffer.allocate(sizeInt) }, 0x1000000);
    };

    private List<BTreeConfig> configList() {
        return Arrays.asList(
                BTreeConfig.builder().blockSize(64).storeNextPointer(false).entryMustBeInLeaf(true).build(),
                BTreeConfig.builder().blockSize(64).storeNextPointer(true).entryMustBeInLeaf(true).build(),
                BTreeConfig.builder().blockSize(64).storeNextPointer(false).entryMustBeInLeaf(false).build(),
                BTreeConfig.builder().blockSize(64).storeNextPointer(true).entryMustBeInLeaf(false).build(),
                BTreeConfig.builder().blockSize(128).storeNextPointer(false).entryMustBeInLeaf(true).build(),
                BTreeConfig.builder().blockSize(128).storeNextPointer(true).entryMustBeInLeaf(true).build(),
                BTreeConfig.builder().blockSize(128).storeNextPointer(false).entryMustBeInLeaf(false).build(),
                BTreeConfig.builder().blockSize(128).storeNextPointer(true).entryMustBeInLeaf(false).build()
        );
    }

    private LongList keysToList(BTree bTree) {
        MutableLongList list = LongLists.mutable.empty();
        try (BTree.Cursor iterator = bTree.allocateCursor()) {
            iterator.descendToStart();
            while (iterator.next()) {
                list.add(iterator.getKey());
            }
        }
        return list;
    }
    
    private void insert(BTree bTree, long key, long value) {
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToKey(key);
            Assert.assertFalse(cursor.elementFound());
            cursor.simpleInsert(key, value);
            cursor.balance();
        }
    }

    private void replace(BTree bTree, long key, long value) {
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToKey(key);
            Assert.assertTrue(cursor.elementFound());
            cursor.setValue(value);
        }
    }

    private long remove(BTree bTree, long key) {
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToKey(key);
            Assert.assertTrue(cursor.elementFound());
            long value = cursor.getValue();
            cursor.simpleRemove();
            cursor.balance();
            return value;
        }
    }

    private boolean isPresent(BTree bTree, long key) {
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToKey(key);
            return cursor.elementFound();
        }
    }
    
    private long findValue(BTree bTree, long key, LongSupplier defaultValue) {
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToKey(key);
            if (cursor.elementFound()) {
                return cursor.getValue();
            } else {
                return defaultValue.getAsLong();
            }
        }
    }

    @DataProvider
    public Object[][] config() {
        return configList().stream().map(c -> new Object[]{ c }).toArray(Object[][]::new);
    }

    @DataProvider
    public Object[][] configRandom() {
        List<BTreeConfig> configs = configList();
        List<Integer> seeds = IntStream.range(0, 20).boxed().collect(Collectors.toList());
        int i = 0;
        Object[][] out = new Object[configs.size() * seeds.size()][];
        for (BTreeConfig config : configs) {
            for (Integer seed : seeds) {
                out[i++] = new Object[]{
                        config, new Random(seed) {
                    @Override
                    public String toString() {
                        return "Random seed = " + seed;
                    }
                } };
            }
        }
        return out;
    }

    @Test(dataProvider = "config", timeOut = 10)
    public void empty(BTreeConfig config) {
        BTree bTree = new BTreeImpl(config);
        bTree.checkInvariants();
        Assert.assertEquals(bTree.toStringFlat(), "[]");
    }

    @Test(dataProvider = "config", timeOut = 10)
    public void singleton(BTreeConfig config) {
        BTree bTree = new BTreeImpl(config);
        insert(bTree, 1, 2);
        bTree.checkInvariants();
        Assert.assertEquals(bTree.toStringFlat(), "[1->2]");
    }

    @Test(dataProvider = "config", timeOut = 50)
    public void sort(BTreeConfig config) {
        BTree bTree = new BTreeImpl(config);
        insert(bTree, 1, 2);
        bTree.checkInvariants();
        insert(bTree, 3, 4);
        bTree.checkInvariants();
        insert(bTree, 0, 8);
        bTree.checkInvariants();
        insert(bTree, 2, 1);
        bTree.checkInvariants();
        replace(bTree, 1, 3);
        bTree.checkInvariants();
        Assert.assertEquals(bTree.toStringFlat(), "[0->8, 1->3, 2->1, 3->4]");
    }

    @Test(dataProvider = "config", timeOut = 100)
    public void resize(BTreeConfig config) {
        // trigger at least one buffer resize
        BTree bTree = new BTreeImpl(config);
        int len = -1;
        for (int i = 0; ; i++) {
            insert(bTree, i, i);
            bTree.checkInvariants();
            int newLen = bTree.toStringBlocksHex().length;
            if (len != -1 && len != newLen) { break; }
            len = newLen;
        }
    }

    @Test(dataProvider = "config")
    public void find(BTreeConfig config) {
        BTree bTree = new BTreeImpl(config);
        for (int i = 0; i < 1000; i++) {
            // test default value
            long l = ThreadLocalRandom.current().nextLong();
            Assert.assertEquals(findValue(bTree, i, () -> l), l);

            insert(bTree, i, i);
            bTree.checkInvariants();
        }
        for (int i = 0; i < 1000; i++) {
            Assert.assertEquals(findValue(bTree, i, () -> {
                throw new AssertionError();
            }), i);
        }
    }

    @Test(dataProvider = "config")
    public void iterate(BTreeConfig config) {
        BTree bTree = new BTreeImpl(config);
        MutableLongList expected = LongLists.mutable.empty();
        for (int i = 0; i < 1000; i++) {
            // test default value
            long l = ThreadLocalRandom.current().nextLong();
            Assert.assertEquals(findValue(bTree, i, () -> l), l);

            insert(bTree, i, i);
            expected.add(i);
            bTree.checkInvariants();

            Assert.assertEquals(keysToList(bTree), expected);
        }
    }

    @Test(dataProvider = "config")
    public void replace(BTreeConfig config) {
        BTree bTree = new BTreeImpl(config);
        for (int i = 0; i < 1000; i++) {
            insert(bTree, i, i);
            bTree.checkInvariants();
        }
        for (int i = 0; i < 1000; i++) {
            replace(bTree, i, i + 1);
            bTree.checkInvariants();
        }
        for (int i = 0; i < 1000; i++) {
            Assert.assertEquals(findValue(bTree, i, () -> {
                throw new AssertionError();
            }), i + 1);
        }
    }

    @Test(dataProvider = "config")
    public void clear(BTreeConfig config) {
        BTree bTree = new BTreeImpl(config);
        for (int i = 0; i < 1000; i++) {
            insert(bTree, i, i);
            bTree.checkInvariants();
        }
        bTree.clear();
        for (int i = 0; i < 1000; i++) {
            long l = ThreadLocalRandom.current().nextLong();
            Assert.assertEquals(findValue(bTree, i, () -> l), l);
        }
        for (int i = 0; i < 1000; i++) {
            insert(bTree, i, i);
            bTree.checkInvariants();
        }
    }

    @Test(dataProvider = "config")
    public void removeStart(BTreeConfig config) {
        BTree bTree = new BTreeImpl(config);
        for (int i = 0; i < 1000; i++) {
            insert(bTree, i, i);
            bTree.checkInvariants();
        }
        for (int i = 0; i < 1000; i++) {
            Assert.assertEquals(remove(bTree, i), i);
            bTree.checkInvariants();
            Assert.assertFalse(isPresent(bTree, i));
        }
    }

    @Test(dataProvider = "config")
    public void removeEnd(BTreeConfig config) {
        BTree bTree = new BTreeImpl(config);
        for (int i = 0; i < 1000; i++) {
            insert(bTree, i, i);
            bTree.checkInvariants();
        }
        for (int i = 1000 - 1; i >= 0; i--) {
            Assert.assertEquals(remove(bTree, i), i);
            bTree.checkInvariants();
        }
    }

    @Test(dataProvider = "config")
    public void removeSecondLeaf(BTreeConfig config) {
        // create trees of depth 3 and remove all keys starting at a cutoff point
        for (int cutoff = 0; ; cutoff++) {
            BTree bTree = new BTreeImpl(config);
            int n = 0;
            for (; bTree.levelCount < 3; n++) {
                insert(bTree, n, n);
                bTree.checkInvariants();
            }
            if (n < cutoff) { break; }
            for (int i = cutoff; i < n; i++) {
                Assert.assertEquals(remove(bTree, i), i);
                bTree.checkInvariants();
            }
        }
    }

    @Test(dataProvider = "configRandom")
    public void addRandom(BTreeConfig config, Random rng) {
        BTree bTree = new BTreeImpl(config);
        List<Integer> indices = IntStream.range(0, 1000).boxed().collect(Collectors.toList());
        Collections.shuffle(indices, rng);
        for (Integer i : indices) {
            insert(bTree, i, i);
            bTree.checkInvariants();
        }
    }

    @Test(dataProvider = "configRandom")
    public void removeRandom(BTreeConfig config, Random rng) {
        BTree bTree = new BTreeImpl(config);
        List<Integer> indices = IntStream.range(0, 1000).boxed().collect(Collectors.toList());
        Collections.shuffle(indices, rng);
        for (Integer i : indices) {
            insert(bTree, i, i);
            bTree.checkInvariants();
        }
        Collections.shuffle(indices, rng);
        for (Integer i : indices) {
            Assert.assertEquals(remove(bTree, i), i.longValue());
            bTree.checkInvariants();
        }
    }

    @Test(dataProvider = "config")
    public void closesAfterResize(BTreeConfig config) {
        AtomicInteger openCount = new AtomicInteger();
        BTree bTree = new BTreeImpl(size -> {
            openCount.incrementAndGet();
            return new ByteBufferBackedLargeByteBuffer(new ByteBuffer[]{ ByteBuffer.allocate(Math.toIntExact(size)) },
                                                       0x1000000) {
                @Override
                public void close() {
                    openCount.decrementAndGet();
                }
            };
        }, config);
        // trigger at least one buffer resize
        int len = -1;
        for (int i = 0; ; i++) {
            insert(bTree, i, i);
            bTree.checkInvariants();
            Assert.assertEquals(openCount.get(), 1);
            int newLen = bTree.toStringBlocksHex().length;
            if (len != -1 && len != newLen) { break; }
            len = newLen;
        }
    }

    @Test(dataProvider = "config")
    public void doesNotCloseAfterRealloc(BTreeConfig config) {
        AtomicInteger openCount = new AtomicInteger();
        class BufImpl extends ByteBufferBackedLargeByteBuffer {
            public BufImpl(long size) {
                super(new ByteBuffer[]{ ByteBuffer.allocate(Math.toIntExact(size)) }, 0x1000000);
            }

            @Override
            public void close() {
                Assert.fail();
            }

            @Override
            public LargeByteBuffer reallocate(long newSize) {
                BufImpl reallocated = new BufImpl(newSize);
                reallocated.copyFrom(this, 0, 0, this.size());
                return reallocated;
            }
        }
        BTree bTree = new BTreeImpl(size -> {
            openCount.incrementAndGet();
            return new BufImpl(size);
        }, config);
        // trigger at least one buffer resize
        int len = -1;
        for (int i = 0; ; i++) {
            insert(bTree, i, i);
            bTree.checkInvariants();
            Assert.assertEquals(openCount.get(), 1);
            int newLen = bTree.toStringBlocksHex().length;
            if (len != -1 && len != newLen) { break; }
            len = newLen;
        }
    }

    @Test(expectedExceptions = IllegalStateException.class,
            // check exact message so we don't run into one of the other ISEs
            expectedExceptionsMessageRegExp = BTree.MESSAGE_POINTER_TOO_SMALL)
    public void failsOnTooManyPages() {
        BTreeConfig config = BTreeConfig.builder()
                .blockSize(64)
                .pointerSize(1)
                .build();
        BTreeImpl bTree = new BTreeImpl(config);
        int maxEntries = 64 * 256 / 6;
        for (int i = 0; i < maxEntries; i++) {
            insert(bTree, i, i & 0xff);
            bTree.checkInvariants();
        }
    }

    /**
     * Keys: 4 byte, values: 2 byte, both unsigned
     */
    private static class BTreeImpl extends BTree {
        // masks that are xord onto keys and values to make storage corruption easier to detect
        private static int mask() {
            //return 0;
            return ThreadLocalRandom.current().nextInt();
        }

        private final int branchKeyMask = mask();
        private final short branchValueMask = (short) mask();
        private final int leafKeyMask = mask();
        private final short leafValueMask = (short) mask();

        private final boolean entryMustBeInLeaf;

        BTreeImpl(BTreeConfig config) {
            this(SIMPLE_ALLOCATOR, config);
        }

        BTreeImpl(LargeByteBufferAllocator allocator, BTreeConfig config) {
            super(allocator, config, config.entryMustBeInLeaf ? 4 : 6, 6);
            entryMustBeInLeaf = config.entryMustBeInLeaf;
        }

        @Override
        protected void writeBranchEntry(LargeByteBuffer lbb, long address, long key, long value) {
            Assert.assertTrue(key >= 0);
            Assert.assertTrue(key < 0x100000000L);
            Assert.assertTrue(value >= 0);
            Assert.assertTrue(value < 0x10000L);
            lbb.setInt(address, (int) (key ^ branchKeyMask));
            if (!entryMustBeInLeaf) {
                lbb.setShort(address + 4, (short) (value ^ branchValueMask));
            }
        }

        @Override
        protected void writeLeafEntry(LargeByteBuffer lbb, long address, long key, long value) {
            Assert.assertTrue(key >= 0);
            Assert.assertTrue(key < 0x100000000L);
            Assert.assertTrue(value >= 0);
            Assert.assertTrue(value < 0x10000L);
            lbb.setInt(address, (int) (key ^ leafKeyMask));
            lbb.setShort(address + 4, (short) (value ^ leafValueMask));
        }

        @Override
        protected long readBranchKey(LargeByteBuffer lbb, long address) {
            return Integer.toUnsignedLong(lbb.getInt(address) ^ branchKeyMask);
        }

        @Override
        protected long readBranchValue(LargeByteBuffer lbb, long address) {
            if (entryMustBeInLeaf) {
                Assert.fail();
                return 0;
            } else {
                return Short.toUnsignedLong((short) (lbb.getShort(address + 4) ^ branchValueMask));
            }
        }

        @Override
        protected long readLeafKey(LargeByteBuffer lbb, long address) {
            return Integer.toUnsignedLong(lbb.getInt(address) ^ leafKeyMask);
        }

        @Override
        protected long readLeafValue(LargeByteBuffer lbb, long address) {
            return Short.toUnsignedLong((short) (lbb.getShort(address + 4) ^ leafValueMask));
        }
    }
}
