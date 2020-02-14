package at.yawk.numaec;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.impl.factory.primitive.LongLongMaps;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public final class LinearHashTableTest {
    static List<LinearHashMapConfig.Builder> configList() {
        return Arrays.asList(
                LinearHashMapConfig.builder().bucketSize(32),
                LinearHashMapConfig.builder().bucketSize(64)
        );
    }

    @DataProvider
    public Object[][] config() {
        return configList().stream().map(c -> new Object[]{ c.build() }).toArray(Object[][]::new);
    }

    private void insert(LinearHashTable lht, long hash, long key, long value) {
        try (LinearHashTable.Cursor cursor = lht.allocateCursor()) {
            cursor.seek(hash, key);
            cursor.insert(hash, key, value);
        }
    }

    @Test(dataProvider = "config")
    public void empty(LinearHashMapConfig config) {
        LinearHashTable lht = new LHTImpl(config);
        lht.checkInvariants();
        Assert.assertEquals(lht.toStringFlat(), "[]");
    }

    @Test(dataProvider = "config")
    public void singleton(LinearHashMapConfig config) {
        LinearHashTable lht = new LHTImpl(config);
        insert(lht, 0x0000000000000000L, 1, 2);
        lht.checkInvariants();
        Assert.assertEquals(lht.toStringFlat(), "[1->2]");
    }

    @Test(dataProvider = "config")
    public void replaceValue(LinearHashMapConfig config) {
        LinearHashTable lht = new LHTImpl(config);
        insert(lht, 0x0000000000000000L, 1, 2);
        lht.checkInvariants();
        try (LinearHashTable.Cursor cursor = lht.allocateCursor()) {
            cursor.seek(0, 1);
            cursor.setValue(3);
        }
        lht.checkInvariants();
        Assert.assertEquals(lht.toStringFlat(), "[1->3]");
    }

    @Test(dataProvider = "config")
    public void clear(LinearHashMapConfig config) {
        LinearHashTable lht = new LHTImpl(config);
        insert(lht, 0x0000000000000000L, 1, 2);
        lht.clear();
        lht.checkInvariants();
        Assert.assertEquals(lht.toStringFlat(), "[]");
        insert(lht, 0x0000000000000000L, 1, 2);
        lht.checkInvariants();
        Assert.assertEquals(lht.toStringFlat(), "[1->2]");
    }

    @Test(dataProvider = "config")
    public void manyItemsSameHash(LinearHashMapConfig config) {
        long hash = 0x0000000000000000L;
        LinearHashTable lht = new LHTImpl(config);
        MutableLongLongMap map = LongLongMaps.mutable.empty();
        for (int i = 0; i < 300; i++) {
            int value = ~i & 0xffff;
            lht.expandToFullLoadCapacity((long) (i * 1.5));
            insert(lht, hash, i, value);
            map.put(i, value);
            lht.checkInvariants();

            try (LinearHashTable.Cursor cursor = lht.allocateCursor()) {
                MutableLongLongMap expected = LongLongMaps.mutable.ofAll(map);
                while (cursor.next()) {
                    long key = cursor.getKey();
                    Assert.assertEquals(cursor.getValue(), expected.removeKeyIfAbsent(key, -1));
                }
                Assert.assertTrue(expected.isEmpty());
            }

            map.forEachKeyValue((k, v) -> {
                try (LinearHashTable.Cursor cursor = lht.allocateCursor()) {
                    cursor.seek(hash, k);
                    Assert.assertTrue(cursor.elementFound());
                    Assert.assertEquals(cursor.getValue(), v);
                }
            });
        }

        for (int i = 0; i < 300; i++) {
            try (LinearHashTable.Cursor cursor = lht.allocateCursor()) {
                cursor.seek(hash, i);
                cursor.remove();
            }
            map.remove(i);
            lht.checkInvariants();

            try (LinearHashTable.Cursor cursor = lht.allocateCursor()) {
                MutableLongLongMap expected = LongLongMaps.mutable.ofAll(map);
                while (cursor.next()) {
                    long key = cursor.getKey();
                    Assert.assertEquals(cursor.getValue(), expected.removeKeyIfAbsent(key, -1));
                }
                Assert.assertTrue(expected.isEmpty());
            }

            map.forEachKeyValue((k, v) -> {
                try (LinearHashTable.Cursor cursor = lht.allocateCursor()) {
                    cursor.seek(hash, k);
                    Assert.assertTrue(cursor.elementFound());
                    Assert.assertEquals(cursor.getValue(), v);
                }
            });
        }
    }

    @Test(dataProvider = "config")
    public void manyItemsReverseInsert(LinearHashMapConfig config) {
        long hash = 0x0000000000000000L;
        LinearHashTable lht = new LHTImpl(config);
        MutableLongLongMap map = LongLongMaps.mutable.empty();
        for (int i = 300; i >= 0; i--) {
            int value = ~i & 0xffff;
            lht.expandToFullLoadCapacity((long) (map.size() * 1.5));
            insert(lht, hash, i, value);
            map.put(i, value);
            lht.checkInvariants();

            try (LinearHashTable.Cursor cursor = lht.allocateCursor()) {
                MutableLongLongMap expected = LongLongMaps.mutable.ofAll(map);
                while (cursor.next()) {
                    long key = cursor.getKey();
                    Assert.assertEquals(cursor.getValue(), expected.removeKeyIfAbsent(key, -1));
                }
                Assert.assertTrue(expected.isEmpty());
            }

            map.forEachKeyValue((k, v) -> {
                try (LinearHashTable.Cursor cursor = lht.allocateCursor()) {
                    cursor.seek(hash, k);
                    Assert.assertTrue(cursor.elementFound());
                    Assert.assertEquals(cursor.getValue(), v);
                }
            });
        }
    }

    @Test(dataProvider = "config")
    public void hashSplit(LinearHashMapConfig config) {
        LongToLongFunction hash = l -> {
            // four buckets.
            long bucket = (l * 7) & 0b11;
            return l | (bucket << 62);
        };

        LinearHashTable lht = new LHTImpl(config);
        MutableLongLongMap map = LongLongMaps.mutable.empty();
        for (int i = 0; i < 300; i++) {
            int value = ~i & 0xffff;
            lht.expandToFullLoadCapacity((long) (i * 1.5));
            insert(lht, hash.valueOf(i), i, value);
            map.put(i, value);
            lht.checkInvariants();

            try (LinearHashTable.Cursor cursor = lht.allocateCursor()) {
                MutableLongLongMap expected = LongLongMaps.mutable.ofAll(map);
                while (cursor.next()) {
                    long key = cursor.getKey();
                    Assert.assertEquals(cursor.getValue(), expected.removeKeyIfAbsent(key, -1));
                }
                Assert.assertTrue(expected.isEmpty());
            }

            map.forEachKeyValue((k, v) -> {
                try (LinearHashTable.Cursor cursor = lht.allocateCursor()) {
                    cursor.seek(hash.valueOf(k), k);
                    Assert.assertTrue(cursor.elementFound());
                    Assert.assertEquals(cursor.getValue(), v);
                }
            });
        }

        for (int i = 0; i < 300; i++) {
            try (LinearHashTable.Cursor cursor = lht.allocateCursor()) {
                cursor.seek(hash.valueOf(i), i);
                cursor.remove();
            }
            map.remove(i);
            lht.checkInvariants();

            try (LinearHashTable.Cursor cursor = lht.allocateCursor()) {
                MutableLongLongMap expected = LongLongMaps.mutable.ofAll(map);
                while (cursor.next()) {
                    long key = cursor.getKey();
                    Assert.assertEquals(cursor.getValue(), expected.removeKeyIfAbsent(key, -1));
                }
                Assert.assertTrue(expected.isEmpty());
            }

            map.forEachKeyValue((k, v) -> {
                try (LinearHashTable.Cursor cursor = lht.allocateCursor()) {
                    cursor.seek(hash.valueOf(k), k);
                    Assert.assertTrue(cursor.elementFound());
                    Assert.assertEquals(cursor.getValue(), v);
                }
            });
        }
    }

    @Test(dataProvider = "config")
    public void splitMid(LinearHashMapConfig config) {
        LinearHashTable lht = new LHTImpl(config);

        insert(lht, 0x8000000000000000L, 0, 0);
        insert(lht, 0x8000000000000000L, 1, 0);
        insert(lht, 0x8000000000000000L, 2, 0);
        insert(lht, 0x8000000000000000L, 3, 0);
        insert(lht, 0x8000000000000000L, 4, 0);
        insert(lht, 0x8000000000000000L, 5, 0);
        insert(lht, 0x8000000000000000L, 6, 0);
        insert(lht, 0x0000000000000000L, 7, 0);
        insert(lht, 0x0000000000000000L, 8, 0);
        insert(lht, 0x0000000000000000L, 9, 0);
        insert(lht, 0x0000000000000000L, 10, 0);
        insert(lht, 0x0000000000000000L, 11, 0);
        lht.expandToFullLoadCapacity(10);
        lht.checkInvariants();
    }

    private static class LHTImpl extends LinearHashTable {
        private static long mask() {
            return ThreadLocalRandom.current().nextLong();
        }

        private final long hashMask = mask();
        private final int keyMask = (int) mask();
        private final short valueMask = (short) mask();

        LHTImpl(LinearHashMapConfig config) {
            super(BTreeTest.SIMPLE_ALLOCATOR, config, 8 + 4 + 2);
        }

        @Override
        protected void write(LargeByteBuffer lbb, long address, long hash, long key, long value) {
            Assert.assertTrue(key >= 0);
            Assert.assertTrue(key < 0x100000000L);
            Assert.assertTrue(value >= 0);
            Assert.assertTrue(value < 0x10000L);
            lbb.setLong(address, hash ^ hashMask);
            lbb.setInt(address + 8, (int) key ^ keyMask);
            lbb.setShort(address + 12, (short) (value ^ valueMask));
        }

        @Override
        protected long readHash(LargeByteBuffer lbb, long address) {
            return lbb.getLong(address) ^ hashMask;
        }

        @Override
        protected long readKey(LargeByteBuffer lbb, long address) {
            return Integer.toUnsignedLong(lbb.getInt(address + 8) ^ keyMask);
        }

        @Override
        protected long readValue(LargeByteBuffer lbb, long address) {
            return Short.toUnsignedLong((short) (lbb.getShort(address + 12) ^ valueMask));
        }
    }
}
