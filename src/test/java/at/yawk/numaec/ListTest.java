package at.yawk.numaec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@SuppressWarnings("resource")
public final class ListTest {
    static void assertThrows(Class<?> exception, Runnable r) {
        try {
            r.run();
        } catch (Exception e) {
            if (exception.isInstance(e)) {
                return;
            }
            throw e;
        }
        Assert.fail("Expected exception " + exception.getName());
    }
    
    private static MutableLongList newMutable(LargeByteBufferAllocator allocator) {
        return MutableLongBufferListFactory.withAllocator(allocator).empty();
    }

    @DataProvider
    public Object[][] allocator() {
        class ChunkAllocator implements LargeByteBufferAllocator {
            private final int chunkSize;

            ChunkAllocator(int chunkSize) {
                this.chunkSize = chunkSize;
            }

            @Override
            public LargeByteBuffer allocate(long size) {
                List<ByteBuffer> buffers = new ArrayList<>();
                while (size > 0) {
                    buffers.add(ByteBuffer.allocate((int) Math.min(size, chunkSize)));
                    size -= chunkSize;
                }
                return new ByteBufferBackedLargeByteBuffer(buffers.toArray(new ByteBuffer[0]), chunkSize);
            }
        }

        return new Object[][]{
                { new ChunkAllocator(8) },
                { new ChunkAllocator(16) },
                { new ChunkAllocator(32) },
                { new ChunkAllocator(64) },
                { new ChunkAllocator(128) },
                { new ChunkAllocator(4096) },
        };
    }

    @Test(dataProvider = "allocator")
    public void iterate(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(2);
        list.add(3);

        MutableLongIterator itr = list.longIterator();
        long l = 1;
        while (itr.hasNext()) {
            long value = itr.next();
            Assert.assertEquals(value, l++);
        }
        Assert.assertEquals(l, 4);
    }

    @Test(dataProvider = "allocator")
    public void iterateRemove(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(2);
        list.add(3);

        MutableLongIterator itr = list.longIterator();
        long l = 1;
        while (itr.hasNext()) {
            long value = itr.next();
            if (l < 3) {
                itr.remove();
            }
            Assert.assertEquals(value, l++);
        }
        Assert.assertEquals(l, 4);
        Assert.assertEquals(list.toString(), "[3]");
    }

    @Test(dataProvider = "allocator")
    public void forEach(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(2);
        list.add(3);

        AtomicLong l = new AtomicLong(1);
        list.forEach(value -> Assert.assertEquals(value, l.getAndIncrement()));
        Assert.assertEquals(l.get(), 4);
    }

    @Test(dataProvider = "allocator")
    public void forEachWithIndex(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(2);
        list.add(3);

        AtomicLong l = new AtomicLong(0);
        list.forEachWithIndex((value, index) -> {
            Assert.assertEquals(index, l.getAndIncrement());
            Assert.assertEquals(value, l.get());
        });
        Assert.assertEquals(l.get(), 3);
    }

    @Test(dataProvider = "allocator")
    public void each(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(2);
        list.add(3);

        AtomicLong l = new AtomicLong(1);
        list.each(value -> Assert.assertEquals(value, l.getAndIncrement()));
        Assert.assertEquals(l.get(), 4);
    }

    @Test(dataProvider = "allocator")
    public void get(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(2);
        list.add(3);

        Assert.assertEquals(list.get(0), 1);
        Assert.assertEquals(list.get(1), 2);
        Assert.assertEquals(list.get(2), 3);
        assertThrows(IndexOutOfBoundsException.class, () -> list.get(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> list.get(3));
        assertThrows(IndexOutOfBoundsException.class, () -> list.get(4));
    }

    @Test(dataProvider = "allocator")
    public void toArray(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(2);
        list.add(3);

        Assert.assertEquals(list.toArray(), new long[]{ 1, 2, 3 });
    }

    @Test(dataProvider = "allocator")
    public void contains(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(2);
        list.add(3);

        Assert.assertTrue(list.contains(1));
        Assert.assertTrue(list.contains(2));
        Assert.assertTrue(list.contains(3));
        Assert.assertFalse(list.contains(-1));
    }

    @Test(dataProvider = "allocator")
    public void dotProduct(LargeByteBufferAllocator allocator) {
        MutableLongList list1 = newMutable(allocator);
        list1.add(1);
        list1.add(2);
        list1.add(3);
        MutableLongList list2 = newMutable(allocator);
        list2.add(300000000000L);
        list2.add(200000000000L);
        list2.add(100000000000L);

        Assert.assertEquals(list1.dotProduct(list2), 1000000000000L);
        // no mutation
        Assert.assertEquals(list1.toString(), "[1, 2, 3]");
        Assert.assertEquals(list2.toString(), "[300000000000, 200000000000, 100000000000]");
    }

    @Test(dataProvider = "allocator", expectedExceptions = IllegalArgumentException.class)
    public void dotProductFail1(LargeByteBufferAllocator allocator) {
        MutableLongList list1 = newMutable(allocator);
        list1.add(1);
        list1.add(2);
        MutableLongList list2 = newMutable(allocator);
        list2.add(300000000000L);
        list2.add(200000000000L);
        list2.add(100000000000L);

        list1.dotProduct(list2);
    }

    @Test(dataProvider = "allocator", expectedExceptions = IllegalArgumentException.class)
    public void dotProductFail2(LargeByteBufferAllocator allocator) {
        MutableLongList list1 = newMutable(allocator);
        list1.add(1);
        list1.add(2);
        list1.add(3);
        MutableLongList list2 = newMutable(allocator);
        list2.add(300000000000L);
        list2.add(200000000000L);

        list1.dotProduct(list2);
    }

    @Test(dataProvider = "allocator")
    public void binarySearch(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(5);
        list.add(10);

        Assert.assertEquals(list.binarySearch(5), 1);
        Assert.assertEquals(list.binarySearch(1), 0);
        Assert.assertEquals(list.binarySearch(10), 2);
        Assert.assertEquals(list.binarySearch(7), ~2);
        Assert.assertEquals(list.binarySearch(4), ~1);
        Assert.assertEquals(list.binarySearch(-1000), ~0);
        Assert.assertEquals(list.binarySearch(1000), ~3);
    }

    @Test(dataProvider = "allocator")
    public void indexOf(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(2);
        list.add(2);
        list.add(3);

        Assert.assertEquals(list.indexOf(1), 0);
        Assert.assertEquals(list.indexOf(2), 1);
        Assert.assertEquals(list.indexOf(3), 3);
        Assert.assertEquals(list.indexOf(-1), -1);
    }

    @Test(dataProvider = "allocator")
    public void lastIndexOf(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(2);
        list.add(2);
        list.add(3);

        Assert.assertEquals(list.lastIndexOf(1), 0);
        Assert.assertEquals(list.lastIndexOf(2), 2);
        Assert.assertEquals(list.lastIndexOf(3), 3);
        Assert.assertEquals(list.lastIndexOf(-1), -1);
    }

    @Test(dataProvider = "allocator")
    public void getLast(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        assertThrows(IndexOutOfBoundsException.class, list::getLast);
        list.add(1);
        list.add(2);
        list.add(3);

        Assert.assertEquals(list.getLast(), 3);
    }

    @Test(dataProvider = "allocator")
    public void getFirst(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        assertThrows(IndexOutOfBoundsException.class, list::getFirst);
        list.add(1);
        list.add(2);
        list.add(3);

        Assert.assertEquals(list.getFirst(), 1);
    }

    @Test(dataProvider = "allocator")
    public void detectIfNone(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(2);
        list.add(3);

        Assert.assertEquals(list.detectIfNone(v -> true, 6), 1);
        Assert.assertEquals(list.detectIfNone(v -> v < 1, 6), 6);
    }

    @Test(dataProvider = "allocator")
    public void count(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(2);
        list.add(3);

        Assert.assertEquals(list.count(i -> i < 3), 2);
    }

    @Test(dataProvider = "allocator")
    public void anySatisfy(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        Assert.assertFalse(list.anySatisfy(v -> true));
        list.add(1);
        list.add(2);
        list.add(3);

        Assert.assertTrue(list.anySatisfy(v -> v < 2));
        Assert.assertTrue(list.anySatisfy(v -> true));
        Assert.assertFalse(list.anySatisfy(v -> v < 1));
    }

    @Test(dataProvider = "allocator")
    public void allSatisfy(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        Assert.assertTrue(list.allSatisfy(v -> false));
        list.add(1);
        list.add(2);
        list.add(3);

        Assert.assertFalse(list.allSatisfy(v -> v < 2));
        Assert.assertTrue(list.allSatisfy(v -> true));
        Assert.assertFalse(list.allSatisfy(v -> v < 1));
    }

    @Test(dataProvider = "allocator")
    public void noneSatisfy(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        Assert.assertTrue(list.noneSatisfy(v -> true));
        list.add(1);
        list.add(2);
        list.add(3);

        Assert.assertFalse(list.noneSatisfy(v -> v < 2));
        Assert.assertFalse(list.noneSatisfy(v -> true));
        Assert.assertTrue(list.noneSatisfy(v -> v < 1));
    }

    @Test(dataProvider = "allocator")
    public void injectInto(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(2);
        list.add(3);

        Assert.assertEquals(list.injectInto("", (String v, long l) -> v + l), "123");
    }

    @Test(dataProvider = "allocator")
    public void injectIntoWithIndex(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(2);
        list.add(3);

        Assert.assertEquals(list.injectIntoWithIndex("", (String v, long l, int i) -> v + i + l), "011223");
    }

    @Test(dataProvider = "allocator")
    public void sum(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(2);
        list.add(3);

        Assert.assertEquals(list.sum(), 6);
    }

    @Test(dataProvider = "allocator")
    public void max(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        assertThrows(IndexOutOfBoundsException.class, list::max);
        list.add(1);
        Assert.assertEquals(list.max(), 1);
        list.add(3);
        Assert.assertEquals(list.max(), 3);
        list.add(2);
        Assert.assertEquals(list.max(), 3);
    }

    @Test(dataProvider = "allocator")
    public void min(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        assertThrows(IndexOutOfBoundsException.class, list::min);
        list.add(2);
        Assert.assertEquals(list.min(), 2);
        list.add(3);
        Assert.assertEquals(list.min(), 2);
        list.add(1);
        Assert.assertEquals(list.min(), 1);
    }

    @Test(dataProvider = "allocator")
    public void size(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        Assert.assertEquals(list.size(), 0);
        list.add(1);
        list.add(3);
        list.add(2);

        Assert.assertEquals(list.size(), 3);
    }

    @Test(dataProvider = "allocator")
    public void toString(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(3);
        list.add(2);

        Assert.assertEquals(list.toString(), "[1, 3, 2]");
    }

    @Test(dataProvider = "allocator")
    public void addAtIndex(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(3);
        list.add(2);
        list.addAtIndex(1, 5);
        Assert.assertEquals(list.toString(), "[1, 5, 3, 2]");
        list.addAtIndex(4, 5);
        Assert.assertEquals(list.toString(), "[1, 5, 3, 2, 5]");
    }

    @Test(dataProvider = "allocator")
    public void addAllAtIndex(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(3);
        list.add(2);
        assertThrows(IndexOutOfBoundsException.class, () -> list.addAllAtIndex(-1, 5));
        assertThrows(IndexOutOfBoundsException.class, () -> list.addAllAtIndex(4, 5));
        Assert.assertTrue(list.addAllAtIndex(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        Assert.assertEquals(list.toString(), "[1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 3, 2]");
        Assert.assertTrue(list.addAllAtIndex(13, 1, 2, 3));
        Assert.assertEquals(list.toString(), "[1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 3, 2, 1, 2, 3]");
        Assert.assertFalse(list.addAllAtIndex(0));
    }

    @Test(dataProvider = "allocator")
    public void addAllAtIndexIterable(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(3);
        list.add(2);
        Assert.assertTrue(list.addAllAtIndex(1, LongLists.immutable.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        Assert.assertEquals(list.toString(), "[1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 3, 2]");
        Assert.assertTrue(list.addAllAtIndex(13, LongLists.immutable.of(1, 2, 3)));
        Assert.assertEquals(list.toString(), "[1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 3, 2, 1, 2, 3]");
    }

    @Test(dataProvider = "allocator")
    public void removeAtIndex(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(3);
        list.add(2);
        assertThrows(IndexOutOfBoundsException.class, () -> list.removeAtIndex(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> list.removeAtIndex(3));
        Assert.assertEquals(list.removeAtIndex(1), 3);
        Assert.assertEquals(list.toString(), "[1, 2]");
    }

    @Test(dataProvider = "allocator")
    public void set(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(3);
        list.add(2);
        assertThrows(IndexOutOfBoundsException.class, () -> list.set(-1, 0));
        assertThrows(IndexOutOfBoundsException.class, () -> list.set(3, 5));
        Assert.assertEquals(list.set(1, 5), 3);
        Assert.assertEquals(list.toString(), "[1, 5, 2]");
    }

    @Test(dataProvider = "allocator")
    public void addAll(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(3);
        list.add(2);
        Assert.assertTrue(list.addAll(0, 0, 2));
        Assert.assertEquals(list.toString(), "[1, 3, 2, 0, 0, 2]");
    }

    @Test(dataProvider = "allocator")
    public void addAllItr(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(3);
        list.add(2);
        Assert.assertTrue(list.addAll(LongLists.immutable.of(0, 0, 2)));
        Assert.assertEquals(list.toString(), "[1, 3, 2, 0, 0, 2]");
    }

    @Test(dataProvider = "allocator")
    public void remove(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(3);
        list.add(3);
        list.add(2);
        Assert.assertTrue(list.remove(3));
        Assert.assertEquals(list.toString(), "[1, 3, 2]");
        Assert.assertTrue(list.remove(3));
        Assert.assertFalse(list.remove(3));
    }

    @Test(dataProvider = "allocator")
    public void clear(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(3);
        list.add(2);
        list.clear();
        Assert.assertEquals(list.toString(), "[]");
    }

    @Test(dataProvider = "allocator")
    public void with(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(3);
        list.add(2);
        Assert.assertSame(list.with(4), list);
        Assert.assertEquals(list.toString(), "[1, 3, 2, 4]");
    }

    @Test(dataProvider = "allocator")
    public void without(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(3);
        list.add(2);
        Assert.assertSame(list.without(3), list);
        Assert.assertEquals(list.toString(), "[1, 2]");
    }

    @Test(dataProvider = "allocator")
    public void withAll(LargeByteBufferAllocator allocator) {
        MutableLongList list = newMutable(allocator);
        list.add(1);
        list.add(3);
        list.add(2);
        Assert.assertSame(list.withAll(LongLists.immutable.of(5, 6)), list);
        Assert.assertEquals(list.toString(), "[1, 3, 2, 5, 6]");
    }

    @Test
    public void reallocDoesNotCloseBuffer() {
        boolean[] reallocated = { false };
        class TestBuffer extends ByteBufferBackedLargeByteBuffer {
            public TestBuffer(long size) {
                super(new ByteBuffer[]{ByteBuffer.allocate((int) size)},
                      Integer.highestOneBit((int) size) << 1);
            }

            @Override
            public LargeByteBuffer reallocate(long newSize) {
                reallocated[0] = true;
                return new TestBuffer(newSize);
            }

            @Override
            public void close() {
                Assert.fail();
            }
        }

        MutableLongList list = newMutable(new LargeByteBufferAllocator() {
            boolean allocated = false;

            @Override
            public LargeByteBuffer allocate(long size) {
                Assert.assertFalse(allocated);
                allocated = true;

                return new TestBuffer(size);
            }
        });
        while (!reallocated[0]) {
            list.add(0);
        }
    }

    @Test
    public void newAllocClosesBuffer() {
        int[] closed = { 0 };
        int[] allocated = { 0 };
        class TestBuffer extends ByteBufferBackedLargeByteBuffer {
            boolean thisClosed = false;

            public TestBuffer(long size) {
                super(new ByteBuffer[]{ByteBuffer.allocate((int) size)},
                      Integer.highestOneBit((int) size) << 1);
                allocated[0]++;
            }

            @Override
            public void close() {
                Assert.assertFalse(thisClosed);
                thisClosed = true;
                closed[0]++;
            }
        }

        MutableLongList list = newMutable(size -> new TestBuffer(size));
        while (closed[0] == 0) {
            list.add(0);
        }
        Assert.assertEquals(closed[0], 1);
        Assert.assertEquals(allocated[0], 2);
    }

    @Test(dataProvider = "allocator")
    public void equals(LargeByteBufferAllocator allocator) {
        Assert.assertEquals(
                newMutable(allocator).with(1).with(2).with(3),
                LongLists.mutable.empty().with(1).with(2).with(3)
        );
    }

    @Test(dataProvider = "allocator")
    public void hashCode(LargeByteBufferAllocator allocator) {
        Assert.assertEquals(
                newMutable(allocator).with(1).with(2).with(3).hashCode(),
                LongLists.mutable.empty().with(1).with(2).with(3).hashCode()
        );
    }
}
