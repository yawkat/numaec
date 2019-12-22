package at.yawk.numaec;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BTreeMapTest {
    @DataProvider
    public Object[][] map() {
        LargeByteBufferAllocator allocator =
                size -> new ByteBufferBackedLargeByteBuffer(new ByteBuffer[]{ ByteBuffer.allocate(Math.toIntExact(size)) },
                                                            0x1000000);
        return BTreeTest.configList().stream()
                .map(cfg -> new Object[]{ new IntDoubleBTreeMap.Mutable(allocator, cfg) })
                .toArray(Object[][]::new);
    }

    @Test(dataProvider = "map")
    public void getZeroByDefault(IntDoubleBTreeMap map) {
        map.checkInvariants();
        Assert.assertEquals(map.get(1), 0.0);
        Assert.assertEquals(map.getIfAbsent(1, 6.0), 6.0);
        Assert.assertFalse(map.containsKey(1));
        map.checkInvariants();
    }

    @Test(dataProvider = "map")
    public void getOrThrow(IntDoubleBTreeMap.Mutable map) {
        map.checkInvariants();
        ListTest.assertThrows(IllegalStateException.class, () -> map.getOrThrow(5));
        map.checkInvariants();
        map.put(5, 2.0);
        map.checkInvariants();
        Assert.assertEquals(map.getOrThrow(5), 2.0);
        map.checkInvariants();
    }

    @Test(dataProvider = "map")
    public void getAfterPut(IntDoubleBTreeMap.Mutable map) {
        map.checkInvariants();
        map.put(1, 5.0);
        map.checkInvariants();
        Assert.assertEquals(map.get(1), 5.0);
        Assert.assertTrue(map.containsKey(1));
        map.checkInvariants();
    }

    @Test(dataProvider = "map")
    public void negativeKey(IntDoubleBTreeMap.Mutable map) {
        // this covers the masking
        map.checkInvariants();
        map.put(-10, -5.0);
        map.checkInvariants();
        Assert.assertEquals(map.get(-10), -5.0);
        Assert.assertTrue(map.containsKey(-10));
        map.checkInvariants();
    }

    @Test(dataProvider = "map")
    public void getAfterReplace(IntDoubleBTreeMap.Mutable map) {
        map.checkInvariants();
        map.put(1, 5.0);
        map.checkInvariants();
        map.put(1, 6.0);
        map.checkInvariants();
        Assert.assertEquals(map.get(1), 6.0);
        Assert.assertEquals(map.getIfAbsent(1, 111.0), 6.0);
        map.checkInvariants();
    }

    @Test(dataProvider = "map")
    public void detectIfNone(IntDoubleBTreeMap.Mutable map) {
        map.checkInvariants();
        map.put(1, 5.0);
        map.put(2, 6.0);
        map.put(3, 7.0);
        map.checkInvariants();
        Assert.assertEquals(map.detectIfNone(d -> d > 7.0, -1.0), -1.0);
        Assert.assertEquals(map.detectIfNone(d -> d > 6.0, -1.0), 7.0);
    }

    @Test(dataProvider = "map")
    public void count(IntDoubleBTreeMap.Mutable map) {
        map.checkInvariants();
        map.put(1, 5.0);
        map.put(2, 6.0);
        map.put(3, 7.0);
        map.checkInvariants();
        Assert.assertEquals(map.count(d -> d > 7.0), 0);
        Assert.assertEquals(map.count(d -> d > 6.0), 1);
    }

    @Test(dataProvider = "map")
    public void anySatisfy(IntDoubleBTreeMap.Mutable map) {
        map.checkInvariants();
        map.put(1, 5.0);
        map.put(2, 6.0);
        map.put(3, 7.0);
        map.checkInvariants();
        Assert.assertFalse(map.anySatisfy(d -> d > 7.0));
        Assert.assertTrue(map.anySatisfy(d -> d > 6.0));
    }

    @Test(dataProvider = "map")
    public void allSatisfy(IntDoubleBTreeMap.Mutable map) {
        map.checkInvariants();
        map.put(1, 5.0);
        map.put(2, 6.0);
        map.put(3, 7.0);
        map.checkInvariants();
        Assert.assertFalse(map.allSatisfy(d -> d > 7.0));
        Assert.assertFalse(map.allSatisfy(d -> d > 6.0));
        Assert.assertTrue(map.allSatisfy(d -> d > 4.0));
    }

    @Test(dataProvider = "map")
    public void manyAccess(IntDoubleBTreeMap.Mutable map) {
        IntConsumer before = i -> {
            double value = i * 1.5;
            Assert.assertFalse(map.containsValue(value));
            Assert.assertFalse(map.containsKey(i));
            Assert.assertFalse(map.contains(value));
            Assert.assertEquals(map.getIfAbsent(i, i * 2.0), i * 2.0);
            Assert.assertEquals(map.get(i), 0.0);
            ListTest.assertThrows(IllegalStateException.class, () -> map.getOrThrow(i));
        };
        IntConsumer after = i -> {
            double value = i * 1.5;
            Assert.assertTrue(map.containsValue(value));
            Assert.assertTrue(map.containsKey(i));
            Assert.assertTrue(map.contains(value));
            Assert.assertEquals(map.getIfAbsent(i, i * 2.0), value);
            Assert.assertEquals(map.get(i), value);
            Assert.assertEquals(map.getOrThrow(i), value);
            Assert.assertEquals(map.min(), 0.0);
        };

        ListTest.assertThrows(NoSuchElementException.class, map::max);
        ListTest.assertThrows(NoSuchElementException.class, map::min);

        for (int i = 0; i < 100; i++) {
            before.accept(i);
        }

        map.forEachKey(k -> Assert.fail());

        double sum = 0;
        for (int i = 0; i < 100; i++) {
            double value = i * 1.5;
            sum += value;
            before.accept(i);
            map.put(i, value);
            after.accept(i);
            map.checkInvariants();

            Assert.assertEquals(map.max(), value);

            AtomicInteger nextKey = new AtomicInteger(0);
            map.forEachKey(k -> Assert.assertTrue(nextKey.compareAndSet(k, k + 1)));
            Assert.assertEquals(nextKey.get(), i + 1);

            nextKey.set(0);
            map.forEachValue(v -> {
                double expected = nextKey.getAndIncrement() * 1.5;
                Assert.assertEquals(v, expected);
            });
            Assert.assertEquals(nextKey.get(), i + 1);

            nextKey.set(0);
            map.forEachKeyValue((k, v) -> {
                Assert.assertTrue(nextKey.compareAndSet(k, k + 1));
                double expected = k * 1.5;
                Assert.assertEquals(v, expected);
            });
            Assert.assertEquals(nextKey.get(), i + 1);

            Assert.assertEquals(
                    map.toArray(),
                    IntStream.range(0, i + 1).mapToDouble(k -> k * 1.5).toArray()
            );
            Assert.assertEquals(map.sum(), sum);
        }

        for (int i = 0; i < 100; i++) {
            after.accept(i);
        }
    }

    @Test(dataProvider = "map")
    public void manyAccessReverse(IntDoubleBTreeMap.Mutable map) {
        IntConsumer before = i -> {
            double value = i * 1.5;
            Assert.assertFalse(map.containsValue(value));
            Assert.assertFalse(map.containsKey(i));
            Assert.assertFalse(map.contains(value));
            Assert.assertEquals(map.getIfAbsent(i, i * 2.0), i * 2.0);
            Assert.assertEquals(map.get(i), 0.0);
            ListTest.assertThrows(IllegalStateException.class, () -> map.getOrThrow(i));
        };
        IntConsumer after = i -> {
            double value = i * 1.5;
            Assert.assertTrue(map.containsValue(value));
            Assert.assertTrue(map.containsKey(i));
            Assert.assertTrue(map.contains(value));
            Assert.assertEquals(map.getIfAbsent(i, i * 2.0), value);
            Assert.assertEquals(map.get(i), value);
            Assert.assertEquals(map.getOrThrow(i), value);
        };

        ListTest.assertThrows(NoSuchElementException.class, map::max);
        ListTest.assertThrows(NoSuchElementException.class, map::min);

        for (int i = 100; i >= 0; i--) {
            before.accept(i);
        }

        map.forEachKey(k -> Assert.fail());

        double sum = 0;
        for (int i = 100; i >= 0; i--) {
            double value = i * 1.5;
            sum += value;
            before.accept(i);
            map.put(i, value);
            after.accept(i);
            map.checkInvariants();

            Assert.assertEquals(map.min(), value);

            Assert.assertEquals(
                    map.toArray(),
                    IntStream.range(i, 101).mapToDouble(k -> k * 1.5).toArray()
            );
            Assert.assertEquals(map.sum(), sum);
        }

        for (int i = 100; i >= 0; i--) {
            after.accept(i);
        }
    }

    @Test(dataProvider = "map")
    public void updateValues(IntDoubleBTreeMap.Mutable map) {
        map.checkInvariants();
        map.put(1, 5.0);
        map.put(2, 6.0);
        map.put(3, 7.0);
        map.checkInvariants();
        map.updateValues(Double::sum);
        map.checkInvariants();
        Assert.assertEquals(map.toString(), "{1=6.0, 2=8.0, 3=10.0}");
    }

    @Test(dataProvider = "map")
    public void manyRemove(IntDoubleBTreeMap.Mutable map) {
        IntConsumer before = i -> {
            double value = i * 1.5;
            Assert.assertFalse(map.containsValue(value));
            Assert.assertFalse(map.containsKey(i));
            Assert.assertFalse(map.contains(value));
            Assert.assertEquals(map.getIfAbsent(i, i * 2.0), i * 2.0);
            Assert.assertEquals(map.get(i), 0.0);
            ListTest.assertThrows(IllegalStateException.class, () -> map.getOrThrow(i));
        };
        IntConsumer after = i -> {
            double value = i * 1.5;
            Assert.assertTrue(map.containsValue(value));
            Assert.assertTrue(map.containsKey(i));
            Assert.assertTrue(map.contains(value));
            Assert.assertEquals(map.getIfAbsent(i, i * 2.0), value);
            Assert.assertEquals(map.get(i), value);
            Assert.assertEquals(map.getOrThrow(i), value);
            Assert.assertEquals(map.min(), 0.0);
        };

        ListTest.assertThrows(NoSuchElementException.class, map::max);
        ListTest.assertThrows(NoSuchElementException.class, map::min);

        for (int i = 0; i < 100; i++) {
            before.accept(i);
        }

        map.forEachKey(k -> Assert.fail());

        for (int i = 0; i < 100; i++) {
            double value = i * 1.5;
            before.accept(i);
            map.put(i, value);
            after.accept(i);
            map.checkInvariants();
        }

        for (int i = 0; i < 100; i++) {
            after.accept(i);
        }

        for (int i = 0; i < 100; i++) {
            map.remove(i);
            map.checkInvariants();
            map.remove(i);
            map.checkInvariants();
            before.accept(i);
        }
    }

    @Test(dataProvider = "map")
    public void removeIfAbsent(IntDoubleBTreeMap.Mutable map) {
        map.checkInvariants();
        map.put(1, 5.0);
        map.put(2, 6.0);
        map.put(3, 7.0);
        map.checkInvariants();
        Assert.assertEquals(map.removeKeyIfAbsent(2, 0.1), 6.0);
        map.checkInvariants();
        Assert.assertEquals(map.removeKeyIfAbsent(2, 0.1), 0.1);
        map.checkInvariants();
    }

    @Test(dataProvider = "map")
    public void manyRemoveIfAbsent(IntDoubleBTreeMap.Mutable map) {
        IntConsumer before = i -> {
            double value = i * 1.5;
            Assert.assertFalse(map.containsValue(value));
            Assert.assertFalse(map.containsKey(i));
            Assert.assertFalse(map.contains(value));
            Assert.assertEquals(map.getIfAbsent(i, i * 2.0), i * 2.0);
            Assert.assertEquals(map.get(i), 0.0);
            ListTest.assertThrows(IllegalStateException.class, () -> map.getOrThrow(i));
        };

        for (int i = 0; i < 100; i++) {
            double value = i * 1.5;
            map.put(i, value);
            map.checkInvariants();
        }

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(map.removeKeyIfAbsent(i, Double.NaN), i * 1.5);
            map.checkInvariants();
            Assert.assertEquals(map.removeKeyIfAbsent(i, Double.NaN), Double.NaN);
            map.checkInvariants();
            before.accept(i);
        }
    }

    @Test(dataProvider = "map")
    public void manyPutIfAbsent(IntDoubleBTreeMap.Mutable map) {
        IntConsumer after = i -> {
            double value = i * 1.5;
            Assert.assertTrue(map.containsValue(value));
            Assert.assertTrue(map.containsKey(i));
            Assert.assertTrue(map.contains(value));
            Assert.assertEquals(map.getIfAbsent(i, i * 2.0), value);
            Assert.assertEquals(map.get(i), value);
            Assert.assertEquals(map.getOrThrow(i), value);
            Assert.assertEquals(map.min(), 0.0);
        };
        for (int i = 0; i < 100; i++) {
            double value = i * 1.5;
            Assert.assertEquals(map.getIfAbsentPut(i, value), value);
            map.checkInvariants();
            Assert.assertEquals(map.getIfAbsentPut(i, value + 1), value);
            after.accept(i);
            map.checkInvariants();
        }
    }

    @Test(dataProvider = "map")
    public void manyPutIfAbsentFn1(IntDoubleBTreeMap.Mutable map) {
        IntConsumer after = i -> {
            double value = i * 1.5;
            Assert.assertTrue(map.containsValue(value));
            Assert.assertTrue(map.containsKey(i));
            Assert.assertTrue(map.contains(value));
            Assert.assertEquals(map.getIfAbsent(i, i * 2.0), value);
            Assert.assertEquals(map.get(i), value);
            Assert.assertEquals(map.getOrThrow(i), value);
            Assert.assertEquals(map.min(), 0.0);
        };
        for (int i = 0; i < 100; i++) {
            double value = i * 1.5;
            Assert.assertEquals(map.getIfAbsentPut(i, () -> value), value);
            map.checkInvariants();
            Assert.assertEquals(map.getIfAbsentPut(i, () -> {
                throw new AssertionError();
            }), value);
            after.accept(i);
            map.checkInvariants();
        }
    }

    @Test(dataProvider = "map")
    public void manyPutIfAbsentFn2(IntDoubleBTreeMap.Mutable map) {
        IntConsumer after = i -> {
            double value = i * 1.5;
            Assert.assertTrue(map.containsValue(value));
            Assert.assertTrue(map.containsKey(i));
            Assert.assertTrue(map.contains(value));
            Assert.assertEquals(map.getIfAbsent(i, i * 2.0), value);
            Assert.assertEquals(map.get(i), value);
            Assert.assertEquals(map.getOrThrow(i), value);
            Assert.assertEquals(map.min(), 0.0);
        };
        for (int i = 0; i < 100; i++) {
            double value = i * 1.5;
            Assert.assertEquals(map.getIfAbsentPutWithKey(i, k -> k * 1.5), value);
            map.checkInvariants();
            Assert.assertEquals(map.getIfAbsentPutWithKey(i, k -> {
                throw new AssertionError();
            }), value);
            after.accept(i);
            map.checkInvariants();
        }
    }

    @Test(dataProvider = "map")
    public void manyPutIfAbsentFn3(IntDoubleBTreeMap.Mutable map) {
        IntConsumer after = i -> {
            double value = i * 1.5;
            Assert.assertTrue(map.containsValue(value));
            Assert.assertTrue(map.containsKey(i));
            Assert.assertTrue(map.contains(value));
            Assert.assertEquals(map.getIfAbsent(i, i * 2.0), value);
            Assert.assertEquals(map.get(i), value);
            Assert.assertEquals(map.getOrThrow(i), value);
            Assert.assertEquals(map.min(), 0.0);
        };
        for (int i = 0; i < 100; i++) {
            double value = i * 1.5;
            Object o = new Object();
            Assert.assertEquals(map.getIfAbsentPutWith(i, (p) -> {
                Assert.assertSame(o, p);
                return value;
            }, o), value);
            map.checkInvariants();
            Assert.assertEquals(map.getIfAbsentPutWith(i, p -> {
                throw new AssertionError();
            }, o), value);
            after.accept(i);
            map.checkInvariants();
        }
    }

    @Test(dataProvider = "map")
    public void manyUpdateValue(IntDoubleBTreeMap.Mutable map) {
        IntConsumer after = i -> {
            double value = i * 1.5;
            Assert.assertTrue(map.containsValue(value));
            Assert.assertTrue(map.containsKey(i));
            Assert.assertTrue(map.contains(value));
            Assert.assertEquals(map.getIfAbsent(i, i * 2.0), value);
            Assert.assertEquals(map.get(i), value);
            Assert.assertEquals(map.getOrThrow(i), value);
            Assert.assertEquals(map.min(), 0.0);
        };
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(map.updateValue(i, i, d -> d), (double) i);
            map.checkInvariants();
            Assert.assertEquals(map.updateValue(i, -100.0, d -> d * 1.5), i * 1.5);
            map.checkInvariants();
            after.accept(i);
            map.checkInvariants();
        }
    }

    @Test(dataProvider = "map")
    public void manyUpdateValues(IntDoubleBTreeMap.Mutable map) {
        IntConsumer after = i -> {
            double value = i * 1.5;
            Assert.assertTrue(map.containsValue(value));
            Assert.assertTrue(map.containsKey(i));
            Assert.assertTrue(map.contains(value));
            Assert.assertEquals(map.getIfAbsent(i, i * 2.0), value);
            Assert.assertEquals(map.get(i), value);
            Assert.assertEquals(map.getOrThrow(i), value);
            Assert.assertEquals(map.min(), 0.0);
        };
        for (int i = 0; i < 100; i++) {
            map.put(i, 1.5);
            map.checkInvariants();
        }
        map.updateValues((k, v) -> k * v);
        for (int i = 0; i < 100; i++) {
            after.accept(i);
        }
    }

    @Test(dataProvider = "map")
    public void manyAdd(IntDoubleBTreeMap.Mutable map) {
        IntConsumer after = i -> {
            double value = i * 1.5;
            Assert.assertTrue(map.containsValue(value));
            Assert.assertTrue(map.containsKey(i));
            Assert.assertTrue(map.contains(value));
            Assert.assertEquals(map.getIfAbsent(i, i * 2.0), value);
            Assert.assertEquals(map.get(i), value);
            Assert.assertEquals(map.getOrThrow(i), value);
            Assert.assertEquals(map.min(), 0.0);
        };
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(map.addToValue(i, i), (double) i);
            map.checkInvariants();
            Assert.assertEquals(map.addToValue(i, i * .5), i * 1.5);
            map.checkInvariants();
            after.accept(i);
        }
    }
}