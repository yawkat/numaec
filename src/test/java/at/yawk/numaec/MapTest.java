package at.yawk.numaec;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.eclipse.collections.api.map.primitive.IntDoubleMap;
import org.eclipse.collections.api.map.primitive.MutableIntDoubleMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableDoubleSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.DoubleSets;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MapTest {
    @DataProvider
    public Object[][] map() {
        LargeByteBufferAllocator allocator =
                size -> new ByteBufferBackedLargeByteBuffer(
                        new ByteBuffer[]{ ByteBuffer.allocate(Math.toIntExact(size)) },
                        0x1000000);
        Stream<Object[]> btreeStream = BTreeTest.configList().stream().map(cfg -> new Object[]{
                new IntDoubleBTreeMap.Mutable(allocator, cfg) });
        Stream<Object[]> lhtStream = LinearHashTableTest.configList().stream().map(cfg -> new Object[]{
                new IntDoubleLinearHashMap.Mutable(allocator, cfg) });
        return Stream.concat(btreeStream, lhtStream).toArray(Object[][]::new);
    }

    private void checkInvariants(IntDoubleMap map) {
        ((BaseIntDoubleMap) map).checkInvariants();
    }

    @Test(dataProvider = "map")
    public void getZeroByDefault(BaseIntDoubleMap map) {
        checkInvariants(map);
        Assert.assertEquals(map.get(1), 0.0);
        Assert.assertEquals(map.getIfAbsent(1, 6.0), 6.0);
        Assert.assertFalse(map.containsKey(1));
        checkInvariants(map);
    }

    @Test(dataProvider = "map")
    public void getOrThrow(MutableIntDoubleMap map) {
        checkInvariants(map);
        ListTest.assertThrows(IllegalStateException.class, () -> map.getOrThrow(5));
        checkInvariants(map);
        map.put(5, 2.0);
        checkInvariants(map);
        Assert.assertEquals(map.getOrThrow(5), 2.0);
        checkInvariants(map);
    }

    @Test(dataProvider = "map")
    public void getAfterPut(MutableIntDoubleMap map) {
        checkInvariants(map);
        map.put(1, 5.0);
        checkInvariants(map);
        Assert.assertEquals(map.get(1), 5.0);
        Assert.assertTrue(map.containsKey(1));
        checkInvariants(map);
    }

    @Test(dataProvider = "map")
    public void negativeKey(MutableIntDoubleMap map) {
        // this covers the masking
        checkInvariants(map);
        map.put(-10, -5.0);
        checkInvariants(map);
        Assert.assertEquals(map.get(-10), -5.0);
        Assert.assertTrue(map.containsKey(-10));
        checkInvariants(map);
    }

    @Test(dataProvider = "map")
    public void getAfterReplace(MutableIntDoubleMap map) {
        checkInvariants(map);
        map.put(1, 5.0);
        checkInvariants(map);
        map.put(1, 6.0);
        checkInvariants(map);
        Assert.assertEquals(map.get(1), 6.0);
        Assert.assertEquals(map.getIfAbsent(1, 111.0), 6.0);
        checkInvariants(map);
    }

    @Test(dataProvider = "map")
    public void detectIfNone(MutableIntDoubleMap map) {
        checkInvariants(map);
        map.put(1, 5.0);
        map.put(2, 6.0);
        map.put(3, 7.0);
        checkInvariants(map);
        Assert.assertEquals(map.detectIfNone(d -> d > 7.0, -1.0), -1.0);
        Assert.assertEquals(map.detectIfNone(d -> d > 6.0, -1.0), 7.0);
    }

    @Test(dataProvider = "map")
    public void count(MutableIntDoubleMap map) {
        checkInvariants(map);
        map.put(1, 5.0);
        map.put(2, 6.0);
        map.put(3, 7.0);
        checkInvariants(map);
        Assert.assertEquals(map.count(d -> d > 7.0), 0);
        Assert.assertEquals(map.count(d -> d > 6.0), 1);
    }

    @Test(dataProvider = "map")
    public void anySatisfy(MutableIntDoubleMap map) {
        checkInvariants(map);
        map.put(1, 5.0);
        map.put(2, 6.0);
        map.put(3, 7.0);
        checkInvariants(map);
        Assert.assertFalse(map.anySatisfy(d -> d > 7.0));
        Assert.assertTrue(map.anySatisfy(d -> d > 6.0));
    }

    @Test(dataProvider = "map")
    public void allSatisfy(MutableIntDoubleMap map) {
        checkInvariants(map);
        map.put(1, 5.0);
        map.put(2, 6.0);
        map.put(3, 7.0);
        checkInvariants(map);
        Assert.assertFalse(map.allSatisfy(d -> d > 7.0));
        Assert.assertFalse(map.allSatisfy(d -> d > 6.0));
        Assert.assertTrue(map.allSatisfy(d -> d > 4.0));
    }

    @Test(dataProvider = "map")
    public void manyAccess(MutableIntDoubleMap map) {
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
            checkInvariants(map);

            Assert.assertEquals(map.max(), value);

            if (map instanceof IntDoubleBTreeMap) {
                // check order of entries

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
            } else if (map instanceof IntDoubleLinearHashMap) {
                // just check entries
                IntSet expectedKeys = IntSets.immutable.ofAll(IntStream.range(0, i + 1));

                MutableIntSet expectedKeysK = IntSets.mutable.ofAll(expectedKeys);
                map.forEachKey(k -> Assert.assertTrue(expectedKeysK.remove(k)));
                Assert.assertTrue(expectedKeysK.isEmpty());

                MutableDoubleSet expectedValues = DoubleSets.mutable.ofAll(
                        IntStream.range(0, i + 1).mapToDouble(k->k*1.5));
                map.forEachValue(v -> Assert.assertTrue(expectedValues.remove(v)));
                Assert.assertTrue(expectedValues.isEmpty());

                MutableIntSet expectedKeysKV = IntSets.mutable.ofAll(expectedKeys);
                map.forEachKeyValue((k, v) -> {
                    Assert.assertTrue(expectedKeysKV.remove(k));
                    double expected = k * 1.5;
                    Assert.assertEquals(v, expected);
                });
                Assert.assertTrue(expectedKeysKV.isEmpty());

                Assert.assertEquals(
                        DoubleSets.immutable.of(map.toArray()),
                        DoubleSets.immutable.ofAll(IntStream.range(0, i + 1).mapToDouble(k -> k * 1.5))
                );
            } else {
                throw new AssertionError();
            }

            Assert.assertEquals(map.sum(), sum);
        }

        for (int i = 0; i < 100; i++) {
            after.accept(i);
        }
    }

    @Test(dataProvider = "map")
    public void manyAccessReverse(MutableIntDoubleMap map) {
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
            checkInvariants(map);

            Assert.assertEquals(map.min(), value);

            if (map instanceof IntDoubleBTreeMap) {
                // check order as well
                Assert.assertEquals(
                        map.toArray(),
                        IntStream.range(i, 101).mapToDouble(k -> k * 1.5).toArray()
                );
            }
            Assert.assertEquals(
                    DoubleSets.immutable.of(map.toArray()),
                    DoubleSets.immutable.ofAll(IntStream.range(i, 101).mapToDouble(k -> k * 1.5))
            );
            Assert.assertEquals(map.sum(), sum);
        }

        for (int i = 100; i >= 0; i--) {
            after.accept(i);
        }
    }

    @Test(dataProvider = "map")
    public void updateValues(MutableIntDoubleMap map) {
        checkInvariants(map);
        map.put(1, 5.0);
        map.put(2, 6.0);
        map.put(3, 7.0);
        checkInvariants(map);
        map.updateValues(Double::sum);
        checkInvariants(map);
        Assert.assertEquals(map.toString(), "{1=6.0, 2=8.0, 3=10.0}");
    }

    @Test(dataProvider = "map")
    public void manyRemove(MutableIntDoubleMap map) {
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
            checkInvariants(map);
        }

        for (int i = 0; i < 100; i++) {
            after.accept(i);
        }

        for (int i = 0; i < 100; i++) {
            map.remove(i);
            checkInvariants(map);
            map.remove(i);
            checkInvariants(map);
            before.accept(i);
        }
    }

    @Test(dataProvider = "map")
    public void removeIfAbsent(MutableIntDoubleMap map) {
        checkInvariants(map);
        map.put(1, 5.0);
        map.put(2, 6.0);
        map.put(3, 7.0);
        checkInvariants(map);
        Assert.assertEquals(map.removeKeyIfAbsent(2, 0.1), 6.0);
        checkInvariants(map);
        Assert.assertEquals(map.removeKeyIfAbsent(2, 0.1), 0.1);
        checkInvariants(map);
    }

    @Test(dataProvider = "map")
    public void manyRemoveIfAbsent(MutableIntDoubleMap map) {
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
            checkInvariants(map);
        }

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(map.removeKeyIfAbsent(i, Double.NaN), i * 1.5);
            checkInvariants(map);
            Assert.assertEquals(map.removeKeyIfAbsent(i, Double.NaN), Double.NaN);
            checkInvariants(map);
            before.accept(i);
        }
    }

    @Test(dataProvider = "map")
    public void manyPutIfAbsent(MutableIntDoubleMap map) {
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
            checkInvariants(map);
            Assert.assertEquals(map.getIfAbsentPut(i, value + 1), value);
            after.accept(i);
            checkInvariants(map);
        }
    }

    @Test(dataProvider = "map")
    public void manyPutIfAbsentFn1(MutableIntDoubleMap map) {
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
            checkInvariants(map);
            Assert.assertEquals(map.getIfAbsentPut(i, () -> {
                throw new AssertionError();
            }), value);
            after.accept(i);
            checkInvariants(map);
        }
    }

    @Test(dataProvider = "map")
    public void manyPutIfAbsentFn2(MutableIntDoubleMap map) {
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
            checkInvariants(map);
            Assert.assertEquals(map.getIfAbsentPutWithKey(i, k -> {
                throw new AssertionError();
            }), value);
            after.accept(i);
            checkInvariants(map);
        }
    }

    @Test(dataProvider = "map")
    public void manyPutIfAbsentFn3(MutableIntDoubleMap map) {
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
            checkInvariants(map);
            Assert.assertEquals(map.getIfAbsentPutWith(i, p -> {
                throw new AssertionError();
            }, o), value);
            after.accept(i);
            checkInvariants(map);
        }
    }

    @Test(dataProvider = "map")
    public void manyUpdateValue(MutableIntDoubleMap map) {
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
            checkInvariants(map);
            Assert.assertEquals(map.updateValue(i, -100.0, d -> d * 1.5), i * 1.5);
            checkInvariants(map);
            after.accept(i);
            checkInvariants(map);
        }
    }

    @Test(dataProvider = "map")
    public void manyUpdateValues(MutableIntDoubleMap map) {
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
            checkInvariants(map);
        }
        map.updateValues((k, v) -> k * v);
        for (int i = 0; i < 100; i++) {
            after.accept(i);
        }
    }

    @Test(dataProvider = "map")
    public void manyAdd(MutableIntDoubleMap map) {
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
            checkInvariants(map);
            Assert.assertEquals(map.addToValue(i, i * .5), i * 1.5);
            checkInvariants(map);
            after.accept(i);
        }
    }
}