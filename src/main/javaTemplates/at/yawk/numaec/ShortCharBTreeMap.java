/* with short|byte|char|int|long|float|double key
        char|byte|short|int|long|float|double value */
/* define aggregate //
// if double|float value //double
// elif short|byte|char|int|long value //long
// endif //
// enddefine*/
package at.yawk.numaec;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.eclipse.collections.api.LazyShortIterable;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.ShortIterable;
import org.eclipse.collections.api.bag.Bag;
import org.eclipse.collections.api.bag.MutableBag;
import org.eclipse.collections.api.bag.primitive.CharBag;
import org.eclipse.collections.api.bag.primitive.MutableCharBag;
import org.eclipse.collections.api.block.function.primitive.CharFunction;
import org.eclipse.collections.api.block.function.primitive.CharFunction0;
import org.eclipse.collections.api.block.function.primitive.CharToCharFunction;
import org.eclipse.collections.api.block.function.primitive.CharToObjectFunction;
import org.eclipse.collections.api.block.function.primitive.ObjectCharToObjectFunction;
import org.eclipse.collections.api.block.function.primitive.ShortCharToCharFunction;
import org.eclipse.collections.api.block.function.primitive.ShortToCharFunction;
import org.eclipse.collections.api.block.predicate.primitive.CharPredicate;
import org.eclipse.collections.api.block.predicate.primitive.ShortCharPredicate;
import org.eclipse.collections.api.block.procedure.Procedure;
import org.eclipse.collections.api.block.procedure.primitive.CharProcedure;
import org.eclipse.collections.api.block.procedure.primitive.ShortCharProcedure;
import org.eclipse.collections.api.block.procedure.primitive.ShortProcedure;
import org.eclipse.collections.api.collection.primitive.MutableCharCollection;
import org.eclipse.collections.api.iterator.CharIterator;
import org.eclipse.collections.api.iterator.MutableCharIterator;
import org.eclipse.collections.api.iterator.ShortIterator;
import org.eclipse.collections.api.map.primitive.CharShortMap;
import org.eclipse.collections.api.map.primitive.ImmutableShortCharMap;
import org.eclipse.collections.api.map.primitive.MutableCharShortMap;
import org.eclipse.collections.api.map.primitive.MutableShortCharMap;
import org.eclipse.collections.api.map.primitive.ShortCharMap;
import org.eclipse.collections.api.set.primitive.MutableShortSet;
import org.eclipse.collections.api.tuple.primitive.ShortCharPair;
import org.eclipse.collections.impl.lazy.AbstractLazyIterable;
import org.eclipse.collections.impl.lazy.primitive.AbstractLazyShortIterable;
import org.eclipse.collections.impl.primitive.AbstractCharIterable;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;

public class ShortCharBTreeMap extends AbstractCharIterable implements ShortCharMap {
    private static final long KEY_MASK = -1L >>> (64 - (Short.BYTES * 8));
    private static final long VALUE_MASK = -1L >>> (64 - (Character.BYTES * 8));

    protected final BTree bTree;
    protected int size = 0;

    private static long toKey(short key) {
        return /* unwrapRaw key */key & KEY_MASK;
    }

    private static long toValue(char value) {
        return /* unwrapRaw value */value & VALUE_MASK;
    }

    private static short fromKey(long key) {
        if (KEY_MASK != -1) { if (key < 0 || key > KEY_MASK) { throw new IllegalArgumentException(); } }
        /* if float key //
        return Float.intBitsToFloat((int) key);
        // elif byte|short|char|int|long|double key */
        return (short) /* wrap key */key;
        /* endif */
    }

    private static char fromValue(long value) {
        if (VALUE_MASK != -1) { if (value < 0 || value > VALUE_MASK) { throw new IllegalArgumentException(); } }
        /* if float value //
        return Float.intBitsToFloat((int) value);
        // elif byte|short|char|int|long|double value */
        return (char) /* wrap value */value;
        /* endif */
    }

    ShortCharBTreeMap(LargeByteBufferAllocator allocator, BTreeConfig config) {
        int leafSize = Short.BYTES + Character.BYTES;
        int branchSize = config.entryMustBeInLeaf ? Short.BYTES : leafSize;
        this.bTree = new BTree(allocator, config, branchSize, leafSize) {
            @Override
            protected void writeBranchEntry(LargeByteBuffer lbb, long address, long key, long value) {
                lbb.setShort(address, fromKey(key));
                if (!config.entryMustBeInLeaf) {
                    lbb.setChar(address + Short.BYTES, fromValue(value));
                }
            }

            @Override
            protected void writeLeafEntry(LargeByteBuffer lbb, long address, long key, long value) {
                lbb.setShort(address, fromKey(key));
                lbb.setChar(address + Short.BYTES, fromValue(value));
            }

            @Override
            protected long readBranchKey(LargeByteBuffer lbb, long address) {
                return toKey(lbb.getShort(address));
            }

            @Override
            protected long readBranchValue(LargeByteBuffer lbb, long address) {
                if (config.entryMustBeInLeaf) {
                    throw new AssertionError();
                } else {
                    return toValue(lbb.getChar(address + Short.BYTES));
                }
            }

            @Override
            protected long readLeafKey(LargeByteBuffer lbb, long address) {
                return toKey(lbb.getShort(address));
            }

            @Override
            protected long readLeafValue(LargeByteBuffer lbb, long address) {
                return toValue(lbb.getChar(address + Short.BYTES));
            }
        };
    }

    @DoNotMutate
    void checkInvariants() {
        bTree.checkInvariants();
        int count = count(x -> true);
        if (count != size()) { throw new AssertionError(); }
    }

    @Override
    public char get(short key) {
        return getIfAbsent(key, (char) 0);
    }

    @Override
    public char getIfAbsent(short key, char ifAbsent) {
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToKey(toKey(key));
            if (cursor.elementFound()) {
                return fromValue(cursor.getValue());
            } else {
                return ifAbsent;
            }
        }
    }

    @Override
    public char getOrThrow(short key) {
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToKey(toKey(key));
            if (cursor.elementFound()) {
                return fromValue(cursor.getValue());
            } else {
                throw new IllegalStateException("Key " + key + " not present");
            }
        }
    }

    @Override
    public boolean containsKey(short key) {
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToKey(toKey(key));
            return cursor.elementFound();
        }
    }

    @Override
    public void forEachKey(ShortProcedure procedure) {
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToStart();
            while (cursor.next()) {
                procedure.value(fromKey(cursor.getKey()));
            }
        }
    }

    @Override
    public void forEachKeyValue(ShortCharProcedure procedure) {
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToStart();
            while (cursor.next()) {
                procedure.value(fromKey(cursor.getKey()),
                                fromValue(cursor.getValue()));
            }
        }
    }

    @Override
    public LazyShortIterable keysView() {
        return new AbstractLazyShortIterable() {
            @Override
            public ShortIterator shortIterator() {
                return new KeyIterator(bTree.allocateCursor());
            }

            @Override
            public void each(ShortProcedure procedure) {
                forEachKey(procedure);
            }
        };
    }

    @Override
    public RichIterable<ShortCharPair> keyValuesView() {
        return new AbstractLazyIterable<ShortCharPair>() {
            @Override
            public Iterator<ShortCharPair> iterator() {
                return new KeyValueIterator(bTree.allocateCursor());
            }

            @Override
            public void each(Procedure<? super ShortCharPair> procedure) {
                iterator().forEachRemaining(procedure);
            }
        };
    }

    @Override
    public CharShortMap flipUniqueValues() {
        throw new UnsupportedOperationException("ShortCharBufferMap.flipUniqueValues not implemented yet");
    }

    @Override
    public ShortCharMap select(ShortCharPredicate predicate) {
        throw new UnsupportedOperationException("ShortCharBufferMap.select not implemented yet");
    }

    @Override
    public ShortCharMap reject(ShortCharPredicate predicate) {
        throw new UnsupportedOperationException("ShortCharBufferMap.reject not implemented yet");
    }

    @Override
    public ImmutableShortCharMap toImmutable() {
        throw new UnsupportedOperationException("ShortCharBufferMap.toImmutable not implemented yet");
    }

    @Override
    public MutableShortSet keySet() {
        throw new UnsupportedOperationException("ShortCharBufferMap.keySet not implemented yet");
    }

    @Override
    public boolean containsValue(char value) {
        long needle = toValue(value);
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToStart();
            while (cursor.next()) {
                if (cursor.getValue() == needle) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void forEachValue(CharProcedure procedure) {
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToStart();
            while (cursor.next()) {
                procedure.value(fromValue(cursor.getValue()));
            }
        }
    }

    @Override
    public MutableCharCollection values() {
        throw new UnsupportedOperationException("ShortCharBufferMap.values not implemented yet");
    }

    @Override
    public CharBag select(CharPredicate predicate) {
        throw new UnsupportedOperationException("ShortCharBufferMap.select not implemented yet");
    }

    @Override
    public CharBag reject(CharPredicate predicate) {
        throw new UnsupportedOperationException("ShortCharBufferMap.reject not implemented yet");
    }

    @Override
    public <V> Bag<V> collect(CharToObjectFunction<? extends V> function) {
        throw new UnsupportedOperationException("ShortCharBufferMap.collect not implemented yet");
    }

    @Override
    public CharIterator charIterator() {
        return new ValueIterator(bTree.allocateCursor());
    }

    @Override
    public char[] toArray() {
        char[] data = new char[size()];
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToStart();
            int i = 0;
            while (cursor.next()) {
                if (i >= data.length) { throw new ConcurrentModificationException(); }
                data[i++] = fromValue(cursor.getValue());
            }
            if (i < data.length) { throw new ConcurrentModificationException(); }
            return data;
        }
    }

    @Override
    public boolean contains(char value) {
        return containsValue(value);
    }

    @Override
    public void forEach(CharProcedure procedure) {
        forEachValue(procedure);
    }

    @Override
    public void each(CharProcedure procedure) {
        forEachValue(procedure);
    }

    @Override
    public char detectIfNone(CharPredicate predicate, char ifNone) {
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToStart();
            while (cursor.next()) {
                char value = fromValue(cursor.getValue());
                if (predicate.accept(value)) {
                    return value;
                }
            }
        }
        return ifNone;
    }

    @Override
    public int count(CharPredicate predicate) {
        int count = 0;
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToStart();
            while (cursor.next()) {
                char value = fromValue(cursor.getValue());
                if (predicate.accept(value)) {
                    count++;
                }
            }
        }
        return count;
    }

    @Override
    public boolean anySatisfy(CharPredicate predicate) {
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToStart();
            while (cursor.next()) {
                char value = fromValue(cursor.getValue());
                if (predicate.accept(value)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean allSatisfy(CharPredicate predicate) {
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToStart();
            while (cursor.next()) {
                char value = fromValue(cursor.getValue());
                if (!predicate.accept(value)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean noneSatisfy(CharPredicate predicate) {
        return !anySatisfy(predicate);
    }

    @Override
    public <T> T injectInto(T injectedValue, ObjectCharToObjectFunction<? super T, ? extends T> function) {
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToStart();
            while (cursor.next()) {
                injectedValue = function.valueOf(injectedValue, fromValue(cursor.getValue()));
            }
        }
        return injectedValue;
    }

    @Override
    public /*aggregate*/long/**/ sum() {
        /*aggregate*/long/**/ sum = 0;
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToStart();
            while (cursor.next()) {
                sum += fromValue(cursor.getValue());
            }
        }
        return sum;
    }

    @Override
    public char max() {
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToStart();
            if (!cursor.next()) { throw new NoSuchElementException(); }
            char max = fromValue(cursor.getValue());
            while (cursor.next()) {
                max = (char) Math.max(max, fromValue(cursor.getValue()));
            }
            return max;
        }
    }

    @Override
    public char min() {
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToStart();
            if (!cursor.next()) { throw new NoSuchElementException(); }
            char min = fromValue(cursor.getValue());
            while (cursor.next()) {
                min = (char) Math.min(min, fromValue(cursor.getValue()));
            }
            return min;
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void appendString(Appendable appendable, String start, String separator, String end) {
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            appendable.append(start);
            cursor.descendToStart();
            boolean first = true;
            while (cursor.next()) {
                if (first) {
                    first = false;
                } else {
                    appendable.append(separator);
                }
                appendable.append(String.valueOf(fromValue(cursor.getValue())));
            }
            appendable.append(end);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("{");
        try (BTree.Cursor cursor = bTree.allocateCursor()) {
            cursor.descendToStart();
            boolean first = true;
            while (cursor.next()) {
                if (first) {
                    first = false;
                } else {
                    builder.append(", ");
                }
                builder.append(fromKey(cursor.getKey()))
                        .append('=')
                        .append(fromValue(cursor.getValue()));
            }
            return builder.append('}').toString();
        }
    }

    private static abstract class BaseIterator {
        /**
         * this cursor is never closed, but since all {@link BTree.Cursor#close()} does is make it available for
         * reuse, that's not too bad.
         */
        protected final BTree.Cursor cursor;
        private boolean peeked;

        BaseIterator(BTree.Cursor cursor) {
            this.cursor = cursor;
        }

        protected void next0() {
            if (!hasNext()) { throw new NoSuchElementException(); }
            peeked = false;
        }

        public boolean hasNext() {
            if (peeked) {
                return true;
            } else {
                return peeked = cursor.next();
            }
        }
    }

    private static class KeyIterator extends BaseIterator implements ShortIterator {
        KeyIterator(BTree.Cursor cursor) {
            super(cursor);
        }

        @Override
        public short next() {
            next0();
            return fromKey(cursor.getKey());
        }
    }

    private static class KeyValueIterator extends BaseIterator implements Iterator<ShortCharPair> {
        KeyValueIterator(BTree.Cursor cursor) {
            super(cursor);
        }

        @Override
        public ShortCharPair next() {
            next0();
            return PrimitiveTuples.pair(fromKey(cursor.getKey()), fromValue(cursor.getValue()));
        }
    }

    private static class ValueIterator extends BaseIterator implements CharIterator, MutableCharIterator {
        ValueIterator(BTree.Cursor cursor) {
            super(cursor);
        }

        @Override
        public char next() {
            next0();
            return fromValue(cursor.getValue());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("ValueIterator.remove not implemented yet");
        }
    }

    public static class Mutable extends ShortCharBTreeMap implements MutableShortCharMap {
        Mutable(LargeByteBufferAllocator allocator, BTreeConfig config) {
            super(allocator, config);
        }

        @Override
        public void put(short key, char value) {
            long k = toKey(key);
            long v = toValue(value);
            try (BTree.Cursor cursor = bTree.allocateCursor()) {
                cursor.descendToKey(k);
                if (cursor.elementFound()) {
                    cursor.setValue(v);
                } else {
                    cursor.simpleInsert(k, v);
                    cursor.balance();
                    size++;
                }
            }
        }

        @Override
        public void putAll(ShortCharMap map) {
            map.forEachKeyValue(this::put);
        }

        @Override
        public void updateValues(ShortCharToCharFunction function) {
            try (BTree.Cursor cursor = bTree.allocateCursor()) {
                cursor.descendToStart();
                while (cursor.next()) {
                    char updated = function.valueOf(fromKey(cursor.getKey()), fromValue(cursor.getValue()));
                    cursor.setValue(toValue(updated));
                }
            }
        }

        @Override
        public void removeKey(short key) {
            long k = toKey(key);
            try (BTree.Cursor cursor = bTree.allocateCursor()) {
                cursor.descendToKey(k);
                if (cursor.elementFound()) {
                    cursor.simpleRemove();
                    cursor.balance();
                    size--;
                }
            }
        }

        @Override
        public void remove(short key) {
            removeKey(key);
        }

        @Override
        public char removeKeyIfAbsent(short key, char value) {
            long k = toKey(key);
            try (BTree.Cursor cursor = bTree.allocateCursor()) {
                cursor.descendToKey(k);
                if (cursor.elementFound()) {
                    char v = fromValue(cursor.getValue());
                    cursor.simpleRemove();
                    cursor.balance();
                    size--;
                    return v;
                } else {
                    return value;
                }
            }
        }

        @Override
        public char getIfAbsentPut(short key, char value) {
            long k = toKey(key);
            try (BTree.Cursor cursor = bTree.allocateCursor()) {
                cursor.descendToKey(k);
                if (cursor.elementFound()) {
                    return fromValue(cursor.getValue());
                } else {
                    cursor.simpleInsert(k, toValue(value));
                    cursor.balance();
                    size++;
                    return value;
                }
            }
        }

        @Override
        public char getIfAbsentPut(short key, CharFunction0 function) {
            long k = toKey(key);
            try (BTree.Cursor cursor = bTree.allocateCursor()) {
                cursor.descendToKey(k);
                if (cursor.elementFound()) {
                    return fromValue(cursor.getValue());
                } else {
                    char v = function.value();
                    cursor.simpleInsert(k, toValue(v));
                    cursor.balance();
                    size++;
                    return v;
                }
            }
        }

        @Override
        public char getIfAbsentPutWithKey(short key, ShortToCharFunction function) {
            long k = toKey(key);
            try (BTree.Cursor cursor = bTree.allocateCursor()) {
                cursor.descendToKey(k);
                if (cursor.elementFound()) {
                    return fromValue(cursor.getValue());
                } else {
                    char v = function.valueOf(key);
                    cursor.simpleInsert(k, toValue(v));
                    cursor.balance();
                    size++;
                    return v;
                }
            }
        }

        @Override
        public <P> char getIfAbsentPutWith(short key, CharFunction<? super P> function, P parameter) {
            long k = toKey(key);
            try (BTree.Cursor cursor = bTree.allocateCursor()) {
                cursor.descendToKey(k);
                if (cursor.elementFound()) {
                    return fromValue(cursor.getValue());
                } else {
                    char v = function.charValueOf(parameter);
                    cursor.simpleInsert(k, toValue(v));
                    cursor.balance();
                    size++;
                    return v;
                }
            }
        }

        @Override
        public char updateValue(short key, char initialValueIfAbsent, CharToCharFunction function) {
            long k = toKey(key);
            try (BTree.Cursor cursor = bTree.allocateCursor()) {
                cursor.descendToKey(k);
                if (cursor.elementFound()) {
                    char updated = function.valueOf(fromValue(cursor.getValue()));
                    cursor.setValue(toValue(updated));
                    return updated;
                } else {
                    char updated = function.valueOf(initialValueIfAbsent);
                    cursor.simpleInsert(k, toValue(updated));
                    cursor.balance();
                    size++;
                    return updated;
                }
            }
        }

        @Override
        public MutableShortCharMap withKeyValue(short key, char value) {
            put(key, value);
            return this;
        }

        @Override
        public MutableShortCharMap withoutKey(short key) {
            removeKey(key);
            return this;
        }

        @Override
        public MutableShortCharMap withoutAllKeys(ShortIterable keys) {
            keys.forEach(this::removeKey);
            return this;
        }

        @Override
        public MutableShortCharMap asUnmodifiable() {
            throw new UnsupportedOperationException("Mutable.asUnmodifiable not implemented yet");
        }

        @Override
        public MutableShortCharMap asSynchronized() {
            throw new UnsupportedOperationException("Mutable.asSynchronized not implemented yet");
        }

        @Override
        public char addToValue(short key, char toBeAdded) {
            long k = toKey(key);
            try (BTree.Cursor cursor = bTree.allocateCursor()) {
                cursor.descendToKey(k);
                if (cursor.elementFound()) {
                    char updated = (char) (fromValue(cursor.getValue()) + toBeAdded);
                    cursor.setValue(toValue(updated));
                    return updated;
                } else {
                    cursor.simpleInsert(k, toValue(toBeAdded));
                    cursor.balance();
                    size++;
                    return toBeAdded;
                }
            }
        }

        @Override
        public void clear() {
            bTree.clear();
            size = 0;
        }

        @Override
        public MutableCharShortMap flipUniqueValues() {
            throw new UnsupportedOperationException("ShortCharBufferMap.Mutable.flipUniqueValues not implemented yet");
        }

        @Override
        public MutableShortCharMap select(ShortCharPredicate predicate) {
            throw new UnsupportedOperationException("ShortCharBufferMap.Mutable.select not implemented yet");
        }

        @Override
        public MutableCharBag select(CharPredicate predicate) {
            throw new UnsupportedOperationException("ShortCharBufferMap.Mutable.select not implemented yet");
        }

        @Override
        public MutableShortCharMap reject(ShortCharPredicate predicate) {
            throw new UnsupportedOperationException("ShortCharBufferMap.Mutable.reject not implemented yet");
        }

        @Override
        public MutableCharBag reject(CharPredicate predicate) {
            throw new UnsupportedOperationException("ShortCharBufferMap.Mutable.reject not implemented yet");
        }

        @Override
        public <V> MutableBag<V> collect(CharToObjectFunction<? extends V> function) {
            throw new UnsupportedOperationException("ShortCharBufferMap.Mutable.collect not implemented yet");
        }

        @Override
        public MutableCharIterator charIterator() {
            return new ShortCharBTreeMap.ValueIterator(bTree.allocateCursor());
        }
    }
}
