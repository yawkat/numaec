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
import org.eclipse.collections.api.bag.Bag;
import org.eclipse.collections.api.bag.primitive.CharBag;
import org.eclipse.collections.api.block.function.primitive.CharToObjectFunction;
import org.eclipse.collections.api.block.function.primitive.ObjectCharToObjectFunction;
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
import org.eclipse.collections.api.map.primitive.ShortCharMap;
import org.eclipse.collections.api.set.primitive.MutableShortSet;
import org.eclipse.collections.api.tuple.primitive.ShortCharPair;
import org.eclipse.collections.impl.lazy.AbstractLazyIterable;
import org.eclipse.collections.impl.lazy.primitive.AbstractLazyShortIterable;
import org.eclipse.collections.impl.primitive.AbstractCharIterable;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;

abstract class BaseShortCharMap extends AbstractCharIterable implements ShortCharMap {
    static final long KEY_MASK = -1L >>> (64 - (Short.BYTES * 8));
    static final long VALUE_MASK = -1L >>> (64 - (Character.BYTES * 8));

    static long toKey(short key) {
        return /* unwrapRaw key */key & KEY_MASK;
    }

    static long toValue(char value) {
        return /* unwrapRaw value */value & VALUE_MASK;
    }

    @SuppressWarnings("ConstantConditions")
    static short fromKey(long key) {
        if (KEY_MASK != -1) { if (key < 0 || key > KEY_MASK) { throw new IllegalArgumentException(); } }
        /* if float key //
        return Float.intBitsToFloat((int) key);
        // elif byte|short|char|int|long|double key */
        return (short) /* wrap key */key;
        /* endif */
    }

    @SuppressWarnings("ConstantConditions")
    static char fromValue(long value) {
        if (VALUE_MASK != -1) { if (value < 0 || value > VALUE_MASK) { throw new IllegalArgumentException(); } }
        /* if float value //
        return Float.intBitsToFloat((int) value);
        // elif byte|short|char|int|long|double value */
        return (char) /* wrap value */value;
        /* endif */
    }

    protected abstract MapStoreCursor iterationCursor();

    protected abstract MapStoreCursor keyCursor(short key);

    @DoNotMutate
    void checkInvariants() {
        int count = count(x -> true);
        if (count != size()) { throw new AssertionError(); }
    }

    @Override
    public char getIfAbsent(short key, char ifAbsent) {
        try (MapStoreCursor cursor = keyCursor(key)) {
            if (cursor.elementFound()) {
                return fromValue(cursor.getValue());
            } else {
                return ifAbsent;
            }
        }
    }

    @Override
    public char getOrThrow(short key) {
        try (MapStoreCursor cursor = keyCursor(key)) {
            if (cursor.elementFound()) {
                return fromValue(cursor.getValue());
            } else {
                throw new IllegalStateException("Key " + key + " not present");
            }
        }
    }

    @Override
    public boolean containsKey(short key) {
        try (MapStoreCursor cursor = keyCursor(key)) {
            return cursor.elementFound();
        }
    }

    @Override
    public void forEachKey(ShortProcedure procedure) {
        try (MapStoreCursor cursor = iterationCursor()) {
            while (cursor.next()) {
                procedure.value(fromKey(cursor.getKey()));
            }
        }
    }

    @Override
    public void forEachKeyValue(ShortCharProcedure procedure) {
        try (MapStoreCursor cursor = iterationCursor()) {
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
                return new KeyIterator(iterationCursor());
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
                return new KeyValueIterator(iterationCursor());
            }

            @Override
            public void each(Procedure<? super ShortCharPair> procedure) {
                iterator().forEachRemaining(procedure);
            }
        };
    }

    @Override
    public boolean containsValue(char value) {
        long needle = toValue(value);
        try (MapStoreCursor cursor = iterationCursor()) {
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
        try (MapStoreCursor cursor = iterationCursor()) {
            while (cursor.next()) {
                procedure.value(fromValue(cursor.getValue()));
            }
        }
    }

    @Override
    public MutableCharIterator charIterator() {
        return new ValueIterator(iterationCursor());
    }

    @Override
    public char[] toArray() {
        char[] data = new char[size()];
        try (MapStoreCursor cursor = iterationCursor()) {
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
    public char detectIfNone(CharPredicate predicate, char ifNone) {
        try (MapStoreCursor cursor = iterationCursor()) {
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
        try (MapStoreCursor cursor = iterationCursor()) {
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
        try (MapStoreCursor cursor = iterationCursor()) {
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
        try (MapStoreCursor cursor = iterationCursor()) {
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
    public <T> T injectInto(T injectedValue, ObjectCharToObjectFunction<? super T, ? extends T> function) {
        try (MapStoreCursor cursor = iterationCursor()) {
            while (cursor.next()) {
                injectedValue = function.valueOf(injectedValue, fromValue(cursor.getValue()));
            }
        }
        return injectedValue;
    }

    @Override
    public /*aggregate*/long/**/ sum() {
        /*aggregate*/
        long/**/ sum = 0;
        try (MapStoreCursor cursor = iterationCursor()) {
            while (cursor.next()) {
                sum += fromValue(cursor.getValue());
            }
        }
        return sum;
    }

    @Override
    public char max() {
        try (MapStoreCursor cursor = iterationCursor()) {
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
        try (MapStoreCursor cursor = iterationCursor()) {
            if (!cursor.next()) { throw new NoSuchElementException(); }
            char min = fromValue(cursor.getValue());
            while (cursor.next()) {
                min = (char) Math.min(min, fromValue(cursor.getValue()));
            }
            return min;
        }
    }

    @Override
    public void appendString(Appendable appendable, String start, String separator, String end) {
        try (MapStoreCursor cursor = iterationCursor()) {
            appendable.append(start);
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
        try (MapStoreCursor cursor = iterationCursor()) {
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

    @Override
    public char get(short key) {
        return getIfAbsent(key, (char) 0);
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
    public boolean noneSatisfy(CharPredicate predicate) {
        return !anySatisfy(predicate);
    }

    private static abstract class BaseIterator {
        /**
         * this cursor is never closed, but since all {@link MapStoreCursor#close()} does is make it available for
         * reuse, that's not too bad.
         */
        protected final MapStoreCursor cursor;
        private boolean peeked;

        BaseIterator(MapStoreCursor cursor) {
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
        KeyIterator(MapStoreCursor cursor) {
            super(cursor);
        }

        @Override
        public short next() {
            next0();
            return fromKey(cursor.getKey());
        }
    }

    private static class KeyValueIterator extends BaseIterator implements Iterator<ShortCharPair> {
        KeyValueIterator(MapStoreCursor cursor) {
            super(cursor);
        }

        @Override
        public ShortCharPair next() {
            next0();
            return PrimitiveTuples.pair(fromKey(cursor.getKey()), fromValue(cursor.getValue()));
        }
    }

    protected static class ValueIterator extends BaseIterator implements CharIterator, MutableCharIterator {
        ValueIterator(MapStoreCursor cursor) {
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
}
