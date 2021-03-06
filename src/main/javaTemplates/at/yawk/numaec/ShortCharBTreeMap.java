/* with short|byte|char|int|long|float|double key
        char|byte|short|int|long|float|double value */
/* define aggregate //
// if double|float value //double
// elif short|byte|char|int|long value //long
// endif //
// enddefine*/
package at.yawk.numaec;

import org.eclipse.collections.api.ShortIterable;
import org.eclipse.collections.api.bag.MutableBag;
import org.eclipse.collections.api.bag.primitive.MutableCharBag;
import org.eclipse.collections.api.block.function.primitive.CharFunction;
import org.eclipse.collections.api.block.function.primitive.CharFunction0;
import org.eclipse.collections.api.block.function.primitive.CharToCharFunction;
import org.eclipse.collections.api.block.function.primitive.CharToObjectFunction;
import org.eclipse.collections.api.block.function.primitive.ShortCharToCharFunction;
import org.eclipse.collections.api.block.function.primitive.ShortToCharFunction;
import org.eclipse.collections.api.block.predicate.primitive.CharPredicate;
import org.eclipse.collections.api.block.predicate.primitive.ShortCharPredicate;
import org.eclipse.collections.api.iterator.MutableCharIterator;
import org.eclipse.collections.api.map.primitive.MutableCharShortMap;
import org.eclipse.collections.api.map.primitive.MutableShortCharMap;
import org.eclipse.collections.api.map.primitive.ShortCharMap;

public class ShortCharBTreeMap extends BaseShortCharMap implements ShortCharBufferMap {
    protected final BTree bTree;
    protected int size = 0;

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

    @Override
    protected MapStoreCursor iterationCursor() {
        BTree.Cursor cursor = bTree.allocateCursor();
        cursor.descendToStart();
        return cursor;
    }

    @Override
    protected MapStoreCursor keyCursor(short key) {
        BTree.Cursor cursor = bTree.allocateCursor();
        cursor.descendToKey(toKey(key));
        return cursor;
    }

    @DoNotMutate
    @Override
    void checkInvariants() {
        super.checkInvariants();
        bTree.checkInvariants();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void close() {
        bTree.close();
    }

    public static class Mutable extends ShortCharBTreeMap implements MutableShortCharBufferMap {
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
            return super.charIterator();
        }
    }
}
