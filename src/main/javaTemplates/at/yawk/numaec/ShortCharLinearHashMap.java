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

class ShortCharLinearHashMap extends BaseShortCharMap implements ShortCharBufferMap {
    private final float loadFactor;
    private final long sipHashK0, sipHashK1;
    private final long hashMask;

    protected final LinearHashTable table;
    protected int size;

    ShortCharLinearHashMap(LargeByteBufferAllocator allocator, LinearHashMapConfig config) {
        this.sipHashK0 = config.sipHashK0.getAsLong();
        this.sipHashK1 = config.sipHashK1.getAsLong();
        this.loadFactor = config.loadFactor;
        int hashLength = config.hashLength;
        this.hashMask = hashLength == 0 ? -1L : ~(-1L >>> hashLength);
        this.table = new LinearHashTable(allocator, config, hashLength + Short.BYTES + Character.BYTES) {
            @Override
            protected void write(LargeByteBuffer lbb, long address, long hash, long key, long value) {
                if (hashLength != 0) {
                    if ((hash & ~hashMask) != 0) {
                        throw new AssertionError();
                    }
                    BTree.uset(lbb, address, hashLength, Long.reverse(hash));
                }
                lbb.setShort(address + hashLength, fromKey(key));
                lbb.setChar(address + hashLength + Short.BYTES, fromValue(value));
            }

            @Override
            protected long readHash(LargeByteBuffer lbb, long address) {
                if (hashLength == 0) {
                    return hash(fromKey(readKey(lbb, address)));
                } else {
                    return Long.reverse(BTree.uget(lbb, address, hashLength));
                }
            }

            @Override
            protected long readKey(LargeByteBuffer lbb, long address) {
                return toKey(lbb.getShort(address + hashLength));
            }

            @Override
            protected long readValue(LargeByteBuffer lbb, long address) {
                return toValue(lbb.getChar(address + hashLength + Short.BYTES));
            }
        };
    }

    protected void ensureCapacity(int capacity) {
        table.expandToFullLoadCapacity((long) (capacity / loadFactor));
    }

    @Override
    protected MapStoreCursor iterationCursor() {
        return table.allocateCursor();
    }

    @Override
    protected MapStoreCursor keyCursor(short key) {
        LinearHashTable.Cursor cursor = table.allocateCursor();
        cursor.seek(hash(key), toKey(key));
        return cursor;
    }

    @DoNotMutate
    @Override
    void checkInvariants() {
        super.checkInvariants();
        table.checkInvariants();
    }

    protected long hash(short key) {
        return SipHash.sipHash2_4_8_to_8(sipHashK0, sipHashK1, toKey(key)) & hashMask;
    }

    @Override
    public void close() {
        table.close();
    }

    @Override
    public int size() {
        return size;
    }

    public static class Mutable extends ShortCharLinearHashMap implements MutableShortCharBufferMap {

        Mutable(LargeByteBufferAllocator allocator, LinearHashMapConfig config) {
            super(allocator, config);
        }

        @Override
        public void put(short key, char value) {
            ensureCapacity(1);
            long h = hash(key);
            long k = toKey(key);
            long v = toValue(value);
            try (LinearHashTable.Cursor cursor = table.allocateCursor()) {
                cursor.seek(h, k);
                if (cursor.elementFound()) {
                    cursor.setValue(v);
                } else {
                    cursor.insert(h, k, v);
                    size++;
                    ensureCapacity(size);
                }
            }
        }

        @Override
        public void putAll(ShortCharMap map) {
            // this is too pessimistic when the given map's keys overlap with outs but probably covers the main use
            // cases just fine
            ensureCapacity(size + map.size());
            map.forEachKeyValue(this::put);
        }

        @Override
        public void updateValues(ShortCharToCharFunction function) {
            try (LinearHashTable.Cursor cursor = table.allocateCursor()) {
                while (cursor.next()) {
                    char updated = function.valueOf(fromKey(cursor.getKey()), fromValue(cursor.getValue()));
                    cursor.setValue(toValue(updated));
                }
            }
        }

        @Override
        public void removeKey(short key) {
            try (LinearHashTable.Cursor cursor = table.allocateCursor()) {
                cursor.seek(hash(key), toKey(key));
                if (cursor.elementFound()) {
                    cursor.remove();
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
            try (LinearHashTable.Cursor cursor = table.allocateCursor()) {
                cursor.seek(hash(key), toKey(key));
                if (cursor.elementFound()) {
                    char v = fromValue(cursor.getValue());
                    cursor.remove();
                    size--;
                    return v;
                } else {
                    return value;
                }
            }
        }

        @Override
        public char getIfAbsentPut(short key, char value) {
            // only resize map if we really need to down below
            ensureCapacity(1);
            try (LinearHashTable.Cursor cursor = table.allocateCursor()) {
                long hash = hash(key);
                cursor.seek(hash, toKey(key));
                if (cursor.elementFound()) {
                    return fromValue(cursor.getValue());
                } else {
                    cursor.insert(hash, toKey(key), toValue(value));
                    size++;
                    ensureCapacity(size);
                    return value;
                }
            }
        }

        @Override
        public char getIfAbsentPut(short key, CharFunction0 function) {
            // only resize map if we really need to down below
            ensureCapacity(1);
            try (LinearHashTable.Cursor cursor = table.allocateCursor()) {
                long hash = hash(key);
                cursor.seek(hash, toKey(key));
                if (cursor.elementFound()) {
                    return fromValue(cursor.getValue());
                } else {
                    char v = function.value();
                    cursor.insert(hash, toKey(key), toValue(v));
                    size++;
                    ensureCapacity(size);
                    return v;
                }
            }
        }

        @Override
        public char getIfAbsentPutWithKey(short key, ShortToCharFunction function) {
            // only resize map if we really need to down below
            ensureCapacity(1);
            try (LinearHashTable.Cursor cursor = table.allocateCursor()) {
                long hash = hash(key);
                cursor.seek(hash, toKey(key));
                if (cursor.elementFound()) {
                    return fromValue(cursor.getValue());
                } else {
                    char v = function.valueOf(key);
                    cursor.insert(hash, toKey(key), toValue(v));
                    size++;
                    ensureCapacity(size);
                    return v;
                }
            }
        }

        @Override
        public <P> char getIfAbsentPutWith(short key, CharFunction<? super P> function, P parameter) {
            // only resize map if we really need to down below
            ensureCapacity(1);
            try (LinearHashTable.Cursor cursor = table.allocateCursor()) {
                long hash = hash(key);
                cursor.seek(hash, toKey(key));
                if (cursor.elementFound()) {
                    return fromValue(cursor.getValue());
                } else {
                    char v = function.charValueOf(parameter);
                    cursor.insert(hash, toKey(key), toValue(v));
                    size++;
                    ensureCapacity(size);
                    return v;
                }
            }
        }

        @Override
        public char updateValue(short key, char initialValueIfAbsent, CharToCharFunction function) {
            // only resize map if we really need to down below
            ensureCapacity(1);
            try (LinearHashTable.Cursor cursor = table.allocateCursor()) {
                long hash = hash(key);
                cursor.seek(hash, toKey(key));
                if (cursor.elementFound()) {
                    char updated = function.valueOf(fromValue(cursor.getValue()));
                    cursor.setValue(toValue(updated));
                    return updated;
                } else {
                    char updated = function.valueOf(initialValueIfAbsent);
                    cursor.insert(hash, toKey(key), toValue(updated));
                    size++;
                    ensureCapacity(size);
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
            // only resize map if we really need to down below
            ensureCapacity(1);
            try (LinearHashTable.Cursor cursor = table.allocateCursor()) {
                long hash = hash(key);
                cursor.seek(hash, toKey(key));
                if (cursor.elementFound()) {
                    char updated = (char) (fromValue(cursor.getValue()) + toBeAdded);
                    cursor.setValue(toValue(updated));
                    return updated;
                } else {
                    cursor.insert(hash, toKey(key), toValue(toBeAdded));
                    size++;
                    ensureCapacity(size);
                    return toBeAdded;
                }
            }
        }

        @Override
        public void clear() {
            table.clear();
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
