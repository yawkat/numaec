/* with short|byte|char|int|long element */
package at.yawk.numaec;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ConcurrentModificationException;
import org.eclipse.collections.api.LazyShortIterable;
import org.eclipse.collections.api.ShortIterable;
import org.eclipse.collections.api.block.function.primitive.ObjectShortIntToObjectFunction;
import org.eclipse.collections.api.block.function.primitive.ObjectShortToObjectFunction;
import org.eclipse.collections.api.block.function.primitive.ShortToObjectFunction;
import org.eclipse.collections.api.block.predicate.primitive.ShortPredicate;
import org.eclipse.collections.api.block.procedure.primitive.ShortIntProcedure;
import org.eclipse.collections.api.block.procedure.primitive.ShortProcedure;
import org.eclipse.collections.api.iterator.MutableShortIterator;
import org.eclipse.collections.api.iterator.ShortIterator;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.ImmutableShortList;
import org.eclipse.collections.api.list.primitive.MutableShortList;
import org.eclipse.collections.api.list.primitive.ShortList;
import org.eclipse.collections.impl.lazy.primitive.ReverseShortIterable;
import org.eclipse.collections.impl.list.mutable.primitive.SynchronizedShortList;
import org.eclipse.collections.impl.list.mutable.primitive.UnmodifiableShortList;
import org.eclipse.collections.impl.primitive.AbstractShortIterable;

/* if int|long element //
import java.util.Spliterator;
// endif */

public class ShortBufferList extends AbstractShortIterable implements ShortList, Closeable {
    private static final int INITIAL_CAPACITY = 16;

    final LargeByteBufferAllocator allocator;
    LargeByteBuffer buffer;
    int size;

    ShortBufferList(LargeByteBufferAllocator allocator) {
        this.allocator = allocator;
        buffer = LargeByteBuffer.EMPTY;
    }

    ShortBufferList(LargeByteBufferAllocator allocator, int initialCapacity) {
        this.allocator = allocator;
        buffer = allocator.allocate(scale(initialCapacity));
    }

    public static ShortBufferList.Mutable newMutable(LargeByteBufferAllocator allocator, int initialCapacity) {
        return new Mutable(allocator, initialCapacity);
    }

    public static ShortBufferList.Mutable newMutable(LargeByteBufferAllocator allocator) {
        return new Mutable(allocator);
    }

    @Override
    public void close() {
        buffer.close();
        buffer = null;
    }

    protected long scale(int index) {
        return ((short) index) * Short.BYTES;
    }

    @Override
    public ShortIterator shortIterator() {
        return new Itr();
    }

    @Override
    public short[] toArray() {
        short[] array = new short[size];
        for (int i = 0; i < size; i++) {
            array[i] = get(i);
        }
        return array;
    }

    @Override
    public boolean contains(short value) {
        for (int i = 0; i < size; i++) {
            if (get(i) == value) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void forEach(ShortProcedure procedure) {
        for (int i = 0; i < size; i++) {
            procedure.value(get(i));
        }
    }

    @Override
    public void each(ShortProcedure procedure) {
        forEach(procedure);
    }

    @Override
    public short get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        } else {
            return buffer.getShort(scale(index));
        }
    }

    @Override
    public long dotProduct(ShortList list) {
        long sum = 0;
        int i = 0;
        ShortIterator itr = list.shortIterator();
        while (i < size && itr.hasNext()) {
            sum += get(i++) * itr.next();
        }
        if (itr.hasNext() || i < size) {
            throw new IllegalArgumentException("Size mismatch");
        }
        return sum;
    }

    @Override
    public int binarySearch(short value) {
        int low = 0;
        int high = size - 1;
        while (low <= high) {
            int mid = (low + high) / 2; // no overflow because low + high <= size
            short pivot = get(mid);
            if (pivot < value) {
                low = mid + 1;
            } else if (pivot > value) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return ~low;
    }

    @Override
    public int indexOf(short value) {
        for (int i = 0; i < size; i++) {
            if (get(i) == value) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public int lastIndexOf(short value) {
        for (int i = size - 1; i >= 0; i--) {
            if (get(i) == value) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public short getLast() {
        return get(size - 1);
    }

    @Override
    public LazyShortIterable asReversed() {
        return ReverseShortIterable.adapt(this);
    }

    @Override
    public short getFirst() {
        return get(0);
    }

    @Override
    public ShortList select(ShortPredicate predicate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ShortList reject(ShortPredicate predicate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> ListIterable<V> collect(ShortToObjectFunction<? extends V> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short detectIfNone(ShortPredicate predicate, short ifNone) {
        for (int i = 0; i < size; i++) {
            short element = get(i);
            if (predicate.accept(element)) {
                return element;
            }
        }
        return ifNone;
    }

    @Override
    public int count(ShortPredicate predicate) {
        int count = 0;
        for (int i = 0; i < size; i++) {
            if (predicate.accept(get(i))) {
                count++;
            }
        }
        return count;
    }

    @Override
    public boolean anySatisfy(ShortPredicate predicate) {
        for (int i = 0; i < size; i++) {
            short element = get(i);
            if (predicate.accept(element)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean allSatisfy(ShortPredicate predicate) {
        for (int i = 0; i < size; i++) {
            short element = get(i);
            if (!predicate.accept(element)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean noneSatisfy(ShortPredicate predicate) {
        return !anySatisfy(predicate);
    }

    @Override
    public <T> T injectInto(T injectedValue, ObjectShortToObjectFunction<? super T, ? extends T> function) {
        for (int i = 0; i < size; i++) {
            injectedValue = function.valueOf(injectedValue, get(i));
        }
        return injectedValue;
    }

    @Override
    public <T> T injectIntoWithIndex(T injectedValue, ObjectShortIntToObjectFunction<? super T, ? extends T> function) {
        for (int i = 0; i < size; i++) {
            injectedValue = function.valueOf(injectedValue, get(i), i);
        }
        return injectedValue;
    }

    @Override
    public long sum() {
        long sum = 0;
        for (int i = 0; i < size; i++) {
            sum += get(i);
        }
        return sum;
    }

    @Override
    public short max() {
        short max = get(0);
        for (int i = 1; i < size; i++) {
            short item = get(i);
            if (item > max) {
                return item;
            }
        }
        return max;
    }

    @Override
    public short min() {
        short min = get(0);
        for (int i = 1; i < size; i++) {
            short item = get(i);
            if (item < min) {
                return item;
            }
        }
        return min;
    }

    @Override
    public ImmutableShortList toImmutable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ShortList distinct() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEachWithIndex(ShortIntProcedure procedure) {
        for (int i = 0; i < size; i++) {
            procedure.value(get(i), i);
        }
    }

    @Override
    public ShortList toReversed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ShortList subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return size;
    }

    /* if int|long element //
    @Override
    public Spliterator.OfShort spliterator() {
        throw new UnsupportedOperationException();
    }
    // endif */

    @Override
    public void appendString(Appendable appendable, String start, String separator, String end) {
        try {
            appendable.append(start);
            for (int i = 0; i < size; i++) {
                if (i != 0) {
                    appendable.append(separator);
                }
                appendable.append(String.valueOf(get(i)));
            }
            appendable.append(end);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    class Itr implements ShortIterator {
        int i;

        @Override
        public short next() {
            return get(i++);
        }

        @Override
        public boolean hasNext() {
            return i < size;
        }
    }

    public static class Mutable extends ShortBufferList implements MutableShortList {
        Mutable(LargeByteBufferAllocator allocator) {
            super(allocator);
        }

        Mutable(LargeByteBufferAllocator allocator, int initialCapacity) {
            super(allocator, initialCapacity);
        }

        private void ensureCapacity(int capacity) {
            long requiredCapacity = scale(capacity);
            long currentCapacity = buffer.size();
            if (requiredCapacity > currentCapacity) {
                long newCapacity = currentCapacity == 0 ? scale(INITIAL_CAPACITY) : currentCapacity;
                while (requiredCapacity > newCapacity) {
                    newCapacity += newCapacity >> 1; // *= 1.5
                }
                LargeByteBuffer reallocated = buffer.reallocate(newCapacity);
                if (reallocated != null) {
                    this.buffer = reallocated;
                } else {
                    @SuppressWarnings("resource")
                    LargeByteBuffer swap = allocator.allocate(newCapacity);
                    try {
                        swap.copyFrom(buffer, 0, 0, scale(size));

                        LargeByteBuffer tmp = swap;
                        swap = this.buffer; // old buffer will be closed
                        this.buffer = tmp;
                    } finally {
                        swap.close();
                    }
                }
            }
        }

        @Override
        public void addAtIndex(int index, short element) {
            if (index == size) {
                add(element);
            } else if (index < 0 || index > size) {
                throw new IndexOutOfBoundsException();
            } else {
                // we may do a redundant copy here, but that's not too bad.
                ensureCapacity(size + 1);
                buffer.copyFrom(buffer, scale(index), scale(index + 1), scale(size - index));
                buffer.setShort(scale(index), element);
                size++;
            }
        }

        @Override
        public boolean addAllAtIndex(int index, short... source) {
            if (index == size) {
                return addAll(source);
            } else if (index < 0 || index > size) {
                throw new IndexOutOfBoundsException();
            } else if (source.length == 0) {
                return false;
            } else {
                // we may do a redundant copy here, but that's not too bad.
                ensureCapacity(size + source.length);
                buffer.copyFrom(buffer, scale(index), scale(index + source.length), scale(size - index));
                for (int i = 0; i < source.length; i++) {
                    buffer.setShort(scale(index + i), source[i]);
                }
                size += source.length;
                return true;
            }
        }

        @Override
        public boolean addAllAtIndex(int index, ShortIterable source) {
            if (index == size) {
                return addAll(source);
            } else if (index < 0 || index > size) {
                throw new IndexOutOfBoundsException();
            } else if (source.isEmpty()) {
                return false;
            } else {
                // we may do a redundant copy here, but that's not too bad.
                int expectedSize = source.size();
                ensureCapacity(size + expectedSize);
                buffer.copyFrom(buffer, scale(index), scale(index + expectedSize), scale(size - index));
                ShortIterator itr = source.shortIterator();
                for (int i = 0; i < expectedSize; i++) {
                    if (!itr.hasNext()) { throw new ConcurrentModificationException(); }
                    buffer.setShort(scale(index + i), itr.next());
                }
                if (itr.hasNext()) { throw new ConcurrentModificationException(); }
                size += expectedSize;
                return true;
            }
        }

        @Override
        public short removeAtIndex(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException();
            } else {
                short value = buffer.getShort(scale(index));
                if (size > 1) {
                    buffer.copyFrom(buffer, scale(index + 1), scale(index), scale(size - index - 1));
                }
                size--;
                return value;
            }
        }

        @Override
        public short set(int index, short element) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException();
            } else {
                short old = buffer.getShort(scale(index));
                buffer.setShort(scale(index), element);
                return old;
            }
        }

        @Override
        public boolean add(short element) {
            ensureCapacity(size + 1);
            buffer.setShort(scale(size), element);
            size++;
            return true;
        }

        @Override
        public boolean addAll(short... source) {
            ensureCapacity(size + source.length);
            for (int i = 0; i < source.length; i++) {
                buffer.setShort(scale(i + size), source[i]);
            }
            size += source.length;
            return source.length > 0;
        }

        @Override
        public boolean addAll(ShortIterable source) {
            int expectedSize = source.size();
            ensureCapacity(size + expectedSize);
            ShortIterator itr = source.shortIterator();
            int i = 0;
            while (itr.hasNext()) {
                if (i > expectedSize) {
                    throw new ConcurrentModificationException();
                }
                buffer.setShort(scale(i + size), itr.next());
                i++;
            }
            size += i;
            return i > 0;
        }

        @Override
        public boolean remove(short value) {
            int ix = indexOf(value);
            if (ix == -1) {
                return false;
            } else {
                removeAtIndex(ix);
                return true;
            }
        }

        @Override
        public boolean removeAll(ShortIterable source) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(short... source) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(ShortIterable elements) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(short... source) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            size = 0;
        }

        @Override
        public MutableShortList with(short element) {
            add(element);
            return this;
        }

        @Override
        public MutableShortList without(short element) {
            remove(element);
            return this;
        }

        @Override
        public MutableShortList withAll(ShortIterable elements) {
            addAll(elements);
            return this;
        }

        @Override
        public MutableShortList withoutAll(ShortIterable elements) {
            removeAll(elements);
            return this;
        }

        @Override
        public MutableShortList reverseThis() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MutableShortList sortThis() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MutableShortList asUnmodifiable() {
            return new UnmodifiableShortList(this);
        }

        @Override
        public MutableShortList asSynchronized() {
            return new SynchronizedShortList(this);
        }

        @Override
        public MutableShortIterator shortIterator() {
            return new Itr();
        }

        @Override
        public MutableShortList select(ShortPredicate predicate) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MutableShortList reject(ShortPredicate predicate) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V> MutableList<V> collect(ShortToObjectFunction<? extends V> function) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MutableShortList toReversed() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MutableShortList distinct() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MutableShortList subList(int fromIndex, int toIndex) {
            throw new UnsupportedOperationException();
        }

        class Itr extends ShortBufferList.Itr implements MutableShortIterator {
            int removalIndex = -1;

            @Override
            public short next() {
                removalIndex = i;
                return super.next();
            }

            @Override
            public void remove() {
                if (removalIndex == -1) { throw new IllegalStateException(); }
                removeAtIndex(removalIndex);
                i--;
                removalIndex = -1;
            }
        }
    }
}
