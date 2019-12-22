/* with short|byte|char|int|long|float|double element */
package at.yawk.numaec;

import org.eclipse.collections.api.ShortIterable;
import org.eclipse.collections.api.factory.list.primitive.MutableShortListFactory;

/* if int|long|double element //
import java.util.stream.ShortStream;
// endif */

public class MutableShortBufferListFactory implements MutableShortListFactory {
    private final LargeByteBufferAllocator allocator;

    private MutableShortBufferListFactory(LargeByteBufferAllocator allocator) {
        this.allocator = allocator;
    }

    public static MutableShortBufferListFactory withAllocator(LargeByteBufferAllocator allocator) {
        return new MutableShortBufferListFactory(allocator);
    }

    @Override
    public MutableShortBufferList empty() {
        return new ShortBufferListImpl.Mutable(allocator);
    }

    public MutableShortBufferList emptyWithInitialCapacity(int initialCapacity) {
        return new ShortBufferListImpl.Mutable(allocator, initialCapacity);
    }

    @Override
    public MutableShortBufferList of() {
        return empty();
    }

    @Override
    public MutableShortBufferList with() {
        return empty();
    }

    @Override
    public MutableShortBufferList of(short... items) {
        MutableShortBufferList list = emptyWithInitialCapacity(items.length);
        list.addAll(items);
        return list;
    }

    @Override
    public MutableShortBufferList with(short... items) {
        return of(items);
    }

    @Override
    public MutableShortBufferList ofAll(ShortIterable items) {
        MutableShortBufferList list = emptyWithInitialCapacity(items.size());
        list.addAll(items);
        return list;
    }

    @Override
    public MutableShortBufferList withAll(ShortIterable items) {
        return ofAll(items);
    }

    @Override
    public MutableShortBufferList ofAll(Iterable<Short> iterable) {
        MutableShortBufferList list = of();
        for (Short element : iterable) {
            list.add(element);
        }
        return list;
    }

    @Override
    public MutableShortBufferList withAll(Iterable<Short> iterable) {
        return ofAll(iterable);
    }

    /* if int|long|double element //
    @Override
    public MutableShortBufferList ofAll(ShortStream stream) {
        MutableShortBufferList list = of();
        stream.forEach(list::add);
        return list;
    }

    @Override
    public MutableShortBufferList withAll(ShortStream stream) {
        return ofAll(stream);
    }
    // endif */
}
