/* with short|byte|char|int|long element */
package at.yawk.numaec;

import org.eclipse.collections.api.ShortIterable;
import org.eclipse.collections.api.factory.list.primitive.MutableShortListFactory;
import org.eclipse.collections.api.list.primitive.MutableShortList;

/* if int|long element //
import java.util.stream.ShortStream;
// endif */

public class MutableShortBufferListFactory implements MutableShortListFactory {
    private final LargeByteBufferAllocator allocator;

    public MutableShortBufferListFactory(LargeByteBufferAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public MutableShortList empty() {
        return new ShortBufferList.Mutable(allocator);
    }

    @Override
    public MutableShortList of() {
        return empty();
    }

    @Override
    public MutableShortList with() {
        return empty();
    }

    @Override
    public MutableShortList of(short... items) {
        ShortBufferList.Mutable list = new ShortBufferList.Mutable(allocator, items.length);
        list.addAll(items);
        return list;
    }

    @Override
    public MutableShortList with(short... items) {
        return of(items);
    }

    @Override
    public MutableShortList ofAll(ShortIterable items) {
        ShortBufferList.Mutable list = new ShortBufferList.Mutable(allocator, items.size());
        list.addAll(items);
        return list;
    }

    @Override
    public MutableShortList withAll(ShortIterable items) {
        return ofAll(items);
    }

    @Override
    public MutableShortList ofAll(Iterable<Short> iterable) {
        MutableShortList list = of();
        for (Short element : iterable) {
            list.add(element);
        }
        return list;
    }

    @Override
    public MutableShortList withAll(Iterable<Short> iterable) {
        return ofAll(iterable);
    }

    /* if int|long element //
    @Override
    public MutableShortList ofAll(ShortStream stream) {
        MutableShortList list = of();
        stream.forEach(list::add);
        return list;
    }

    @Override
    public MutableShortList withAll(ShortStream stream) {
        return ofAll(stream);
    }
    // endif */
}
