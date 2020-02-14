package at.yawk.numaec;

import org.eclipse.collections.api.block.function.primitive.CharFunction;
import org.eclipse.collections.api.block.function.primitive.ShortFunction;
import org.eclipse.collections.api.factory.map.primitive.MutableShortCharMapFactory;
import org.eclipse.collections.api.map.primitive.MutableShortCharMap;
import org.eclipse.collections.api.map.primitive.ShortCharMap;

public interface MutableShortCharBufferMapFactory extends MutableShortCharMapFactory {
    @Override
    MutableShortCharBufferMap empty();

    @Override
    default MutableShortCharBufferMap of() {
        return empty();
    }

    @Override
    default MutableShortCharBufferMap with() {
        return empty();
    }

    @Override
    MutableShortCharBufferMap ofInitialCapacity(int capacity);

    @Override
    default MutableShortCharBufferMap withInitialCapacity(int capacity) {
        return ofInitialCapacity(capacity);
    }

    @Override
    MutableShortCharBufferMap ofAll(ShortCharMap map);

    @Override
    default MutableShortCharBufferMap withAll(ShortCharMap map) {
        return ofAll(map);
    }

    @Override
    default <T> MutableShortCharMap from(
            Iterable<T> iterable, ShortFunction<? super T> keyFunction, CharFunction<? super T> valueFunction
    ) {
        MutableShortCharBufferMap n = empty();
        for (T t : iterable) {
            n.put(keyFunction.shortValueOf(t), valueFunction.charValueOf(t));
        }
        return n;
    }
}
