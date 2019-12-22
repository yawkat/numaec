package at.yawk.numaec;

import org.eclipse.collections.api.block.function.primitive.CharFunction;
import org.eclipse.collections.api.block.function.primitive.ShortFunction;
import org.eclipse.collections.api.factory.map.primitive.MutableShortCharMapFactory;
import org.eclipse.collections.api.map.primitive.MutableShortCharMap;
import org.eclipse.collections.api.map.primitive.ShortCharMap;

public final class MutableShortCharBTreeMapFactory implements MutableShortCharMapFactory {
    private final LargeByteBufferAllocator allocator;
    private final BTreeConfig config;

    private MutableShortCharBTreeMapFactory(LargeByteBufferAllocator allocator, BTreeConfig config) {
        this.allocator = allocator;
        this.config = config;
    }

    public static MutableShortCharMapFactory withAllocator(LargeByteBufferAllocator allocator) {
        return withAllocatorAndConfig(allocator, BTreeConfig.builder().build());
    }

    public static MutableShortCharMapFactory withAllocatorAndConfig(
            LargeByteBufferAllocator allocator, BTreeConfig config) {
        return new MutableShortCharBTreeMapFactory(allocator, config);
    }

    @Override
    public MutableShortCharMap empty() {
        return new ShortCharBTreeMap.Mutable(allocator, config);
    }

    @Override
    public MutableShortCharMap of() {
        return empty();
    }

    @Override
    public MutableShortCharMap with() {
        return empty();
    }

    @Override
    public MutableShortCharMap ofInitialCapacity(int capacity) {
        return empty();
    }

    @Override
    public MutableShortCharMap withInitialCapacity(int capacity) {
        return ofInitialCapacity(capacity);
    }

    @Override
    public MutableShortCharMap ofAll(ShortCharMap map) {
        MutableShortCharMap n = empty();
        n.putAll(map);
        return n;
    }

    @Override
    public MutableShortCharMap withAll(ShortCharMap map) {
        return ofAll(map);
    }

    @Override
    public <T> MutableShortCharMap from(
            Iterable<T> iterable, ShortFunction<? super T> keyFunction, CharFunction<? super T> valueFunction
    ) {
        MutableShortCharMap n = empty();
        for (T t : iterable) {
            n.put(keyFunction.shortValueOf(t), valueFunction.charValueOf(t));
        }
        return n;
    }
}
