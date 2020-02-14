package at.yawk.numaec;

import org.eclipse.collections.api.factory.map.primitive.MutableShortCharMapFactory;
import org.eclipse.collections.api.map.primitive.ShortCharMap;

public final class MutableShortCharLinearHashMapFactory implements MutableShortCharBufferMapFactory {
    private final LargeByteBufferAllocator allocator;
    private final LinearHashMapConfig config;

    private MutableShortCharLinearHashMapFactory(LargeByteBufferAllocator allocator, LinearHashMapConfig config) {
        this.allocator = allocator;
        this.config = config;
    }

    public static MutableShortCharMapFactory withAllocator(LargeByteBufferAllocator allocator) {
        return withAllocatorAndConfig(allocator, LinearHashMapConfig.builder().build());
    }

    public static MutableShortCharMapFactory withAllocatorAndConfig(
            LargeByteBufferAllocator allocator, LinearHashMapConfig config
    ) {
        return new MutableShortCharLinearHashMapFactory(allocator, config);
    }

    @Override
    public MutableShortCharBufferMap empty() {
        return new ShortCharLinearHashMap.Mutable(allocator, config);
    }

    @Override
    public MutableShortCharBufferMap ofInitialCapacity(int capacity) {
        ShortCharLinearHashMap.Mutable map = new ShortCharLinearHashMap.Mutable(allocator, config);
        map.ensureCapacity(capacity);
        return map;
    }

    @Override
    public MutableShortCharBufferMap ofAll(ShortCharMap map) {
        MutableShortCharBufferMap n = ofInitialCapacity(map.size());
        n.putAll(map);
        return n;
    }
}
