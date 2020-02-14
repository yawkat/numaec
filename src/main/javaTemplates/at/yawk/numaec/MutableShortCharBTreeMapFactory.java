package at.yawk.numaec;

import org.eclipse.collections.api.map.primitive.ShortCharMap;

public final class MutableShortCharBTreeMapFactory implements MutableShortCharBufferMapFactory {
    private final LargeByteBufferAllocator allocator;
    private final BTreeConfig config;

    private MutableShortCharBTreeMapFactory(LargeByteBufferAllocator allocator, BTreeConfig config) {
        this.allocator = allocator;
        this.config = config;
    }

    public static MutableShortCharBufferMapFactory withAllocator(LargeByteBufferAllocator allocator) {
        return withAllocatorAndConfig(allocator, BTreeConfig.builder().build());
    }

    public static MutableShortCharBufferMapFactory withAllocatorAndConfig(
            LargeByteBufferAllocator allocator, BTreeConfig config
    ) {
        return new MutableShortCharBTreeMapFactory(allocator, config);
    }

    @Override
    public MutableShortCharBufferMap empty() {
        return new ShortCharBTreeMap.Mutable(allocator, config);
    }

    @Override
    public MutableShortCharBufferMap ofInitialCapacity(int capacity) {
        return empty();
    }

    @Override
    public MutableShortCharBufferMap ofAll(ShortCharMap map) {
        MutableShortCharBufferMap n = ofInitialCapacity(map.size());
        n.putAll(map);
        return n;
    }
}
