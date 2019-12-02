package at.yawk.numaec;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class BumpPointerFileAllocator implements LargeByteBufferAllocator, Closeable {
    private static final int MAP_SIZE_BITS = 30;

    private final FileChannel channel;

    public BumpPointerFileAllocator(FileChannel channel) {
        this.channel = channel;
    }

    @Override
    public LargeByteBuffer allocate(long size) {
        if (size == 0) {
            return LargeByteBuffer.EMPTY;
        }
        try {
            long start = channel.size();
            //noinspection resource
            channel.truncate(start + size);
            ByteBuffer[] parts = new ByteBuffer[(int) (((size - 1) >> MAP_SIZE_BITS) + 1)];
            for (int i = 0; i < parts.length; i++) {
                long partStart = start + ((long) i << MAP_SIZE_BITS);
                long partEnd = Math.min(start + size, start + (((long) i + 1) << MAP_SIZE_BITS));
                parts[i] = channel.map(FileChannel.MapMode.PRIVATE, partStart, partEnd - partStart);
            }
            return new ByteBufferBackedLargeByteBuffer(parts, 1 << MAP_SIZE_BITS);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
