package at.yawk.numaec;

public interface LargeByteBufferAllocator {
    LargeByteBuffer allocate(long size);
}
