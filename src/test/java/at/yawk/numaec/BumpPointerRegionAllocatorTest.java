package at.yawk.numaec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BumpPointerRegionAllocatorTest {
    @DataProvider
    public Object[][] overlapSizes() {
        return new Object[][] {
                {1, 1}, {1, 2}, {1, 4}, {1, 8}, {1, 16},
                {2, 2}, {2, 4}, {2, 8}, {2, 16},
                {3, 4}, {3, 8}, {3, 16},
                {4, 4}, {4, 8}, {4, 16},
                {7, 8}, {7, 16},
                {8, 8}, {8, 16},
                {15, 16},
                {16, 16},
        };
    }

    @Test(dataProvider = "overlapSizes")
    public void checkOverlap(int size, int align) {
        BumpPointerRegionAllocator allocator = BumpPointerRegionAllocator.builder(
                s -> new ByteBufferBackedLargeByteBuffer(new ByteBuffer[]{ ByteBuffer.allocate((int) s) }, 0x1000))
                .regionSize(0x1000)
                .align(align)
                .build();
        List<LargeByteBuffer> buffers = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            buffers.add(allocator.allocate(size));
        }
        byte b = 0;
        for (LargeByteBuffer buffer : buffers) {
            Assert.assertEquals(buffer.size(), size);
            for (int i = 0; i < size; i++) {
                buffer.setByte(i, b++);
            }
        }
        byte c = 0;
        for (LargeByteBuffer buffer : buffers) {
            for (int i = 0; i < size; i++) {
                Assert.assertEquals(buffer.getByte(i), c++);
            }
        }
    }
}