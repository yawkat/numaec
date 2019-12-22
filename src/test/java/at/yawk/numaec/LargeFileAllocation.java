package at.yawk.numaec;

import java.nio.file.Paths;
import org.testng.Assert;

public final class LargeFileAllocation {
    private static final long GiB = 1024L * 1024 * 1024;

    /*
     * allocate and use a memory region that is bigger than the memory. This ensures BumpPointerFileAllocator actually
     * uses the disk for storage
     */

    public static void main(String[] args) throws Exception {
        System.out.println("Opening");
        try (BumpPointerFileAllocator allocator = BumpPointerFileAllocator.fromTempDirectory(Paths.get("/var/tmp"))) {

            System.out.println("Allocating ");
            long size = 32L * GiB;
            LargeByteBuffer buffer = allocator.allocate(size);
            for (long l = 0; l < size; l++) {
                if (l % GiB == 0) {
                    System.out.println("Writing data: " + l / GiB + " GiB done");
                }
                buffer.setByte(l, (byte) l);
            }
            for (long l = 0; l < size; l++) {
                if (l % GiB == 0) {
                    System.out.println("Verifying data: " + l / GiB + " GiB done");
                }
                Assert.assertEquals(buffer.getByte(l), (byte) l);
            }
        }
    }
}
